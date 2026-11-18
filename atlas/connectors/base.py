"""The connector contract.

A connector declares what it supplies and implements two methods. The
pipeline knows nothing about any particular provider.

The discipline that makes this hold:

  * a connector never aggregates
  * a connector never writes to the serving tables
  * a connector never knows about another connector

The moment one of them does something special, the framework stops being
a framework and you have two pipelines wearing a trench coat.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import httpx

from ..acquisition import AcquisitionResult, Artifact, ObservationRow
from ..config import settings
from ..storage import Storage, content_hash


@dataclass(frozen=True)
class ConnectorSpec:
    """What a connector declares about itself. Read by the pipeline and
    by the status endpoint; never inspected by other connectors."""
    source_id: str
    name: str
    # Indicator source_codes this connector supplies, mapped to the grain
    # they genuinely arrive at. The API refuses finer requests rather than
    # inventing an answer.
    indicators: dict[str, str] = field(default_factory=dict)
    expected_frequency: str = "irregular"
    expected_lag_days: int = 0
    licence: str = ""
    attribution_text: str = ""


class Connector(ABC):
    spec: ConnectorSpec

    def __init__(self, storage: Storage, client: httpx.Client | None = None):
        self.storage = storage
        self._client = client
        self._requests_made = 0

    # -- acquisition -----------------------------------------------------

    @abstractmethod
    def fetch(self, **kwargs: Any) -> AcquisitionResult:
        """Get the bytes and land them in the raw store.

        Returns an AcquisitionResult. Must not parse, aggregate, or write
        to the serving tables.
        """

    # -- processing ------------------------------------------------------

    @abstractmethod
    def parse(self, payload: Any, artifact: Artifact, resolver: "Resolver") -> list[ObservationRow]:
        """Turn one raw artifact into standardised rows.

        Takes the already-fetched payload, never a live response, so this
        can be re-run freely without touching the network.
        """

    # -- shared plumbing --------------------------------------------------

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=settings.http_timeout_seconds,
                headers={"User-Agent": settings.user_agent},
                follow_redirects=True,
            )
        return self._client

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        """Every outbound request goes through here, so they are counted.

        settings.max_requests_per_run was declared with a rationale and
        never read by anything, which made it a comment shaped like a
        control. It matters more than it looks: area-weighting county
        rainfall would multiply a run's request count by roughly a
        hundred, against an endpoint whose commercial terms are not yet
        settled. A cap that stops the run is cheaper than finding out
        from the provider.
        """
        if self._requests_made >= settings.max_requests_per_run:
            raise RequestBudgetExceeded(
                f"{self.spec.source_id}: this run has made "
                f"{self._requests_made} requests, which reaches "
                f"ATLAS_MAX_REQUESTS_PER_RUN="
                f"{settings.max_requests_per_run}. Raise it deliberately "
                "rather than by accident."
            )
        self._requests_made += 1
        return self.client.get(url, **kwargs)

    def _land(self, result: AcquisitionResult, payload: Any, request: dict,
              http_status: int | None = None,
              raw_bytes: bytes | None = None) -> Artifact:
        """Write one payload to the raw store and record it on the result.

        `raw_bytes` is the response exactly as it arrived. The hash is
        taken from that, never from what we wrote: a re-serialisation
        normalises key order and whitespace, so two different responses
        can hash the same and an upstream revision becomes invisible.
        The hash exists to detect that revision, so hashing our own
        output defeats its only purpose. Where there is no response --
        a fixture replayed offline -- the stored body is all there is.
        """
        key, body = self.storage.put_json(self.spec.source_id, payload)
        artifact = Artifact(
            key=key,
            content_hash=content_hash(raw_bytes if raw_bytes is not None else body),
            byte_size=len(body),
            http_status=http_status,
            request=request,
            fetched_at=datetime.now(timezone.utc),
        )
        result.add(artifact)
        return artifact


class RequestBudgetExceeded(RuntimeError):
    """A run hit its own ceiling on outbound requests.

    Not a provider error. ATLAS stopped itself.
    """


class Resolver:
    """Maps a source's vocabulary onto ATLAS identifiers.

    Passed into parse() so a connector never reaches into the database
    itself, which keeps parsing testable without one.
    """

    def __init__(self, places: dict[tuple[str, str], int],
                 indicators: dict[tuple[str, str], dict]):
        self._places = places
        self._indicators = indicators

    def place(self, source_id: str, source_code: str) -> int | None:
        """None means the code is unknown. The caller skips the row --
        it never invents a place."""
        return self._places.get((source_id, source_code))

    def indicator(self, source_id: str, source_code: str) -> dict | None:
        return self._indicators.get((source_id, source_code))

    @classmethod
    def from_db(cls, conn) -> "Resolver":
        from .. import db  # noqa: PLC0415 - avoids a cycle at import time

        places = {
            (r["source_id"], r["source_code"]): r["place_id"]
            for r in db.query(conn, "SELECT source_id, source_code, place_id FROM place_crosswalk")
        }
        indicators = {
            (r["source_id"], r["source_code"]): r
            for r in db.query(
                conn,
                "SELECT indicator_id, source_id, source_code, code, unit, "
                "agg_rule, native_grain FROM indicator",
            )
        }
        return cls(places, indicators)
