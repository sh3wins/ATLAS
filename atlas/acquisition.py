"""The acquisition boundary.

A connector has two halves and the line between them is the thing worth
getting right:

  Acquisition  gets the bytes. It may call an API once, page through a
               hundred responses, download a bulk archive, or scrape a
               table. It is the only part of ATLAS that touches the
               network, and it writes raw to the object store and stops.

  Processing   works from an AcquisitionResult and never knows which of
               those happened.

`artifacts` is a list rather than a single blob on purpose: that single
decision is what lets a paged API and a bulk download share one shape,
and it is most of the flexibility the design has.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterator


class AcquisitionStatus(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"   # some chunks succeeded; retry the rest
    FAILED = "failed"


@dataclass(frozen=True)
class Artifact:
    """One object landed in the raw store."""
    key: str                       # storage key (S3 or local, same layout)
    content_hash: str
    byte_size: int
    content_type: str = "application/json"
    http_status: int | None = None
    request: dict[str, Any] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class AcquisitionResult:
    source_id: str
    artifacts: list[Artifact] = field(default_factory=list)
    status: AcquisitionStatus = AcquisitionStatus.COMPLETE
    # Chunks that failed, in whatever shape the connector uses to describe
    # a chunk. Acquisition must be resumable: forty-five years of daily
    # weather across five countries will not complete in one run, and a
    # timeout should cost one chunk rather than the whole backfill.
    failed_chunks: list[dict[str, Any]] = field(default_factory=list)
    error: str | None = None

    def __iter__(self) -> Iterator[Artifact]:
        return iter(self.artifacts)

    def __len__(self) -> int:
        return len(self.artifacts)

    @property
    def ok(self) -> bool:
        return self.status is AcquisitionStatus.COMPLETE

    def add(self, artifact: Artifact) -> None:
        self.artifacts.append(artifact)

    def fail_chunk(self, chunk: dict[str, Any], error: str) -> None:
        self.failed_chunks.append({**chunk, "error": error})
        self.status = (
            AcquisitionStatus.FAILED if not self.artifacts else AcquisitionStatus.PARTIAL
        )


@dataclass(frozen=True)
class ObservationRow:
    """What processing produces. Maps one-to-one onto the fact table.

    `value` and `missing_reason` are mutually exclusive: every row carries
    either a number or a stated reason there isn't one. "Measured zero"
    and "nobody surveyed" are different facts that look identical in a
    CSV, and conflating them drags every average you compute.
    """
    place_id: int
    indicator_id: int
    period_start: str          # ISO date
    period_end: str            # ISO date
    grain: str
    value: float | None
    source_id: str
    raw_ref: str | None = None
    quality: str = "reported"
    missing_reason: str | None = None

    def __post_init__(self) -> None:
        has_value = self.value is not None
        has_reason = self.missing_reason is not None
        if has_value == has_reason:
            raise ValueError(
                f"exactly one of value/missing_reason required, got "
                f"value={self.value!r} missing_reason={self.missing_reason!r}"
            )

    def as_params(self) -> dict[str, Any]:
        return {
            "place_id": self.place_id,
            "indicator_id": self.indicator_id,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "grain": self.grain,
            "value": self.value,
            "missing_reason": self.missing_reason,
            "source_id": self.source_id,
            "raw_ref": self.raw_ref,
            "quality": self.quality,
        }
