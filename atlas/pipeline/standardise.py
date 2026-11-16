"""Raw to standardised.

Reads artifacts from the object store, hands them to the connector that
produced them, and writes the resulting rows to the fact table. Knows
nothing about any particular provider.

The step is idempotent: the fact table's primary key plus ON CONFLICT
means a re-run overwrites rather than appends. You will run this
constantly while developing.
"""
from __future__ import annotations

from dataclasses import dataclass

import psycopg

from .. import db
from ..acquisition import AcquisitionResult, Artifact
from ..connectors import Resolver, get_connector
from ..storage import Storage


@dataclass
class StandardiseReport:
    source_id: str
    artifacts_read: int = 0
    rows_written: int = 0
    artifacts_failed: int = 0
    errors: list[str] = None

    def __post_init__(self):
        if self.errors is None:
            self.errors = []


def record_acquisition(conn: psycopg.Connection, result: AcquisitionResult) -> None:
    """Register every landed artifact in raw_object.

    Done as its own step so that a crash between acquisition and parsing
    loses no bytes: the files are in the store and registered, and the
    parse can be re-run.
    """
    for artifact in result.artifacts:
        db.record_raw_object(
            conn,
            s3_key=artifact.key,
            source_id=result.source_id,
            request=artifact.request,
            fetched_at=artifact.fetched_at,
            content_hash=artifact.content_hash,
            http_status=artifact.http_status,
            content_type=artifact.content_type,
            byte_size=artifact.byte_size,
        )


def standardise_pending(conn: psycopg.Connection, storage: Storage,
                        source_id: str) -> StandardiseReport:
    """Parse every unparsed artifact for one source."""
    report = StandardiseReport(source_id=source_id)
    connector = get_connector(source_id, storage=storage)
    resolver = Resolver.from_db(conn)

    for raw in db.unparsed_objects(conn, source_id):
        key = raw["s3_key"]
        try:
            # A SAVEPOINT, not the whole transaction. A bare rollback here
            # would discard every artifact already parsed in this run while
            # the report still counted them as written -- the run would
            # claim success having committed nothing.
            with conn.transaction():
                payload = storage.get_json(key)
                artifact = Artifact(
                    key=key,
                    content_hash="",      # already recorded at acquisition
                    byte_size=0,
                    request=raw["request"] or {},
                    fetched_at=raw["fetched_at"],
                )
                rows = connector.parse(payload, artifact, resolver)
                written = db.upsert_observations(
                    conn, [r.as_params() for r in rows])
                db.mark_parsed(conn, key)

            report.artifacts_read += 1
            report.rows_written += written
        except Exception as exc:  # noqa: BLE001
            # Only this artifact is rolled back; it stays unparsed, so a
            # fix plus a re-run picks it up. Earlier artifacts in this run
            # keep their writes.
            report.artifacts_failed += 1
            report.errors.append(f"{key}: {exc}")

    return report
