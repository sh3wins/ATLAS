"""Freshness tracking.

The reliability promise covers ingestion, validation, availability and
delivery. It does not cover whether an upstream provider changes its
schema, restates a figure, or publishes something wrong. A promise
nobody can verify is a slogan, so this is what makes it checkable.

Two failure modes, kept apart on purpose:

  failing  ATLAS could not fetch. Our problem. Should page someone.
  stale    the source has not published. Not our problem, and conflating
           the two either blames us for their schedule or hides a broken
           pipeline behind their slowness.

The staleness rule is per source. ERA5 reanalysis runs months behind
real time by design and World Bank indicators arrive annually; a global
"stale after 30 days" would flag correct data and train users to ignore
the warning.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import psycopg

from .. import db

# How much past the expected cadence counts as late rather than due.
GRACE = {
    "daily": timedelta(days=3),
    "weekly": timedelta(days=10),
    "monthly": timedelta(days=45),
    "quarterly": timedelta(days=120),
    "annual": timedelta(days=450),
    "irregular": timedelta(days=3650),
}

CADENCE = {
    "daily": timedelta(days=1),
    "weekly": timedelta(days=7),
    "monthly": timedelta(days=31),
    "quarterly": timedelta(days=92),
    "annual": timedelta(days=366),
    "irregular": timedelta(days=3650),
}


def record_attempt(conn: psycopg.Connection, source_id: str, *, succeeded: bool,
                   error: str | None = None,
                   source_publication: date | None = None) -> None:
    now = datetime.now(timezone.utc)
    if succeeded:
        db.execute(
            conn,
            """
            UPDATE dataset_freshness
            SET last_attempted_update   = %s,
                last_successful_update  = %s,
                last_source_publication = COALESCE(%s, last_source_publication),
                consecutive_failures    = 0,
                last_error              = NULL,
                updated_at              = now()
            WHERE source_id = %s
            """,
            (now, now, source_publication, source_id),
        )
    else:
        db.execute(
            conn,
            """
            UPDATE dataset_freshness
            SET last_attempted_update = %s,
                consecutive_failures  = consecutive_failures + 1,
                last_error            = %s,
                updated_at            = now()
            WHERE source_id = %s
            """,
            (now, error, source_id),
        )
    recompute(conn, source_id)


def classify(*, expected_frequency: str | None, last_successful,
             consecutive_failures: int, now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)

    # A broken pipeline is our failure and outranks everything else.
    if consecutive_failures >= 3:
        return "failing"
    if last_successful is None:
        return "unknown"

    frequency = expected_frequency or "irregular"
    cadence = CADENCE.get(frequency, CADENCE["irregular"])
    grace = GRACE.get(frequency, GRACE["irregular"])

    age = now - last_successful
    if age <= cadence:
        return "current"
    if age <= grace:
        return "due"
    return "stale"


def recompute(conn: psycopg.Connection, source_id: str | None = None) -> None:
    where = "WHERE f.source_id = %s" if source_id else ""
    params = (source_id,) if source_id else None
    rows = db.query(
        conn,
        f"""
        SELECT f.source_id, f.last_successful_update, f.consecutive_failures,
               s.expected_frequency
        FROM dataset_freshness f JOIN source s USING (source_id) {where}
        """,
        params,
    )
    for row in rows:
        status = classify(
            expected_frequency=row["expected_frequency"],
            last_successful=row["last_successful_update"],
            consecutive_failures=row["consecutive_failures"],
        )
        db.execute(
            conn,
            "UPDATE dataset_freshness SET freshness_status = %s, updated_at = now() "
            "WHERE source_id = %s",
            (status, row["source_id"]),
        )


def status_report(conn: psycopg.Connection) -> list[dict]:
    """What the public status endpoint serves."""
    return db.query(
        conn,
        """
        SELECT s.source_id,
               s.name,
               s.expected_frequency,
               s.licence,
               f.last_attempted_update,
               f.last_successful_update,
               f.last_source_publication,
               f.freshness_status,
               f.consecutive_failures,
               f.last_error
        FROM source s JOIN dataset_freshness f USING (source_id)
        ORDER BY s.source_id
        """,
    )
