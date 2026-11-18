"""Freshness tracking.

The reliability promise covers ingestion, validation, availability and
delivery. It does not cover whether an upstream provider changes its
schema, restates a figure, or publishes something wrong. A promise
nobody can verify is a slogan, so this is what makes it checkable.

Two genuinely independent axes, which earlier versions of this module
conflated:

  Is ATLAS fetching?        consecutive_failures, last_successful_update
                            -> `failing`. Ours. Should page someone.

  Has the source published? last_source_publication
                            -> `due` / `stale`. Not ours. Should not page.

The distinction only holds because `stale` is computed from the newest
period the SOURCE has published, never from when ATLAS last ran. A
pipeline that fetches perfectly from a dormant source must read `stale`,
not `current`.

Thresholds are per source. The Open-Meteo archive publishes ERA5 about
five days behind real time by design (expected_lag_days=5) and World Bank
indicators arrive annually; one global rule would flag correct data and
train users to ignore the warning.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import psycopg

from .. import db

# How often the source is expected to publish.
CADENCE = {
    "daily": timedelta(days=1),
    "weekly": timedelta(days=7),
    "monthly": timedelta(days=31),
    "quarterly": timedelta(days=92),
    "annual": timedelta(days=366),
    "irregular": timedelta(days=3650),
}

# How far past the expected point counts as `due` rather than `stale`.
# Additional to the cadence, not an absolute age.
TOLERANCE = {
    "daily": timedelta(days=3),
    "weekly": timedelta(days=7),
    "monthly": timedelta(days=21),
    "quarterly": timedelta(days=60),
    "annual": timedelta(days=120),
    "irregular": timedelta(days=3650),
}


def _as_utc(value) -> datetime | None:
    """Postgres hands back a date for last_source_publication and a
    timestamptz for the update columns. Normalise both."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    raise TypeError(f"cannot interpret {value!r} as a time")


def classify(*, expected_frequency: str | None, last_successful,
             consecutive_failures: int, last_source_publication=None,
             expected_lag: timedelta | None = None,
             run_interval: timedelta | None = None,
             now: datetime | None = None) -> str:
    """Return one of: failing, stale, due, current, unknown.

    `last_source_publication` is the newest PERIOD the source has
    published (a date), not when it published it. `expected_lag` is how
    far behind real time that source normally runs -- ERA5 is months
    behind by design, so its newest period is always old and that is
    correct, not stale.
    """
    now = now or datetime.now(timezone.utc)
    frequency = expected_frequency or "irregular"
    cadence = CADENCE.get(frequency, CADENCE["irregular"])
    tolerance = TOLERANCE.get(frequency, TOLERANCE["irregular"])
    lag = expected_lag or timedelta(0)

    # --- axis 1: is ATLAS fetching? -------------------------------------
    # Ours to fix, so it outranks anything about the source.
    if consecutive_failures >= 3:
        return "failing"

    last_successful = _as_utc(last_successful)
    if last_successful is None:
        return "unknown"

    # A scheduler that silently stopped produces no errors at all, so
    # consecutive_failures stays 0 while nothing runs. Age of the last
    # SUCCESS catches that -- but only against OUR schedule. Measuring it
    # against the source's publication cadence, as an earlier version did,
    # allowed 486 days of silence for an annual source before anyone was
    # told. With no run_interval recorded we cannot know, so we say
    # nothing rather than guess.
    if run_interval is not None:
        # One missed run can be a blip; a second means nothing is running.
        # The grace is additive and capped, not a multiple of the interval:
        # 2x looks tidy but scales with the interval, so a 180-day boundary
        # refresh would buy a dead scheduler most of a year of silence.
        grace = min(run_interval, timedelta(days=2))
        if now - last_successful > run_interval + grace:
            return "failing"

    # --- axis 2: has the source published? ------------------------------
    # Not ours to fix. Never reported as `failing`.
    published = _as_utc(last_source_publication)
    if published is None:
        return "unknown"

    source_age = now - published
    expected = cadence + lag
    if source_age <= expected:
        return "current"
    if source_age <= expected + tolerance:
        return "due"
    return "stale"


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


def observed_publication(conn: psycopg.Connection, source_id: str) -> date | None:
    """The newest period ATLAS actually holds for a source.

    Derived from the fact table rather than trusted from a header, so it
    cannot drift from what was really ingested.
    """
    row = db.query_one(
        conn,
        "SELECT MAX(period_end) AS newest FROM observation "
        "WHERE source_id = %s AND value IS NOT NULL",
        (source_id,),
    )
    return row["newest"] if row else None


def record_publication(conn: psycopg.Connection, source_id: str,
                       published: date | None) -> None:
    """Store what the SOURCE said about its own publishing.

    Only a declaration by the source reaches this column. Anything read
    off ATLAS's own tables is observed, not declared, and keeping the two
    apart is the whole point: a number we produced cannot be evidence
    about somebody else's publishing schedule.
    """
    if published is not None:
        db.execute(
            conn,
            "UPDATE dataset_freshness SET last_source_publication = %s, "
            "updated_at = now() WHERE source_id = %s",
            (published, source_id),
        )
    recompute(conn, source_id)


def recompute(conn: psycopg.Connection, source_id: str | None = None) -> None:
    where = "WHERE f.source_id = %s" if source_id else ""
    params = (source_id,) if source_id else None
    rows = db.query(
        conn,
        f"""
        SELECT f.source_id, f.last_successful_update, f.consecutive_failures,
               f.last_source_publication, s.expected_frequency, s.expected_lag,
               s.run_interval
        FROM dataset_freshness f JOIN source s USING (source_id) {where}
        """,
        params,
    )
    for row in rows:
        # The source's own word beats anything inferred from our tables.
        # MAX(period_end) cannot outrun what ATLAS asked for, so a
        # backfill with a fixed end date would otherwise report a healthy
        # source as dormant for ever.
        declared = row["last_source_publication"]
        if declared is not None:
            published, basis = declared, "declared"
        else:
            observed = observed_publication(conn, row["source_id"])
            published, basis = ((observed, "observed") if observed
                                else (None, "none"))
        status = classify(
            expected_frequency=row["expected_frequency"],
            last_successful=row["last_successful_update"],
            consecutive_failures=row["consecutive_failures"],
            last_source_publication=published,
            expected_lag=row["expected_lag"],
            run_interval=row["run_interval"],
        )
        db.execute(
            conn,
            "UPDATE dataset_freshness "
            "SET freshness_status = %s, publication_basis = %s, "
            "    updated_at = now() "
            "WHERE source_id = %s",
            (status, basis, row["source_id"]),
        )


def status_report(conn: psycopg.Connection) -> list[dict]:
    """What the public status endpoint serves.

    The state is worked out here, not read back from dataset_freshness.
    That column is only written when an ingest runs, so a source nobody
    has ingested for a month keeps whatever it was last told -- and a
    cached `current` about a pipeline that stopped in March is the exact
    failure this module exists to prevent. Reading it live costs one
    extra query per source and cannot go stale.

    The stored value is still reported, as `stored_status` with the time
    it was written, because a gap between the two says something worth
    seeing: nothing has run against that source since.
    """
    rows = db.query(
        conn,
        """
        SELECT s.source_id, s.name, s.expected_frequency, s.licence,
               s.expected_lag, s.run_interval,
               f.last_attempted_update, f.last_successful_update,
               f.last_source_publication, f.consecutive_failures,
               f.last_error,
               f.freshness_status AS stored_status,
               f.updated_at       AS stored_status_written_at
        FROM source s JOIN dataset_freshness f USING (source_id)
        ORDER BY s.source_id
        """,
    )

    now = datetime.now(timezone.utc)
    report = []
    for row in rows:
        declared = row["last_source_publication"]
        if declared is not None:
            published, basis = declared, "declared"
        else:
            observed = observed_publication(conn, row["source_id"])
            published, basis = ((observed, "observed") if observed
                                else (None, "none"))
        report.append({
            "source_id": row["source_id"],
            "name": row["name"],
            "licence": row["licence"],
            "expected_frequency": row["expected_frequency"],
            "freshness_status": classify(
                expected_frequency=row["expected_frequency"],
                last_successful=row["last_successful_update"],
                consecutive_failures=row["consecutive_failures"],
                last_source_publication=published,
                expected_lag=row["expected_lag"],
                run_interval=row["run_interval"],
                now=now,
            ),
            "status_computed_at": now,
            # What the source itself said, or null if it says nothing.
            "declared_publication": declared,
            # The date actually used, which may be inferred from our own
            # holdings. publication_basis says which.
            "publication": published,
            "publication_basis": basis,
            "last_attempted_update": row["last_attempted_update"],
            "last_successful_update": row["last_successful_update"],
            "consecutive_failures": row["consecutive_failures"],
            "last_error": row["last_error"],
            "stored_status": row["stored_status"],
            "stored_status_written_at": row["stored_status_written_at"],
        })
    return report
