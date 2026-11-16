"""Database access.

Thin deliberately: psycopg with explicit SQL rather than an ORM, because
the interesting work here is the shape of the queries and hiding them
behind objects would obscure it.
"""
from __future__ import annotations

import json
from contextlib import contextmanager
from typing import Any, Iterable, Iterator

import psycopg
from psycopg.rows import dict_row

from .config import settings


@contextmanager
def connect(url: str | None = None) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(url or settings.database_url, row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(conn: psycopg.Connection, sql: str, params: Iterable | None = None) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def query_one(conn: psycopg.Connection, sql: str, params: Iterable | None = None) -> dict | None:
    rows = query(conn, sql, params)
    return rows[0] if rows else None


def execute(conn: psycopg.Connection, sql: str, params: Iterable | None = None) -> int:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.rowcount


# --- raw objects --------------------------------------------------------

def record_raw_object(
    conn: psycopg.Connection,
    s3_key: str,
    source_id: str,
    request: dict[str, Any],
    fetched_at,
    content_hash: str,
    http_status: int | None = None,
    content_type: str | None = None,
    byte_size: int | None = None,
) -> None:
    """Register a fetched file. Idempotent on the key, so a replayed
    acquisition does not error."""
    execute(
        conn,
        """
        INSERT INTO raw_object (s3_key, source_id, request, fetched_at,
                                content_hash, http_status, content_type, byte_size)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (s3_key) DO NOTHING
        """,
        (s3_key, source_id, json.dumps(request), fetched_at,
         content_hash, http_status, content_type, byte_size),
    )


def mark_parsed(conn: psycopg.Connection, s3_key: str) -> None:
    execute(conn, "UPDATE raw_object SET parsed_at = now() WHERE s3_key = %s", (s3_key,))


def unparsed_objects(conn: psycopg.Connection, source_id: str) -> list[dict]:
    return query(
        conn,
        """
        SELECT s3_key, source_id, request, fetched_at
        FROM raw_object
        WHERE source_id = %s AND parsed_at IS NULL
        ORDER BY fetched_at
        """,
        (source_id,),
    )


# --- reference data -----------------------------------------------------

def resolve_place(conn: psycopg.Connection, source_id: str, source_code: str) -> int | None:
    """Map a source's own code to an ATLAS place_id via the crosswalk.

    Returns None when the code is unknown, which the caller must treat as a
    reason to skip the row rather than to invent a place.
    """
    row = query_one(
        conn,
        "SELECT place_id FROM place_crosswalk WHERE source_id = %s AND source_code = %s",
        (source_id, source_code),
    )
    return row["place_id"] if row else None


def indicator_by_source_code(conn: psycopg.Connection, source_id: str,
                             source_code: str) -> dict | None:
    return query_one(
        conn,
        """
        SELECT indicator_id, code, unit, agg_rule, native_grain, direction
        FROM indicator WHERE source_id = %s AND source_code = %s
        """,
        (source_id, source_code),
    )


def enabled_countries(conn: psycopg.Connection) -> list[dict]:
    return query(
        conn,
        """
        SELECT country_iso3, name, subnational_level, level_name, timezone
        FROM country_config WHERE enabled ORDER BY country_iso3
        """,
    )


# --- observations -------------------------------------------------------

UPSERT_OBSERVATION = """
INSERT INTO observation (place_id, indicator_id, period_start, period_end, grain,
                         value, missing_reason, source_id, raw_ref, quality)
VALUES (%(place_id)s, %(indicator_id)s, %(period_start)s, %(period_end)s, %(grain)s,
        %(value)s, %(missing_reason)s, %(source_id)s, %(raw_ref)s, %(quality)s)
ON CONFLICT (place_id, indicator_id, period_start, grain, source_id)
DO UPDATE SET value          = EXCLUDED.value,
              missing_reason = EXCLUDED.missing_reason,
              period_end     = EXCLUDED.period_end,
              raw_ref        = EXCLUDED.raw_ref,
              quality        = EXCLUDED.quality,
              ingested_at    = now()
-- Skip rows whose content is unchanged, so rowcount is "rows that
-- actually changed" rather than "rows sent". Re-parsing is meant to be
-- free and frequent; a run that reports 4,732 observations written when
-- it changed nothing trains you to ignore the number.
WHERE observation.value          IS DISTINCT FROM EXCLUDED.value
   OR observation.missing_reason IS DISTINCT FROM EXCLUDED.missing_reason
   OR observation.period_end     IS DISTINCT FROM EXCLUDED.period_end
   OR observation.raw_ref        IS DISTINCT FROM EXCLUDED.raw_ref
   OR observation.quality        IS DISTINCT FROM EXCLUDED.quality
"""


def upsert_observations(conn: psycopg.Connection, rows: list[dict]) -> int:
    """Write observations idempotently.

    The ON CONFLICT clause is what makes re-runs safe. You will re-run this
    constantly while developing, and a pipeline that quietly doubles its
    data each time is a miserable bug to find.
    """
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(UPSERT_OBSERVATION, rows)
        return cur.rowcount
