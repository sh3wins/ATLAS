"""Aggregation.

The rule comes from the indicator registry, never from this module. Add
an indicator, declare its agg_rule, and roll-ups work without anyone
touching this file. That is the whole point: nothing here knows what
rainfall is.

Direction of travel is one-way. Coarser is combining real observations;
finer is invention. Monthly to daily means making up numbers, and a
chart will present them as fact.
"""
from __future__ import annotations

from datetime import date

import psycopg

from .. import db

# Coarseness order. A roll-up is only legal from left to right.
GRAIN_ORDER = ["hour", "day", "month", "quarter", "year"]

# agg_rule -> the SQL that implements it.
#
# `last` and `first` need the value at a period boundary rather than an
# aggregate, so they use a window function. Population is the reason:
# summing twelve monthly population figures gives nonsense, and averaging
# them gives a number nobody asked for.
AGG_SQL = {
    "sum": "SUM(value)",
    "mean": "AVG(value)",
    "min": "MIN(value)",
    "max": "MAX(value)",
    "last": "(ARRAY_AGG(value ORDER BY period_start DESC))[1]",
    "first": "(ARRAY_AGG(value ORDER BY period_start ASC))[1]",
}

PERIOD_TRUNC = {"month": "month", "quarter": "quarter", "year": "year"}


def can_roll_up(source_grain: str, target_grain: str) -> bool:
    try:
        return GRAIN_ORDER.index(target_grain) > GRAIN_ORDER.index(source_grain)
    except ValueError:
        return False


class RefusedInvention(ValueError):
    """Raised on a request to produce a finer grain than the data has.

    Not an error to work around. If a researcher needs daily data and
    only monthly exists, the honest answer is that it is not available at
    that grain.
    """


def rollup(conn: psycopg.Connection, indicator_code: str, target_grain: str,
           place_ids: list[int] | None = None) -> int:
    """Aggregate an indicator's native-grain observations to target_grain.

    Writes the result back into `observation` with quality='aggregated',
    so a consumer can always tell a rolled-up figure from a reported one.
    """
    indicator = db.query_one(
        conn,
        "SELECT indicator_id, code, agg_rule, native_grain, source_id "
        "FROM indicator WHERE code = %s",
        (indicator_code,),
    )
    if indicator is None:
        raise KeyError(f"unknown indicator {indicator_code!r}")

    native = indicator["native_grain"]
    if target_grain == native:
        return 0
    if not can_roll_up(native, target_grain):
        raise RefusedInvention(
            f"{indicator_code} exists at {native} grain; "
            f"{target_grain} would be finer. Not available at that grain."
        )

    agg = AGG_SQL.get(indicator["agg_rule"])
    if agg is None:
        raise KeyError(f"unknown agg_rule {indicator['agg_rule']!r}")

    trunc = PERIOD_TRUNC.get(target_grain)
    if trunc is None:
        raise KeyError(f"cannot roll up to {target_grain!r}")

    where_place = "AND place_id = ANY(%(place_ids)s)" if place_ids else ""

    # period_end is the last day actually covered, NOT the nominal end of
    # the period. Three months of daily data rolled to 'year' therefore
    # ends 31 March, not 31 December -- so a consumer can see at a glance
    # that the year is partial instead of reading a quarter's rainfall as
    # an annual total.
    #
    # A period with no data at all produces no row, rather than a zero.
    sql = f"""
    INSERT INTO observation (place_id, indicator_id, period_start, period_end,
                             grain, value, missing_reason, source_id, raw_ref, quality)
    SELECT place_id,
           indicator_id,
           DATE_TRUNC('{trunc}', period_start)::date AS period_start,
           MAX(period_end) AS period_end,
           %(target_grain)s,
           {agg} AS value,
           NULL,
           source_id,
           NULL,
           'aggregated'
    FROM observation
    WHERE indicator_id = %(indicator_id)s
      AND grain = %(native)s
      AND value IS NOT NULL
      {where_place}
    GROUP BY place_id, indicator_id, DATE_TRUNC('{trunc}', period_start), source_id
    ON CONFLICT (place_id, indicator_id, period_start, grain, source_id)
    DO UPDATE SET value       = EXCLUDED.value,
                  period_end  = EXCLUDED.period_end,
                  quality     = 'aggregated',
                  ingested_at = now()
    """
    params = {
        "indicator_id": indicator["indicator_id"],
        "native": native,
        "target_grain": target_grain,
        "place_ids": place_ids,
    }
    return db.execute(conn, sql, params)


def coverage(conn: psycopg.Connection) -> list[dict]:
    """Which indicators exist, for which places, over which periods.

    Cheap to build and does more for credibility than any dashboard:
    researchers need the shape of the gaps before they build on the data.
    """
    return db.query(
        conn,
        """
        SELECT i.code                              AS indicator,
               i.unit,
               o.grain,
               o.source_id,
               COUNT(DISTINCT o.place_id)          AS places,
               MIN(o.period_start)                 AS earliest,
               MAX(o.period_end)                   AS latest,
               COUNT(*)                            AS observations,
               COUNT(*) FILTER (WHERE o.value IS NULL) AS missing
        FROM observation o
        JOIN indicator i USING (indicator_id)
        GROUP BY i.code, i.unit, o.grain, o.source_id
        ORDER BY i.code, o.grain
        """,
    )
