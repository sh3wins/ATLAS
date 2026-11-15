"""The public API.

Three things here are deliberate rather than incidental:

1. Every response that carries values also carries the attribution for
   the sources those values came from. CC BY 4.0 requires crediting the
   source and indicating what changed; with an API returning arbitrary
   slices, a line in a page footer does not discharge that.

2. /observations can return provenance inline. A value, the file it came
   from, when it was fetched, and what was done to it -- that is the
   difference between a provenance claim and a provenance feature.

3. /status is public. It shows every source's freshness, including when
   ATLAS itself is failing to fetch. None of the comparable platforms
   publish this.
"""
from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Query

from .. import db
from ..pipeline import aggregate, freshness

app = FastAPI(
    title="ATLAS",
    version="0.1.0",
    description=(
        "East African data you can trace: county resolution, kept current, "
        "consumable by code."
    ),
)


def get_conn():
    with db.connect() as conn:
        yield conn


# --- reference ----------------------------------------------------------

@app.get("/places")
def list_places(
    country: str | None = Query(None, min_length=3, max_length=3),
    level: str | None = None,
    conn=Depends(get_conn),
) -> dict[str, Any]:
    clauses, params = ["TRUE"], []
    if country:
        clauses.append("p.country_iso3 = %s")
        params.append(country.upper())
    if level:
        clauses.append("p.level = %s")
        params.append(level)

    rows = db.query(
        conn,
        f"""
        SELECT p.place_id, p.name, p.level, p.level_name, p.country_iso3, p.parent_id
        FROM place p WHERE {' AND '.join(clauses)}
        ORDER BY p.country_iso3, p.level, p.name
        """,
        params,
    )
    return {"count": len(rows), "places": rows}


@app.get("/indicators")
def list_indicators(conn=Depends(get_conn)) -> dict[str, Any]:
    rows = db.query(
        conn,
        """
        SELECT i.code, i.name, i.definition, i.unit, i.agg_rule,
               i.native_grain, i.direction, i.source_id, i.source_code
        FROM indicator i ORDER BY i.code
        """,
    )
    return {"count": len(rows), "indicators": rows}


# --- values -------------------------------------------------------------

@app.get("/observations")
def list_observations(
    indicator: str = Query(..., description="ATLAS indicator code"),
    place_id: int | None = None,
    country: str | None = Query(None, min_length=3, max_length=3),
    grain: str = "year",
    start: date | None = None,
    end: date | None = None,
    provenance: bool = Query(False, description="Include the source file for each value"),
    limit: int = Query(1000, le=10000),
    conn=Depends(get_conn),
) -> dict[str, Any]:
    spec = db.query_one(
        conn, "SELECT indicator_id, code, unit, native_grain FROM indicator WHERE code = %s",
        (indicator,),
    )
    if spec is None:
        raise HTTPException(404, f"unknown indicator {indicator!r}")

    # Refuse a finer grain rather than inventing one. If a researcher
    # needs daily and only monthly exists, "not available at that grain"
    # is the honest answer and a feature of a trustworthy platform.
    if grain != spec["native_grain"] and not aggregate.can_roll_up(spec["native_grain"], grain):
        raise HTTPException(
            422,
            f"{indicator} exists at {spec['native_grain']} grain; "
            f"{grain} would be finer. Not available at that grain.",
        )

    clauses = ["o.indicator_id = %s", "o.grain = %s"]
    params: list[Any] = [spec["indicator_id"], grain]
    if place_id:
        clauses.append("o.place_id = %s")
        params.append(place_id)
    if country:
        clauses.append("p.country_iso3 = %s")
        params.append(country.upper())
    if start:
        clauses.append("o.period_start >= %s")
        params.append(start)
    if end:
        clauses.append("o.period_end <= %s")
        params.append(end)
    params.append(limit)

    provenance_cols = (
        ", r.request ->> 'url' AS source_url, r.fetched_at, r.content_hash"
        if provenance else ""
    )
    provenance_join = "LEFT JOIN raw_object r ON r.s3_key = o.raw_ref" if provenance else ""

    rows = db.query(
        conn,
        f"""
        SELECT p.name AS place, p.place_id, p.level,
               o.period_start, o.period_end, o.grain,
               o.value, o.missing_reason, o.quality, o.source_id, o.raw_ref
               {provenance_cols}
        FROM observation o
        JOIN place p USING (place_id)
        {provenance_join}
        WHERE {' AND '.join(clauses)}
        ORDER BY p.name, o.period_start
        LIMIT %s
        """,
        params,
    )

    return {
        "indicator": spec["code"],
        "unit": spec["unit"],
        "grain": grain,
        "count": len(rows),
        "observations": rows,
        "attribution": _attribution(conn, {r["source_id"] for r in rows}),
    }


@app.get("/coverage")
def coverage(conn=Depends(get_conn)) -> dict[str, Any]:
    """The shape of the gaps. Researchers need this before they build on
    the data, and saying it plainly beats being found out."""
    return {"coverage": aggregate.coverage(conn)}


# --- operations ---------------------------------------------------------

@app.get("/status")
def status(conn=Depends(get_conn)) -> dict[str, Any]:
    report = freshness.status_report(conn)
    return {
        "promise": (
            "ATLAS commits to ingestion, validation, availability and delivery. "
            "It does not warrant that an upstream provider never changes its "
            "schema, restates a figure, or publishes inaccurate information."
        ),
        "sources": report,
    }


def _attribution(conn, source_ids: set[str]) -> list[dict]:
    """Licence terms for exactly the sources in this response."""
    if not source_ids:
        return []
    return db.query(
        conn,
        """
        SELECT source_id, name, licence, licence_url, attribution_text
        FROM source WHERE source_id = ANY(%s) AND attribution_required
        ORDER BY source_id
        """,
        (sorted(source_ids),),
    )
