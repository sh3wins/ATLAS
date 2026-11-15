#!/usr/bin/env python
"""Run a source end to end: acquire, record, standardise, aggregate.

    python scripts/ingest.py worldbank  --start-year 2000
    python scripts/ingest.py open-meteo --start 2024-01-01 --end 2024-12-31

Acquisition writes raw and stops. Standardisation reads from the store,
never from a live response, so a parser fix is a re-run rather than a
re-fetch -- which matters when the budget is 10,000 calls a day.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Make the repo root importable when run as a script, so `python
# scripts/<name>.py` works from anywhere without PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from atlas import db
from atlas.connectors import get_connector
from atlas.pipeline import aggregate, freshness, standardise
from atlas.storage import get_storage


def enabled_country_codes(conn) -> list[str]:
    return [c["country_iso3"] for c in db.enabled_countries(conn)]


def subnational_points(conn, iso3_list: list[str]) -> list[dict]:
    """Centroids of every enabled subnational place.

    Open-Meteo speaks coordinates, so this is how a county becomes
    something the weather API can answer for.
    """
    rows = db.query(
        conn,
        """
        SELECT place_id,
               ST_Y(ST_Centroid(geom))::numeric(9,4) AS lat,
               ST_X(ST_Centroid(geom))::numeric(9,4) AS lon
        FROM place
        WHERE country_iso3 = ANY(%s) AND level <> 'country' AND geom IS NOT NULL
        ORDER BY place_id
        """,
        (iso3_list,),
    )
    return [{"place_id": r["place_id"], "lat": float(r["lat"]), "lon": float(r["lon"])}
            for r in rows]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=["worldbank", "open-meteo"])
    parser.add_argument("--indicator", help="Source's own indicator code")
    parser.add_argument("--start-year", type=int, default=1990)
    parser.add_argument("--start", help="ISO date, Open-Meteo only")
    parser.add_argument("--end", help="ISO date, Open-Meteo only")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Parse what is already in the raw store")
    args = parser.parse_args()

    storage = get_storage()

    with db.connect() as conn:
        countries = enabled_country_codes(conn)
        if not countries:
            print("no countries enabled in country_config", file=sys.stderr)
            return 1

        if not args.skip_fetch:
            connector = get_connector(args.source, storage=storage)
            try:
                if args.source == "worldbank":
                    result = connector.fetch(
                        countries=countries,
                        indicator=args.indicator or "NY.GDP.PCAP.CD",
                        start_year=args.start_year,
                    )
                else:
                    points = subnational_points(conn, countries)
                    if not points:
                        print("no subnational places with geometry; "
                              "run load_places.py first", file=sys.stderr)
                        return 1
                    result = connector.fetch(
                        points=points,
                        start_date=args.start or "2024-01-01",
                        end_date=args.end or "2024-12-31",
                        variable=args.indicator or "precipitation_sum",
                    )
            except Exception as exc:  # noqa: BLE001
                freshness.record_attempt(conn, args.source, succeeded=False, error=str(exc))
                print(f"acquisition failed: {exc}", file=sys.stderr)
                return 1

            standardise.record_acquisition(conn, result)
            print(f"acquired {len(result)} artifact(s), status={result.status.value}")

            # A partial run is still progress: the artifacts that landed
            # get parsed, and the failed chunks are reported for retry
            # rather than costing the whole backfill.
            if result.failed_chunks:
                print(f"{len(result.failed_chunks)} chunk(s) failed, retryable:",
                      file=sys.stderr)
                for chunk in result.failed_chunks[:5]:
                    print(f"  {chunk.get('error')}", file=sys.stderr)

            freshness.record_attempt(conn, args.source, succeeded=result.ok,
                                     error=result.error)

        report = standardise.standardise_pending(conn, storage, args.source)
        print(f"parsed {report.artifacts_read} artifact(s) -> "
              f"{report.rows_written} observation(s)")
        for err in report.errors[:5]:
            print(f"  parse error: {err}", file=sys.stderr)

        # Roll up to the grains ATLAS publishes. The rule comes from the
        # registry: rainfall sums, GDP takes the last value.
        for indicator in db.query(
            conn, "SELECT code, native_grain FROM indicator WHERE source_id = %s",
            (args.source,),
        ):
            for target in ("month", "year"):
                if aggregate.can_roll_up(indicator["native_grain"], target):
                    n = aggregate.rollup(conn, indicator["code"], target)
                    print(f"rolled {indicator['code']} -> {target}: {n} row(s)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
