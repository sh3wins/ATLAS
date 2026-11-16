#!/usr/bin/env python
"""Run a source end to end: acquire, register, standardise, aggregate.

    python scripts/ingest.py worldbank  --start-year 2000
    python scripts/ingest.py open-meteo --start 2024-01-01 --end 2024-12-31

Each stage commits on its own. That is the point, not tidiness: when the
whole run shared one transaction, a failure in aggregation rolled back the
`raw_object` rows too. The files stayed in the store but became invisible
to `unparsed_objects`, so they could never be parsed and never retried --
the only recovery was a re-fetch, burning the call budget this design
exists to protect.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from atlas import db                                          # noqa: E402
from atlas.connectors import get_connector                    # noqa: E402
from atlas.pipeline import aggregate, freshness, places, standardise  # noqa: E402
from atlas.storage import get_storage                         # noqa: E402

ROLLUP_TARGETS = ("month", "quarter", "year")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=["worldbank", "open-meteo"])
    parser.add_argument("--indicator", help="Source's own indicator code")
    parser.add_argument("--start-year", type=int, default=1990)
    parser.add_argument("--end-year", type=int)
    parser.add_argument("--start", help="ISO date, Open-Meteo only")
    parser.add_argument("--end", help="ISO date, Open-Meteo only")
    parser.add_argument("--skip-fetch", action="store_true",
                        help="Parse what is already in the raw store")
    args = parser.parse_args()

    storage = get_storage()

    # --- stage 1: read configuration -------------------------------------
    with db.connect() as conn:
        countries = [c["country_iso3"] for c in db.enabled_countries(conn)]
        if not countries:
            print("no countries enabled in country_config", file=sys.stderr)
            return 1
        points = (places.subnational_points(conn, countries)
                  if args.source == "open-meteo" else [])
        if args.source == "open-meteo" and not args.skip_fetch:
            if not points:
                print("no subnational places with geometry; run load_places.py first",
                      file=sys.stderr)
                return 1
            # Idempotent, and cheap insurance: a place loaded before this
            # existed would otherwise have no coordinate code to resolve.
            places.ensure_coordinate_crosswalk(conn, countries)

    # --- stage 2: acquire, then register what landed ---------------------
    if not args.skip_fetch:
        connector = get_connector(args.source, storage=storage)
        try:
            if args.source == "worldbank":
                result = connector.fetch(
                    countries=countries,
                    indicator=args.indicator or "NY.GDP.PCAP.CD",
                    start_year=args.start_year,
                    end_year=args.end_year,
                )
            else:
                result = connector.fetch(
                    points=points,
                    start_date=args.start or "2024-01-01",
                    end_date=args.end or "2024-12-31",
                    variable=args.indicator or "precipitation_sum",
                )
        except Exception as exc:  # noqa: BLE001
            with db.connect() as conn:
                freshness.record_attempt(conn, args.source, succeeded=False,
                                         error=str(exc))
            print(f"acquisition failed: {exc}", file=sys.stderr)
            return 1

        # Committed before anything parses, so a later failure cannot
        # orphan these files.
        with db.connect() as conn:
            standardise.record_acquisition(conn, result)
        print(f"acquired {len(result)} artifact(s), status={result.status.value}")

        if result.failed_chunks:
            print(f"{len(result.failed_chunks)} chunk(s) failed, retryable:",
                  file=sys.stderr)
            for chunk in result.failed_chunks[:5]:
                print(f"  {chunk.get('error')}", file=sys.stderr)

        # A partial run is progress, not an outage: the artifacts that
        # landed are real. Only a run that produced nothing counts as a
        # failed attempt.
        with db.connect() as conn:
            freshness.record_attempt(
                conn, args.source,
                succeeded=bool(result.artifacts),
                error=result.error or (str(result.failed_chunks[:3])
                                       if result.failed_chunks else None),
            )

    # --- stage 3: standardise --------------------------------------------
    with db.connect() as conn:
        report = standardise.standardise_pending(conn, storage, args.source)
    print(f"parsed {report.artifacts_read} artifact(s) -> "
          f"{report.rows_written} observation(s) written")
    if report.artifacts_failed:
        print(f"{report.artifacts_failed} artifact(s) left unparsed for retry:",
              file=sys.stderr)
        for err in report.errors[:5]:
            print(f"  {err}", file=sys.stderr)

    # --- stage 4: aggregate ----------------------------------------------
    with db.connect() as conn:
        for indicator in db.query(
            conn, "SELECT code, native_grain FROM indicator WHERE source_id = %s",
            (args.source,),
        ):
            for target in ROLLUP_TARGETS:
                if aggregate.can_roll_up(indicator["native_grain"], target):
                    n = aggregate.rollup(conn, indicator["code"], target)
                    print(f"rolled {indicator['code']} -> {target}: {n} row(s)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
