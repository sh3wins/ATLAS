#!/usr/bin/env python
"""Run the whole pipeline against recorded fixtures, with no network.

    python scripts/demo.py

Proves the path end to end: places loaded with geometry, two sources
landed in the raw store, standardised, aggregated by their own rules,
and queried together. Every number stays traceable to the file it came
from.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from atlas import db                                    # noqa: E402
from atlas.connectors import GeoBoundariesConnector     # noqa: E402
from atlas.pipeline import aggregate, freshness, places, standardise  # noqa: E402
from atlas.storage import LocalStorage, content_hash    # noqa: E402

FIXTURES = ROOT / "tests" / "fixtures"


SCRATCH_HINTS = ("demo", "test", "scratch", "sandbox", "tmp")


def _refuse_production(url: str, force: bool) -> None:
    """Refuse to write fixture data into anything that looks real.

    This script inserts five invented counties. Run against a database
    holding real observations, those fixtures silently become part of the
    dataset -- and a platform whose whole claim is traceability cannot
    have demo rows indistinguishable from ingested ones.
    """
    if force:
        return
    name = url.rstrip("/").rsplit("/", 1)[-1].split("?")[0].lower()
    if not any(hint in name for hint in SCRATCH_HINTS):
        raise SystemExit(
            f"refusing to run: database {name!r} does not look like a scratch "
            f"database.\n"
            f"This script writes invented fixture places and observations.\n"
            f"Point ATLAS_DATABASE_URL at a throwaway database whose name "
            f"contains one of {', '.join(SCRATCH_HINTS)}, e.g.\n"
            f"  createdb atlas_demo\n"
            f'  $env:ATLAS_DATABASE_URL = "postgresql://postgres@localhost/atlas_demo"\n'
            f"Or pass --force if you genuinely mean to write fixtures here."
        )


def rule(title: str) -> None:
    print(f"\n\033[1m{title}\033[0m\n" + "-" * len(title))


def main() -> int:
    force = "--force" in sys.argv
    _refuse_production(os.environ.get("ATLAS_DATABASE_URL", ""), force)
    storage = LocalStorage(root=os.environ.get("ATLAS_LOCAL_STORAGE", "./.atlas-raw"))

    with db.connect() as conn:
        rule("1. Places")
        kenya = places.upsert_country(conn, "KEN", "Kenya")
        gb = json.loads((FIXTURES / "geoboundaries_ken_adm1.json").read_text())
        rows = GeoBoundariesConnector.to_places(gb, kenya, "county")
        print(f"   {places.upsert_subnational(conn, rows)} counties loaded with geometry")

        hit = places.place_containing(conn, lat=-1.2921, lon=36.8219)
        print(f"   spatial join: (-1.2921, 36.8219) -> {hit['name']} {hit['level_name']}")
        print(f"   offshore point -> {places.place_containing(conn, lat=-5.0, lon=45.0)}")

        county_ids = [p["place_id"] for p in places.subnational_points(conn, ["KEN"])]

        rule("2. Acquisition (replayed from fixtures)")
        wb = json.loads((FIXTURES / "worldbank_gdp_ken.json").read_text())
        wb_key, wb_body = storage.put_json("worldbank", wb)
        db.record_raw_object(
            conn, wb_key, "worldbank",
            {"url": "https://api.worldbank.org/v2/country/KEN/indicator/NY.GDP.PCAP.CD"},
            "2026-11-16T00:00:00Z", content_hash(wb_body), 200, "application/json",
            len(wb_body))
        print(f"   worldbank  -> {wb_key}")

        om = json.loads((FIXTURES / "open_meteo_ken_jan2024.json").read_text())
        om["places"] = county_ids
        om_key, om_body = storage.put_json("open-meteo", om)
        db.record_raw_object(
            conn, om_key, "open-meteo",
            {"url": "https://archive-api.open-meteo.com/v1/archive"},
            "2026-11-16T00:00:00Z", content_hash(om_body), 200, "application/json",
            len(om_body))
        print(f"   open-meteo -> {om_key}")

        rule("3. Standardise")
        for source in ("worldbank", "open-meteo"):
            report = standardise.standardise_pending(conn, storage, source)
            print(f"   {source:<11} {report.artifacts_read} artifact -> "
                  f"{report.rows_written} observations")
            freshness.record_attempt(conn, source, succeeded=True)

        rule("4. Aggregate (rules from the registry, not the code)")
        for code, target in (("precip_total_mm", "month"), ("precip_total_mm", "year")):
            n = aggregate.rollup(conn, code, target)
            agg = db.query_one(conn, "SELECT agg_rule FROM indicator WHERE code=%s",
                               (code,))["agg_rule"]
            print(f"   {code} -> {target:<6} via {agg.upper():<5} {n} rows")
        try:
            aggregate.rollup(conn, "gdp_per_capita_usd", "day")
        except aggregate.RefusedInvention as exc:
            print(f"   refused: {exc}")

        rule("5. The cross-source query")
        for row in db.query(conn, """
            SELECT p.name AS place, i.code AS indicator,
                   EXTRACT(YEAR FROM o.period_start)::int AS yr,
                   ROUND(o.value::numeric, 1) AS value, i.unit, o.quality, o.source_id
            FROM observation o JOIN place p USING (place_id)
            JOIN indicator i USING (indicator_id)
            WHERE o.grain = 'year' AND o.value IS NOT NULL
              AND EXTRACT(YEAR FROM o.period_start) IN (2023, 2024)
            ORDER BY i.code, p.name LIMIT 8
        """):
            print(f"   {row['place']:<9} {row['indicator']:<19} {row['yr']} "
                  f"{row['value']:>9} {row['unit']:<4} {row['quality']:<10} {row['source_id']}")

        rule("6. Provenance: a number back to its file")
        prov = db.query_one(conn, """
            SELECT ROUND(o.value::numeric,1) AS value, r.request->>'url' AS url,
                   r.fetched_at::date AS fetched, LEFT(r.content_hash, 16) AS hash
            FROM observation o JOIN raw_object r ON r.s3_key = o.raw_ref
            WHERE o.value IS NOT NULL AND o.source_id='worldbank' LIMIT 1
        """)
        print(f"   value {prov['value']}  <-  {prov['url']}")
        print(f"   fetched {prov['fetched']}, sha256 {prov['hash']}...")

        rule("7. Coverage: the shape of the gaps")
        for c in aggregate.coverage(conn):
            print(f"   {c['indicator']:<19} {c['grain']:<6} {c['places']:>2} places  "
                  f"{c['earliest']} to {c['latest']}  "
                  f"{c['observations']:>4} obs, {c['missing']} missing")

        rule("8. Status")
        for s in freshness.status_report(conn):
            print(f"   {s['source_id']:<14} {s['freshness_status']:<8} "
                  f"{s['expected_frequency'] or '-':<10} {s['licence']}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
