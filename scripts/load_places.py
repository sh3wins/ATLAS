#!/usr/bin/env python
"""Load countries and their subnational boundaries into `place`.

    python scripts/load_places.py --country KEN

Loads the country row, then its boundaries from geoBoundaries at the
level `country_config` declares for it. Kenya's adm1 is a county,
Uganda's a district, Tanzania's a region -- the script never knows which,
it reads the config.

Idempotent: re-running updates geometry rather than duplicating places.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Make the repo root importable when run as a script, so `python
# scripts/<name>.py` works from anywhere without PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from atlas import db
from atlas.connectors import GeoBoundariesConnector
from atlas.pipeline import freshness, standardise
from atlas.storage import get_storage

COUNTRY_NAMES = {
    "KEN": "Kenya", "UGA": "Uganda", "TZA": "Tanzania",
    "RWA": "Rwanda", "BDI": "Burundi",
}


def upsert_country(conn, iso3: str, name: str) -> int:
    existing = db.query_one(
        conn,
        "SELECT place_id FROM place WHERE country_iso3 = %s AND level = 'country'",
        (iso3,),
    )
    if existing:
        return existing["place_id"]
    row = db.query_one(
        conn,
        """
        INSERT INTO place (name, level, country_iso3, parent_id)
        VALUES (%s, 'country', %s, NULL) RETURNING place_id
        """,
        (name, iso3),
    )
    # World Bank speaks ISO3, so the country's crosswalk entry is what
    # lets its annual series land against this place.
    db.execute(
        conn,
        """
        INSERT INTO place_crosswalk (source_id, source_code, place_id, confidence)
        VALUES ('worldbank', %s, %s, 'exact')
        ON CONFLICT (source_id, source_code) DO NOTHING
        """,
        (iso3, row["place_id"]),
    )
    return row["place_id"]


def upsert_subnational(conn, places: list[dict]) -> int:
    written = 0
    for place in places:
        existing = db.query_one(
            conn,
            """
            SELECT p.place_id FROM place_crosswalk c JOIN place p USING (place_id)
            WHERE c.source_id = 'geoboundaries' AND c.source_code = %s
            """,
            (place["source_code"],),
        )
        if existing:
            db.execute(
                conn,
                "UPDATE place SET name = %s, geom = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326) "
                "WHERE place_id = %s",
                (place["name"], place["geojson"], existing["place_id"]),
            )
            written += 1
            continue

        row = db.query_one(
            conn,
            """
            INSERT INTO place (name, level, level_name, country_iso3, parent_id, geom)
            VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326))
            RETURNING place_id
            """,
            (place["name"], place["level"], place["level_name"],
             place["country_iso3"], place["parent_id"], place["geojson"]),
        )
        db.execute(
            conn,
            """
            INSERT INTO place_crosswalk (source_id, source_code, place_id, confidence)
            VALUES ('geoboundaries', %s, %s, 'exact')
            ON CONFLICT (source_id, source_code) DO NOTHING
            """,
            (place["source_code"], row["place_id"]),
        )
        written += 1
    return written


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--country", required=True, help="ISO3, e.g. KEN")
    parser.add_argument("--from-file", help="Load a saved geoBoundaries payload instead of fetching")
    args = parser.parse_args()

    iso3 = args.country.upper()
    storage = get_storage()

    with db.connect() as conn:
        config = db.query_one(
            conn, "SELECT * FROM country_config WHERE country_iso3 = %s", (iso3,)
        )
        if config is None:
            print(f"{iso3} is not in country_config", file=sys.stderr)
            return 1
        if not config["enabled"]:
            print(f"{iso3} is configured but not enabled; flip country_config.enabled",
                  file=sys.stderr)
            return 1

        country_id = upsert_country(conn, iso3, COUNTRY_NAMES.get(iso3, iso3))
        print(f"country {iso3} -> place_id {country_id}")

        connector = GeoBoundariesConnector(storage=storage)

        if args.from_file:
            payload = json.loads(open(args.from_file).read())
        else:
            result = connector.fetch(iso3=iso3, level=config["subnational_level"])
            if not result.artifacts:
                freshness.record_attempt(conn, "geoboundaries", succeeded=False,
                                         error=str(result.failed_chunks))
                print(f"fetch failed: {result.failed_chunks}", file=sys.stderr)
                return 1
            standardise.record_acquisition(conn, result)
            payload = storage.get_json(result.artifacts[0].key)
            freshness.record_attempt(conn, "geoboundaries", succeeded=True)

        places = connector.to_places(payload, country_id, config["level_name"])
        written = upsert_subnational(conn, places)
        print(f"{written} {config['level_name']}s loaded for {iso3}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
