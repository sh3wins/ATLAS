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
from atlas.pipeline import freshness, places, standardise
from atlas.storage import get_storage

COUNTRY_NAMES = {
    "KEN": "Kenya", "UGA": "Uganda", "TZA": "Tanzania",
    "RWA": "Rwanda", "BDI": "Burundi",
}


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

        country_id = places.upsert_country(conn, iso3, COUNTRY_NAMES.get(iso3, iso3))
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

        rows = connector.to_places(payload, country_id, config["level_name"])
        written = places.upsert_subnational(conn, rows)
        print(f"{written} {config['level_name']}s loaded for {iso3}")

        # Coordinate-keyed sources need a crosswalk entry per centroid
        # before they can be fetched; see places.ensure_coordinate_crosswalk.
        mapped = places.ensure_coordinate_crosswalk(conn, [iso3])
        print(f"{mapped} centroid(s) registered for coordinate-keyed sources")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
