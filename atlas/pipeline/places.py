"""Loading places.

Kept in the package rather than in the script so the tests exercise the
same code that runs in production.

Idempotent throughout: re-running updates geometry and names rather than
duplicating places, because boundary files get re-released.
"""
from __future__ import annotations

import psycopg

from .. import db


def upsert_country(conn: psycopg.Connection, iso3: str, name: str) -> int:
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
    # World Bank speaks ISO3. This crosswalk row is what lets its annual
    # series find this place.
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


def upsert_subnational(conn: psycopg.Connection, places: list[dict]) -> int:
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
                """
                UPDATE place SET name = %s,
                                 geom = ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)
                WHERE place_id = %s
                """,
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
        # geoBoundaries' own ID is a crosswalk entry, never a primary key.
        # If they retire or renumber it, nothing in ATLAS breaks.
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


def subnational_points(conn: psycopg.Connection, iso3_list: list[str]) -> list[dict]:
    """Centroids of enabled subnational places.

    Open-Meteo speaks coordinates, so this is how a county becomes
    something a weather API can answer for.
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


def place_containing(conn: psycopg.Connection, lat: float, lon: float,
                     level: str = "adm1") -> dict | None:
    """The spatial join: which region holds this coordinate.

    A point can legitimately fall in no region -- just offshore, or in a
    gap in the boundary file. Returning None is the honest answer.
    """
    return db.query_one(
        conn,
        """
        SELECT place_id, name, level_name, country_iso3
        FROM place
        WHERE level = %s
          AND geom IS NOT NULL
          AND ST_Contains(geom, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        LIMIT 1
        """,
        (level, lon, lat),
    )
