"""End-to-end against a real Postgres, using recorded fixtures.

Exercises the full path: land raw -> register -> standardise -> aggregate
-> query across two sources that agree about nothing.
"""
from __future__ import annotations

import pytest

from atlas import db
from atlas.acquisition import AcquisitionResult, Artifact
from atlas.connectors import GeoBoundariesConnector
from atlas.pipeline import aggregate, freshness, places, standardise
from atlas.storage import content_hash


@pytest.fixture
def loaded(conn, storage, fixtures):
    """Places loaded, both sources landed in the raw store and parsed."""
    kenya = places.upsert_country(conn, "KEN", "Kenya")
    rows = GeoBoundariesConnector.to_places(fixtures["geoboundaries"], kenya, "county")
    places.upsert_subnational(conn, rows)

    county_ids = [
        r["place_id"] for r in db.query(
            conn, "SELECT place_id FROM place WHERE level = 'adm1' ORDER BY place_id")
    ]

    # Land the World Bank payload exactly as acquisition would.
    wb_key, wb_body = storage.put_json("worldbank", fixtures["worldbank"])
    db.record_raw_object(
        conn, wb_key, "worldbank",
        {"url": "https://api.worldbank.org/v2/country/KEN/indicator/NY.GDP.PCAP.CD"},
        "2026-11-16T00:00:00Z", content_hash(wb_body), 200, "application/json", len(wb_body))

    # Open-Meteo, re-pointed at the real county place_ids.
    om_payload = {**fixtures["open_meteo"], "places": county_ids}
    om_key, om_body = storage.put_json("open-meteo", om_payload)
    db.record_raw_object(
        conn, om_key, "open-meteo",
        {"url": "https://archive-api.open-meteo.com/v1/archive"},
        "2026-11-16T00:00:00Z", content_hash(om_body), 200, "application/json", len(om_body))

    standardise.standardise_pending(conn, storage, "worldbank")
    standardise.standardise_pending(conn, storage, "open-meteo")
    return {"kenya": kenya, "counties": county_ids,
            "wb_key": wb_key, "om_key": om_key}


# --- places -------------------------------------------------------------

def test_counties_load_with_geometry(conn, loaded):
    rows = db.query(conn, "SELECT name, level, level_name, country_iso3, parent_id, "
                          "geom IS NOT NULL AS has_geom FROM place WHERE level = 'adm1'")
    assert len(rows) == 5
    assert all(r["has_geom"] and r["level_name"] == "county" for r in rows)
    assert all(r["parent_id"] == loaded["kenya"] for r in rows)


def test_spatial_join_places_a_coordinate(conn, loaded):
    """Nairobi's coordinates land in Nairobi county."""
    hit = places.place_containing(conn, lat=-1.2921, lon=36.8219)
    assert hit is not None and hit["name"] == "Nairobi"


def test_point_outside_every_boundary_returns_none(conn, loaded):
    """A point in the Indian Ocean is honestly nowhere, not nearest-match."""
    assert places.place_containing(conn, lat=-5.0, lon=45.0) is None


def test_place_loading_is_idempotent(conn, loaded, fixtures):
    before = db.query_one(conn, "SELECT count(*) AS n FROM place")["n"]
    rows = GeoBoundariesConnector.to_places(fixtures["geoboundaries"], loaded["kenya"], "county")
    places.upsert_subnational(conn, rows)
    assert db.query_one(conn, "SELECT count(*) AS n FROM place")["n"] == before


# --- standardisation ----------------------------------------------------

def test_both_sources_landed_observations(conn, loaded):
    counts = {
        r["source_id"]: r["n"] for r in db.query(
            conn, "SELECT source_id, count(*) AS n FROM observation GROUP BY source_id")
    }
    assert counts["worldbank"] == 10          # ten years
    assert counts["open-meteo"] == 5 * 31     # five counties, January


def test_provenance_resolves_to_the_fetched_file(conn, loaded):
    row = db.query_one(
        conn,
        """
        SELECT o.value, r.request ->> 'url' AS url, r.content_hash
        FROM observation o JOIN raw_object r ON r.s3_key = o.raw_ref
        WHERE o.source_id = 'worldbank' AND o.value IS NOT NULL LIMIT 1
        """,
    )
    assert row["url"].startswith("https://api.worldbank.org")
    assert len(row["content_hash"]) == 64


def test_standardising_twice_does_not_duplicate(conn, storage, loaded):
    before = db.query_one(conn, "SELECT count(*) AS n FROM observation")["n"]
    db.execute(conn, "UPDATE raw_object SET parsed_at = NULL")   # force a re-parse
    standardise.standardise_pending(conn, storage, "worldbank")
    standardise.standardise_pending(conn, storage, "open-meteo")
    assert db.query_one(conn, "SELECT count(*) AS n FROM observation")["n"] == before


def test_missing_values_carry_a_reason(conn, loaded):
    rows = db.query(
        conn, "SELECT source_id, missing_reason FROM observation WHERE value IS NULL")
    assert rows                                     # fixtures contain real gaps
    assert all(r["missing_reason"] == "not_collected" for r in rows)


# --- aggregation --------------------------------------------------------

def test_rainfall_rolls_up_by_summing(conn, loaded):
    written = aggregate.rollup(conn, "precip_total_mm", "month")
    assert written == 5                              # one month, five counties

    daily, monthly = db.query(
        conn,
        """
        SELECT grain, SUM(value) AS total FROM observation
        WHERE indicator_id = (SELECT indicator_id FROM indicator WHERE code='precip_total_mm')
          AND place_id = %s AND grain IN ('day','month')
        GROUP BY grain ORDER BY grain
        """,
        (loaded["counties"][0],),
    )
    assert pytest.approx(float(daily["total"]), rel=1e-6) == float(monthly["total"])


def test_aggregated_rows_are_labelled_as_such(conn, loaded):
    aggregate.rollup(conn, "precip_total_mm", "month")
    rows = db.query(conn, "SELECT DISTINCT quality FROM observation WHERE grain = 'month'")
    assert [r["quality"] for r in rows] == ["aggregated"]


def test_refuses_to_invent_a_finer_grain(conn, loaded):
    """GDP is annual. Producing daily GDP would mean making up numbers,
    and a chart would present them as fact."""
    with pytest.raises(aggregate.RefusedInvention, match="Not available at that grain"):
        aggregate.rollup(conn, "gdp_per_capita_usd", "day")


def test_aggregation_is_idempotent(conn, loaded):
    aggregate.rollup(conn, "precip_total_mm", "month")
    first = db.query_one(conn, "SELECT count(*) AS n FROM observation WHERE grain='month'")["n"]
    aggregate.rollup(conn, "precip_total_mm", "month")
    assert db.query_one(
        conn, "SELECT count(*) AS n FROM observation WHERE grain='month'")["n"] == first


# --- the whole point ----------------------------------------------------

def test_cross_source_join(conn, loaded):
    """Two sources that agreed on nothing -- country code vs coordinates,
    annual vs daily, JSON vs reanalysis -- in one result set, every value
    traceable to the file it came from."""
    aggregate.rollup(conn, "precip_total_mm", "year")

    rows = db.query(
        conn,
        """
        SELECT p.name AS place, i.code AS indicator, i.unit,
               EXTRACT(YEAR FROM o.period_start)::int AS year,
               ROUND(o.value::numeric, 1) AS value, o.quality, o.source_id
        FROM observation o
        JOIN place p USING (place_id)
        JOIN indicator i USING (indicator_id)
        WHERE o.grain = 'year' AND EXTRACT(YEAR FROM o.period_start) = 2024
          AND o.value IS NOT NULL
        ORDER BY i.code, p.name
        """,
    )
    sources = {r["source_id"] for r in rows}
    assert sources == {"open-meteo"}, "2024 GDP is null in the fixture, as upstream"

    rows_2023 = db.query(
        conn,
        """
        SELECT i.code, o.source_id FROM observation o JOIN indicator i USING (indicator_id)
        WHERE o.grain='year' AND EXTRACT(YEAR FROM o.period_start)=2023 AND o.value IS NOT NULL
        """,
    )
    assert {r["source_id"] for r in rows_2023} == {"worldbank"}


def test_coverage_reports_the_gaps(conn, loaded):
    report = aggregate.coverage(conn)
    by_code = {(r["indicator"], r["grain"]): r for r in report}
    gdp = by_code[("gdp_per_capita_usd", "year")]
    assert gdp["observations"] == 10
    assert gdp["missing"] == 1          # 2024 is null upstream
    assert gdp["places"] == 1


# --- freshness ----------------------------------------------------------

def test_failure_and_staleness_are_different_states(conn, loaded):
    from datetime import datetime, timedelta, timezone
    now = datetime.now(timezone.utc)

    # Our pipeline is broken -- should page someone.
    assert freshness.classify(expected_frequency="daily", last_successful=now,
                              consecutive_failures=3) == "failing"
    # We fetched fine; the source simply hasn't published. Not our fault.
    assert freshness.classify(expected_frequency="daily",
                              last_successful=now - timedelta(days=30),
                              consecutive_failures=0) == "stale"
    assert freshness.classify(expected_frequency="daily", last_successful=now,
                              consecutive_failures=0) == "current"


def test_staleness_rule_is_per_source(conn, loaded):
    """ERA5 runs months behind by design; a global 30-day rule would flag
    correct data and train users to ignore the warning."""
    from datetime import datetime, timedelta, timezone
    sixty_days_ago = datetime.now(timezone.utc) - timedelta(days=60)

    assert freshness.classify(expected_frequency="daily",
                              last_successful=sixty_days_ago,
                              consecutive_failures=0) == "stale"
    assert freshness.classify(expected_frequency="annual",
                              last_successful=sixty_days_ago,
                              consecutive_failures=0) == "current"


def test_recording_a_failure_updates_status(conn, loaded):
    freshness.record_attempt(conn, "worldbank", succeeded=False, error="timeout")
    freshness.record_attempt(conn, "worldbank", succeeded=False, error="timeout")
    freshness.record_attempt(conn, "worldbank", succeeded=False, error="timeout")
    row = db.query_one(
        conn, "SELECT freshness_status, consecutive_failures, last_error "
              "FROM dataset_freshness WHERE source_id = 'worldbank'")
    assert row["freshness_status"] == "failing"
    assert row["consecutive_failures"] == 3
    assert row["last_error"] == "timeout"
