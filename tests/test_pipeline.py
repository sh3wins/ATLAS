"""End-to-end against a real Postgres, using recorded fixtures.

Exercises the full path: land raw -> register -> standardise -> aggregate
-> query across two sources that agree about nothing.
"""
from __future__ import annotations

import pytest

from atlas import db
from atlas.acquisition import AcquisitionResult, Artifact
from atlas.connectors import GeoBoundariesConnector
from atlas.connectors.open_meteo import coord_code
from atlas.pipeline import aggregate, freshness, places, standardise
from atlas.storage import content_hash


@pytest.fixture
def loaded(conn, storage, fixtures):
    """Places loaded, both sources landed in the raw store and parsed."""
    kenya = places.upsert_country(conn, "KEN", "Kenya")
    rows = GeoBoundariesConnector.to_places(fixtures["geoboundaries"], kenya, "county")
    places.upsert_subnational(conn, rows)

    # Coordinates are Open-Meteo's codes for these places; without the
    # crosswalk entries nothing in its response resolves.
    places.ensure_coordinate_crosswalk(conn, ["KEN"])
    points = places.subnational_points(conn, ["KEN"])
    county_ids = [p["place_id"] for p in points]
    county_codes = [coord_code(p["lat"], p["lon"]) for p in points]

    # Land the World Bank payload exactly as acquisition would.
    wb_key, wb_body = storage.put_json("worldbank", fixtures["worldbank"])
    db.record_raw_object(
        conn, wb_key, "worldbank",
        {"url": "https://api.worldbank.org/v2/country/KEN/indicator/NY.GDP.PCAP.CD"},
        "2026-11-16T00:00:00Z", content_hash(wb_body), 200, "application/json", len(wb_body))

    # Open-Meteo, re-pointed at the real county place_ids.
    om_payload = {**fixtures["open_meteo"], "codes": county_codes}
    om_key, om_body = storage.put_json("open-meteo", om_payload)
    db.record_raw_object(
        conn, om_key, "open-meteo",
        {"url": "https://archive-api.open-meteo.com/v1/archive"},
        "2026-11-16T00:00:00Z", content_hash(om_body), 200, "application/json", len(om_body))

    standardise.standardise_pending(conn, storage, "worldbank")
    standardise.standardise_pending(conn, storage, "open-meteo")
    return {"kenya": kenya, "counties": county_ids, "codes": county_codes,
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
#
# The two axes must stay independent: "ATLAS could not fetch" is ours,
# "the source has not published" is not. An earlier version of these
# tests asserted independence while actually varying last_successful on
# both sides -- it passed, and the implementation was conflating them.

from datetime import timedelta  # noqa: E402


def _t(**kw):
    """classify() with healthy defaults, overridden per test."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    args = dict(
        expected_frequency="daily",
        last_successful=now,                      # we fetched just now
        consecutive_failures=0,                   # nothing is erroring
        last_source_publication=now.date(),       # source is up to date
        expected_lag=timedelta(0),
        run_interval=timedelta(days=1),          # ATLAS runs this one daily
        now=now,
    )
    args.update(kw)
    return freshness.classify(**args)


def test_healthy_pipeline_and_fresh_source_is_current(conn, loaded):
    assert _t() == "current"


def test_dormant_source_is_stale_while_the_pipeline_is_perfect(conn, loaded):
    """The case the old test claimed to cover and did not.

    ATLAS fetched seconds ago and has never errored. Only the source's
    newest published period is old. That must not read `current`, and
    must not read `failing` either -- it is not our fault.
    """
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    status = _t(last_source_publication=(now - timedelta(days=400)).date())
    assert status == "stale"


def test_broken_pipeline_is_failing_even_when_the_source_is_fresh(conn, loaded):
    """Mirror image: the source published today, we cannot fetch."""
    assert _t(consecutive_failures=3) == "failing"


def test_a_scheduler_that_silently_stopped_reads_failing(conn, loaded):
    """No errors are raised when nothing runs at all, so
    consecutive_failures stays 0. Age of the last success catches it --
    measured against ATLAS's own schedule."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    assert _t(last_successful=now - timedelta(days=30)) == "failing"


def test_dead_scheduler_is_measured_against_our_schedule_not_the_sources(conn, loaded):
    """An earlier version compared the age of our last run against the
    SOURCE's publication cadence, so an annual source tolerated 486 days
    of silence from a dead cron before anyone was told."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    forty_days = dict(expected_frequency="annual",
                      last_successful=now - timedelta(days=40),
                      last_source_publication=now.date())

    # Monthly schedule, 40 days of silence -> our cron is dead.
    assert _t(**forty_days, run_interval=timedelta(days=30)) == "failing"
    # No schedule recorded: say nothing rather than guess either way.
    assert _t(**forty_days, run_interval=None) != "failing"


def test_expected_lag_keeps_a_reanalysis_source_current(conn, loaded):
    """ERA5's newest period is always months old by design. Without
    expected_lag that reads stale, and users learn to ignore the field."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    old_period = (now - timedelta(days=60)).date()

    assert _t(last_source_publication=old_period,
              expected_lag=timedelta(0)) == "stale"
    assert _t(last_source_publication=old_period,
              expected_lag=timedelta(days=90)) == "current"


def test_never_fetched_is_unknown_not_current(conn, loaded):
    assert _t(last_successful=None) == "unknown"


def test_unknown_source_publication_is_not_reported_as_current(conn, loaded):
    assert _t(last_source_publication=None) == "unknown"


def test_recording_a_failure_updates_status(conn, loaded):
    for _ in range(3):
        freshness.record_attempt(conn, "worldbank", succeeded=False,
                                 error="timeout")
    row = db.query_one(
        conn, "SELECT freshness_status, consecutive_failures, last_error "
              "FROM dataset_freshness WHERE source_id = 'worldbank'")
    assert row["freshness_status"] == "failing"
    assert row["consecutive_failures"] == 3
    assert row["last_error"] == "timeout"


def test_source_publication_is_derived_from_what_was_ingested(conn, loaded):
    """Taken from the fact table, not trusted from a response header, so
    it cannot claim data ATLAS does not actually hold."""
    newest = freshness.observed_publication(conn, "worldbank")
    held = db.query_one(
        conn, "SELECT MAX(period_end) AS m FROM observation "
              "WHERE source_id='worldbank' AND value IS NOT NULL")["m"]
    assert newest == held


# --- failure handling ----------------------------------------------------

def test_one_bad_artifact_does_not_discard_the_good_ones(conn, storage, loaded):
    """A per-artifact failure used to roll back the whole transaction,
    silently dropping artifacts already written while still counting them
    as successful in the report."""
    from atlas.storage import content_hash

    before = db.query_one(
        conn, "SELECT count(*) AS n FROM observation "
              "WHERE source_id='open-meteo'")["n"]

    # One valid artifact, one whose coordinate/location counts disagree --
    # the Open-Meteo parser refuses to guess the alignment and raises.
    code = loaded["codes"][0]
    good = {"codes": [code], "variable": "precipitation_sum",
            "locations": [{"daily": {"time": ["2024-06-01"],
                                     "precipitation_sum": [4.2]}}]}
    bad = {"codes": [code, code, code], "variable": "precipitation_sum",
           "locations": [{"daily": {"time": ["2024-06-01"],
                                    "precipitation_sum": [1.0]}}]}

    for payload in (good, bad):
        key, body = storage.put_json("open-meteo", payload)
        db.record_raw_object(conn, key, "open-meteo", {"url": "x"},
                             "2026-11-16T00:00:00Z", content_hash(body),
                             200, "application/json", len(body))

    report = standardise.standardise_pending(conn, storage, "open-meteo")

    assert report.artifacts_failed == 1
    assert report.artifacts_read == 1
    # The good artifact's row survived the bad one's rollback.
    after = db.query_one(
        conn, "SELECT count(*) AS n FROM observation "
              "WHERE source_id='open-meteo'")["n"]
    assert after == before + 1
    assert report.rows_written == 1


def test_report_counts_match_what_was_committed(conn, storage, loaded):
    """The number the run prints must equal the number in the database."""
    from atlas.storage import content_hash

    payload = {"codes": [loaded["codes"][0]],
               "variable": "precipitation_sum",
               "locations": [{"daily": {"time": ["2024-07-01", "2024-07-02"],
                                        "precipitation_sum": [1.5, 2.5]}}]}
    key, body = storage.put_json("open-meteo", payload)
    db.record_raw_object(conn, key, "open-meteo", {"url": "x"},
                         "2026-11-16T00:00:00Z", content_hash(body),
                         200, "application/json", len(body))

    before = db.query_one(conn, "SELECT count(*) AS n FROM observation")["n"]
    report = standardise.standardise_pending(conn, storage, "open-meteo")
    after = db.query_one(conn, "SELECT count(*) AS n FROM observation")["n"]

    assert report.rows_written == after - before == 2


# --- acquisition resumability -------------------------------------------

def test_a_failed_chunk_marks_the_result_partial_not_complete():
    """The resumability design had no test coverage at all."""
    from atlas.acquisition import AcquisitionResult, AcquisitionStatus, Artifact

    result = AcquisitionResult(source_id="open-meteo")
    assert result.status is AcquisitionStatus.COMPLETE and result.ok

    result.add(Artifact(key="k1", content_hash="h", byte_size=1))
    result.fail_chunk({"batch": 2}, "read timed out")

    assert result.status is AcquisitionStatus.PARTIAL
    assert not result.ok
    assert len(result.artifacts) == 1          # the good batch is kept
    assert result.failed_chunks[0]["batch"] == 2
    assert "timed out" in result.failed_chunks[0]["error"]


def test_a_run_where_everything_failed_is_failed_not_partial():
    from atlas.acquisition import AcquisitionResult, AcquisitionStatus

    result = AcquisitionResult(source_id="worldbank")
    result.fail_chunk({"page": 1}, "read timed out")

    assert result.status is AcquisitionStatus.FAILED
    assert not result.ok
    assert result.artifacts == []


# --- country configuration ----------------------------------------------

def test_every_country_has_its_own_day_boundary(conn, loaded):
    """F11: Africa/Nairobi was hardcoded in the connector, which is an
    hour wrong for Rwanda and Burundi. The zone belongs to the country."""
    rows = db.query(conn, "SELECT country_iso3, timezone FROM country_config")
    zones = {r["country_iso3"]: r["timezone"] for r in rows}
    assert len(zones) == 5
    assert all(zones.values()), "a missing zone must stop a fetch, not default"
    assert zones["RWA"] != zones["KEN"]
    assert zones["BDI"] != zones["KEN"]


def test_points_know_which_country_they_are_in(conn, loaded):
    """Without this the ingest script cannot attach the right zone."""
    points = places.subnational_points(conn, ["KEN"])
    assert points and all(p["country_iso3"] == "KEN" for p in points)


# --- F6: whose word the freshness signal rests on -----------------------

def test_a_declared_publication_date_beats_our_own_output(conn, loaded):
    """MAX(period_end) can never outrun what ATLAS asked for, so a
    backfill bounded at a fixed end date reported every source as dormant
    for ever. The World Bank states `lastupdated`; that is the answer."""
    from datetime import date

    freshness.record_attempt(conn, "worldbank", succeeded=True)
    freshness.record_publication(conn, "worldbank", date(2026, 11, 11))

    row = db.query_one(
        conn, "SELECT last_source_publication, publication_basis "
              "FROM dataset_freshness WHERE source_id = 'worldbank'")
    assert row["last_source_publication"] == date(2026, 11, 11)
    assert row["publication_basis"] == "declared"


def test_an_inferred_date_is_never_stored_as_the_sources_own(conn, loaded):
    """Open-Meteo's archive declares no publication date. The fallback is
    still useful, but it has to be labelled as ours rather than theirs."""
    freshness.record_attempt(conn, "open-meteo", succeeded=True)

    row = db.query_one(
        conn, "SELECT last_source_publication, publication_basis "
              "FROM dataset_freshness WHERE source_id = 'open-meteo'")
    assert row["publication_basis"] == "observed"
    assert row["last_source_publication"] is None


def test_status_says_which_basis_each_source_is_on(conn, loaded):
    """A consumer cannot weigh `stale` without knowing whether it came
    from the source or from us."""
    freshness.record_attempt(conn, "open-meteo", succeeded=True)
    report = {r["source_id"]: r for r in freshness.status_report(conn)}
    assert report["open-meteo"]["publication_basis"] in {"observed", "none"}
