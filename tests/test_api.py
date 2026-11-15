"""API tests against a seeded database."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from atlas import db
from atlas.api.main import app, get_conn
from atlas.connectors import GeoBoundariesConnector
from atlas.pipeline import aggregate, places, standardise
from atlas.storage import content_hash


@pytest.fixture
def client(conn, storage, fixtures):
    kenya = places.upsert_country(conn, "KEN", "Kenya")
    rows = GeoBoundariesConnector.to_places(fixtures["geoboundaries"], kenya, "county")
    places.upsert_subnational(conn, rows)
    county_ids = [r["place_id"] for r in db.query(
        conn, "SELECT place_id FROM place WHERE level='adm1' ORDER BY place_id")]

    wb_key, wb_body = storage.put_json("worldbank", fixtures["worldbank"])
    db.record_raw_object(
        conn, wb_key, "worldbank",
        {"url": "https://api.worldbank.org/v2/country/KEN/indicator/NY.GDP.PCAP.CD"},
        "2026-11-16T00:00:00Z", content_hash(wb_body), 200, "application/json", len(wb_body))

    om_payload = {**fixtures["open_meteo"], "places": county_ids}
    om_key, om_body = storage.put_json("open-meteo", om_payload)
    db.record_raw_object(
        conn, om_key, "open-meteo", {"url": "https://archive-api.open-meteo.com/v1/archive"},
        "2026-11-16T00:00:00Z", content_hash(om_body), 200, "application/json", len(om_body))

    standardise.standardise_pending(conn, storage, "worldbank")
    standardise.standardise_pending(conn, storage, "open-meteo")
    aggregate.rollup(conn, "precip_total_mm", "year")

    app.dependency_overrides[get_conn] = lambda: conn
    yield TestClient(app)
    app.dependency_overrides.clear()


def test_places_endpoint(client):
    body = client.get("/places", params={"country": "KEN", "level": "adm1"}).json()
    assert body["count"] == 5
    assert {p["level_name"] for p in body["places"]} == {"county"}


def test_indicators_endpoint(client):
    body = client.get("/indicators").json()
    codes = {i["code"] for i in body["indicators"]}
    assert codes == {"gdp_per_capita_usd", "precip_total_mm"}
    gdp = next(i for i in body["indicators"] if i["code"] == "gdp_per_capita_usd")
    # The provider's own code stays visible so a researcher can go and
    # verify the definition at the source.
    assert gdp["source_code"] == "NY.GDP.PCAP.CD"
    assert gdp["agg_rule"] == "last"


def test_observations_carry_attribution(client):
    """CC BY 4.0 requires crediting the source. With an API returning
    arbitrary slices, a page footer does not discharge that."""
    body = client.get("/observations", params={"indicator": "gdp_per_capita_usd"}).json()
    assert body["count"] == 10
    attributions = {a["source_id"]: a for a in body["attribution"]}
    assert "worldbank" in attributions
    assert attributions["worldbank"]["licence"] == "CC-BY-4.0"
    assert attributions["worldbank"]["attribution_text"]


def test_attribution_covers_only_the_sources_used(client):
    body = client.get("/observations", params={"indicator": "precip_total_mm",
                                               "grain": "year"}).json()
    assert {a["source_id"] for a in body["attribution"]} == {"open-meteo"}


def test_provenance_is_available_per_value(client):
    body = client.get("/observations", params={"indicator": "gdp_per_capita_usd",
                                               "provenance": True}).json()
    with_value = [o for o in body["observations"] if o["value"] is not None]
    assert with_value[0]["source_url"].startswith("https://api.worldbank.org")
    assert with_value[0]["fetched_at"]
    assert len(with_value[0]["content_hash"]) == 64


def test_missing_values_are_returned_as_missing_not_zero(client):
    body = client.get("/observations", params={"indicator": "gdp_per_capita_usd"}).json()
    gaps = [o for o in body["observations"] if o["value"] is None]
    assert len(gaps) == 1
    assert gaps[0]["missing_reason"] == "not_collected"


def test_api_refuses_a_finer_grain_than_the_data_has(client):
    response = client.get("/observations", params={"indicator": "gdp_per_capita_usd",
                                                   "grain": "day"})
    assert response.status_code == 422
    assert "Not available at that grain" in response.json()["detail"]


def test_unknown_indicator_is_404(client):
    assert client.get("/observations", params={"indicator": "nope"}).status_code == 404


def test_status_endpoint_states_the_promise_and_the_sources(client):
    body = client.get("/status").json()
    assert "does not warrant" in body["promise"]
    sources = {s["source_id"]: s for s in body["sources"]}
    assert {"worldbank", "open-meteo", "geoboundaries"} <= set(sources)
    assert "freshness_status" in sources["worldbank"]


def test_coverage_endpoint_shows_the_gaps(client):
    body = client.get("/coverage").json()
    gdp = next(c for c in body["coverage"]
               if c["indicator"] == "gdp_per_capita_usd" and c["grain"] == "year")
    assert gdp["observations"] == 10
    assert gdp["missing"] == 1
