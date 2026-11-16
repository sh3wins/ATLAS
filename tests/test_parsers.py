"""Parser tests. No network, no database.

These run against recorded fixtures shaped like the real responses.
"""
from __future__ import annotations

import pytest

from atlas.acquisition import Artifact, ObservationRow
from atlas.connectors import OpenMeteoConnector, WorldBankConnector
from atlas.connectors.base import Resolver

ARTIFACT = Artifact(key="source=test/year=2026/month=11/day=16/abc.json",
                    content_hash="x", byte_size=1)


@pytest.fixture
def resolver():
    return Resolver(
        places={
            ("worldbank", "KEN"): 1,
            # Open-Meteo's code for a place is its coordinate, resolved
            # through the crosswalk like any other source code.
            ("open-meteo", "-1.3000,36.8500"): 101,
            ("open-meteo", "-4.0250,39.6500"): 102,
            ("open-meteo", "-0.1250,34.7500"): 103,
            ("open-meteo", "-0.4500,36.0000"): 104,
            ("open-meteo", "3.2500,35.1500"): 105,
        },
        indicators={
            ("worldbank", "NY.GDP.PCAP.CD"): {
                "indicator_id": 10, "code": "gdp_per_capita_usd",
                "unit": "USD", "agg_rule": "last", "native_grain": "year"},
            ("open-meteo", "precipitation_sum"): {
                "indicator_id": 20, "code": "precip_total_mm",
                "unit": "mm", "agg_rule": "sum", "native_grain": "day"},
        },
    )


# --- World Bank ---------------------------------------------------------

def test_worldbank_parses_years(fixtures, resolver, storage):
    rows = WorldBankConnector(storage).parse(fixtures["worldbank"], ARTIFACT, resolver)
    assert len(rows) == 10
    assert {r.grain for r in rows} == {"year"}
    assert all(r.place_id == 1 and r.indicator_id == 10 for r in rows)

    y2023 = next(r for r in rows if r.period_start == "2023-01-01")
    assert y2023.period_end == "2023-12-31"
    assert y2023.value is not None
    assert y2023.quality == "reported"
    assert y2023.raw_ref == ARTIFACT.key


def test_worldbank_null_year_becomes_missing_reason_not_zero(fixtures, resolver, storage):
    """The newest year is routinely null. That is the source not having
    published -- recording it as 0.0 would drag every average computed."""
    rows = WorldBankConnector(storage).parse(fixtures["worldbank"], ARTIFACT, resolver)
    y2024 = next(r for r in rows if r.period_start == "2024-01-01")
    assert y2024.value is None
    assert y2024.missing_reason == "not_collected"


def test_worldbank_skips_unknown_country(fixtures, resolver, storage):
    """An unmapped code is skipped, never guessed into a place."""
    payload = [fixtures["worldbank"][0],
               [{**fixtures["worldbank"][1][0], "countryiso3code": "ZZZ",
                 "country": {"id": "ZZ", "value": "Nowhere"}}]]
    assert WorldBankConnector(storage).parse(payload, ARTIFACT, resolver) == []


def test_worldbank_tolerates_broken_shape(resolver, storage):
    assert WorldBankConnector(storage).parse({"error": "nope"}, ARTIFACT, resolver) == []


# --- Open-Meteo ---------------------------------------------------------

def test_open_meteo_parses_daily(fixtures, resolver, storage):
    rows = OpenMeteoConnector(storage).parse(fixtures["open_meteo"], ARTIFACT, resolver)
    assert len(rows) == 5 * 31
    assert {r.grain for r in rows} == {"day"}
    assert {r.place_id for r in rows} == {101, 102, 103, 104, 105}


def test_open_meteo_labels_era5_as_modelled(fixtures, resolver, storage):
    """ERA5 is reanalysis assimilating observations, not a gauge reading.
    Good data, and not a direct measurement."""
    rows = OpenMeteoConnector(storage).parse(fixtures["open_meteo"], ARTIFACT, resolver)
    assert {r.quality for r in rows} == {"modelled"}


def test_open_meteo_gap_is_not_zero(fixtures, resolver, storage):
    rows = OpenMeteoConnector(storage).parse(fixtures["open_meteo"], ARTIFACT, resolver)
    gaps = [r for r in rows if r.value is None]
    assert len(gaps) == 5                       # one per location
    assert {r.missing_reason for r in gaps} == {"not_collected"}
    assert all(r.period_start == "2024-01-04" for r in gaps)


def test_open_meteo_refuses_misaligned_response(fixtures, resolver, storage):
    """Responses come back in request order, which is the only thing tying
    a location to its place. Guessing an alignment would attribute one
    county's rainfall to another."""
    broken = {**fixtures["open_meteo"], "codes": ["-1.3000,36.8500", "-4.0250,39.6500"]}
    with pytest.raises(ValueError, match="refusing to guess"):
        OpenMeteoConnector(storage).parse(broken, ARTIFACT, resolver)


# --- the row contract ---------------------------------------------------

def test_row_requires_exactly_one_of_value_or_reason():
    base = dict(place_id=1, indicator_id=1, period_start="2024-01-01",
                period_end="2024-12-31", grain="year", source_id="worldbank")

    with pytest.raises(ValueError):
        ObservationRow(**base, value=1.0, missing_reason="suppressed")
    with pytest.raises(ValueError):
        ObservationRow(**base, value=None, missing_reason=None)

    assert ObservationRow(**base, value=0.0).value == 0.0       # a real zero is fine
    assert ObservationRow(**base, value=None, missing_reason="suppressed").value is None


# --- the surrogate-key hazard -------------------------------------------

def test_open_meteo_refuses_a_legacy_place_id_payload(fixtures, resolver, storage):
    """Artifacts landed before coordinates became the source code carry
    ATLAS place_ids. Those are GENERATED ALWAYS AS IDENTITY, so after a
    rebuild they may name different counties. Re-parsing one must fail
    loudly rather than attribute rainfall to the wrong place."""
    legacy = {"places": [101, 102, 103, 104, 105],
              "variable": "precipitation_sum",
              "locations": fixtures["open_meteo"]["locations"]}
    with pytest.raises(ValueError, match="legacy artifact"):
        OpenMeteoConnector(storage).parse(legacy, ARTIFACT, resolver)


def test_open_meteo_refuses_a_unit_it_did_not_expect(fixtures, resolver, storage):
    """The response states its own units. A provider default change, or a
    self-hosted endpoint answering in inches, is a silent 25.4x error."""
    payload = {**fixtures["open_meteo"],
               "locations": [{**loc, "daily_units": {"precipitation_sum": "inch"}}
                             for loc in fixtures["open_meteo"]["locations"]]}
    with pytest.raises(ValueError, match="refusing to store"):
        OpenMeteoConnector(storage).parse(payload, ARTIFACT, resolver)


def test_open_meteo_refuses_a_location_with_no_series(fixtures, resolver, storage):
    """An HTTP 200 whose body carries no series for one coordinate used to
    yield zero rows for that county, silently, and mark the artifact
    parsed -- losing its whole period permanently."""
    payload = {**fixtures["open_meteo"],
               "locations": [{"daily": {}}] + fixtures["open_meteo"]["locations"][1:]}
    with pytest.raises(ValueError, match="no precipitation_sum series"):
        OpenMeteoConnector(storage).parse(payload, ARTIFACT, resolver)


def test_worldbank_does_not_fall_back_to_the_iso2_code(resolver, storage):
    """country.id is ISO2 ("KE"); the crosswalk is seeded with ISO3. The
    old fallback produced a code that never resolves and dropped the row
    with no counter."""
    payload = [{"page": 1, "pages": 1},
               [{"indicator": {"id": "NY.GDP.PCAP.CD"},
                 "country": {"id": "KE", "value": "Kenya"},
                 "countryiso3code": "",
                 "date": "2023", "value": 1942.59}]]
    connector = WorldBankConnector(storage)
    assert connector.parse(payload, ARTIFACT, resolver) == []
    assert connector.unresolved == [
        {"iso3": None, "indicator": "NY.GDP.PCAP.CD", "period": "2023"}]


# --- timezone: East Africa is not one zone ------------------------------

def test_open_meteo_refuses_a_point_with_no_timezone(storage):
    """A wrong day boundary does not fail. It returns a complete,
    plausible series in which every daily total is cut at the wrong hour.
    Falling back to Kenya's zone would do exactly that for Rwanda, so a
    missing zone has to stop the run."""
    connector = OpenMeteoConnector(storage)
    with pytest.raises(ValueError, match="no timezone"):
        connector.fetch(points=[{"place_id": 1, "country_iso3": "RWA",
                                 "lat": -1.95, "lon": 30.06}],
                        start_date="2024-01-01", end_date="2024-01-02")


def test_points_in_two_zones_are_fetched_separately(storage):
    """Kenya is UTC+3 and Rwanda UTC+2, so they cannot share a request."""
    import httpx

    seen = []

    def handler(request):
        seen.append(dict(request.url.params))
        return httpx.Response(200, json={"daily": {"time": [],
                                                   "precipitation_sum": []}})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    connector = OpenMeteoConnector(storage, client=client)
    connector.fetch(
        points=[{"place_id": 1, "country_iso3": "KEN", "lat": -1.3,
                 "lon": 36.8, "timezone": "Africa/Nairobi"},
                {"place_id": 2, "country_iso3": "RWA", "lat": -1.95,
                 "lon": 30.06, "timezone": "Africa/Kigali"}],
        start_date="2024-01-01", end_date="2024-01-02")

    assert len(seen) == 2
    assert {s["timezone"] for s in seen} == {"Africa/Nairobi", "Africa/Kigali"}
    # Each request carries only the coordinates belonging to its zone.
    assert all("," not in s["latitude"] for s in seen)


def test_worldbank_reads_the_declared_publication_date(resolver, storage):
    """The metadata element says when the series was last revised. It is
    the only statement about the source's publishing that does not come
    from reading our own database back."""
    from datetime import date

    payload = [{"page": 1, "pages": 1, "lastupdated": "2026-11-11"},
               [{"indicator": {"id": "NY.GDP.PCAP.CD"},
                 "countryiso3code": "KEN", "date": "2023", "value": 1942.59}]]
    connector = WorldBankConnector(storage)
    connector.parse(payload, ARTIFACT, resolver)
    assert connector.source_publication == date(2026, 11, 11)


def test_a_missing_or_broken_lastupdated_declares_nothing(resolver, storage):
    """Silence is not a date. Guessing one here would reintroduce exactly
    the circularity this replaced."""
    for meta in ({"page": 1}, {"page": 1, "lastupdated": ""},
                 {"page": 1, "lastupdated": "not-a-date"}):
        connector = WorldBankConnector(storage)
        connector.parse([meta, []], ARTIFACT, resolver)
        assert connector.source_publication is None


# --- recorded live responses --------------------------------------------
#
# Everything above runs against fixtures written from the documentation.
# These run against responses recorded from the real APIs by
# scripts/record_fixtures.py, and both cases below are shapes no
# hand-written fixture had.

import json  # noqa: E402
from pathlib import Path  # noqa: E402

LIVE = Path(__file__).parent / "fixtures" / "live"


def live(name):
    return json.loads((LIVE / f"{name}.json").read_text(encoding="utf-8"))


def test_worldbank_error_object_is_not_an_empty_result(resolver, storage):
    """The API answers 200 for an unknown country. Silently returning no
    rows would make a typo look like a source with nothing to report."""
    connector = WorldBankConnector(storage)
    with pytest.raises(ValueError, match="error object"):
        connector.parse(live("worldbank_unknown_country"), ARTIFACT, resolver)
    with pytest.raises(ValueError, match="error object"):
        connector.parse(live("worldbank_unknown_indicator"), ARTIFACT, resolver)


def test_real_open_meteo_units_match_the_registry(resolver, storage):
    """The F3 unit check was written against our own fixture. This is the
    live response agreeing with it."""
    payload = {"codes": ["-1.2921,36.8219", "-4.0435,39.6682"],
               "variable": "precipitation_sum",
               "locations": live("open_meteo_nairobi")}
    rows = OpenMeteoConnector(storage).parse(payload, ARTIFACT, resolver)
    assert rows == []          # these coordinates are not in the crosswalk
    assert live("open_meteo_nairobi")[0]["daily_units"]["precipitation_sum"] == "mm"
    assert live("open_meteo_nairobi")[0]["utc_offset_seconds"] == 10800
