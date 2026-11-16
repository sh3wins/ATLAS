"""Open-Meteo historical weather connector (ERA5 reanalysis).

Identity vocabulary: WGS84 coordinates.
Time vocabulary:     daily values in a named timezone.

Two things this connector has to get right that the World Bank one
doesn't:

1. Coordinates, not codes. The response carries no identifier, so the
   coordinate the request was issued for IS the source's code for that
   place, crosswalked like any other. Never the ATLAS place_id, which is
   assigned by the database and renumbers on a rebuild: see coord_code.

2. A local calendar. Daily values are requested in a named zone, so a
   "day" means a local day. The zone comes from the point's country
   (country_config.timezone), because East Africa is not one zone:
   Kenya, Uganda and Tanzania are UTC+3, Rwanda and Burundi UTC+2.
   Converting those boundaries to UTC, or applying one country's zone to
   another, moves observations between days without any error.

Values are labelled `modelled`: ERA5 is a reanalysis that assimilates
observations, not a rain gauge reading. Good data, widely used, and not a
direct measurement.
"""
from __future__ import annotations

from typing import Any

from ..acquisition import AcquisitionResult, Artifact, ObservationRow
from ..config import settings
from .base import Connector, ConnectorSpec, Resolver

# Open-Meteo accepts comma-separated coordinates. Batching matters more
# than the daily call ceiling: one request for 47 counties beats 47.
MAX_COORDS_PER_REQUEST = 50


def coord_code(lat: float, lon: float) -> str:
    """Open-Meteo's identifier for a place: its coordinate.

    The response carries no place code, so something has to tie a location
    back to a place. It must NOT be an ATLAS place_id: those are
    `GENERATED ALWAYS AS IDENTITY`, so rebuilding the serving database
    renumbers them and re-parsing old raw files would write one county's
    rainfall against another -- a plausible value, right unit, wrong place,
    no error. The coordinate is the source's own handle and survives any
    rebuild, so it goes in the crosswalk like every other source code.
    """
    return f"{float(lat):.4f},{float(lon):.4f}"


class OpenMeteoConnector(Connector):
    spec = ConnectorSpec(
        source_id="open-meteo",
        name="Open-Meteo Historical Weather API (ERA5)",
        indicators={"precipitation_sum": "day"},
        expected_frequency="daily",
        expected_lag_days=5,
        licence="CC-BY-4.0",
        attribution_text=(
            "Weather data by Open-Meteo.com, generated using Copernicus "
            "Climate Change Service information"
        ),
    )

    def fetch(self, points: list[dict], start_date: str, end_date: str,
              variable: str = "precipitation_sum", timezone: str | None = None,
              **_: Any) -> AcquisitionResult:
        """Fetch daily values for a set of points.

        `points` is [{"place_id": int, "country_iso3": str, "lat": float,
        "lon": float, "timezone": str}, ...].

        The zone comes from each point's own country, not from this
        connector. A daily total is cut at local midnight, so asking for
        Kigali in Africa/Nairobi does not fail: it returns a complete,
        plausible series in which every day is bounded an hour early and
        every total is misdated. Points are therefore grouped by zone
        before batching, and a point with no zone stops the run.

        Each batch is an independently retryable chunk, so a timeout
        costs one batch rather than the whole backfill.
        """
        result = AcquisitionResult(source_id=self.spec.source_id)

        by_zone: dict[str, list[dict]] = {}
        for point in points:
            zone = point.get("timezone") or timezone
            if not zone:
                raise ValueError(
                    f"{self.spec.source_id}: no timezone for place "
                    f"{point.get('place_id')} in "
                    f"{point.get('country_iso3') or 'an unknown country'}. "
                    "Defaulting would misdate every value rather than lose "
                    "it, so this is refused."
                )
            by_zone.setdefault(zone, []).append(point)

        for zone, zone_points in sorted(by_zone.items()):
            for batch in _chunks(zone_points, MAX_COORDS_PER_REQUEST):
                params = {
                    "latitude": ",".join(str(p["lat"]) for p in batch),
                    "longitude": ",".join(str(p["lon"]) for p in batch),
                    "start_date": start_date,
                    "end_date": end_date,
                    "daily": variable,
                    "timezone": zone,
                }
                if settings.open_meteo_api_key:
                    params["apikey"] = settings.open_meteo_api_key

                # Coordinates ride along as the source's own codes.
                # Resolving them to places happens at parse time, through
                # the crosswalk.
                codes = [coord_code(p["lat"], p["lon"]) for p in batch]
                chunk = {
                    "url": settings.open_meteo_base,
                    "params": {k: v for k, v in params.items() if k != "apikey"},
                    "codes": codes,
                    "variable": variable,
                    "timezone": zone,
                }

                try:
                    response = self.client.get(settings.open_meteo_base,
                                               params=params)
                    response.raise_for_status()
                    payload = response.json()
                except Exception as exc:  # noqa: BLE001
                    # A rejected request answers with {"error": true,
                    # "reason": "..."}. "Invalid date" is worth keeping;
                    # "400 Bad Request" sends someone reading logs.
                    detail = str(exc)
                    failed = getattr(exc, "response", None)
                    if failed is not None:
                        try:
                            body = failed.json()
                        except Exception:  # noqa: BLE001
                            body = None
                        if isinstance(body, dict) and body.get("reason"):
                            detail = f"{detail}: {body['reason']}"
                    result.fail_chunk(chunk, detail)
                    continue

                # A single coordinate returns an object; several return a
                # list. Normalising here means parse() only sees a list.
                if isinstance(payload, dict):
                    payload = [payload]

                self._land(result, {"codes": codes, "variable": variable,
                                    "timezone": zone, "locations": payload},
                           chunk, response.status_code)

        return result

    def parse(self, payload: Any, artifact: Artifact,
              resolver: Resolver) -> list[ObservationRow]:
        rows: list[ObservationRow] = []
        self.unresolved: list[str] = []
        # The archive response carries no publication date, so there is
        # nothing to declare. Freshness falls back to what ATLAS holds and
        # labels it `observed` rather than passing it off as the source's.
        self.source_publication = None
        if not isinstance(payload, dict):
            return rows

        variable = payload.get("variable") or "precipitation_sum"
        locations = payload.get("locations") or []
        codes = payload.get("codes")

        if codes is None:
            if "places" in payload:
                # Landed before coordinates became the source code. Its
                # place_ids refer to a numbering that may no longer hold,
                # and guessing would silently misattribute counties.
                raise ValueError(
                    f"{artifact.key}: legacy artifact keyed on ATLAS place_id; "
                    "re-fetch it rather than re-parsing"
                )
            return rows

        indicator = resolver.indicator(self.spec.source_id, variable)
        if indicator is None:
            return rows

        # Responses come back in request order, which is the only thing
        # tying a location to its coordinate. A length mismatch means that
        # assumption broke, and inventing an alignment would attribute one
        # county's rainfall to another.
        if len(codes) != len(locations):
            raise ValueError(
                f"{self.spec.source_id}: {len(codes)} coordinates requested but "
                f"{len(locations)} returned -- refusing to guess the alignment"
            )

        for code, location in zip(codes, locations):
            place_id = resolver.place(self.spec.source_id, code)
            if place_id is None:
                self.unresolved.append(code)
                continue

            # The response states its own units. Not checking them is how a
            # provider default change, or a self-hosted endpoint answering
            # in inches, becomes a silent 25.4x error in a platform whose
            # stated purpose is unit harmonisation.
            declared = (location.get("daily_units") or {}).get(variable)
            expected = indicator.get("unit")
            if declared and expected and declared != expected:
                raise ValueError(
                    f"{self.spec.source_id}: {variable} returned in {declared!r} "
                    f"but the registry declares {expected!r} -- refusing to store"
                )

            daily = location.get("daily") or {}
            dates = daily.get("time")
            values = daily.get(variable)
            if not dates or values is None:
                # An HTTP 200 whose body carries no series for this point.
                # Skipping it loses that place's whole period permanently
                # and marks the artifact parsed, so fail loudly instead.
                raise ValueError(
                    f"{self.spec.source_id}: no {variable} series for {code} "
                    "in an otherwise successful response"
                )
            if len(dates) != len(values):
                raise ValueError(
                    f"{self.spec.source_id}: {len(dates)} dates but "
                    f"{len(values)} values for {code}"
                )

            for date, raw_value in zip(dates, values):
                if raw_value is None:
                    value, missing_reason = None, "not_collected"
                else:
                    value, missing_reason = float(raw_value), None

                rows.append(
                    ObservationRow(
                        place_id=place_id,
                        indicator_id=indicator["indicator_id"],
                        period_start=date,
                        period_end=date,
                        grain="day",
                        value=value,
                        missing_reason=missing_reason,
                        source_id=self.spec.source_id,
                        raw_ref=artifact.key,
                        quality="modelled",
                    )
                )
        return rows


def _chunks(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]
