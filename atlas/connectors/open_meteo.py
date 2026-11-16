"""Open-Meteo historical weather connector (ERA5 reanalysis).

Identity vocabulary: WGS84 coordinates.
Time vocabulary:     daily values in a named timezone.

Two things this connector has to get right that the World Bank one
doesn't:

1. Coordinates, not codes. The response carries no identifier, so the
   coordinate the request was issued for IS the source's code for that
   place, crosswalked like any other. Never the ATLAS place_id, which is
   assigned by the database and renumbers on a rebuild: see coord_code.

2. A local calendar. Daily values are requested with
   timezone=Africa/Nairobi, so a "day" means a Kenyan day. Converting
   those boundaries to UTC would shift them three hours and silently move
   observations between days.

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
              variable: str = "precipitation_sum", timezone: str = "Africa/Nairobi",
              **_: Any) -> AcquisitionResult:
        """Fetch daily values for a set of points.

        `points` is [{"place_id": int, "lat": float, "lon": float}, ...].
        Batched; each batch is an independently retryable chunk, so a
        timeout costs one batch rather than the whole backfill.
        """
        result = AcquisitionResult(source_id=self.spec.source_id)

        for batch in _chunks(points, MAX_COORDS_PER_REQUEST):
            params = {
                "latitude": ",".join(str(p["lat"]) for p in batch),
                "longitude": ",".join(str(p["lon"]) for p in batch),
                "start_date": start_date,
                "end_date": end_date,
                "daily": variable,
                "timezone": timezone,
            }
            if settings.open_meteo_api_key:
                params["apikey"] = settings.open_meteo_api_key

            # Coordinates ride along as the source's own codes. Resolving
            # them to places happens at parse time, through the crosswalk.
            codes = [coord_code(p["lat"], p["lon"]) for p in batch]
            chunk = {
                "url": settings.open_meteo_base,
                "params": {k: v for k, v in params.items() if k != "apikey"},
                "codes": codes,
                "variable": variable,
                "timezone": timezone,
            }

            try:
                response = self.client.get(settings.open_meteo_base, params=params)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:  # noqa: BLE001
                result.fail_chunk(chunk, str(exc))
                continue

            # A single coordinate returns an object; several return a
            # list. Normalising here means parse() only ever sees a list.
            if isinstance(payload, dict):
                payload = [payload]

            self._land(result, {"codes": codes, "variable": variable,
                                "timezone": timezone, "locations": payload},
                       chunk, response.status_code)

        return result

    def parse(self, payload: Any, artifact: Artifact,
              resolver: Resolver) -> list[ObservationRow]:
        rows: list[ObservationRow] = []
        self.unresolved: list[str] = []
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
