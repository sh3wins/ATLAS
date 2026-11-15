"""Open-Meteo historical weather connector (ERA5 reanalysis).

Identity vocabulary: WGS84 coordinates.
Time vocabulary:     daily values in a named timezone.

Two things this connector has to get right that the World Bank one
doesn't:

1. Coordinates, not codes. Each request is tied to a place by the
   centroid it was issued for, so the chunk carries the place_id and the
   parse step reads it back. There is no code in the response to look up.

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

            # place_ids ride along in the chunk: the response has no code
            # to look up, so the mapping has to be carried, not inferred.
            chunk = {
                "url": settings.open_meteo_base,
                "params": {k: v for k, v in params.items() if k != "apikey"},
                "place_ids": [p["place_id"] for p in batch],
                "variable": variable,
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

            self._land(result, {"places": chunk["place_ids"], "variable": variable,
                                "locations": payload},
                       chunk, response.status_code)

        return result

    def parse(self, payload: Any, artifact: Artifact,
              resolver: Resolver) -> list[ObservationRow]:
        rows: list[ObservationRow] = []
        if not isinstance(payload, dict):
            return rows

        place_ids = payload.get("places") or []
        variable = payload.get("variable") or "precipitation_sum"
        locations = payload.get("locations") or []

        indicator = resolver.indicator(self.spec.source_id, variable)
        if indicator is None:
            return rows

        # Responses come back in request order, which is the only thing
        # tying a location to its place. A length mismatch means that
        # assumption broke, and inventing an alignment would silently
        # attribute one county's rainfall to another.
        if len(place_ids) != len(locations):
            raise ValueError(
                f"{self.spec.source_id}: {len(place_ids)} places requested but "
                f"{len(locations)} returned -- refusing to guess the alignment"
            )

        for place_id, location in zip(place_ids, locations):
            daily = location.get("daily") or {}
            dates = daily.get("time") or []
            values = daily.get(variable) or []
            if len(dates) != len(values):
                raise ValueError(
                    f"{self.spec.source_id}: {len(dates)} dates but {len(values)} values"
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
