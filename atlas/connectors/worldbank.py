"""World Bank Open Data connector.

Identity vocabulary: ISO3 country codes.
Time vocabulary:     calendar years, as a bare "2023" string.

The API returns a two-element array: metadata first, then the records.
Pagination lives in the metadata, so acquisition pages until `page`
reaches `pages`, landing each page as its own artifact.
"""
from __future__ import annotations

from typing import Any

from ..acquisition import AcquisitionResult, Artifact, ObservationRow
from ..config import settings
from .base import Connector, ConnectorSpec, Resolver

PER_PAGE = 500


class WorldBankConnector(Connector):
    spec = ConnectorSpec(
        source_id="worldbank",
        name="World Bank Open Data",
        indicators={"NY.GDP.PCAP.CD": "year"},
        expected_frequency="annual",
        expected_lag_days=365,
        licence="CC-BY-4.0",
        attribution_text="World Bank Open Data (CC BY 4.0)",
    )

    def fetch(self, countries: list[str], indicator: str,
              start_year: int = 1960, end_year: int | None = None,
              **_: Any) -> AcquisitionResult:
        """One call per indicator covering all countries at once.

        The API accepts semicolon-separated country codes, so five
        countries cost one request rather than five.
        """
        result = AcquisitionResult(source_id=self.spec.source_id)
        code_list = ";".join(countries)
        url = f"{settings.worldbank_base}/country/{code_list}/indicator/{indicator}"

        date_range = f"{start_year}:{end_year}" if end_year else None
        page = 1
        while True:
            params = {"format": "json", "per_page": PER_PAGE, "page": page}
            if date_range:
                params["date"] = date_range

            chunk = {"url": url, "params": dict(params)}
            try:
                response = self.client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
            except Exception as exc:  # noqa: BLE001 - recorded, not swallowed
                result.fail_chunk(chunk, str(exc))
                break

            # A two-element array is the contract. Anything else means the
            # API changed shape, which is a failure to record rather than
            # guess around.
            if not isinstance(payload, list) or len(payload) < 2:
                result.fail_chunk(chunk, f"unexpected response shape: {type(payload).__name__}")
                break

            self._land(result, payload, chunk, response.status_code)

            meta = payload[0] or {}
            total_pages = int(meta.get("pages") or 1)
            if page >= total_pages:
                break
            page += 1

        return result

    def parse(self, payload: Any, artifact: Artifact,
              resolver: Resolver) -> list[ObservationRow]:
        rows: list[ObservationRow] = []
        if not isinstance(payload, list) or len(payload) < 2:
            return rows

        records = payload[1] or []
        for record in records:
            iso3 = record.get("countryiso3code") or (record.get("country") or {}).get("id")
            indicator_code = (record.get("indicator") or {}).get("id")
            year = record.get("date")
            if not (iso3 and indicator_code and year):
                continue

            place_id = resolver.place(self.spec.source_id, iso3)
            indicator = resolver.indicator(self.spec.source_id, indicator_code)
            # An unknown code is skipped, never guessed into a place.
            if place_id is None or indicator is None:
                continue

            raw_value = record.get("value")
            # The newest year is routinely null. That is the source not
            # having published, not a zero -- so it is recorded as a
            # genuine absence with a reason, which the fact table requires.
            if raw_value is None:
                value, missing_reason = None, "not_collected"
            else:
                value, missing_reason = float(raw_value), None

            rows.append(
                ObservationRow(
                    place_id=place_id,
                    indicator_id=indicator["indicator_id"],
                    period_start=f"{year}-01-01",
                    period_end=f"{year}-12-31",
                    grain="year",
                    value=value,
                    missing_reason=missing_reason,
                    source_id=self.spec.source_id,
                    raw_ref=artifact.key,
                    quality="reported",
                )
            )
        return rows
