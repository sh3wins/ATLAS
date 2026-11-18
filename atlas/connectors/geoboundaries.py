"""geoBoundaries loader.

Not an observation connector: it populates `place` rather than the fact
table, so it implements fetch() and a loader instead of parse().

Uses the gbOpen release deliberately. geoBoundaries also ships
gbHumanitarian, mirrored from UN SALB, which is documented as not for
commercial use -- easy to grab by accident and not notice for a year.
"""
from __future__ import annotations

import json
from typing import Any

from ..acquisition import AcquisitionResult, Artifact, ObservationRow
from ..config import settings
from .base import Connector, ConnectorSpec, Resolver

# ATLAS's generic levels, mapped to geoBoundaries' names. Nothing here
# says "county": Kenya's adm1 is a county, Uganda's a district,
# Tanzania's a region, and the table holds all three the same way.
LEVEL_MAP = {"country": "ADM0", "adm1": "ADM1", "adm2": "ADM2", "adm3": "ADM3"}


class GeoBoundariesConnector(Connector):
    spec = ConnectorSpec(
        source_id="geoboundaries",
        name="geoBoundaries (gbOpen)",
        expected_frequency="irregular",
        licence="CC-BY-4.0",
        attribution_text=(
            "Administrative boundaries from geoBoundaries (gbOpen), "
            "William & Mary geoLab"
        ),
    )

    def fetch(self, iso3: str, level: str = "adm1", **_: Any) -> AcquisitionResult:
        """Two hops: metadata, then the GeoJSON it points at."""
        result = AcquisitionResult(source_id=self.spec.source_id)
        gb_level = LEVEL_MAP[level]
        meta_url = f"{settings.geoboundaries_base}/{iso3}/{gb_level}/"
        chunk = {"url": meta_url, "iso3": iso3, "level": level}

        try:
            meta_response = self.get(meta_url)
            meta_response.raise_for_status()
            meta = meta_response.json()
            if isinstance(meta, list):
                meta = meta[0]

            download_url = meta.get("gjDownloadURL")
            if not download_url:
                result.fail_chunk(chunk, "metadata carried no gjDownloadURL")
                return result

            geo_response = self.get(download_url)
            geo_response.raise_for_status()
            geojson = geo_response.json()
        except Exception as exc:  # noqa: BLE001
            result.fail_chunk(chunk, str(exc))
            return result

        self._land(
            result,
            {"iso3": iso3, "level": level, "metadata": meta, "geojson": geojson},
            {**chunk, "geojson_url": download_url},
            geo_response.status_code,
        )
        return result

    def parse(self, payload: Any, artifact: Artifact,
              resolver: Resolver) -> list[ObservationRow]:
        """Boundaries are reference data, not observations."""
        return []

    # -- loading ----------------------------------------------------------

    @staticmethod
    def to_places(payload: dict, country_place_id: int | None,
                  level_name: str) -> list[dict]:
        """Turn a landed GeoJSON payload into place rows.

        Returns dicts rather than writing, so this is testable without a
        database and the caller controls the transaction.
        """
        iso3 = payload["iso3"]
        level = payload["level"]
        features = (payload.get("geojson") or {}).get("features") or []

        places = []
        for feature in features:
            props = feature.get("properties") or {}
            name = props.get("shapeName") or props.get("shapeISO") or props.get("shapeID")
            if not name:
                continue
            places.append(
                {
                    "name": name,
                    "level": level,
                    "level_name": level_name,
                    "country_iso3": iso3,
                    "parent_id": country_place_id,
                    # geoBoundaries' own ID becomes a crosswalk entry, never
                    # a primary key: if they retire it, nothing breaks.
                    "source_code": props.get("shapeID") or name,
                    "geojson": json.dumps(feature.get("geometry")),
                }
            )
        return places
