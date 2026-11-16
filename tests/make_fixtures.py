"""Generate recorded API fixtures.

Shapes match the real responses:

  World Bank  two-element array -- metadata, then records carrying
              countryiso3code, date and value, with the newest year null
              (which is normal, and must become a missing_reason rather
              than a zero).

  Open-Meteo  the wrapper the connector lands: the place_ids it asked
              for, the variable, and one location object per point, each
              with daily.time and daily.<variable> arrays.

  geoBoundaries  metadata plus a GeoJSON FeatureCollection. Five Kenyan
              counties with approximate bounding boxes -- rough, but real
              enough that a point-in-polygon test means something.
"""
from __future__ import annotations

import json
import random
from pathlib import Path

HERE = Path(__file__).parent / "fixtures"
HERE.mkdir(parents=True, exist_ok=True)

COUNTIES = [
    ("KEN-ADM1-001", "Nairobi", 36.65, 37.05, -1.44, -1.16),
    ("KEN-ADM1-002", "Mombasa", 39.55, 39.75, -4.10, -3.95),
    ("KEN-ADM1-003", "Kisumu", 34.45, 35.05, -0.35, 0.10),
    ("KEN-ADM1-004", "Nakuru", 35.50, 36.50, -1.20, 0.30),
    ("KEN-ADM1-005", "Turkana", 34.30, 36.00, 1.50, 5.00),
]


def box(w, e, s, n):
    return {"type": "Polygon",
            "coordinates": [[[w, s], [e, s], [e, n], [w, n], [w, s]]]}


def world_bank():
    rows, value = [], 1100.0
    for year in range(2015, 2025):
        # The newest year is routinely null in the real API.
        v = None if year >= 2024 else round(value, 1)
        rows.append({
            "indicator": {"id": "NY.GDP.PCAP.CD", "value": "GDP per capita (current US$)"},
            "country": {"id": "KE", "value": "Kenya"},
            "countryiso3code": "KEN",
            "date": str(year),
            "value": v,
            "unit": "",
            "obs_status": "",
            "decimal": 1,
        })
        value *= 1.06
    rows.reverse()  # the API returns newest first
    meta = {"page": 1, "pages": 1, "per_page": 500, "total": len(rows),
            "sourceid": "2", "lastupdated": "2026-10-01"}
    return [meta, rows]


def open_meteo(codes):
    random.seed(7)
    dates = [f"2024-01-{d:02d}" for d in range(1, 32)]
    locations = []
    for i, _ in enumerate(codes):
        values = [round(random.uniform(0, 18), 1) for _ in dates]
        values[3] = None  # a genuine gap, must not become a zero
        locations.append({
            "latitude": -1.29 + i, "longitude": 36.82 + i,
            "generationtime_ms": 0.21, "utc_offset_seconds": 10800,
            "timezone": "Africa/Nairobi", "timezone_abbreviation": "EAT",
            "elevation": 1693.0,
            "daily_units": {"time": "iso8601", "precipitation_sum": "mm"},
            "daily": {"time": dates, "precipitation_sum": values},
        })
    return {"codes": list(codes), "variable": "precipitation_sum",
            "timezone": "Africa/Nairobi", "locations": locations}


def geoboundaries():
    features = [
        {"type": "Feature",
         "properties": {"shapeName": name, "shapeID": sid,
                        "shapeGroup": "KEN", "shapeType": "ADM1"},
         "geometry": box(w, e, s, n)}
        for sid, name, w, e, s, n in COUNTIES
    ]
    return {
        "iso3": "KEN", "level": "adm1",
        "metadata": {"boundaryISO": "KEN", "boundaryType": "ADM1",
                     "boundaryLicense": "CC BY 4.0",
                     "gjDownloadURL": "https://example.invalid/KEN_ADM1.geojson"},
        "geojson": {"type": "FeatureCollection", "features": features},
    }


def main():
    (HERE / "worldbank_gdp_ken.json").write_text(json.dumps(world_bank(), indent=1))
    (HERE / "open_meteo_ken_jan2024.json").write_text(
        json.dumps(open_meteo(
            [f"{(s_ + n) / 2:.4f},{(w + e) / 2:.4f}"
             for _, _, w, e, s_, n in COUNTIES]), indent=1))
    (HERE / "geoboundaries_ken_adm1.json").write_text(json.dumps(geoboundaries(), indent=1))
    for p in sorted(HERE.glob("*.json")):
        print(f"{p.name:34} {p.stat().st_size:>7,} bytes")


if __name__ == "__main__":
    main()
