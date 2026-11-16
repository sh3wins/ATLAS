#!/usr/bin/env python
"""Record real upstream responses as regression fixtures.

Every fixture in tests/fixtures was written from the API documentation.
That makes the parser tests a check that the parsers match our reading of
the docs, which is not the same as a check that they match the APIs.

This records what the sources actually return, with the URL, the time and
a SHA-256 beside each file, so a later shape change shows up as a failing
test instead of a silent gap. It records the awkward cases too: a response
with nulls, an unknown country, and a second timezone.

    python scripts/record_fixtures.py

Writes tests/fixtures/live/. Nothing here asserts anything -- recording is
deliberately separate from testing, so a bad recording cannot quietly
become the expected answer.
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx  # noqa: E402

from atlas.config import settings  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "live"

WB = "https://api.worldbank.org/v2"
GB = "https://www.geoboundaries.org/api/current/gbOpen"

CASES = [
    # name, url, params, what this one is for
    ("worldbank_gdp_ken", f"{WB}/country/KEN/indicator/NY.GDP.PCAP.CD",
     {"format": "json", "per_page": 100},
     "the ordinary case, and the newest year is routinely null"),

    ("worldbank_unknown_country", f"{WB}/country/ZZZ/indicator/NY.GDP.PCAP.CD",
     {"format": "json", "per_page": 5},
     "what an unknown code returns -- an error object, not an HTTP error"),

    ("worldbank_unknown_indicator", f"{WB}/country/KEN/indicator/NOT.AN.INDICATOR",
     {"format": "json", "per_page": 5},
     "an indicator that does not exist"),

    ("open_meteo_nairobi", settings.open_meteo_base,
     {"latitude": "-1.2921,-4.0435", "longitude": "36.8219,39.6682",
      "start_date": "2024-01-01", "end_date": "2024-01-05",
      "daily": "precipitation_sum", "timezone": "Africa/Nairobi"},
     "two coordinates in one request, Kenya's day boundary"),

    ("open_meteo_kigali", settings.open_meteo_base,
     {"latitude": "-1.9441", "longitude": "30.0619",
      "start_date": "2024-01-01", "end_date": "2024-01-05",
      "daily": "precipitation_sum", "timezone": "Africa/Kigali"},
     "a single coordinate, and the other East African timezone"),

    ("open_meteo_future_dates", settings.open_meteo_base,
     {"latitude": "-1.2921", "longitude": "36.8219",
      "start_date": "2099-01-01", "end_date": "2099-01-05",
      "daily": "precipitation_sum", "timezone": "Africa/Nairobi"},
     "a window the archive cannot have -- the empty/error shape"),

    ("geoboundaries_ken_adm1", f"{GB}/KEN/ADM1/", {},
     "metadata only; the geometry download is fetched separately"),
]


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    manifest = []
    failures = 0

    with httpx.Client(timeout=settings.http_timeout_seconds,
                      follow_redirects=True) as client:
        for name, url, params, why in CASES:
            print(f"{name} ... ", end="", flush=True)
            try:
                response = client.get(url, params=params)
            except Exception as exc:  # noqa: BLE001
                print(f"FAILED: {exc}")
                failures += 1
                manifest.append({"name": name, "url": url, "params": params,
                                 "why": why, "error": str(exc)})
                continue

            body = response.content
            path = OUT / f"{name}.json"
            try:
                # Pretty-print when it is JSON, so a diff is readable.
                path.write_text(
                    json.dumps(response.json(), indent=1, ensure_ascii=False),
                    encoding="utf-8")
                parsed = True
            except ValueError:
                path.write_bytes(body)
                parsed = False

            manifest.append({
                "name": name,
                "url": str(response.request.url),
                "why": why,
                "status": response.status_code,
                "content_type": response.headers.get("content-type"),
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                # Hash of the bytes as they arrived, which is the thing
                # storage.content_hash does not currently do.
                "sha256_of_response_bytes": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
                "json": parsed,
            })
            print(f"{response.status_code}, {len(body)} bytes")

    (OUT / "manifest.json").write_text(
        json.dumps(manifest, indent=1), encoding="utf-8")

    print(f"\nwrote {len(manifest)} entries to {OUT}")
    if failures:
        print(f"{failures} case(s) could not be fetched", file=sys.stderr)
    print("These are recordings, not assertions. Read them, then write the "
          "tests that pin the shapes you care about.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
