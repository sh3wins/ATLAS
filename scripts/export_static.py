#!/usr/bin/env python
"""Export the API to static JSON for GitHub Pages.

GitHub Pages serves files, not programs, so this writes out what the API
would answer and a page that reads those files. It is a SNAPSHOT: the
numbers are correct as of the moment it runs and do not update
themselves. The page says so, because a frozen copy of a platform whose
selling point is freshness has to be honest about being frozen.

Responses come from the real application through FastAPI's test client
rather than from queries written again here. A second copy of the query
logic would drift from the first, which is the mistake F21 was about.

    python scripts/export_static.py

Writes docs/data/. Commit docs/ and point GitHub Pages at it.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fastapi.testclient import TestClient  # noqa: E402

from atlas.api.main import app  # noqa: E402

OUT = ROOT / "docs" / "data"


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    client = TestClient(app)
    written: list[dict] = []

    def dump(path: str, name: str) -> dict | None:
        response = client.get(path)
        if response.status_code != 200:
            print(f"  {name}: HTTP {response.status_code} -- skipped",
                  file=sys.stderr)
            return None
        body = response.json()
        target = OUT / f"{name}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(body, indent=1, default=str),
                          encoding="utf-8")
        size = target.stat().st_size
        written.append({"name": name, "path": path, "bytes": size})
        print(f"  {name:38} {size:>8,} bytes")
        return body

    print("exporting:")
    dump("/status", "status")
    dump("/coverage", "coverage")
    dump("/places?country=KEN", "places")
    indicators = dump("/indicators", "indicators")

    # One file per indicator per grain it can honestly answer. The API
    # refuses a grain finer than the source publishes, so a 422 here is
    # the system working and the file is simply not written.
    grains = ["day", "month", "quarter", "year"]
    for indicator in (indicators or {}).get("indicators", []):
        code = indicator["code"]
        for grain in grains:
            dump(f"/observations?indicator={code}&grain={grain}&limit=10000",
                 f"observations/{code}.{grain}")

    manifest = {
        "snapshot_taken_at": datetime.now(timezone.utc).isoformat(),
        "note": ("A static export of the ATLAS API. Correct as of the "
                 "timestamp above and not updated since. Freshness states "
                 "in status.json describe the pipeline at export time."),
        "files": written,
    }
    (OUT / "manifest.json").write_text(json.dumps(manifest, indent=1),
                                       encoding="utf-8")
    total = sum(f["bytes"] for f in written)
    print(f"\n{len(written)} files, {total:,} bytes total -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
