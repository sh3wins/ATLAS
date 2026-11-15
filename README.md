# ATLAS

East African data you can trace: county resolution, kept current,
consumable by code.

A harmonisation layer, not a catalogue and not a dashboard. Sources that
disagree about places, time and units land in one comparable shape, and
every published number stays traceable to the file it came from.

**Scope:** Kenya, Uganda, Tanzania, Rwanda, Burundi — all five supported
in the architecture, Kenya enabled in the rollout.

---

## Status

Working end to end against live upstream APIs, verified on Windows 11 with
PostgreSQL 16 + PostGIS 3.7 on 16 Nov 2026: 47 Kenyan counties loaded from
geoBoundaries, 66 years of World Bank GDP, and 4,277 daily rainfall
observations from Open-Meteo, aggregated and served over the API.

| Part | State |
| --- | --- |
| Schema (7 tables, PostGIS) | Done, tested |
| Acquisition framework | Done, tested |
| World Bank connector | Done, verified against live API |
| Open-Meteo connector | Done, verified against live API |
| geoBoundaries loader | Done, tested |
| Standardisation | Done, tested |
| Aggregation engine | Done, tested |
| Freshness tracking | Done, tested |
| Public API | Done, tested |
| S3 backend | Written, exercised only via the local backend |
| Auth, tiering, billing | Not started |
| Front end | Not started |
| Deployment | Not started |

36 tests pass, and all three connectors have now run against the real
APIs.

Three bugs found on first real use and fixed: a missing `sys.path` shim in
two scripts (tests couldn't see it, since pytest sets the path itself),
`demo.py` writing fixtures into a production database, and the year rollup
labelling a partial period as a full year.

---

## Quick start

```bash
createdb atlas
for f in migrations/0*.sql; do psql -v ON_ERROR_STOP=1 -d atlas -f "$f"; done

pip install -r requirements.txt
export ATLAS_DATABASE_URL="postgresql://localhost/atlas"

# Full pipeline against fixtures, no network needed
python scripts/demo.py

# Or against the real APIs
python scripts/load_places.py --country KEN
python scripts/ingest.py worldbank --start-year 2000
python scripts/ingest.py open-meteo --start 2024-01-01 --end 2024-12-31

uvicorn atlas.api.main:app --reload
```

---

## How it is put together

### Three layers

```
S3 (raw)  ->  Parquet/standardised  ->  Postgres + PostGIS (serving)
```

**Raw** is the response exactly as it arrived, plus the URL, timestamp
and SHA-256. It is the provenance store, and it means a parser bug costs
a re-run rather than a re-fetch — which matters when Open-Meteo's free
tier allows roughly 10,000 calls a day.

**Serving** is disposable: everything in Postgres should be rebuildable
from raw. A value that exists only there is a hole in the provenance
claim, so corrections go in code, never typed into the database.

Athena is deliberately **not** used yet. At county-aggregate volumes
Postgres is faster and simpler; Athena earns its place when you start
querying raw grids.

### Acquisition is separate from processing

`fetch()` may call an API once, page through a hundred responses, or
download a bulk archive. It is the only part of ATLAS that touches the
network, and it returns an `AcquisitionResult` whose `artifacts` is a
**list** — that one decision is what lets paging and bulk downloads share
a shape, and it is most of the flexibility the design has.

`parse()` works from a landed artifact and never knows which of those
happened.

Acquisition is resumable: each chunk retries independently, so a timeout
during a multi-year backfill costs one chunk, not the run.

### The rules live in data, not code

`indicator.agg_rule` says how a series combines. Rainfall sums, GDP takes
the last value, temperature averages. The aggregation code reads that
column and knows nothing about rainfall. Adding an indicator is an
`INSERT`.

### Honest absence

Every observation carries either a value or a `missing_reason`, enforced
by a check constraint. "Measured zero" and "nobody surveyed" are
different facts that look identical in a CSV, and conflating them drags
every average you compute.

Rolling finer than a source's native grain is refused rather than
interpolated — `gdp_per_capita_usd` exists at year grain, so a daily
request returns 422, not a fabricated number.

### Attribution is plumbing

Every API response carries the licence and required credit for exactly
the sources it used. CC BY 4.0 requires crediting the source and
indicating what changed; with an API returning arbitrary slices, a page
footer does not discharge that.

### The reliability promise

ATLAS commits to ingestion, validation, availability and delivery. It
does not warrant that an upstream provider never changes its schema or
restates a figure. `/status` makes that checkable — and it separates two
failures that are usually conflated:

- `failing` — ATLAS could not fetch. Our problem.
- `stale` — the source has not published. Not our problem.

Staleness thresholds are per source, because ERA5 runs months behind by
design while World Bank indicators arrive annually. A global rule would
flag correct data and train users to ignore the warning.

---

## Layout

```
migrations/      001-005, run in order
atlas/
  config.py      environment settings
  db.py          psycopg access, explicit SQL
  storage.py     S3 and local backends, identical key layout
  acquisition.py AcquisitionResult, Artifact, ObservationRow
  connectors/    base contract + worldbank, open_meteo, geoboundaries
  pipeline/      places, standardise, aggregate, freshness
  api/main.py    FastAPI
scripts/         load_places, ingest, demo
tests/           36 tests, fixtures matching real response shapes
```

---

## Before going live

1. **Open-Meteo licensing.** The data is CC BY 4.0 and redistributable
   commercially. The free hosted API is non-commercial only. With paid
   tiers planned, you need a commercial plan, a self-hosted instance, or
   to pull ERA5 directly from Copernicus. `ATLAS_OPEN_METEO_BASE` makes
   that an environment change.
2. **geoBoundaries release.** The code uses `gbOpen`. Do not switch to
   `gbHumanitarian` — it is documented as not for commercial use.
3. **Verify the live response shapes** on first run. Parsers were written
   against documented shapes and recorded fixtures, not live calls.
4. **Third source.** Pick one that arrives differently — a bulk file or a
   published table — so it stresses the acquisition boundary rather than
   confirming it.

## Known limitations

- **`ATLAS_HTTP_TIMEOUT` must be raised outside Europe/North America.** From
  Nairobi, a single small World Bank call takes ~35 seconds; the 60-second
  default times out on a full page. 300 works. This is a property of where
  ATLAS runs, not a bug.
- **Single-centroid weather sampling.** Each county's rainfall comes from the
  ERA5 cell containing its centroid. For small coastal counties the centroid
  can sit on a cell that is mostly ocean, which reads wetter than land — and
  Mombasa duly topped the first rainfall ranking. Area-weighted sampling
  across all cells intersecting a county is the fix.
- **Null handling is fixture-tested only.** World Bank has published through
  2025, so no null years appeared in the live response. The
  `missing_reason = not_collected` path works against the fixture and has
  not met real upstream nulls.

## Configuration

| Variable | Default |
| --- | --- |
| `ATLAS_DATABASE_URL` | `postgresql://localhost/atlas` |
| `ATLAS_STORAGE_BACKEND` | `local` (`s3` in production) |
| `ATLAS_S3_BUCKET` | `atlas-raw` |
| `ATLAS_LOCAL_STORAGE` | `./.atlas-raw` |
| `ATLAS_OPEN_METEO_BASE` | the free archive endpoint |
| `ATLAS_OPEN_METEO_KEY` | unset |

## Tests

```bash
python -m pytest tests/ -q                      # parsers only
export ATLAS_TEST_DATABASE_URL=postgresql://localhost/atlas_test
python -m pytest tests/ -q                      # full suite
```

The database fixture drops and recreates the schema each session, so the
migrations are exercised on every run.
