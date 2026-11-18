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

70 tests pass against a real PostgreSQL 16 + PostGIS from an empty
schema, with every migration applied on each run. All three connectors
have run against the real APIs.

Most fixtures were written from the API documentation, so most parser
tests check that the parsers match our reading of the docs rather than
the APIs. `tests/fixtures/live/` holds seven responses recorded from the
real endpoints by `scripts/record_fixtures.py`, with the URL, timestamp
and a SHA-256 of the bytes as they arrived. Recording those immediately
found two defects no hand-written fixture had reached: the World Bank
answers **HTTP 200 with an error object** for an unknown country or
indicator, which the parser had been treating as an empty result, and
Open-Meteo's rejection body carries a `reason` that was being thrown
away. More live cases are worth recording.

Three bugs found on first real use and fixed: a missing `sys.path` shim in
two scripts (tests couldn't see it, since pytest sets the path itself),
`demo.py` writing fixtures into a production database, and the year rollup
labelling a partial period as a full year.

Two review rounds since: an external code review found eleven defects in
these files, all fixed, and running the suite afterwards found two more in
those fixes. A documentation review added a backlog of verification work.
The largest open items are provenance on aggregated rows, single-point
weather sampling, and the unverified S3 backend. The tracked list is
longer than this README; see `AUDIT-2026-11-16.md`.

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

Chunks fail independently: a timeout during a multi-year backfill costs
one chunk rather than the run, and the failed chunk is recorded with
enough detail to reissue it. Nothing retries it yet — `failed_chunks`
has no consumer, so "resumable" describes the shape of the design, not a
working behaviour.

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

`failing` is measured against ATLAS's own schedule
(`source.run_interval`), so a scheduler that dies silently is caught
within one interval plus a short grace. Measuring it against the
source's publication cadence, as an earlier version did, let an annual
source sit quiet for 486 days before anyone was told.

Staleness thresholds are per source, because the Open-Meteo archive runs
about five days behind real time by design while World Bank indicators
arrive annually. A global rule would flag correct data and train users
to ignore the warning.

Where the publication signal comes from is recorded alongside it, in
`dataset_freshness.publication_basis`, because the two sources answer
very differently:

- `declared` — the source stated its own date. The World Bank's response
  carries `lastupdated`, which is read at parse time and is the only
  statement about its publishing that does not come from reading our own
  database back.
- `observed` — inferred from the newest period ATLAS holds. Still useful,
  and never written into the source's own column, because it cannot
  outrun what ATLAS asked for: a backfill bounded at 2024-12-31 would
  otherwise report a healthy source as dormant for ever. Open-Meteo's
  archive declares nothing, so it sits here.
- `none` — unknown, and said so.

---

## Layout

```
migrations/      001-009, run in order
atlas/
  config.py      environment settings
  db.py          psycopg access, explicit SQL
  storage.py     S3 and local backends, identical key layout
  acquisition.py AcquisitionResult, Artifact, ObservationRow
  connectors/    base contract + worldbank, open_meteo, geoboundaries
  pipeline/      places, standardise, aggregate, freshness
  api/main.py    FastAPI
scripts/         load_places, ingest, demo
tests/           70 tests; fixtures/ written from documented shapes,
                 fixtures/live/ recorded from the real APIs
```

---

## Before going live

1. **Open-Meteo licensing.** Weather data is ERA5 and ERA5-Land
   reanalysis, delivered through Open-Meteo's archive endpoint. The
   dataset and the delivery channel are separate things, and so are
   their terms: the data is CC BY 4.0 and redistributable commercially,
   while the free hosted API is non-commercial only. There is no direct
   Copernicus CDS connector. With paid tiers planned the options are a
   commercial Open-Meteo plan, a self-hosted instance, or a CDS
   connector that does not exist yet. `ATLAS_OPEN_METEO_BASE` makes the
   first two an environment change.
2. **geoBoundaries release.** The code uses `gbOpen`. Do not switch to
   `gbHumanitarian` — it is documented as not for commercial use.
3. **Keep recording live response shapes.** `scripts/record_fixtures.py`
   captures the happy path, both timezones, an unknown country, an
   unknown indicator and an impossible date window. Rate limiting,
   partial responses and a mid-series schema change are not covered.
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
- **Aggregated rows carry no provenance.** A monthly or yearly row has no
  `raw_ref` and no count of the days behind it, so a month assembled from
  nine days of data is indistinguishable from a complete one.
- **A geoBoundaries re-release would duplicate every county.** `shapeID`
  is release-scoped and `place` has no unique key across releases.
- **S3 is written but unverified** against a real bucket, as the status
  table says. Do not describe it as production storage.

## Reading an empty result

`/observations` never answers an empty list without saying which kind of
empty it is, because three very different situations used to look
identical:

| `availability` | Means |
| --- | --- |
| `materialised` | Rows returned |
| `not_materialised` | The grain is legal and nobody has built the roll-up yet |
| `no_match_for_filters` | The roll-up exists; your place or date filters excluded everything |
| `no_data` | Nothing for this indicator at all |

"Nobody aggregated it yet" reading as "no rain fell" is the one that
matters.

## Configuration

| Variable | Default |
| --- | --- |
| `ATLAS_DATABASE_URL` | `postgresql://localhost/atlas` |
| `ATLAS_STORAGE_BACKEND` | `local` (`s3` in production) |
| `ATLAS_S3_BUCKET` | `atlas-raw` |
| `ATLAS_LOCAL_STORAGE` | `./.atlas-raw` |
| `ATLAS_OPEN_METEO_BASE` | the free archive endpoint |
| `ATLAS_OPEN_METEO_KEY` | unset |
| `ATLAS_MAX_REQUESTS_PER_RUN` | `2000`, enforced -- a run that reaches it stops |
| `ATLAS_HTTP_TIMEOUT` | `60`; raise to `300` outside Europe/North America |

## Tests

```bash
python -m pytest tests/ -q                      # parsers only
export ATLAS_TEST_DATABASE_URL=postgresql://localhost/atlas_test
python -m pytest tests/ -q                      # full suite
```

The database fixture drops and recreates the schema each session, so the
migrations are exercised on every run.

Without `ATLAS_TEST_DATABASE_URL` the database tests **fail** rather than
skip. They used to skip silently, which printed a green result for a run
that exercised a third of the suite -- including in CI, where a missing
database would have looked like success. To run the unit tests alone, say
so explicitly with `ATLAS_ALLOW_DB_TEST_SKIP=1`.
