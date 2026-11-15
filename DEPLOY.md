# Deploying ATLAS to Render

Everything below assumes the repo at `github.com/sh3wins/ATLAS` with
`render.yaml` and `Dockerfile` committed.

## What you get

| Resource | What it is | Rough cost |
| --- | --- | --- |
| `atlas-db` | Managed Postgres 16, Frankfurt, 1 GB | ~$7/month |
| `atlas-api` | The FastAPI service, Docker | free tier or ~$7/month |
| `atlas-ingest-worldbank` | Monthly cron | pennies |

Frankfurt is the closest Render region to Nairobi. The region cannot be
changed later without recreating the database.

## 1. Account

Sign up at render.com and connect your GitHub account. Grant it access to
the `ATLAS` repository (you can scope it to just that one).

## 2. Create the Blueprint

Dashboard → **New** → **Blueprint** → pick the `ATLAS` repo.

Render reads `render.yaml` and shows the three resources it will create.
Apply.

If it objects to a plan name, delete the `plan:` line it names and apply
again — defaults are fine to start.

## 3. Enable PostGIS

The database comes up without it. From the database's page copy the
**External Database URL**, then on your machine:

```
psql "<external-url>" -c "CREATE EXTENSION IF NOT EXISTS postgis"
psql "<external-url>" -c "SELECT postgis_version()"
```

## 4. Run the migrations

```
psql "<external-url>" -v ON_ERROR_STOP=1 -f migrations/002_sources.sql
psql "<external-url>" -v ON_ERROR_STOP=1 -f migrations/003_places.sql
psql "<external-url>" -v ON_ERROR_STOP=1 -f migrations/004_indicators.sql
psql "<external-url>" -v ON_ERROR_STOP=1 -f migrations/005_observations.sql
```

`001_extensions.sql` is skipped — step 3 already did it.

## 5. Load the data

```
$env:ATLAS_DATABASE_URL = "<external-url>"
$env:ATLAS_HTTP_TIMEOUT = "300"
.\.venv\Scripts\python.exe scripts\load_places.py --country KEN
.\.venv\Scripts\python.exe scripts\ingest.py worldbank
.\.venv\Scripts\python.exe scripts\ingest.py open-meteo --start 2024-01-01 --end 2024-03-31
```

Run from your machine, not from Render — your laptop already has the venv,
and this only needs doing once.

## 6. Check it

`https://atlas-api-<suffix>.onrender.com/status` — the URL is on the
service page. Then `/docs` for the interactive API.

## Known gaps

- **Raw storage is on the container filesystem** (`ATLAS_STORAGE_BACKEND=local`),
  which Render wipes on every deploy. The provenance claim depends on raw
  files surviving, so this must move to S3 or a Render disk before anyone
  relies on it. Nothing in the pipeline changes — only the env var.
- **No Open-Meteo cron.** `ingest.py` takes a fixed date window, so a
  schedule would re-fetch the same dates forever. It needs an incremental
  mode that reads `dataset_freshness.last_successful_update` first.
- **No auth or rate limiting.** The API is fully open. Fine while nobody
  knows the URL, not fine once it is public.
- **Free-tier web services sleep** after inactivity, so the first request
  after a quiet period is slow.

## Cost control

Set a spend limit in Render's billing settings before you apply the
Blueprint. The database bills whether or not anything queries it.
