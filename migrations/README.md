# ATLAS migrations

Run in order. Each file is idempotent only in the sense that it will fail
loudly if run twice — there is no migration tool here yet, so track what
you have applied.

```
createdb atlas

psql -d atlas -v ON_ERROR_STOP=1 -f 001_extensions.sql
psql -d atlas -v ON_ERROR_STOP=1 -f 002_sources.sql
psql -d atlas -v ON_ERROR_STOP=1 -f 003_places.sql
psql -d atlas -v ON_ERROR_STOP=1 -f 004_indicators.sql
psql -d atlas -v ON_ERROR_STOP=1 -f 005_observations.sql
```

`001` needs superuser rights to create the PostGIS extension. The rest do not.

## Order matters

`place_crosswalk` references `source`, and `observation` references almost
everything, so sources come before places and the fact table comes last.

## What's seeded

- Three sources: World Bank, Open-Meteo, geoBoundaries — with licence and
  attribution text, because CC BY 4.0 requires crediting the source and
  indicating what changed, and that has to travel with the data.
- Five countries in `country_config`, with Kenya the only one enabled.
  Adding Uganda should be flipping that flag and loading data, with no
  code change anywhere.
- Two indicators: `gdp_per_capita_usd` (annual, `last`) and
  `precip_total_mm` (daily, `sum`).

## Verified

Run against PostgreSQL 16 with PostGIS 3.4. All five apply clean, and:

- the cross-source join returns World Bank and Open-Meteo values in one
  result set
- `raw_ref` resolves a value back to its fetch URL and timestamp
- `ST_Contains` places Nairobi's coordinates inside the right county
- the fact table rejects: a value with a missing-reason, a NULL with no
  reason, an inverted period, an unknown grain, an unknown source, and a
  duplicate key
- `ON CONFLICT ... DO UPDATE` on the primary key makes re-runs safe
