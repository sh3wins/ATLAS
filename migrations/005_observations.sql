-- ATLAS 005: raw objects and the fact table
--
-- raw_object is the provenance store: one row per file landed in S3,
-- recording where it came from and when. observation is the only table
-- that grows without bound; everything else is reference data.

CREATE TABLE raw_object (
    s3_key        TEXT PRIMARY KEY,

    source_id     TEXT NOT NULL REFERENCES source (source_id),

    -- Whatever identifies what was asked for: a URL with params, a file
    -- path, a query. JSONB because acquisition modes differ -- an HTTP
    -- call, a bulk download and a scraped table have nothing in common
    -- except that something was requested.
    request       JSONB NOT NULL,

    fetched_at    TIMESTAMPTZ NOT NULL,
    http_status   INTEGER,
    -- SHA-256 of the body. This is what detects a source silently revising
    -- history -- which reanalysis data and restated economic figures both do.
    content_hash  TEXT NOT NULL,
    content_type  TEXT,
    byte_size     BIGINT,

    -- Acquisition writes raw and stops; processing reads from here, never
    -- from a live response. That is what makes re-parsing free -- and with
    -- Open-Meteo's call budget, re-fetching is what you cannot afford to
    -- do casually.
    parsed_at     TIMESTAMPTZ,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX raw_object_source_idx ON raw_object (source_id, fetched_at DESC);
CREATE INDEX raw_object_hash_idx   ON raw_object (content_hash);
CREATE INDEX raw_object_unparsed_idx ON raw_object (source_id)
    WHERE parsed_at IS NULL;


-- The fact table. Long and narrow: one row per observation, never a column
-- per indicator. Adding an indicator is an INSERT, not a migration.

CREATE TABLE observation (
    place_id       BIGINT NOT NULL REFERENCES place (place_id),
    indicator_id   BIGINT NOT NULL REFERENCES indicator (indicator_id),

    -- An observation covers a stretch of time, not an instant. A single
    -- `date` column forces you to guess later whether 2026-03-01 means the
    -- 1st of March or the whole of March -- different numbers, easily mixed up.
    period_start   DATE NOT NULL,
    period_end     DATE NOT NULL,
    grain          TEXT NOT NULL
                   CHECK (grain IN ('hour', 'day', 'month', 'quarter', 'year')),

    -- NULL means genuinely absent. See the constraint below: absence must
    -- carry a reason, because "no survey" and "measured zero" are different
    -- facts that look identical in a CSV.
    value          DOUBLE PRECISION,
    missing_reason TEXT
                   CHECK (missing_reason IN ('not_collected', 'suppressed', 'not_applicable')),

    source_id      TEXT NOT NULL REFERENCES source (source_id),

    -- The thread back to the original file. A researcher clicks a number,
    -- you follow this to the S3 object, and from there to the request URL
    -- and fetch timestamp. Without it, "full provenance" is a claim on a
    -- marketing page. With it, it is a link.
    raw_ref        TEXT REFERENCES raw_object (s3_key),

    -- 'modelled' matters with these sources: Open-Meteo's history is ERA5
    -- reanalysis, a model output assimilating observations, not a rain
    -- gauge reading. Good data, widely used, and not a direct measurement.
    quality        TEXT NOT NULL DEFAULT 'reported'
                   CHECK (quality IN ('reported', 'aggregated', 'provisional', 'modelled')),

    ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Deterministic key: a re-run overwrites rather than appends. You will
    -- re-run constantly, and a pipeline that quietly doubles its data each
    -- time is a miserable bug to find.
    --
    -- source_id is in the key on purpose: two sources may report the same
    -- indicator for the same place and period without colliding.
    PRIMARY KEY (place_id, indicator_id, period_start, grain, source_id),

    CHECK (period_end >= period_start),
    -- A value or a reason for its absence. Never neither, never both.
    CHECK ((value IS NULL) = (missing_reason IS NOT NULL))
);

-- Time-series reads: one indicator across a span.
CREATE INDEX observation_indicator_period_idx
    ON observation (indicator_id, period_start);

-- Place profile reads: everything known about one county.
CREATE INDEX observation_place_period_idx
    ON observation (place_id, period_start);

-- Following values back to their source file.
CREATE INDEX observation_raw_ref_idx ON observation (raw_ref);

COMMENT ON TABLE observation IS
    'Append-only in spirit: corrections arrive by re-running the pipeline, '
    'never by hand-editing rows. A value that exists only here and not in '
    'S3 is a hole in the provenance claim.';
