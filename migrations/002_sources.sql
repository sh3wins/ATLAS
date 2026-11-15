-- ATLAS 002: sources
--
-- Every provider ATLAS ingests from. Referenced by almost everything else,
-- which is what keeps licence and attribution in exactly one place: any
-- path to a value can reach the terms it was published under.

CREATE TABLE source (
    source_id            TEXT PRIMARY KEY,   -- 'worldbank', 'open-meteo', 'geoboundaries'
    name                 TEXT NOT NULL,
    homepage_url         TEXT,

    -- Licence terms. CC BY 4.0 requires crediting the source AND indicating
    -- what was changed, so this has to travel with every value ATLAS
    -- republishes -- not sit in a footer.
    licence              TEXT NOT NULL,      -- 'CC-BY-4.0', 'ODbL-1.0', ...
    licence_url          TEXT,
    attribution_required BOOLEAN NOT NULL DEFAULT TRUE,
    attribution_text     TEXT,               -- exact wording the provider asks for

    -- Freshness expectations. Per source, because the right answer differs
    -- wildly: ERA5 reanalysis runs months behind by design, World Bank
    -- indicators arrive annually. A single global staleness rule would flag
    -- correct data as stale and train users to ignore the warning.
    expected_frequency   TEXT                -- 'daily', 'monthly', 'annual', 'irregular'
                         CHECK (expected_frequency IN
                                ('daily', 'weekly', 'monthly', 'quarterly',
                                 'annual', 'irregular')),
    expected_lag         INTERVAL,           -- how far behind real time it normally runs

    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);


-- Operational state per source. Deliberately separate from `source`:
-- that table is configuration you edit, this is state the pipeline writes.

CREATE TABLE dataset_freshness (
    source_id               TEXT PRIMARY KEY REFERENCES source (source_id),

    last_attempted_update   TIMESTAMPTZ,
    last_successful_update  TIMESTAMPTZ,
    -- The newest period the SOURCE itself has published. Distinct from the
    -- two above on purpose: "we failed to fetch" is ATLAS's problem,
    -- "they haven't published yet" is not. Only the first should page you.
    last_source_publication DATE,

    freshness_status        TEXT NOT NULL DEFAULT 'unknown'
                            CHECK (freshness_status IN
                                   ('current', 'due', 'stale', 'failing', 'unknown')),
    last_error              TEXT,
    consecutive_failures    INTEGER NOT NULL DEFAULT 0,

    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE dataset_freshness IS
    'Pipeline state. Surface this on a public status endpoint: a reliability '
    'promise nobody can verify is a slogan.';


-- Seed the two first-slice sources.

INSERT INTO source (source_id, name, homepage_url, licence, licence_url,
                    attribution_required, attribution_text,
                    expected_frequency, expected_lag)
VALUES
    ('worldbank',
     'World Bank Open Data',
     'https://data.worldbank.org',
     'CC-BY-4.0',
     'https://creativecommons.org/licenses/by/4.0/',
     TRUE,
     'World Bank Open Data (CC BY 4.0)',
     'annual',
     INTERVAL '12 months'),

    ('open-meteo',
     'Open-Meteo Historical Weather API (ERA5)',
     'https://open-meteo.com',
     'CC-BY-4.0',
     'https://open-meteo.com/en/licence',
     TRUE,
     'Weather data by Open-Meteo.com, generated using Copernicus Climate Change Service information',
     'daily',
     INTERVAL '5 days'),

    ('geoboundaries',
     'geoBoundaries (gbOpen)',
     'https://www.geoboundaries.org',
     'CC-BY-4.0',
     'https://www.geoboundaries.org',
     TRUE,
     'Administrative boundaries from geoBoundaries (gbOpen), William & Mary geoLab',
     'irregular',
     NULL);

INSERT INTO dataset_freshness (source_id)
SELECT source_id FROM source;
