-- ATLAS 004: the indicator registry
--
-- An indicator is not just a name. It is a definition, a unit, and a rule
-- for how it combines. Without those recorded here, the system cannot know
-- that summing twelve months of rainfall is right and summing twelve
-- months of population is nonsense.
--
-- The aggregation code reads agg_rule from this table. It never knows
-- anything about rainfall specifically.

CREATE TABLE indicator (
    indicator_id  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    code          TEXT NOT NULL UNIQUE,   -- ATLAS's own stable name
    name          TEXT NOT NULL,
    definition    TEXT,                   -- what it actually measures, in words
    unit          TEXT NOT NULL,

    -- How values combine when rolling up to a coarser period.
    agg_rule      TEXT NOT NULL
                  CHECK (agg_rule IN ('sum', 'mean', 'last', 'first', 'min', 'max')),

    -- The finest grain this indicator genuinely exists at. The API refuses
    -- requests finer than this rather than inventing an answer.
    native_grain  TEXT NOT NULL
                  CHECK (native_grain IN ('hour', 'day', 'month', 'quarter', 'year')),

    -- Which way is "good", for ranking and map colouring. Higher GDP is
    -- good, higher child mortality is not, higher rainfall is neither --
    -- and a chart that colours everything green-for-high misleads on half
    -- of them.
    direction     TEXT NOT NULL DEFAULT 'neutral'
                  CHECK (direction IN ('higher_better', 'lower_better', 'neutral')),

    source_id     TEXT NOT NULL REFERENCES source (source_id),
    -- The provider's own identifier, kept visible so a researcher can go
    -- verify the definition at the source. Provenance at the definition
    -- level, not just the value level.
    source_code   TEXT NOT NULL,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Two sources publishing "maize production" are two indicators, never
    -- one merged series. Different methodologies, different revisions.
    -- Showing that they disagree is a feature; silently merging them is
    -- how a platform becomes untrustworthy.
    UNIQUE (source_id, source_code)
);

CREATE INDEX indicator_source_idx ON indicator (source_id);


-- First-slice indicators: one annual economic series keyed by country
-- code, one daily weather series keyed by coordinates. They disagree about
-- identity AND about time, which is exactly what makes them a real test of
-- the join.

INSERT INTO indicator (code, name, definition, unit, agg_rule, native_grain,
                       direction, source_id, source_code)
VALUES
    ('gdp_per_capita_usd',
     'GDP per capita',
     'Gross domestic product divided by midyear population, current US dollars.',
     'USD',
     'last',          -- a stock, not a flow: summing years is meaningless
     'year',
     'higher_better',
     'worldbank',
     'NY.GDP.PCAP.CD'),

    ('precip_total_mm',
     'Total precipitation',
     'Sum of precipitation over the period, ERA5 reanalysis.',
     'mm',
     'sum',           -- a flow: twelve months of rain add up
     'day',
     'neutral',
     'open-meteo',
     'precipitation_sum');
