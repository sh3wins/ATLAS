-- ATLAS 003: places and the source crosswalk
--
-- One row per real-world geographic entity. ATLAS owns the IDs; every
-- external code (KEN, a district number, a station ID) is an attribute
-- mapped in via place_crosswalk, never a primary key. When a source
-- retires or renames a code, nothing in ATLAS breaks.

CREATE TABLE place (
    place_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    name         TEXT NOT NULL,
    -- Generic level, so five countries with different hierarchies fit one
    -- table. 'adm1' is a Kenyan county, a Ugandan district, a Tanzanian
    -- region. Nothing anywhere should hardcode 'county' or the number 47.
    level        TEXT NOT NULL
                 CHECK (level IN ('country', 'adm1', 'adm2', 'adm3', 'point')),
    -- What that level is called locally. Display only; never join on it.
    level_name   TEXT,

    -- ISO3 of the country this belongs to. Denormalised on purpose: saves a
    -- recursive walk on the very common "everything in Kenya" query.
    country_iso3 CHAR(3) NOT NULL,

    parent_id    BIGINT REFERENCES place (place_id),

    -- Polygon for an area, point for a station. SRID 4326 is plain
    -- lat/long, which geoBoundaries and Open-Meteo both speak.
    geom         geometry(Geometry, 4326),

    -- A region is a definition with a lifespan, not a permanent fact.
    -- NULL valid_to means current. Leave both NULL for now -- the columns
    -- exist so Kenya's 2013 reorganisation or Uganda's district splits
    -- don't force a migration later.
    valid_from   DATE,
    valid_to     DATE,

    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    CHECK (valid_to IS NULL OR valid_from IS NULL OR valid_to >= valid_from)
);

CREATE INDEX place_parent_idx  ON place (parent_id);
CREATE INDEX place_country_idx ON place (country_iso3, level);
CREATE INDEX place_geom_idx    ON place USING GIST (geom);


-- How each source refers to a place.
--
-- A separate table rather than columns on place, because one place carries
-- codes from many sources and sources keep arriving. `confidence` records
-- whether a match was certain or a judgement call -- which matters when a
-- number looks wrong and you need to know whether the join or the data is
-- at fault.

CREATE TABLE place_crosswalk (
    source_id   TEXT   NOT NULL REFERENCES source (source_id),
    source_code TEXT   NOT NULL,
    place_id    BIGINT NOT NULL REFERENCES place (place_id),

    confidence  TEXT   NOT NULL DEFAULT 'exact'
                CHECK (confidence IN ('exact', 'fuzzy', 'manual')),
    note        TEXT,

    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One code from one source resolves to exactly one place.
    PRIMARY KEY (source_id, source_code)
);

CREATE INDEX place_crosswalk_place_idx ON place_crosswalk (place_id);


-- Which admin level ATLAS treats as "subnational" for each country, so the
-- API can answer "give me the subnational breakdown" without knowing that
-- Kenya means counties and Tanzania means regions.

CREATE TABLE country_config (
    country_iso3     CHAR(3) PRIMARY KEY,
    name             TEXT NOT NULL,
    subnational_level TEXT NOT NULL
                     CHECK (subnational_level IN ('adm1', 'adm2', 'adm3')),
    level_name       TEXT NOT NULL,
    enabled          BOOLEAN NOT NULL DEFAULT FALSE
);

-- All five supported in the architecture. Only Kenya enabled for rollout:
-- adding Uganda should be flipping this flag and loading data, with no
-- code change. If anything needs rewriting to accept country two, that is
-- a design bug worth fixing while it costs an afternoon.
INSERT INTO country_config (country_iso3, name, subnational_level, level_name, enabled)
VALUES
    ('KEN', 'Kenya',    'adm1', 'county',   TRUE),
    ('UGA', 'Uganda',   'adm1', 'district', FALSE),
    ('TZA', 'Tanzania', 'adm1', 'region',   FALSE),
    ('RWA', 'Rwanda',   'adm1', 'province', FALSE),
    ('BDI', 'Burundi',  'adm1', 'province', FALSE);
