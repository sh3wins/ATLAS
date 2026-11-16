-- ATLAS 008: where the "has the source published?" signal comes from.
--
-- Freshness derived last_source_publication from MAX(period_end) in our
-- own observation table. That is circular in two ways. A parser that
-- silently drops the newest rows makes a healthy source look dormant.
-- And a backfill bounded at a fixed end date freezes the signal at that
-- date for good: ATLAS asked for data up to 2024-12-31, so MAX(period_end)
-- can never move past it, and the source reads stale forever however
-- often it publishes.
--
-- The World Bank response declares `lastupdated`. Open-Meteo declares
-- nothing. That difference must stay visible instead of being averaged
-- into one column, so the basis is stored beside the date.

ALTER TABLE dataset_freshness
    ADD COLUMN publication_basis TEXT NOT NULL DEFAULT 'none'
    CHECK (publication_basis IN ('declared', 'observed', 'none'));

COMMENT ON COLUMN dataset_freshness.publication_basis IS
    'declared: the source stated its own publication date. observed: '
    'inferred from the newest period ATLAS holds, which can never outrun '
    'what ATLAS asked for. none: unknown.';

COMMENT ON COLUMN dataset_freshness.last_source_publication IS
    'What the SOURCE said about its own publishing. Written only from a '
    'declaration by the source, never from ATLAS output.';
