-- ATLAS 009: index the source-scoped scans.
--
-- observed_publication() runs MAX(period_end) WHERE source_id = ? on every
-- freshness recompute, and status_report now does it per source on every
-- read of /status. Without an index that is a sequential scan of the whole
-- observation table each time, which is cheap at 20,000 rows and is not
-- cheap later.
--
-- period_end is included so the MAX comes from the index. value IS NOT NULL
-- matches the query, which only counts observations that actually hold a
-- value: a recorded gap is not evidence that the source published.

CREATE INDEX IF NOT EXISTS observation_source_period_idx
    ON observation (source_id, period_end DESC)
    WHERE value IS NOT NULL;
