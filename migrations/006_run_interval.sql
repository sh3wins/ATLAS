-- ATLAS 006: how often ATLAS itself is scheduled to run a source.
--
-- Distinct from expected_frequency, which is how often the SOURCE
-- publishes. Conflating them made the dead-scheduler check useless: it
-- compared the age of our last successful run against the source's
-- publication cadence, so an annual source tolerated 486 days of silence
-- from a dead cron, and an irregular one tolerated twenty years.

ALTER TABLE source ADD COLUMN run_interval INTERVAL;

COMMENT ON COLUMN source.run_interval IS
    'How often ATLAS is scheduled to fetch this source. NULL means unknown, '
    'and the dead-scheduler check is skipped rather than guessed.';

-- Matches the schedules in render.yaml.
UPDATE source SET run_interval = INTERVAL '1 month'  WHERE source_id = 'worldbank';
UPDATE source SET run_interval = INTERVAL '1 day'    WHERE source_id = 'open-meteo';
UPDATE source SET run_interval = INTERVAL '180 days' WHERE source_id = 'geoboundaries';
