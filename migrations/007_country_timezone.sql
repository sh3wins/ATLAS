-- ATLAS 007: each country's own local day.
--
-- Daily values are requested in a named timezone, so a "day" means a
-- local day. The Open-Meteo connector hardcoded Africa/Nairobi: right
-- for Kenya, Uganda and Tanzania (UTC+3), an hour out for Rwanda and
-- Burundi (UTC+2). A wrong day boundary does not fail. It returns a
-- full series in which every daily total is cut at the wrong hour, so
-- the values look plausible and are misdated -- the same failure mode
-- as a unit mismatch, and just as invisible.

ALTER TABLE country_config ADD COLUMN timezone TEXT;

UPDATE country_config SET timezone = 'Africa/Nairobi'       WHERE country_iso3 = 'KEN';
UPDATE country_config SET timezone = 'Africa/Kampala'       WHERE country_iso3 = 'UGA';
UPDATE country_config SET timezone = 'Africa/Dar_es_Salaam' WHERE country_iso3 = 'TZA';
UPDATE country_config SET timezone = 'Africa/Kigali'        WHERE country_iso3 = 'RWA';
UPDATE country_config SET timezone = 'Africa/Bujumbura'     WHERE country_iso3 = 'BDI';

ALTER TABLE country_config ALTER COLUMN timezone SET NOT NULL;

COMMENT ON COLUMN country_config.timezone IS
    'IANA zone that defines a local day for daily observations. Required, '
    'with no default: a missing zone must stop a fetch, because guessing '
    'one misdates every value rather than losing it.';
