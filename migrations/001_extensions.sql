-- ATLAS 001: extensions
-- Run once, as a superuser, against the atlas database.

CREATE EXTENSION IF NOT EXISTS postgis;

-- Confirms PostGIS is live. Expect something like "3.4.2".
SELECT postgis_version();
