from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).parent / "fixtures"

TEST_DB = os.environ.get("ATLAS_TEST_DATABASE_URL")


def load_fixture(name: str):
    return json.loads((FIXTURES / f"{name}.json").read_text())


@pytest.fixture(scope="session")
def fixtures():
    return {
        "worldbank": load_fixture("worldbank_gdp_ken"),
        "open_meteo": load_fixture("open_meteo_ken_jan2024"),
        "geoboundaries": load_fixture("geoboundaries_ken_adm1"),
    }


@pytest.fixture
def storage(tmp_path):
    from atlas.storage import LocalStorage
    return LocalStorage(root=str(tmp_path / "raw"))


@pytest.fixture(scope="session")
def database_url():
    """A throwaway database with the migrations applied.

    Skips rather than fails when no Postgres is configured, so the pure
    unit tests still run anywhere.
    """
    if not TEST_DB:
        if os.environ.get("ATLAS_ALLOW_DB_TEST_SKIP") == "1":
            pytest.skip("ATLAS_ALLOW_DB_TEST_SKIP=1: database tests skipped "
                        "on purpose")
        pytest.fail(
            "ATLAS_TEST_DATABASE_URL is not set, so two thirds of this suite "
            "cannot run. It used to skip silently, which printed a green "
            "result for a run that exercised almost nothing -- including in "
            "CI, where a missing database would have looked like success. "
            "Set the variable, or set ATLAS_ALLOW_DB_TEST_SKIP=1 to say out "
            "loud that you are running without a database."
        )

    # Start from nothing, so the migrations are genuinely exercised on
    # every run rather than only the first.
    reset = subprocess.run(
        ["psql", "-v", "ON_ERROR_STOP=1", "-q", "-d", TEST_DB,
         "-c", "DROP SCHEMA IF EXISTS public CASCADE; CREATE SCHEMA public;"],
        capture_output=True, text=True,
    )
    if reset.returncode != 0:
        pytest.fail(f"schema reset: {reset.stderr}")

    for migration in sorted((ROOT / "migrations").glob("0*.sql")):
        result = subprocess.run(
            ["psql", "-v", "ON_ERROR_STOP=1", "-q", "-d", TEST_DB, "-f", str(migration)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            pytest.fail(f"{migration.name}: {result.stderr}")
    return TEST_DB


@pytest.fixture
def conn(database_url, monkeypatch):
    import psycopg
    from psycopg.rows import dict_row

    monkeypatch.setenv("ATLAS_DATABASE_URL", database_url)
    connection = psycopg.connect(database_url, row_factory=dict_row)

    # Each test starts from the seeded reference data and nothing else.
    # A rollback alone is not enough: a test that commits deliberately
    # would otherwise leak its rows into whatever runs next.
    with connection.cursor() as cur:
        cur.execute(
            "TRUNCATE observation, raw_object, place_crosswalk, place "
            "RESTART IDENTITY CASCADE"
        )
        cur.execute(
            """
            UPDATE dataset_freshness
            SET last_attempted_update = NULL, last_successful_update = NULL,
                last_source_publication = NULL, freshness_status = 'unknown',
                last_error = NULL, consecutive_failures = 0
            """
        )
    connection.commit()

    try:
        yield connection
        connection.rollback()
    finally:
        connection.close()
