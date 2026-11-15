"""Configuration, read from the environment.

Nothing here reaches the network or a database at import time, so the
module is safe to import in tests.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env(key: str, default: str | None = None) -> str | None:
    value = os.environ.get(key, default)
    return value.strip() if isinstance(value, str) else value


@dataclass(frozen=True)
class Settings:
    # --- database -------------------------------------------------------
    database_url: str = _env("ATLAS_DATABASE_URL", "postgresql://localhost/atlas")

    # --- raw object store -----------------------------------------------
    # "s3" in production, "local" for development and tests. The local
    # backend writes the same key layout to disk, so the partition scheme
    # is exercised from day one rather than discovered in production.
    storage_backend: str = _env("ATLAS_STORAGE_BACKEND", "local")
    s3_bucket: str = _env("ATLAS_S3_BUCKET", "atlas-raw")
    s3_region: str = _env("AWS_REGION", "eu-west-1")
    local_storage_root: str = _env("ATLAS_LOCAL_STORAGE", "./.atlas-raw")

    # --- upstream sources -----------------------------------------------
    worldbank_base: str = _env("ATLAS_WORLDBANK_BASE", "https://api.worldbank.org/v2")

    # Open-Meteo's free endpoint is licensed for non-commercial use only.
    # A commercial deployment must either subscribe (which swaps this host
    # for the customer- prefixed one) or self-host. Keeping it in config
    # means that switch is an environment change, not a code change.
    open_meteo_base: str = _env(
        "ATLAS_OPEN_METEO_BASE", "https://archive-api.open-meteo.com/v1/archive"
    )
    open_meteo_api_key: str | None = _env("ATLAS_OPEN_METEO_KEY")

    geoboundaries_base: str = _env(
        "ATLAS_GEOBOUNDARIES_BASE", "https://www.geoboundaries.org/api/current/gbOpen"
    )

    # --- politeness ------------------------------------------------------
    # The free Open-Meteo tier allows roughly 10,000 calls a day. Batching
    # coordinates matters more than this limit, but a ceiling stops a
    # runaway backfill burning the budget in one night.
    http_timeout_seconds: int = int(_env("ATLAS_HTTP_TIMEOUT", "60"))
    max_requests_per_run: int = int(_env("ATLAS_MAX_REQUESTS_PER_RUN", "2000"))
    user_agent: str = _env(
        "ATLAS_USER_AGENT", "ATLAS/0.1 (East Africa data platform; +https://example.org)"
    )


settings = Settings()
