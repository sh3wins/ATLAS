"""The raw object store.

Acquisition writes here and stops. Processing reads from here, never from
a live HTTP response. That ordering is what makes re-parsing free: with
Open-Meteo's call budget and forty-five years of ERA5 history, re-fetching
is the thing you cannot afford to do casually.

Two backends behind one interface. Both use the identical key layout, so
the partition scheme is exercised in development rather than discovered in
production:

    source=<id>/year=<YYYY>/month=<MM>/day=<DD>/<hash>.<ext>

Athena bills on data scanned. Without those partitions every query reads
the whole bucket, and adding them later means rewriting the layout.
"""
from __future__ import annotations

import hashlib
import json
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

from .config import settings


def content_hash(body: bytes) -> str:
    """SHA-256 of the body.

    This is what detects a source silently revising history, which both
    reanalysis data and restated economic figures do.
    """
    return hashlib.sha256(body).hexdigest()


def build_key(source_id: str, body: bytes, extension: str = "json",
              when: datetime | None = None) -> str:
    when = when or datetime.now(timezone.utc)
    digest = content_hash(body)[:16]
    return (
        f"source={source_id}"
        f"/year={when:%Y}/month={when:%m}/day={when:%d}"
        f"/{digest}.{extension}"
    )


class Storage(ABC):
    @abstractmethod
    def put(self, key: str, body: bytes, content_type: str = "application/json") -> str:
        """Write bytes at key. Returns the key."""

    @abstractmethod
    def get(self, key: str) -> bytes:
        """Read bytes back."""

    @abstractmethod
    def exists(self, key: str) -> bool:
        ...

    def put_json(self, source_id: str, payload: object,
                 when: datetime | None = None) -> tuple[str, bytes]:
        body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
        key = build_key(source_id, body, "json", when)
        self.put(key, body)
        return key, body

    def get_json(self, key: str):
        return json.loads(self.get(key))


class LocalStorage(Storage):
    """Filesystem backend. Same key layout as S3, so nothing downstream
    can tell the difference."""

    def __init__(self, root: str | None = None):
        self.root = Path(root or settings.local_storage_root)

    def _path(self, key: str) -> Path:
        return self.root / key

    def put(self, key: str, body: bytes, content_type: str = "application/json") -> str:
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return key

    def get(self, key: str) -> bytes:
        return self._path(key).read_bytes()

    def exists(self, key: str) -> bool:
        return self._path(key).exists()


class S3Storage(Storage):
    """S3 backend. boto3 is imported lazily so the package works, and the
    tests run, without AWS credentials present."""

    def __init__(self, bucket: str | None = None, region: str | None = None):
        import boto3  # noqa: PLC0415 - deliberate lazy import

        self.bucket = bucket or settings.s3_bucket
        self.client = boto3.client("s3", region_name=region or settings.s3_region)

    def put(self, key: str, body: bytes, content_type: str = "application/json") -> str:
        self.client.put_object(
            Bucket=self.bucket, Key=key, Body=body, ContentType=content_type
        )
        return key

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def exists(self, key: str) -> bool:
        from botocore.exceptions import ClientError  # noqa: PLC0415

        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError:
            return False


def get_storage() -> Storage:
    if settings.storage_backend == "s3":
        return S3Storage()
    return LocalStorage()
