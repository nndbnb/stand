"""ЛР3: фоновая синхронизация закрытых JSONL.gz в S3-совместимое озеро.

При COLLECTOR_LAKE_PREFIX=raw:
  data/raw/stream/dt=.../hour=.../part-*.jsonl.gz
    -> s3://lake/raw/stream/dt=.../hour=.../part-*.jsonl.gz
  data/raw/history/dt=.../candles-*.jsonl.gz
    -> s3://lake/raw/history/dt=.../candles-*.jsonl.gz

Только завершённые .jsonl.gz, никаких .tmp или WAL. Повторное сканирование
не перезаливает существующий объект с тем же размером.
"""

from __future__ import annotations

import os
import threading
from pathlib import Path
from typing import Callable

import boto3
from botocore.client import Config as S3Config
from botocore.exceptions import ClientError, BotoCoreError


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "http://s3:9000"),
        region_name=os.environ.get("S3_REGION", "stand"),
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_KEY"],
        config=S3Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=5, read_timeout=30,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


class LakeSync:
    def __init__(
        self,
        raw_dir: Path,
        stop: threading.Event,
        log: Callable[..., None],
        interval: int = 30,
    ) -> None:
        self.raw_dir = raw_dir
        self.stop = stop
        self.log = log
        self.interval = interval
        prefix = os.environ.get("COLLECTOR_LAKE_PREFIX", "").strip().strip("/")
        if ".." in prefix.split("/") or not prefix:
            raise ValueError("COLLECTOR_LAKE_PREFIX должен быть непустым безопасным префиксом")
        self.prefix = prefix
        self.bucket = os.environ.get("S3_BUCKET", "lake")
        self.client = make_s3_client()
        self.synced: dict[str, tuple[int, int]] = {}
        self.thread: threading.Thread | None = None

    def iter_files(self):
        for layer in ("stream", "history"):
            root = self.raw_dir / layer
            if root.is_dir():
                yield from root.rglob("*.jsonl.gz")

    def object_key(self, path: Path) -> str:
        return self.prefix + "/" + path.relative_to(self.raw_dir).as_posix()

    def sync_one(self, path: Path) -> bool:
        try:
            if not path.is_file():
                return False
            info = path.stat()
            key = self.object_key(path)
            signature = (info.st_size, info.st_mtime_ns)
            if self.synced.get(key) == signature:
                return False
            try:
                existing = self.client.head_object(Bucket=self.bucket, Key=key)
            except ClientError as exc:
                status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                if status != 404:
                    raise
            else:
                if int(existing["ContentLength"]) != info.st_size:
                    raise ValueError(
                        f"S3 key конфликтует с локальным размером: {key}; "
                        f"local={info.st_size}, remote={existing['ContentLength']}"
                    )
                self.synced[key] = signature
                return False

            # Сырые объекты иммутабельны после закрытия сегмента.
            self.client.upload_file(str(path), self.bucket, key)
            existing = self.client.head_object(Bucket=self.bucket, Key=key)
            if int(existing["ContentLength"]) != info.st_size:
                raise RuntimeError(f"Размер объекта после загрузки не совпадает: {key}")
            if path.stat().st_size != info.st_size:
                raise RuntimeError(f"Файл изменился при загрузке: {path}")
            self.synced[key] = signature
            self.log("info", "lake_uploaded", object_key=key, bytes=info.st_size)
            return True
        except (ClientError, BotoCoreError, OSError, ValueError, RuntimeError) as exc:
            self.log("error", "lake_sync_failed", file=str(path), error=str(exc))
            return False

    def scan_once(self) -> tuple[int, int]:
        total = uploaded = 0
        for path in self.iter_files():
            if self.stop.is_set():
                break
            total += 1
            if self.sync_one(path):
                uploaded += 1
        return total, uploaded

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                found, uploaded = self.scan_once()
                self.log("info", "lake_sync_scan", files=found, uploaded=uploaded)
            except (ClientError, BotoCoreError, OSError) as exc:
                self.log("error", "lake_scan_failed", error=str(exc))
            self.stop.wait(self.interval)

    def start(self) -> None:
        self.thread = threading.Thread(target=self.run, daemon=True, name="lake-sync")
        self.thread.start()

    def close(self) -> None:
        if self.thread is not None:
            self.thread.join(timeout=5)
