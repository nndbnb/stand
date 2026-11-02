#!/usr/bin/env python3
"""ЛР3: сверка закрытых raw-файлов на диске с SeaweedFS.

Запуск: docker compose run --rm -T --no-deps --entrypoint python collector /app/src/lake_audit.py
Не изменяет ни локальные файлы, ни S3-объекты.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from lake_sync import make_s3_client


def main() -> int:
    root = Path(os.environ.get("COLLECTOR_RAW_DIR", "/data/raw"))
    bucket = os.environ.get("S3_BUCKET", "lake")
    prefix = os.environ.get("COLLECTOR_LAKE_PREFIX", "").strip().strip("/")
    if not prefix:
        print("ERROR: COLLECTOR_LAKE_PREFIX не задан", file=sys.stderr)
        return 2
    client = make_s3_client()

    local: dict[str, int] = {}
    for layer in ("stream", "history"):
        for path in (root / layer).rglob("*.jsonl.gz"):
            if path.is_file():
                key = prefix + "/" + path.relative_to(root).as_posix()
                local[key] = path.stat().st_size

    remote: dict[str, int] = {}
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix + "/"
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            remote[key] = int(obj["Size"])

    missing = sorted(set(local) - set(remote))
    unexpected = sorted(set(remote) - set(local))
    mismatched = sorted(k for k in local.keys() & remote.keys()
                        if local[k] != remote[k])
    print(f"LOCAL objects={len(local)} bytes={sum(local.values())}")
    print(f"S3    objects={len(remote)} bytes={sum(remote.values())}")
    print(f"MISSING={len(missing)} MISMATCHED_SIZE={len(mismatched)} "
          f"EXTRA={len(unexpected)}")
    for title, values in (("MISSING", missing), ("MISMATCHED_SIZE", mismatched),
                          ("EXTRA", unexpected)):
        if values:
            print(f"{title} examples:")
            for value in values[:5]:
                print(" ", value)
    # EXTRA допустимы, когда локальная retention уже удалила давние файлы.
    if missing or mismatched:
        print("RESULT: INCOMPLETE")
        return 1
    print("RESULT: OK (локальное закрытое сырьё доступно в озере)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
