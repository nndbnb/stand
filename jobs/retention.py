#!/usr/bin/env python3
"""ЛР3: политика хранения объектов SeaweedFS (S3 API).

Примеры:
  python3 jobs/retention.py --layer clean --days 2 --dry-run
  python3 jobs/retention.py --layer clean --days 90 --dry-run
  python3 jobs/retention.py --layer clean --days 90 --apply

Без --apply ничего не удаляет. Защитные ограничения:
* только префиксы raw/stream, raw/history, clean;
* дата извлекается из dt=YYYY-MM-DD или event_date=YYYY-MM-DD;
* объекты без даты или в текущей партиции не удаляются;
* политикой по умолчанию управляет оператор, а не работающий сборщик.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, date, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
PARTITION_RE = re.compile(r"(?:^|/)(?:dt|event_date)=(\d{4}-\d{2}-\d{2})(?:/|$)")
LAYERS = {
    "raw.stream": "raw/stream/",
    "raw.history": "raw/history/",
    "clean": "clean/",
}


def read_env():
    env = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def make_client(env):
    return boto3.client(
        "s3",
        endpoint_url=env.get("S3_ENDPOINT", "http://localhost:9000")
        .replace("http://s3:9000", "http://localhost:9000"),
        region_name=env.get("S3_REGION", "stand"),
        aws_access_key_id=env["S3_ACCESS_KEY"],
        aws_secret_access_key=env["S3_SECRET_KEY"],
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def plan(objects, prefix, cutoff):
    result = []
    for obj in objects:
        key = obj["Key"]
        if not key.startswith(prefix):
            continue
        match = PARTITION_RE.search(key)
        if not match:
            continue
        try:
            day = date.fromisoformat(match.group(1))
        except ValueError:
            continue
        if day < cutoff:
            result.append({"Key": key, "Size": int(obj.get("Size", 0)), "Day": day})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=list(LAYERS), required=True)
    parser.add_argument("--days", type=int, required=True, help="Хранить последние N суток")
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--dry-run", action="store_true")
    action.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.days <= 0:
        parser.error("--days должно быть положительным")
    env = read_env()
    bucket = env.get("S3_BUCKET", "lake")
    prefix = LAYERS[args.layer]
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=args.days)
    client = make_client(env)
    objects = [obj for pg in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix) for obj in pg.get("Contents", [])]
    doomed = plan(objects, prefix, cutoff)
    print(f"LAYER={args.layer} PREFIX={prefix} RETAIN_DAYS={args.days} "
          f"CUTOFF={cutoff} MODE={'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"OBJECTS_SCANNED={len(objects)} OBJECTS_SELECTED={len(doomed)} "
          f"BYTES_SELECTED={sum(x['Size'] for x in doomed)}")
    for obj in doomed[:30]:
        print(f"  {obj['Day']}: {obj['Key']} ({obj['Size']} bytes)")
    if len(doomed) > 30:
        print(f"  ...ещё {len(doomed)-30} объектов")
    if args.apply:
        # По 1000 объектов — лимит DeleteObjects API.
        for i in range(0, len(doomed), 1000):
            response = client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": x["Key"]} for x in doomed[i:i+1000]],
                        "Quiet": True},
            )
            if response.get("Errors"):
                raise RuntimeError(f"Не удалены объекты: {response['Errors'][:5]}")
        print(f"DELETED={len(doomed)}")
    else:
        print("RESULT: DRY-RUN, ничего не удалено")
    return 0


if __name__ == "__main__":
    sys.exit(main())
