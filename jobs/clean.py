#!/usr/bin/env python3
"""ЛР3: выполнить jobs/clean.sql для одних UTC-суток или всех завершённых суток.

  python3 jobs/clean.py --day 2026-10-19
  python3 jobs/clean.py --all
  python3 jobs/clean.py --all --dry-run

Читает только S3, не удаляет raw или Parquet ЛР2. Требует запущенный SeaweedFS
и настроенный .env. Обязательно наличие закрытых объектов D и D+1 в S3.
Наличие файлов D+1 не доказывает полную доставку всех файлов суток.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys

import boto3
from botocore.config import Config
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "jobs/clean.sql"
DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")


def read_env():
    variables = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        variables[k.strip()] = v.strip().strip('"').strip("'")
    return variables


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def s3_client(env):
    endpoint = env.get("S3_ENDPOINT", "http://s3:9000")
    if endpoint == "http://s3:9000":
        endpoint = "http://localhost:9000"
    return boto3.client(
        "s3", endpoint_url=endpoint,
        region_name=env.get("S3_REGION", "stand"),
        aws_access_key_id=env["S3_ACCESS_KEY"],
        aws_secret_access_key=env["S3_SECRET_KEY"],
        config=Config(signature_version="s3v4",
                      s3={"addressing_style": "path"}),
    )


def list_stream_days(client, env):
    """Source of truth: closed *.jsonl.gz objects in S3, not local raw/."""
    bucket = env.get("S3_BUCKET", "lake")
    prefix = env.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/") + "/stream/dt="
    days = set()
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            rest = key[len(prefix):]
            day, sep, tail = rest.partition("/")
            if (sep and DATE_PATTERN.fullmatch(day)
                    and tail.startswith("hour=") and key.endswith(".jsonl.gz")):
                try:
                    date.fromisoformat(day)
                except ValueError:
                    continue
                days.add(day)
    return sorted(days)


def prepare(day, env, available_days):
    d = date.fromisoformat(day)
    tomorrow = (d + timedelta(days=1)).isoformat()
    if d >= datetime.now(timezone.utc).date():
        raise ValueError(f"Дата {day} ещё не закрыта в UTC")
    if day not in available_days:
        raise ValueError(f"В S3 нет закрытого сырья дня {day}")
    if tomorrow not in available_days:
        raise ValueError(f"В S3 нет закрытого сырья D+1 ({tomorrow}); партиция не публикуется")
    prefix = env.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/")
    bucket = env.get("S3_BUCKET", "lake")
    source_paths = [
        f"s3://{bucket}/{prefix}/stream/dt={part}/hour=*/*.jsonl.gz"
        for part in (day, tomorrow)
    ]
    sql = TEMPLATE.read_text(encoding="utf-8")
    assert "__RAW_FILES__" in sql and "__OUT_OBJECT__" in sql
    output = f"s3://{bucket}/clean/event_date={day}/part-00000.parquet"
    rendered = (sql.replace("__RAW_FILES__", ", ".join(quote(x) for x in source_paths))
               .replace("__DAY__", day)
               .replace("__NEXT_DAY__", tomorrow)
               .replace("__OUT_OBJECT__", output))
    return source_paths, output, rendered


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    p = parser.add_mutually_exclusive_group(required=True)
    p.add_argument("--day", help="YYYY-MM-DD")
    p.add_argument("--all", action="store_true", help="Все закрытые дни с raw/stream")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = read_env()
    if env.get("S3") != "seaweedfs":
        parser.error("Для этого варианта ожидается S3=seaweedfs в .env")
    available_days = set(list_stream_days(s3_client(env), env))
    days = sorted(available_days) if args.all else [args.day]
    if not days:
        parser.error("Не найдено закрытых raw.stream дней")

    init = f"""LOAD httpfs;
CREATE OR REPLACE SECRET lr3_s3
(TYPE s3, KEY_ID {quote(env['S3_ACCESS_KEY'])},
 SECRET {quote(env['S3_SECRET_KEY'])},
 ENDPOINT {quote(env.get('S3_ENDPOINT','http://s3:9000').replace('http://','').replace('https://',''))},
 URL_STYLE 'path', USE_SSL false, REGION {quote(env.get('S3_REGION','stand'))});
"""
    error_count = 0
    skipped = 0
    for day in days:
        if date.fromisoformat(day) >= datetime.now(timezone.utc).date():
            print(f"SKIP {day}: текущие сутки UTC", flush=True)
            continue
        try:
            sources, output, sql = prepare(day, env, available_days)
        except ValueError as exc:
            if args.all:
                skipped += 1
                print(f"SKIP {day}: {exc}", flush=True)
                continue
            print(f"ERROR {day}: {exc}", file=sys.stderr)
            return 1
        try:
            print(f"{'PLAN' if args.dry_run else 'RUN'} {day}: "
                  f"raw_days={len(sources)} -> {output}", flush=True)
            if args.dry_run:
                for source in sources:
                    print("  ", source)
                continue
            result = subprocess.run(
                ["docker", "compose", "run", "--rm", "-T", "duckdb",
                 "-bail", "-f", "/dev/stdin"],
                cwd=ROOT, input=init + sql, text=True, stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT, check=False,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stdout[-4000:])
            print(result.stdout[-1800:], flush=True)
            print(f"OK {day}", flush=True)
        except (ValueError, RuntimeError, OSError, KeyError) as exc:
            error_count += 1
            print(f"ERROR {day}: {exc}", file=sys.stderr, flush=True)
            if not args.all:
                return 1
    print(f"SUMMARY days={len(days)} skipped={skipped} errors={error_count}")
    return 1 if error_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
