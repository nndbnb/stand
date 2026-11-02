#!/usr/bin/env python3
"""ЛР4: загрузка clean.events из S3 в Lakekeeper/Iceberg через DuckDB.

  python3 jobs/iceberg_load.py --day 2026-10-19
  python3 jobs/iceberg_load.py --all --dry-run
  python3 jobs/iceberg_load.py --all

Сутки до последней имеющейся партиции загружаются одним интервалом;
последние — по часам (24 попытки, пустые интервалы учитываются как ноль).
Повторная обработка удаляет строки только выбранного интервала.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / "jobs/iceberg_load.sql"
DAY_RE = re.compile(r"^clean/event_date=(\d{4}-\d{2}-\d{2})/part-00000\.parquet$")


def read_env():
    variables = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            variables[k.strip()] = v.strip().strip('"').strip("'")
    return variables


def make_client(env):
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


def list_days(s3, bucket):
    dates = set()
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="clean/event_date="
    ):
        for obj in page.get("Contents", []):
            match = DAY_RE.fullmatch(obj["Key"])
            if match:
                dates.add(match.group(1))
    return sorted(dates)


def get_intervals(day, hourly):
    base = datetime.combine(date.fromisoformat(day), datetime.min.time())
    if hourly:
        for h in range(24):
            start = base + timedelta(hours=h)
            yield start, start + timedelta(hours=1)
    else:
        yield base, base + timedelta(days=1)


def render(start, end, src):
    sql = TEMPLATE.read_text(encoding="utf-8")
    for key, val in {
        "__START__": start.strftime("%Y-%m-%d %H:%M:%S"),
        "__END__": end.strftime("%Y-%m-%d %H:%M:%S"),
        "__SOURCE__": src,
    }.items():
        if key not in sql:
            raise RuntimeError("Отсутствует шаблон " + key)
        sql = sql.replace(key, val)
    return sql


def run_sql(sql):
    proc = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "duckdb",
         "-init", "init.sql", "-bail", "-f", "/dev/stdin"],
        cwd=ROOT, input=sql, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
    )
    if proc.returncode:
        raise RuntimeError(proc.stdout[-5000:])
    return proc.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--day", help="YYYY-MM-DD")
    group.add_argument("--all", action="store_true")
    parser.add_argument("--hourly", action="store_true",
                        help="Загрузка выбранного дня по часам")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = read_env()
    bucket = env.get("S3_BUCKET", "lake")
    days = list_days(make_client(env), bucket)
    if not days:
        parser.error("Нет партиций clean.events в S3")
    if args.day:
        date.fromisoformat(args.day)
        if args.day not in days:
            parser.error("Нет clean.events за " + args.day)
        days = [args.day]
    if not (ROOT / "data/duckdb/init.sql").is_file() and not args.dry_run:
        parser.error("Нет data/duckdb/init.sql")
    completed = 0
    for day in days:
        hourly = args.hourly or (args.all and day == days[-1])
        src = f"s3://{bucket}/clean/event_date={day}/part-00000.parquet"
        for start, end in get_intervals(day, hourly):
            label = start.strftime("%Y-%m-%d %H:%M") if hourly else day
            print(("PLAN" if args.dry_run else "RUN") + " " + label, flush=True)
            if not args.dry_run:
                try:
                    print(run_sql(render(start, end, src))[-1600:], flush=True)
                except RuntimeError as exc:
                    print("ERROR " + label + ": " + str(exc), file=sys.stderr)
                    return 1
            completed += 1
    print(f"SUMMARY intervals={completed} errors=0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
