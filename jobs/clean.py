#!/usr/bin/env python3
"""ЛР3: выполнить jobs/clean.sql для одних UTC-суток или всех завершённых суток.

  python3 jobs/clean.py --day 2026-10-19
  python3 jobs/clean.py --all
  python3 jobs/clean.py --all --dry-run

Читает только S3, не удаляет raw или Parquet ЛР2. Требует запущенный SeaweedFS
и настроенный .env. Срок закрытия дня: D+1 00:00 UTC.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/stream"
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


def candidates():
    for p in sorted(RAW.glob("dt=*")):
        day = p.name.removeprefix("dt=")
        if DATE_PATTERN.fullmatch(day) and any(p.glob("hour=*/*.jsonl.gz")):
            yield day


def prepare(day, env):
    d = date.fromisoformat(day)
    if d >= datetime.now(timezone.utc).date():
        raise ValueError(f"Дата {day} ещё не закрыта в UTC")
    source_days = (d, d + timedelta(days=1))
    source_paths = []
    for datepart in source_days:
        folder = RAW / f"dt={datepart.isoformat()}"
        if any(folder.glob("hour=*/*.jsonl.gz")):
            source_paths.append(
                f"s3://{env.get('S3_BUCKET', 'lake')}/"
                f"{env.get('COLLECTOR_LAKE_PREFIX', 'raw').strip('/')}/"
                f"stream/dt={datepart.isoformat()}/hour=*/*.jsonl.gz"
            )
    if not source_paths:
        raise ValueError(f"Нет закрытых файлов получения для {day} и следующего дня")
    sql = TEMPLATE.read_text(encoding="utf-8")
    assert "__RAW_FILES__" in sql and "__OUT_OBJECT__" in sql
    output = f"s3://{env.get('S3_BUCKET','lake')}/clean/event_date={day}/part-00000.parquet"
    rendered = (sql.replace("__RAW_FILES__", ", ".join(quote(x) for x in source_paths))
               .replace("__DAY__", day)
               .replace("__NEXT_DAY__", source_days[1].isoformat())
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
    days = list(candidates()) if args.all else [args.day]
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
    for day in days:
        if date.fromisoformat(day) >= datetime.now(timezone.utc).date():
            print(f"SKIP {day}: текущие сутки UTC", flush=True)
            continue
        try:
            sources, output, sql = prepare(day, env)
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
    print(f"SUMMARY days={len(days)} errors={error_count}")
    return 1 if error_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
