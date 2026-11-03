#!/usr/bin/env python3
"""ЛР3: очищенный исторический срез MOEX (candles), отдельно от marketdata.

Условия варианта №12: история должна быть обработана, но свечи имеют отличную
схему и не должны попадать в clean.events. Пишем clean/history/event_date=D.
"""
from __future__ import annotations

import argparse
import re
import subprocess
import sys
from datetime import date
from pathlib import Path

from clean import read_env, quote, s3_client

ROOT = Path(__file__).resolve().parents[1]
PAT = re.compile(r"^raw/history/dt=(\d{4}-\d{2}-\d{2})/candles-.*\.jsonl\.gz$")


def days_in_s3(client, bucket, prefix):
    days = set()
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=f"{prefix}/history/dt="
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            marker = f"{prefix}/history/dt="
            if not key.startswith(marker) or not key.endswith(".jsonl.gz"):
                continue
            rest = key[len(marker):]
            day, _, name = rest.partition("/")
            if name.startswith("candles-"):
                try:
                    date.fromisoformat(day)
                except ValueError:
                    continue
                days.add(day)
    return sorted(days)


def make_sql(env, day):
    bucket = env.get("S3_BUCKET", "lake")
    prefix = env.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/")
    source = f"s3://{bucket}/{prefix}/history/dt={day}/candles-*.jsonl.gz"
    dest = f"s3://{bucket}/clean/history/event_date={day}/part-00000.parquet"
    endpoint = env.get("S3_ENDPOINT", "http://s3:9000")
    sql = f"""
LOAD httpfs;
CREATE OR REPLACE SECRET lr3_history_s3
(TYPE s3, KEY_ID {quote(env['S3_ACCESS_KEY'])},
 SECRET {quote(env['S3_SECRET_KEY'])},
 ENDPOINT {quote(endpoint.replace('http://','').replace('https://',''))},
 URL_STYLE 'path', USE_SSL {'true' if endpoint.startswith('https://') else 'false'},
 REGION {quote(env.get('S3_REGION','stand'))});
SET TimeZone='UTC';
SET threads=2;
SET memory_limit='2048MB';
COPY (
WITH src AS (
 SELECT *, filename AS _file
 FROM read_json({quote(source)},
 columns={{
   'open':'DOUBLE','close':'DOUBLE','high':'DOUBLE','low':'DOUBLE',
   'value':'DOUBLE','volume':'BIGINT','begin':'VARCHAR','end':'VARCHAR',
   'SECID':'VARCHAR','BOARDID':'VARCHAR','event_key':'VARCHAR',
   'event_time':'VARCHAR','received_at':'VARCHAR','source':'VARCHAR'}},
 format='newline_delimited', hive_partitioning=false, filename=true)
), typed AS (
 SELECT "open","close","high","low","value","volume","begin","end",
        "SECID","BOARDID","event_key",
        CAST(TRY_CAST("event_time" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS event_time,
        CAST(TRY_CAST("received_at" AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS TIMESTAMP_MS) AS received_at,
        "source", _file
 FROM src
), ranked AS (
 SELECT *, ROW_NUMBER() OVER
   (PARTITION BY event_key ORDER BY received_at DESC NULLS LAST, _file DESC) AS rn
 FROM typed
 WHERE event_key IS NOT NULL AND event_key <> ''
   AND event_time IS NOT NULL AND SECID IS NOT NULL
   AND source='moex' AND CAST(event_time AS DATE)=DATE {quote(day)}
)
SELECT "open","close","high","low","value","volume","begin","end",
       "SECID","BOARDID","event_key",event_time,received_at,"source",
       CAST(event_time AS DATE) AS event_date
FROM ranked WHERE rn=1
) TO {quote(dest)}
(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 32768, OVERWRITE_OR_IGNORE TRUE);
SELECT COUNT(*) AS rows,COUNT(DISTINCT event_key) AS unique_keys
FROM read_parquet({quote(dest)}, hive_partitioning=true);
"""
    return source, dest, sql


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    grp = parser.add_mutually_exclusive_group(required=True)
    grp.add_argument("--day")
    grp.add_argument("--all", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    env = read_env()
    days = days_in_s3(s3_client(env), env.get("S3_BUCKET", "lake"),
                      env.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/"))
    targets = days if args.all else [args.day]
    errors = 0
    for day in targets:
        try:
            date.fromisoformat(day)
            if day not in days:
                raise ValueError(f"В S3 нет сырья исторического среза за {day}")
            source, dest, sql = make_sql(env, day)
            print(f"{'PLAN' if args.dry_run else 'RUN'} history {day}: {source} -> {dest}", flush=True)
            if args.dry_run:
                continue
            proc = subprocess.run(["docker","compose","run","--rm","-T","duckdb",
                "-bail","-f","/dev/stdin"], cwd=ROOT, input=sql,
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if proc.returncode:
                raise RuntimeError(proc.stdout[-2500:])
            print(proc.stdout[-600:],flush=True)
        except (ValueError,RuntimeError,KeyError,OSError) as exc:
            errors += 1
            print(f"ERROR history {day}: {exc}",file=sys.stderr)
    print(f"HISTORY SUMMARY days={len(targets)} errors={errors}")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
