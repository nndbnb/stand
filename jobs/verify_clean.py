#!/usr/bin/env python3
"""ЛР3: проверка clean Parquet в SeaweedFS и отсечения партиций DuckDB.

Запуск:
  python3 jobs/verify_clean.py --day 2026-10-19
Проверка не меняет данные и работает без секретов в Git.
"""
from __future__ import annotations

import argparse
import subprocess
from pathlib import Path

from clean import read_env, quote

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", default="2026-10-19")
    args = parser.parse_args()
    env = read_env()
    url = env.get("S3_ENDPOINT", "http://s3:9000")
    endpoint = url.replace("http://", "").replace("https://", "")
    ssl = "true" if url.startswith("https://") else "false"
    files = f"s3://{env['S3_BUCKET']}/clean/event_date=*/*.parquet"
    day = args.day
    from datetime import date
    date.fromisoformat(day)
    sql = f"""LOAD httpfs;
CREATE OR REPLACE SECRET lr3_verify_s3 (
 TYPE s3, KEY_ID {quote(env['S3_ACCESS_KEY'])},
 SECRET {quote(env['S3_SECRET_KEY'])},
 ENDPOINT {quote(endpoint)}, URL_STYLE 'path',
 USE_SSL {ssl}, REGION {quote(env.get('S3_REGION', 'stand'))}
);
SET TimeZone='UTC';
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT event_date) AS partitions
FROM read_parquet({quote(files)}, hive_partitioning=true);
SELECT COUNT(*) AS day_rows,
       COUNT(DISTINCT event_key) AS unique_keys,
       COUNT(*) FILTER (
         WHERE event_key IS NULL OR event_time IS NULL
            OR CAST(event_time AS DATE) <> event_date
       ) AS invalid_rows
FROM read_parquet({quote(files)}, hive_partitioning=true)
WHERE event_date=DATE {quote(day)};
EXPLAIN ANALYZE SELECT COUNT(*) AS rows, AVG(LAST) AS avg_last
FROM read_parquet({quote(files)}, hive_partitioning=true);
EXPLAIN ANALYZE SELECT COUNT(*) AS rows, AVG(LAST) AS avg_last
FROM read_parquet({quote(files)}, hive_partitioning=true)
WHERE event_date=DATE {quote(day)};
"""
    result = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "duckdb",
         "-bail", "-f", "/dev/stdin"], cwd=ROOT,
        input=sql, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        check=False,
    )
    print(result.stdout)
    if result.returncode:
        raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
