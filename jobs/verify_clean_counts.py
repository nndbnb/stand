#!/usr/bin/env python3
"""ЛР3 п.8: read-only comparison raw D/D+1 -> clean D, with rejection counts."""
import argparse
import subprocess
from datetime import date, timedelta
from pathlib import Path
from clean import read_env, quote, s3_client, list_stream_days

ROOT = Path(__file__).resolve().parents[1]


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--day", default="2026-10-19")
    args = ap.parse_args()
    d = date.fromisoformat(args.day)
    d2 = (d + timedelta(days=1)).isoformat()
    env = read_env()
    days = set(list_stream_days(s3_client(env), env))
    if args.day not in days or d2 not in days:
        ap.error("For a valid comparison raw D AND raw D+1 must exist in S3")
    prefix = env.get("COLLECTOR_LAKE_PREFIX","raw").strip("/")
    bucket = env.get("S3_BUCKET","lake")
    paths = ", ".join(quote(f"s3://{bucket}/{prefix}/stream/dt={day}/hour=*/*.jsonl.gz")
                      for day in (args.day,d2))
    clean = f"s3://{bucket}/clean/event_date={args.day}/part-00000.parquet"
    endpoint = env.get("S3_ENDPOINT","http://s3:9000")
    sql = f"""
LOAD httpfs;
CREATE OR REPLACE SECRET lr34_counts
(TYPE s3, KEY_ID {quote(env['S3_ACCESS_KEY'])},
 SECRET {quote(env['S3_SECRET_KEY'])},
 ENDPOINT {quote(endpoint.replace('http://','').replace('https://',''))},
 URL_STYLE 'path', USE_SSL {'true' if endpoint.startswith('https://') else 'false'},
 REGION {quote(env.get('S3_REGION','stand'))});
SET TimeZone='UTC';
WITH raw AS (
 SELECT event_key,
        TRY_CAST(event_time AS TIMESTAMPTZ) AT TIME ZONE 'UTC' AS ts,
        source, SECID, BOARDID, LAST, BID, OFFER, QTY, VALUE
 FROM read_json([{paths}], columns={{
 'event_key':'VARCHAR','event_time':'VARCHAR','source':'VARCHAR',
 'SECID':'VARCHAR','BOARDID':'VARCHAR','LAST':'DOUBLE',
 'BID':'DOUBLE','OFFER':'DOUBLE','QTY':'BIGINT','VALUE':'DOUBLE'}},
 format='newline_delimited', hive_partitioning=false)
), classified AS (
 SELECT *, (ts>=TIMESTAMP '{args.day} 00:00:00'
             AND ts<TIMESTAMP '{d2} 00:00:00') AS day_match,
        (event_key IS NOT NULL AND event_key<>''
         AND ts IS NOT NULL AND SECID IS NOT NULL AND SECID<>''
         AND source='moex' AND BOARDID='TQBR'
         AND (LAST IS NOT NULL OR BID IS NOT NULL OR OFFER IS NOT NULL
              OR QTY IS NOT NULL OR VALUE IS NOT NULL)) AS valid_content
 FROM raw
)
SELECT COUNT(*) AS input_raw_D_and_D1,
       COUNT(*) FILTER (WHERE day_match) AS event_day_rows,
       COUNT(*) FILTER (WHERE day_match AND NOT valid_content) AS invalid_or_empty,
       COUNT(*) FILTER (WHERE day_match AND valid_content) AS eligible_rows,
       COUNT(DISTINCT event_key) FILTER (WHERE day_match AND valid_content) AS expected_unique_keys,
       (SELECT COUNT(*) FROM read_parquet({quote(clean)},hive_partitioning=true)) AS actual_clean_rows,
       (SELECT COUNT(DISTINCT event_key) FROM read_parquet({quote(clean)},hive_partitioning=true)) AS actual_clean_unique_keys
FROM classified;
"""
    result = subprocess.run(["docker","compose","run","--rm","-T","duckdb","-bail","-f","/dev/stdin"],
                            cwd=ROOT,input=sql,text=True,capture_output=True)
    print(result.stdout)
    if result.returncode:
        raise SystemExit(result.stderr[-2500:])


if __name__ == "__main__":
    main()
