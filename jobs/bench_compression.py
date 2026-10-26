#!/usr/bin/env python3
"""ЛР2 / вариант 12: сравнение 4 кодеков Parquet × 3 ROW_GROUP_SIZE.

Работает на ОДНОЙ И ТОЙ ЖЕ уже проверенной порции 61 644 событий v1.
Запуск из ~/stand:
    python3 jobs/bench_compression.py --dry-run
    python3 jobs/bench_compression.py --limit 1
    python3 jobs/bench_compression.py

Результаты: data/duckdb/bench/results.csv, row_groups.csv, *.parquet, *.log.
Основные parquet.events и parquet.candles НЕ изменяются.
"""

from __future__ import annotations

import argparse
import csv
import re
import statistics
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_DIR = ROOT / "data" / "duckdb"
BENCH = DUCKDB_DIR / "bench"
SOURCE = (
    DUCKDB_DIR / "parquet" / "events" / "event_date=2026-10-19"
    / "recv-20261019-16_0.parquet"
)
SOURCE_IN_CONTAINER = (
    "/data/duckdb/parquet/events/event_date=2026-10-19/"
    "recv-20261019-16_0.parquet"
)
CODECS = ("uncompressed", "snappy", "gzip", "zstd")
GROUP_SIZES = (2048, 8192, 32768)
EXPECTED_ROWS = 61644
TIME_RE = re.compile(r"Run Time \(s\): real ([0-9]+(?:\.[0-9]+)?)")


def run_duckdb(script: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "duckdb",
         "-bail", "-f", "/dev/stdin"],
        cwd=ROOT,
        input=script,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(
            f"DuckDB error={result.returncode}\n{result.stdout[-5000:]}"
        )
    return result.stdout


def build_sql(codec: str, group_size: int, filename: str) -> str:
    path = f"/data/duckdb/bench/{filename}"
    audit = f"/data/duckdb/bench/{codec}_{group_size}_audit.csv"
    meta = f"/data/duckdb/bench/{codec}_{group_size}_groups.csv"
    totals = f"/data/duckdb/bench/{codec}_{group_size}_aggregates.csv"
    query = f"""SELECT date_trunc('hour', event_time) AS event_hour_utc,
       BOARDID,
       count(*) AS events,
       count(LAST) AS priced_events,
       round(avg(LAST), 6) AS avg_last
FROM read_parquet('{path}')
GROUP BY 1, 2
ORDER BY 1, 2"""
    return f"""SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;

COPY (
  SELECT * FROM read_parquet(
    '{SOURCE_IN_CONTAINER}', hive_partitioning=true
  )
)
TO '{path}'
(FORMAT PARQUET, COMPRESSION {codec.upper()},
 ROW_GROUP_SIZE {group_size});

COPY (
  SELECT count(*) AS rows,
         count(DISTINCT event_key) AS unique_keys,
         count(LAST) AS priced_events
  FROM read_parquet('{path}')
)
TO '{audit}' (HEADER TRUE, DELIMITER ',');

COPY ({query})
TO '{totals}' (HEADER TRUE, DELIMITER ',');

COPY (
  SELECT row_group_id,
         max(row_group_num_rows) AS rows_in_group,
         max(row_group_bytes) AS logical_bytes,
         sum(total_compressed_size) AS compressed_column_bytes
  FROM parquet_metadata('{path}')
  GROUP BY row_group_id
  ORDER BY row_group_id
)
TO '{meta}' (HEADER TRUE, DELIMITER ',');

.timer on
{query};
{query};
{query};
"""


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def perform(codec: str, group_size: int, baseline: list[dict] | None):
    filename = f"{codec}_{group_size}.parquet"
    output = BENCH / filename
    # Только экспериментальный файл с точным именем; основной набор не трогать.
    output.unlink(missing_ok=True)
    script = build_sql(codec, group_size, filename)
    output_text = run_duckdb(script)
    (BENCH / f"{codec}_{group_size}.log").write_text(
        output_text, encoding="utf-8"
    )
    timings = [float(v) for v in TIME_RE.findall(output_text)]
    if len(timings) != 3:
        raise RuntimeError(
            f"Ожидалось 3 времени для {filename}, найдено {len(timings)}. "
            "Проверь .timer в DuckDB и *.log."
        )
    audit_rows = read_csv(BENCH / f"{codec}_{group_size}_audit.csv")
    if len(audit_rows) != 1:
        raise RuntimeError(f"Аудит {filename}: ожидалась одна строка")
    audit = audit_rows[0]
    rows, uniq = int(audit["rows"]), int(audit["unique_keys"])
    if rows != EXPECTED_ROWS or uniq != rows:
        raise RuntimeError(f"{filename}: rows={rows}, unique={uniq}")

    aggregates = read_csv(BENCH / f"{codec}_{group_size}_aggregates.csv")
    if baseline is not None and aggregates != baseline:
        raise RuntimeError(
            f"Несовпадение результатов аналитического запроса: {filename}"
        )
    if len(aggregates) != 2 or sum(int(r["events"]) for r in aggregates) != rows:
        raise RuntimeError(f"Некорректная группировка: {filename}")

    groups = read_csv(BENCH / f"{codec}_{group_size}_groups.csv")
    if not groups or sum(int(r["rows_in_group"]) for r in groups) != rows:
        raise RuntimeError(f"Некорректная метаинформация ROW_GROUP: {filename}")
    if not output.is_file():
        raise RuntimeError(f"Parquet не создан: {output}")

    return {
        "codec": codec,
        "row_group_size_requested": group_size,
        "file_bytes": output.stat().st_size,
        "file_mib": f"{output.stat().st_size / 1024 ** 2:.3f}",
        "rows": rows,
        "row_groups_actual": len(groups),
        "row_group_min_rows": min(int(r["rows_in_group"]) for r in groups),
        "row_group_max_rows": max(int(r["rows_in_group"]) for r in groups),
        "time_1_s": timings[0],
        "time_2_s": timings[1],
        "time_3_s": timings[2],
        "median_query_s": statistics.median(timings),
    }, aggregates, groups


def write_csv(path: Path, rows: list[dict]):
    if not rows:
        return
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=0,
                        help="Сколько комбинаций протестировать; 0 = все 12")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit не может быть отрицательным")
    jobs = [(c, g) for c in CODECS for g in GROUP_SIZES]
    if args.limit:
        jobs = jobs[:args.limit]
    print(f"Комбинаций: {len(jobs)}", flush=True)
    for codec, group in jobs:
        print(f"  {codec} / ROW_GROUP_SIZE={group}")
    if args.dry_run:
        return
    if not SOURCE.is_file():
        raise SystemExit(f"Нет контрольного Parquet v1: {SOURCE}")
    BENCH.mkdir(parents=True, exist_ok=True)
    measurements = []
    detail = []
    baseline = None
    for i, (codec, group) in enumerate(jobs, 1):
        print(f"[{i}/{len(jobs)}] {codec} / {group}", flush=True)
        try:
            result, current_aggregates, groups = perform(codec, group, baseline)
        except Exception as exc:
            print(f"ERROR {codec}/{group}: {exc}", file=sys.stderr, flush=True)
            raise SystemExit(1) from exc
        if baseline is None:
            baseline = current_aggregates
        measurements.append(result)
        for group_info in groups:
            detail.append({"codec": codec,
                           "row_group_size_requested": group, **group_info})
        write_csv(BENCH / "results.csv", measurements)
        write_csv(BENCH / "row_groups.csv", detail)
        print(
            f"OK {codec}/{group}: bytes={result['file_bytes']} "
            f"groups={result['row_groups_actual']} "
            f"median={result['median_query_s']:.6f}s",
            flush=True,
        )
    print(f"RESULT: OK, {len(measurements)} комбинаций, "
          f"таблица: {BENCH / 'results.csv'}", flush=True)
    print("Выбор кодека и ROW_GROUP_SIZE делаем по фактическим результатам.",
          flush=True)


if __name__ == "__main__":
    main()
