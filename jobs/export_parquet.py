#!/usr/bin/env python3
"""ЛР2, вариант 12: пакетное JSONL.gz -> Parquet через контейнер DuckDB.

Запуск на HOST Ubuntu из ~/stand:
  python3 jobs/export_parquet.py --dataset all --limit 2
  python3 jobs/export_parquet.py --dataset all

Не удаляет и не изменяет raw. Каждый UTC-час потока / день свечей
обрабатывается отдельно; файлы других часов/дней не удаляются.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
WORK = ROOT / "data" / "duckdb"
OUT = WORK / "parquet"
STAGING = WORK / ".parquet_stage"
AVRO = ROOT / "schemas" / "event.avsc"

CANDLE_COLUMNS = [
    ("open", "DOUBLE"), ("close", "DOUBLE"), ("high", "DOUBLE"),
    ("low", "DOUBLE"), ("value", "DOUBLE"), ("volume", "BIGINT"),
    ("begin", "VARCHAR"), ("end", "VARCHAR"), ("SECID", "VARCHAR"),
    ("BOARDID", "VARCHAR"), ("event_key", "VARCHAR"),
    ("event_time", "VARCHAR"), ("received_at", "VARCHAR"),
    ("source", "VARCHAR"),
]
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
HOUR_RE = re.compile(r"^\d{2}$")


def event_columns() -> list[tuple[str, str]]:
    with AVRO.open(encoding="utf-8") as file:
        schema = json.load(file)
    cols: list[tuple[str, str]] = []
    for field in schema["fields"]:
        name = field["name"]
        typ = field["type"]
        if isinstance(typ, list):
            typ = next((x for x in typ if x != "null"), None)
        if isinstance(typ, dict):
            typ = typ["type"]
        if typ == "long" or typ == "int":
            sql_type = "BIGINT"
        elif typ in ("double", "float"):
            sql_type = "DOUBLE"
        elif typ == "string":
            sql_type = "VARCHAR"
        else:
            raise ValueError(f"Неизвестный Avro тип {name}: {typ}")
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
            raise ValueError(f"Неожиданное имя поля: {name}")
        if name in ("event_time", "received_at"):
            sql_type = "VARCHAR"
        cols.append((name, sql_type))
    if len({name for name, _ in cols}) != len(cols):
        raise ValueError("Повторяющиеся имена в Avro-схеме")
    if not {"event_key", "event_time", "received_at"}.issubset(dict(cols)):
        raise ValueError("Не хватает обязательных служебных полей")
    return cols


def discover(dataset: str, start: str | None, end: str | None, selected_hour: str | None):
    groups: dict[tuple[str, str], list[Path]] = {}
    if dataset in ("events", "all"):
        for path in sorted((RAW / "stream").glob("dt=*/hour=*/part-*.jsonl.gz")):
            dt = path.parent.parent.name.removeprefix("dt=")
            hour = path.parent.name.removeprefix("hour=")
            if not DATE_RE.fullmatch(dt) or not HOUR_RE.fullmatch(hour):
                continue
            if not 0 <= int(hour) < 24:
                continue
            if selected_hour is not None and hour != selected_hour:
                continue
            # Не захватываем закрывающиеся прямо сейчас файлы сборщика.
            if (dt, hour) >= (datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                             datetime.now(timezone.utc).strftime("%H")):
                continue
            if start and dt < start or end and dt > end:
                continue
            groups.setdefault(("events", f"{dt}:{hour}"), []).append(path)
    if dataset in ("candles", "all"):
        for path in sorted((RAW / "history").glob("dt=*/candles-*.jsonl.gz")):
            dt = path.parent.name.removeprefix("dt=")
            if not DATE_RE.fullmatch(dt):
                continue
            if start and dt < start or end and dt > end:
                continue
            groups.setdefault(("candles", dt), []).append(path)
    return sorted(groups.items(), key=lambda item: item[0])


def sql_quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def make_sql(columns: list[tuple[str, str]], source: str, destination: str,
             pattern: str, audit: str) -> str:
    typed_fields = []
    for name, _ in columns:
        if name in ("event_time", "received_at"):
            typed_fields.append(
                f'CAST(CAST("{name}" AS TIMESTAMPTZ) AT TIME ZONE '
                f"'UTC' AS TIMESTAMP_MS) AS \"{name}\""
            )
        else:
            typed_fields.append(f'"{name}"')
    col_dict = ",\n         ".join(f"{sql_quote(n)}: {sql_quote(t)}" for n, t in columns)
    projection = ",\n         ".join(typed_fields)
    return f"""
SET TimeZone = 'UTC';
SET threads = 1;
SET memory_limit = '1024MB';
SET preserve_insertion_order = false;

COPY (
  WITH src AS (
    SELECT * FROM read_json(
      {sql_quote(source)},
      columns = {{
         {col_dict}
      }},
      format = 'newline_delimited',
      hive_partitioning = false
    )
  ), typed AS (
    SELECT {projection}
    FROM src
  )
  SELECT *, CAST(event_time AS DATE) AS event_date FROM typed
)
TO {sql_quote(destination)}
(
  FORMAT PARQUET,
  COMPRESSION ZSTD,
  PARTITION_BY (event_date),
  OVERWRITE_OR_IGNORE TRUE,
  FILENAME_PATTERN {sql_quote(pattern + "_{i}")},
  ROW_GROUP_SIZE 100000
);

COPY (
  SELECT count(*) AS rows,
         count(DISTINCT event_key) AS unique_keys,
         count(*) FILTER (
           WHERE event_key IS NULL OR event_key = ''
           OR event_time IS NULL
           OR CAST(event_time AS DATE) <> event_date
         ) AS bad_rows
  FROM read_parquet(
    {sql_quote(destination + "/event_date=*/*.parquet")},
    hive_partitioning = true
  )
) TO {sql_quote(audit)} (HEADER TRUE, DELIMITER ',');
"""


def signature(paths: list[Path]) -> list[tuple[str, int, int]]:
    return [(str(path), path.stat().st_size, path.stat().st_mtime_ns)
            for path in paths]


def input_count(paths: list[Path]) -> int:
    rows = 0
    for path in paths:
        with gzip.open(path, "rb") as src:
            for _ in src:
                rows += 1
    return rows


def batch_source(kind: str, ident: str):
    if kind == "events":
        day, hour = ident.split(":")
        source = f"/data/raw/stream/dt={day}/hour={hour}/part-*.jsonl.gz"
        pattern = f"recv-{day.replace('-', '')}-{hour}"
        return source, pattern
    day = ident
    return f"/data/raw/history/dt={day}/candles-*.jsonl.gz", (
        f"candles-{day.replace('-', '')}"
    )


def run_batch(kind: str, ident: str, paths: list[Path],
              columns: list[tuple[str, str]]) -> None:
    source, pattern = batch_source(kind, ident)
    stage = Path(tempfile.mkdtemp(prefix=f"lr2_{kind}_", dir=STAGING))
    try:
        stage_relative = stage.relative_to(WORK).as_posix()
        container_dest = "/data/duckdb/" + stage_relative + "/output"
        container_audit = "/data/duckdb/" + stage_relative + "/audit.csv"
        before = signature(paths)
        expected = input_count(paths)
        if expected == 0:
            raise ValueError("Пустой входной набор")
        sql = make_sql(columns, source, container_dest, pattern, container_audit)
        process = subprocess.run(
            ["docker", "compose", "run", "--rm", "-T", "duckdb",
             "-bail", "-f", "/dev/stdin"],
            cwd=ROOT, input=sql, text=True, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, check=False,
        )
        if process.returncode:
            raise RuntimeError(
                f"DuckDB завершился с кодом {process.returncode}:\n"
                + process.stdout[-4000:]
            )
        with (stage / "audit.csv").open(newline="", encoding="utf-8") as file:
            audits = list(csv.DictReader(file))
        if len(audits) != 1:
            raise RuntimeError("Ожидалась одна строка аудита Parquet")
        actual = int(audits[0]["rows"])
        unique = int(audits[0]["unique_keys"])
        bad = int(audits[0]["bad_rows"])
        if actual != expected or actual != unique or bad:
            raise RuntimeError(
                f"Сверка не прошла: raw={expected}, parquet={actual}, "
                f"unique={unique}, bad={bad}"
            )

        output_files = sorted((stage / "output").glob("event_date=*/*.parquet"))
        if not output_files:
            raise RuntimeError("DuckDB не создал файлы Parquet")
        # После чтения убеждаемся, что состав и размер закрытых входных файлов
        # не изменились в ходе преобразования.
        if kind == "events":
            day, hour = ident.split(":")
            current_paths = sorted((RAW / "stream" / f"dt={day}" /
                                    f"hour={hour}").glob("part-*.jsonl.gz"))
        else:
            current_paths = sorted((RAW / "history" /
                                    f"dt={ident}").glob("candles-*.jsonl.gz"))
        if signature(current_paths) != before:
            raise RuntimeError("Исходные файлы изменились во время обработки; повторить пакет")

        target = OUT / kind
        target.mkdir(parents=True, exist_ok=True)
        # Удаляем ТОЛЬКО результаты того же часа/дня, а не соседние пакеты.
        # Сначала создан и проверен полный staging-набор.
        existing = list(target.glob(f"event_date=*/{pattern}_*.parquet"))
        for path in existing:
            path.unlink()
        moved = []
        for staged in output_files:
            folder = target / staged.parent.name
            folder.mkdir(parents=True, exist_ok=True)
            dst = folder / staged.name
            os.replace(staged, dst)
            moved.append(dst)
        bytes_written = sum(p.stat().st_size for p in moved)
        print(f"OK {kind} {ident}: raw={expected} parquet={actual} "
              f"unique={unique} files={len(moved)} bytes={bytes_written}", flush=True)
    finally:
        shutil.rmtree(stage, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=("events", "candles", "all"),
                        default="all")
    parser.add_argument("--start-date", help="YYYY-MM-DD, включительно")
    parser.add_argument("--end-date", help="YYYY-MM-DD, включительно")
    parser.add_argument("--hour", help="HH; для events выбрать конкретный час UTC")
    parser.add_argument("--limit", type=int, default=0,
                        help="Максимум пакетов; 0 = весь архив")
    parser.add_argument("--dry-run", action="store_true",
                        help="Показать порции без изменения файлов")
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("--limit не может быть отрицательным")
    if args.hour is not None and not re.fullmatch(r"(0[0-9]|1[0-9]|2[0-3])", args.hour):
        parser.error("--hour: ожидается HH от 00 до 23")
    for value in (args.start_date, args.end_date):
        if value is not None:
            try:
                datetime.strptime(value, "%Y-%m-%d")
            except ValueError:
                parser.error("Неверный формат даты: ожидается YYYY-MM-DD")
    if args.start_date and args.end_date and args.start_date > args.end_date:
        parser.error("--start-date позже --end-date")

    cols_events = event_columns()
    STAGING.mkdir(parents=True, exist_ok=True)
    groups = discover(args.dataset, args.start_date, args.end_date, args.hour)
    print(f"Найдено порций: {len(groups)}", flush=True)
    if args.limit:
        groups = groups[:args.limit]
    if not groups:
        raise SystemExit("Не обнаружены закрытые файлы для выгрузки")
    for index, ((kind, ident), paths) in enumerate(groups, 1):
        print(f"[{index}/{len(groups)}] {kind} {ident}: "
              f"{len(paths)} gzip-файлов", flush=True)
        if args.dry_run:
            continue
        columns = cols_events if kind == "events" else CANDLE_COLUMNS
        try:
            run_batch(kind, ident, paths, columns)
        except Exception as exc:
            print(f"ERROR {kind} {ident}: {exc}", file=sys.stderr, flush=True)
            raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
