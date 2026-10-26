#!/usr/bin/env python3
"""ЛР2: Parquet v2 для выбранной UTC-даты.

Обычный режим: только новую завершённую дату; v1 не изменяется.
Для демонстрации 26 октября:
  python3 jobs/export_v2_day.py --day 2026-10-26 \
    --snapshot-incomplete-day --replace-existing-day
При замене прежняя v1-партиция резервируется после успешной проверки v2.
В режиме снимка текущий UTC-час пропускается. Raw JSONL.gz не удаляются.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/stream"
WORK = ROOT / "data/duckdb"
OUT = WORK / "parquet/events"
STAGE_ROOT = WORK / ".parquet_stage"
BACKUP = WORK / "parquet_v1_backup"
SCHEMA = ROOT / "schemas/event_v2.avsc"
TEMPLATE = ROOT / "jobs/to_parquet_v2_day.sql"


def schema_columns():
    with SCHEMA.open(encoding="utf-8") as handle:
        fields = json.load(handle)["fields"]
    if len(fields) != 60 or not any(f["name"] == "NUMTRADES" for f in fields):
        raise ValueError("Ожидалась схема v2 из 60 полей, включая NUMTRADES")
    result = []
    for field in fields:
        name = field["name"]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError(f"Неверное имя поля: {name}")
        type_ = field["type"]
        if isinstance(type_, list):
            type_ = next(t for t in type_ if t != "null")
        if isinstance(type_, dict):
            type_ = type_["type"]
        mapping = {"long": "BIGINT", "int": "BIGINT", "double": "DOUBLE",
                   "float": "DOUBLE", "string": "VARCHAR"}
        sqltype = mapping[type_]
        if name in ("event_time", "received_at"):
            sqltype = "VARCHAR"
        result.append((name, sqltype))
    return result


def source_groups(day: date, snapshot_incomplete_day: bool = False):
    groups = []
    now_utc = datetime.now(timezone.utc)
    open_hour = (now_utc.date().isoformat(), now_utc.hour)
    for date_received in (day, day + timedelta(days=1)):
        for hour in range(24):
            # Текущий UTC-час может ещё пополняться: не включаем его.
            if snapshot_incomplete_day and (date_received.isoformat(), hour) >= open_hour:
                continue
            folder = RAW / f"dt={date_received.isoformat()}" / f"hour={hour:02d}"
            paths = sorted(folder.glob("part-*.jsonl.gz"))
            if paths:
                groups.append((date_received.isoformat(), f"{hour:02d}", paths))
    return groups


def file_signature(groups):
    return [(str(p), p.stat().st_size, p.stat().st_mtime_ns)
            for _, _, paths in groups for p in paths]


def utc_event_date(value):
    if not isinstance(value, str):
        raise ValueError("event_time: ожидалась строка ISO8601")
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError("event_time: отсутствует часовой пояс")
    return timestamp.astimezone(timezone.utc).date()


def raw_counts(groups, day, sqlite_path):
    counts = {}
    total = 0
    connection = sqlite3.connect(sqlite_path)
    try:
        connection.execute("CREATE TABLE keys (event_key TEXT PRIMARY KEY)")
        for recv_day, hour, paths in groups:
            matching = 0
            for path in paths:
                with gzip.open(path, "rt", encoding="utf-8") as handle:
                    for lineno, line in enumerate(handle, 1):
                        try:
                            obj = json.loads(line)
                            if utc_event_date(obj.get("event_time")) != day:
                                continue
                            key = obj["event_key"]
                            if not isinstance(key, str) or not key:
                                raise ValueError("пустой event_key")
                            connection.execute("INSERT INTO keys VALUES (?)", (key,))
                        except Exception as exc:
                            raise ValueError(f"{path}:{lineno}: {exc}") from exc
                        matching += 1
                        total += 1
            if matching:
                counts[(recv_day, hour)] = matching
        connection.commit()
    finally:
        connection.close()
    return counts, total


def sql_script(template, cols, day, following, recv_day, hour, output, audit):
    column_map = ",\n".join(
        f"        '{name}': '{sqltype}'" for name, sqltype in cols
    )
    select = []
    for name, _ in cols:
        if name in ("event_time", "received_at"):
            select.append(
                f'      CAST(CAST("{name}" AS TIMESTAMPTZ) AT TIME ZONE '
                f"'UTC' AS TIMESTAMP_MS) AS \"{name}\""
            )
        else:
            select.append(f'      "{name}"')
    prefix = f"v2-recv-{recv_day.replace('-', '')}-{hour}"
    params = {
        "__SOURCE_GLOB__": (
            f"/data/raw/stream/dt={recv_day}/hour={hour}/part-*.jsonl.gz"),
        "__COLUMN_MAP__": column_map,
        "__PROJECTION__": ",\n".join(select),
        "__DAY__": day.isoformat(),
        "__NEXT_DAY__": following.isoformat(),
        "__STAGING_OUTPUT__": output,
        "__PREFIX__": prefix,
        "__AUDIT_FILE__": audit,
    }
    query = template
    for key, value in params.items():
        query = query.replace(key, value)
    unresolved = re.findall(r"__[A-Z_]+__", query)
    if unresolved:
        raise ValueError(f"Не заменены параметры SQL: {unresolved}")
    return query, prefix


def run_sql(query):
    res = subprocess.run(
        ["docker", "compose", "run", "--rm", "-T", "duckdb",
         "-bail", "-f", "/dev/stdin"],
        cwd=ROOT, input=query, text=True, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, check=False,
    )
    if res.returncode:
        raise RuntimeError(
            f"DuckDB code={res.returncode}\n{res.stdout[-4000:]}"
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True, help="Новая дата event_time UTC YYYY-MM-DD")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--snapshot-incomplete-day", action="store_true",
                        help="Снимок только завершённых часов получения UTC")
    parser.add_argument("--replace-existing-day", action="store_true",
                        help="Резервировать существующую v1-партицию после проверки v2")
    args = parser.parse_args()
    try:
        day = date.fromisoformat(args.day)
    except ValueError as exc:
        parser.error(str(exc))
    if day.isoformat() != args.day:
        parser.error("Дата должна быть YYYY-MM-DD")
    following = day + timedelta(days=1)
    if args.snapshot_incomplete_day and day > datetime.now(timezone.utc).date():
        parser.error("Нельзя делать снимок будущих UTC-суток")

    if args.dry_run:
        groups = source_groups(day, args.snapshot_incomplete_day)
        print(f"Предварительный просмотр: {day}; каталогов-часов: {len(groups)}")
        for recv_day, hour, paths in groups:
            print(f"  {recv_day} hour={hour}: {len(paths)} gzip-файлов")
        return

    # Для отбора поздних событий следующая дата получения тоже должна завершиться.
    today = datetime.now(timezone.utc).date()
    if today <= following and not args.snapshot_incomplete_day:
        raise SystemExit(
            f"UTC-дата {day} ещё не готова: дождитесь закрытия {following} UTC "
            "или используйте --snapshot-incomplete-day для частичного снимка"
        )
    target = OUT / f"event_date={args.day}"
    backup_target = BACKUP / target.name
    if args.replace_existing_day:
        if not target.is_dir() or not list(target.glob("recv-*.parquet")):
            raise SystemExit(f"Для замены необходима существующая v1-партиция: {target}")
        if backup_target.exists():
            raise SystemExit(f"Резервная папка уже существует: {backup_target}")
        if list(target.glob("v2-recv-*.parquet")):
            raise SystemExit("Партиция уже содержит v2; замена заблокирована")
    elif target.exists():
        raise SystemExit(
            f"ОСТАНОВЛЕНО: {target} уже существует. "
            "Для резервируемой замены укажите --replace-existing-day."
        )
    if not any(OUT.glob("event_date=*/*.parquet")):
        raise SystemExit("Старые файлы Parquet v1 не найдены; нечего сравнивать.")

    groups = source_groups(day, args.snapshot_incomplete_day)
    if not groups:
        raise SystemExit(f"Нет исходных gzip для {day} и {following}")
    print(f"Дата события UTC: {day}; каталогов-часов получения: {len(groups)}",
          flush=True)
    STAGE_ROOT.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix="lr2_v2_day_", dir=STAGE_ROOT))
    try:
        signature_before = file_signature(groups)
        counts, expected = raw_counts(groups, day, staging / "keys.sqlite3")
        if not expected:
            raise RuntimeError(f"В сырье нет событий с event_date={day}")
        print(f"Исходные события новой даты: {expected}, "
              f"пакетов с событиями: {len(counts)}", flush=True)
        cols = schema_columns()
        template = TEMPLATE.read_text(encoding="utf-8")
        output = "/data/duckdb/" + (staging.relative_to(WORK) / "output").as_posix()
        converted_total = 0
        with_numtrades = 0
        for i, ((recv_day, hour), expected_hour) in enumerate(counts.items(), 1):
            prefix = f"v2-recv-{recv_day.replace('-', '')}-{hour}"
            audit_path = staging / f"audit-{prefix}.csv"
            audit = "/data/duckdb/" + audit_path.relative_to(WORK).as_posix()
            query, prefix = sql_script(template, cols, day, following,
                                       recv_day, hour, output, audit)
            run_sql(query)
            with audit_path.open(encoding="utf-8", newline="") as file:
                audit_rows = list(csv.DictReader(file))
            if len(audit_rows) != 1:
                raise RuntimeError(f"Нет результатов аудита: {recv_day} {hour}")
            result = audit_rows[0]
            rows = int(result["row_count"])
            unique = int(result["distinct_keys"])
            invalid = int(result["invalid_rows"])
            non_null = int(result["rows_with_numtrades"])
            if rows != expected_hour or unique != rows or invalid != 0:
                raise RuntimeError(
                    f"Не прошла сверка {recv_day} {hour}: raw={expected_hour} "
                    f"parquet={rows} unique={unique} invalid={invalid}"
                )
            converted_total += rows
            with_numtrades += non_null
            print(f"[{i}/{len(counts)}] OK {recv_day}:{hour}: "
                  f"rows={rows}, NUMTRADES_not_null={non_null}", flush=True)

        parquet_day = staging / "output" / f"event_date={args.day}"
        files = sorted(parquet_day.glob("*.parquet"))
        if not files or converted_total != expected:
            raise RuntimeError("Parquet неполный после преобразования")
        if not with_numtrades:
            raise RuntimeError("Во всех событиях NUMTRADES=null; демонстрация v2 невозможна")
        if file_signature(groups) != signature_before:
            raise RuntimeError("Сырые файлы изменились при работе; повторите загрузку")
        # Одно атомарное переименование готовой партиции на том же диске.
        OUT.mkdir(parents=True, exist_ok=True)
        if args.replace_existing_day:
            BACKUP.mkdir(parents=True, exist_ok=True)
            if backup_target.exists() or not target.is_dir():
                raise RuntimeError("Исходная партиция или резерв изменились")
            os.rename(target, backup_target)
            try:
                os.rename(parquet_day, target)
            except OSError:
                os.rename(backup_target, target)
                raise
            print(f"Резервная копия v1: {backup_target}", flush=True)
        else:
            if target.exists():
                raise RuntimeError(f"Партиция появилась во время записи: {target}")
            os.rename(parquet_day, target)
        print(f"RESULT: OK v2 day={day} rows={converted_total} "
              f"NUMTRADES_not_null={with_numtrades} files={len(files)}",
              flush=True)
        print(f"Parquet: {target}", flush=True)
        if args.snapshot_incomplete_day:
            print("ВНИМАНИЕ: снимок закрытых часов, а не полные UTC-сутки.", flush=True)
    finally:
        shutil.rmtree(staging, ignore_errors=True)


if __name__ == "__main__":
    main()
