#!/usr/bin/env python3
"""ЛР2 / вариант 12: Avro v1, проверка кодирования и чтения одного UTC-часа.

Запуск: python schemas/check.py --day YYYY-MM-DD --hour HH
Час выбирается по времени получения из путей raw.stream/dt=.../hour=...
"""

import argparse
import gzip
import json
import sys
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from pathlib import Path

from fastavro import parse_schema, reader, writer
from fastavro.validation import validate

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = ROOT / "schemas" / "event.avsc"
# Поле NUMTRADES присутствует в сырье; v1 намеренно его не сохраняет.
RESERVED_V2_FIELD = "NUMTRADES"


def as_utc_millis(value):
    if not isinstance(value, str):
        raise TypeError("event_time должен быть ISO-строкой")
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("event_time без часового пояса")
    dt = dt.astimezone(timezone.utc)
    if dt.microsecond % 1000 != 0:
        raise ValueError("event_time имеет субмиллисекундную точность; усечение запрещено")
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (dt - epoch) // timedelta(milliseconds=1)


def project(obj, schema_fields, schema_names):
    if not isinstance(obj, dict):
        raise TypeError("ожидался JSON-объект")
    unknown = set(obj) - schema_names - {RESERVED_V2_FIELD}
    if unknown:
        raise ValueError(f"исходные поля вне схемы: {sorted(unknown)}")
    row = {field["name"]: obj.get(field["name"]) for field in schema_fields}
    row["event_time"] = as_utc_millis(obj.get("event_time"))
    return row


def records(paths, schema_fields, schema_names, parsed_schema, stats=None):
    for path in paths:
        with gzip.open(path, "rb") as handle:
            for lineno, raw in enumerate(handle, 1):
                if stats is not None:
                    stats["input_bytes"] += len(raw)
                    stats["read"] += 1
                try:
                    obj = json.loads(raw)
                    row = project(obj, schema_fields, schema_names)
                    validate(row, parsed_schema, raise_errors=True)
                except Exception as exc:
                    if stats is not None:
                        stats["errors"] += 1
                        if stats["errors"] <= 30:
                            print(f"ERROR {path}:{lineno}: {exc}", file=sys.stderr)
                    continue
                if stats is not None:
                    stats["encoded"] += 1
                    compact = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
                    stats["projected_json_bytes"] += len(compact.encode("utf-8"))
                yield row


def decoded_normalized(row):
    result = dict(row)
    value = result["event_time"]
    # fastavro декодирует logicalType=timestamp-millis в datetime UTC.
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("декодированное время без часового пояса")
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        result["event_time"] = (value.astimezone(timezone.utc) - epoch) // timedelta(milliseconds=1)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", required=True, help="UTC-дата получения, YYYY-MM-DD")
    parser.add_argument("--hour", required=True, help="UTC-час получения, 00..23")
    args = parser.parse_args()
    try:
        datetime.strptime(args.day, "%Y-%m-%d")
    except ValueError as exc:
        parser.error(f"неверная дата: {exc}")
    if len(args.hour) != 2 or not args.hour.isdigit() or not 0 <= int(args.hour) <= 23:
        parser.error("--hour: ожидается HH от 00 до 23")

    folder = ROOT / "data" / "raw" / "stream" / f"dt={args.day}" / f"hour={args.hour}"
    paths = sorted(folder.glob("*.jsonl.gz"))
    if not paths:
        raise SystemExit(f"Нет закрытых файлов выбранного часа: {folder}")

    with SCHEMA_PATH.open(encoding="utf-8") as f:
        schema_json = json.load(f)
    parsed = parse_schema(schema_json)
    field_defs = schema_json["fields"]
    names = {item["name"] for item in field_defs}
    if RESERVED_V2_FIELD in names:
        raise SystemExit("Проверка предназначена для Avro v1, поле NUMTRADES — для v2")

    output = ROOT / "data" / "duckdb" / f"lr2-{args.day}-{args.hour}.avro"
    output.parent.mkdir(parents=True, exist_ok=True)
    stats = {"input_bytes": 0, "read": 0, "encoded": 0, "errors": 0, "projected_json_bytes": 0}
    input_gzip_bytes = sum(p.stat().st_size for p in paths)

    with output.open("wb") as f:
        writer(f, parsed, records(paths, field_defs, names, parsed, stats), codec="null")

    decoded_count = 0
    mismatch_count = 0
    with output.open("rb") as f:
        expected = records(paths, field_defs, names, parsed)
        actual = reader(f)
        for decoded_count, (before, after) in enumerate(zip_longest(expected, actual), 1):
            if before is None or after is None or before != decoded_normalized(after):
                mismatch_count += 1
                if mismatch_count <= 5:
                    print(f"ROUNDTRIP_MISMATCH #{decoded_count}", file=sys.stderr)

    print(f"UTC-час получения: {args.day} {args.hour}:00–{args.hour}:59")
    print(f"Закрытых файлов: {len(paths)}")
    print(f"Полный JSONL без gzip, байт: {stats['input_bytes']}")
    print(f"Полный JSONL в gzip, байт: {input_gzip_bytes}")
    print(f"JSON-проекция полей Avro v1 без gzip, байт: {stats['projected_json_bytes']}")
    print(f"Двоичный Avro OCF (codec=null), байт: {output.stat().st_size}")
    print(f"Прочитано: {stats['read']}")
    print(f"Закодировано: {stats['encoded']}")
    print(f"Ошибок кодирования: {stats['errors']}")
    print(f"Декодировано: {decoded_count}")
    print(f"Несовпадений после декодирования: {mismatch_count}")
    print(f"Файл Avro: {output}")
    print("NUMTRADES исключено из v1: прямое сопоставление полного JSONL и Avro — не идентичные наборы полей.")
    if stats["errors"] or mismatch_count or decoded_count != stats["encoded"]:
        raise SystemExit(1)
    print("RESULT: OK")


if __name__ == "__main__":
    main()
