#!/usr/bin/env python3
"""ЛР2 / вариант 12: проверка Avro v1 ↔ Avro v2 на реальных событиях MOEX.

Пример: python schemas/check_evolution.py --day 2026-10-19 --hour 16
Проверка ограничена первыми 1000 строками одного часа получения UTC.
Исходные JSONL.gz не изменяются.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
from itertools import islice
from pathlib import Path

from fastavro import parse_schema, reader, writer
from check import decoded_normalized, project

ROOT = Path(__file__).resolve().parents[1]
VERSION_1 = ROOT / "schemas" / "event.avsc"
VERSION_2 = ROOT / "schemas" / "event_v2.avsc"


def load(path):
    with path.open(encoding="utf-8") as file:
        return json.load(file)


def rows_for_hour(day, hour, limit):
    folder = ROOT / "data" / "raw" / "stream" / f"dt={day}" / f"hour={hour}"
    files = sorted(folder.glob("part-*.jsonl.gz"))
    if not files:
        raise RuntimeError(f"Нет закрытых файлов: {folder}")
    count = 0
    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as data:
            for line in data:
                if count >= limit:
                    return
                yield json.loads(line)
                count += 1


def encode(schema, records):
    buffer = io.BytesIO()
    writer(buffer, schema, records, codec="null")
    return buffer.getvalue()


def decode(data, schema):
    return [decoded_normalized(row) for row in reader(io.BytesIO(data), reader_schema=schema)]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", default="2026-10-19")
    parser.add_argument("--hour", default="16")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()
    if args.limit <= 0:
        parser.error("--limit должен быть больше нуля")
    if len(args.hour) != 2 or not args.hour.isdigit() or not 0 <= int(args.hour) <= 23:
        parser.error("--hour должен быть в формате HH (00..23)")

    s1 = load(VERSION_1)
    s2 = load(VERSION_2)
    base = [(f["name"], f["type"]) for f in s1["fields"]]
    old_in_new = [(f["name"], f["type"]) for f in s2["fields"] if f["name"] != "NUMTRADES"]
    if base != old_in_new or len(s2["fields"]) != len(s1["fields"]) + 1:
        raise RuntimeError("v2 меняет старые поля или содержит не одно новое поле")
    num = next(f for f in s2["fields"] if f["name"] == "NUMTRADES")
    if num["type"] != ["null", "long"] or num.get("default", "MISSING") is not None:
        raise RuntimeError("NUMTRADES должен быть nullable long с default=null")

    av1, av2 = parse_schema(s1), parse_schema(s2)
    source = list(rows_for_hour(args.day, args.hour, args.limit))
    if not source:
        raise RuntimeError("Нет событий в выбранном часе")

    f1, f2 = s1["fields"], s2["fields"]
    names1 = {f["name"] for f in f1}
    names2 = {f["name"] for f in f2}
    old_rows = [project(row, f1, names1) for row in source]
    new_rows = [project(row, f2, names2) for row in source]
    if not any(row["NUMTRADES"] is not None for row in new_rows):
        raise RuntimeError("В выборке нет значений NUMTRADES; выберите другой час")
    if any(row["NUMTRADES"] is not None and
           (not isinstance(row["NUMTRADES"], int) or isinstance(row["NUMTRADES"], bool))
           for row in new_rows):
        raise RuntimeError("NUMTRADES содержит нецелое значение")

    old_data = encode(av1, old_rows)
    new_data = encode(av2, new_rows)

    # Старый writer v1 -> новый reader v2: дополнительное поле должно
    # получить default null, а старые поля обязаны остаться без изменений.
    upgraded = decode(old_data, av2)
    assert len(upgraded) == len(source)
    for old, new in zip(old_rows, upgraded):
        assert new.pop("NUMTRADES") is None
        assert new == old

    # Новый writer v2 -> старый reader v1: неизвестное новое поле игнорируется.
    downgraded = decode(new_data, av1)
    assert downgraded == old_rows

    # Новый writer -> новый reader: NUMTRADES тоже сохраняется без изменений.
    same_version = decode(new_data, av2)
    assert same_version == new_rows

    print(f"UTC-час получения: {args.day} {args.hour}")
    print(f"Реальных событий: {len(source)}")
    print(f"Полей v1: {len(f1)}; полей v2: {len(f2)}")
    print(f"NUMTRADES с ненулевым значением: {sum(row['NUMTRADES'] is not None for row in new_rows)}")
    print(f"v1 writer -> v2 reader: OK, default=null ({len(upgraded)} записей)")
    print(f"v2 writer -> v1 reader: OK, лишнее поле игнорируется ({len(downgraded)} записей)")
    print(f"v2 writer -> v2 reader: OK, NUMTRADES сохранено ({len(same_version)} записей)")
    print("RESULT: OK")


if __name__ == "__main__":
    main()
