"""LR7 daily steps. CLI runs inside the orchestrator, with the same code as Dagster."""
import argparse
import hashlib
import json
import os
import uuid
from contextlib import closing
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import clean
import iceberg_load

JOBS = Path(__file__).resolve().parent


def duck():
    import duckdb
    db = duckdb.connect()
    env = os.environ
    endpoint = urlsplit(env["S3_ENDPOINT"])
    q = clean.quote
    db.execute("LOAD httpfs; LOAD iceberg; SET TimeZone='UTC'; SET threads=2")
    db.execute(f"""CREATE SECRET s3sec (TYPE s3, KEY_ID {q(env['S3_ACCESS_KEY'])},
               SECRET {q(env['S3_SECRET_KEY'])}, ENDPOINT {q(endpoint.netloc)},
               URL_STYLE 'path', USE_SSL {str(endpoint.scheme == 'https').lower()},
               REGION {q(env['S3_REGION'])})""")
    db.execute(f"CREATE SECRET ic (TYPE iceberg, TOKEN '', ENDPOINT {q(env['CATALOG_URI'])})")
    db.execute(f"ATTACH {q(env['CATALOG_WAREHOUSE'])} AS lake "
               "(TYPE iceberg, SECRET ic, ACCESS_DELEGATION_MODE 'none')")
    return db


def clickhouse():
    import clickhouse_connect
    return clickhouse_connect.get_client(host=os.environ["WAREHOUSE_HOST"],
                                         username=os.environ["WAREHOUSE_USER"],
                                         password=os.environ["WAREHOUSE_PASSWORD"])


def bounds(day):
    start = datetime.combine(day, time.min)
    return start, start + timedelta(days=1)


def clean_day(day):
    following = day + timedelta(days=1)
    prefix = os.environ.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/")
    base = f"s3://{os.environ['S3_BUCKET']}/{prefix}/stream"
    with closing(duck()) as db:
        days = [str(d) for d in (day, following) if db.execute(
            "SELECT count(*) FROM glob(?)", [f"{base}/dt={d}/hour=*/*.jsonl.gz"]).fetchone()[0]]
        _, _, sql = clean.prepare(str(day), os.environ, days)
        row = db.execute(sql).fetchone()
    return {"rows": row[0], "distinct_keys": row[1]}


def load_day(day):
    start, end = bounds(day)
    source = f"s3://{os.environ['S3_BUCKET']}/clean/event_date={day}/part-00000.parquet"
    with closing(duck()) as db:
        row = db.execute(iceberg_load.render(start, end, source)).fetchone()
    return {"rows": row[0]}


def groups(db, ch, day, target="mart.events_hourly"):
    cols = "SECID, events, entities, qty_sum"
    expected = db.execute(f"SELECT hour(event_hour), {cols} FROM lake.clean.events_hourly "
                          "WHERE event_date = ?", [day]).fetchall()
    actual = ch.query(f"SELECT toHour(event_hour), {cols} FROM {target} "
                      "WHERE event_date = {day:Date}", parameters={"day": day}).result_rows
    return sorted(expected), sorted(actual)


def compare(expected, actual):
    for name, rows in (("Iceberg", expected), ("ClickHouse", actual)):
        if len({(h, sec) for h, sec, *_ in rows}) != len(rows):
            raise ValueError(f"{name}: повтор группы (час, SECID)")
    if expected != actual:
        raise ValueError(f"Состав групп или значения расходятся: Iceberg {len(expected)}, CH {len(actual)}")
    return {"rows": len(actual), "events": sum(r[2] for r in actual),
            "entities_sum": sum(r[3] for r in actual), "qty_sum": sum(r[4] for r in actual)}


def snapshot(day):
    with closing(duck()) as db, closing(clickhouse()) as ch:
        left, right = groups(db, ch, day)
        summary = compare(left, right)
        digest = hashlib.sha256(json.dumps(left, separators=(",", ":")).encode()).hexdigest()
        return {"day": str(day), "groups_sha256": digest, "iceberg_rows": len(left), "mart_rows": len(right), **summary}


def publish(day):
    env = os.environ
    with closing(duck()) as db, closing(clickhouse()) as ch:
        params = {"uri": env["CATALOG_URI"], "key": env["S3_ACCESS_KEY"], "secret": env["S3_SECRET_KEY"],
                  "endpoint": env["S3_ENDPOINT"].rstrip("/") + "/" + env["S3_BUCKET"],
                  "warehouse": env["CATALOG_WAREHOUSE"]}
        ch.command("""CREATE DATABASE IF NOT EXISTS lake_ice
                   ENGINE=DataLakeCatalog(%(uri)s, %(key)s, %(secret)s)
                   SETTINGS catalog_type='rest', storage_endpoint=%(endpoint)s, warehouse=%(warehouse)s""",
                   parameters=params)
        for table in ("events_hourly", "events_hourly_load"):
            ch.command(f"ALTER TABLE mart.{table} ADD COLUMN IF NOT EXISTS entities UInt64 DEFAULT 0, "
                       "ADD COLUMN IF NOT EXISTS qty_sum Int64 DEFAULT 0")
        stage = "mart.lr7_stage_" + uuid.uuid4().hex
        ch.command(f"CREATE TABLE {stage} AS mart.events_hourly")
        try:
            columns = "event_date, event_hour, SECID, events, entities, qty_sum"
            ch.command(f"INSERT INTO {stage} ({columns}) SELECT {columns} "
                       "FROM lake_ice.`clean.events_hourly` WHERE event_date = {day:Date}",
                       parameters={"day": day})
            left, right = groups(db, ch, day, stage)
            compare(left, right)
            if right:
                ch.command(f"ALTER TABLE mart.events_hourly REPLACE PARTITION '{day}' FROM {stage}")
            elif ch.query("SELECT count() FROM mart.events_hourly WHERE event_date = {day:Date}",
                          parameters={"day": day}).result_rows[0][0]:
                ch.command(f"ALTER TABLE mart.events_hourly DROP PARTITION '{day}'")
            return compare(*groups(db, ch, day))
        finally:
            ch.command(f"DROP TABLE IF EXISTS {stage}")


def quality(day, inject_missing=False):
    start, end = bounds(day)
    # For the negative control only, append one malformed event to the query input.
    extra = (" UNION ALL SELECT NULL, ?, ?, 'moex', 'TEST', 'TQBR', 1, 1.0" if inject_missing else "")
    sql = """WITH sample AS (SELECT event_key, event_time, received_at, source, SECID, BOARDID, QTY, LAST
             FROM lake.clean.events WHERE event_time >= ? AND event_time < ?""" + extra + """)
             SELECT max(event_time), count(*) FILTER (WHERE nullif(trim(event_key),'') IS NULL
             OR event_time IS NULL OR received_at IS NULL OR nullif(trim(SECID),'') IS NULL
             OR nullif(trim(BOARDID),'') IS NULL OR nullif(trim(source),'') IS NULL),
             count(*) FILTER (WHERE QTY < 0 OR LAST < 0 OR BOARDID <> 'TQBR' OR source <> 'moex')
             FROM sample"""
    with closing(duck()) as db:
        last, missing, invalid = db.execute(sql, [start, end] + ([start, start] if extra else [])).fetchone()
    lag = None if last is None else (end - last).total_seconds() / 3600
    result = {"day": str(day), "last_event_utc": str(last), "age_hours_at_midnight": lag,
              "freshness": "WARN" if lag is None or lag > 6 else "OK", "threshold_hours": 6,
              "missing_required": missing, "invalid_values": invalid, "negative_control": inject_missing}
    if missing or invalid:
        raise ValueError(json.dumps(result, ensure_ascii=False))
    return result


def history():
    with closing(duck()) as db:
        rows = db.execute("SELECT CAST(event_time AS DATE) AS d, count(*) FROM lake.clean.events "
                          "WHERE event_time < CAST(current_date AS TIMESTAMP) GROUP BY d ORDER BY d").fetchall()
        paths = db.execute("SELECT DISTINCT regexp_extract(file, '/dt=([0-9-]+)/', 1) FROM glob(?)",
                           [f"s3://{os.environ['S3_BUCKET']}/{os.environ.get('COLLECTOR_LAKE_PREFIX','raw').strip('/')}"
                            "/stream/dt=*/hour=*/*.jsonl.gz"]).fetchall()
    today = datetime.now(timezone.utc).date()
    raw = {date.fromisoformat(k) for k, in paths if k}
    days = {d for d, _ in rows}
    closed = {d for d in raw if d < today and d + timedelta(days=1) in raw}
    print("UTC date     Iceberg rows")
    for d, n in rows:
        print(d, n)
    return {"first": str(min(days)) if days else None, "last": str(max(days)) if days else None,
            "span_days": (max(days) - min(days)).days + 1 if days else 0,
            "event_days": len(days), "closed_raw_days_without_events": sorted(map(str, closed-days))}


def audit_history():
    """Compare event membership and multiplicity against the LR3 selection for each closed raw day."""
    with closing(duck()) as db:
        prefix = os.environ.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/")
        pattern = f"s3://{os.environ['S3_BUCKET']}/{prefix}/stream/dt=*/hour=*/*.jsonl.gz"
        dates = {date.fromisoformat(d) for d, in db.execute(
            "SELECT DISTINCT regexp_extract(file, '/dt=([0-9-]+)/', 1) FROM glob(?)", [pattern]).fetchall()}
        available = {str(d) for d in dates}
        today = datetime.now(timezone.utc).date()
        actual_start, actual_end = db.execute(
            "SELECT min(event_time)::DATE, max(event_time)::DATE FROM lake.clean.events "
            "WHERE event_time < current_date").fetchone()
        span = (actual_end - actual_start).days + 1 if actual_start else 0
        failures = []
        checked = 0
        for d in sorted(dates):
            if d >= today:
                continue
            if d + timedelta(days=1) not in dates:
                failures.append(f"{d}: нет закрытого сырья D+1")
                continue
            _, _, rendered = clean.prepare(str(d), os.environ, available)
            # Reuse precisely the SELECT that LR3 writes to Parquet, with its deduplication.
            _, sep, tail = rendered.partition("COPY (")
            select, close, _ = tail.partition("\n)\nTO ")
            if not sep or not close:
                raise ValueError("Изменилась форма COPY в clean.sql")
            sql = f"""WITH expected AS MATERIALIZED (SELECT event_key FROM ({select}) AS cleaned),
                         actual AS MATERIALIZED (SELECT event_key FROM lake.clean.events
                           WHERE event_time >= ? AND event_time < ?)
                      SELECT (SELECT count(*) FROM (SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)),
                             (SELECT count(*) FROM (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))"""
            missing, excess = db.execute(sql, bounds(d)).fetchone()
            print(f"{d} missing={missing} excess={excess}", flush=True)
            checked += 1
            if missing or excess:
                failures.append(str(d))
        if not checked or failures or span < 28:
            raise ValueError(f"История: проверено суток {checked}, интервал {span} дней, расхождения {failures}")
        return {"checked_raw_days": checked, "first_event": str(actual_start),
                "last_event": str(actual_end), "span_days": span}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("step", choices=["clean", "load", "publish", "check", "compare", "history", "audit-history"])
    parser.add_argument("day", nargs="?", type=date.fromisoformat)
    args = parser.parse_args()
    if args.step == "audit-history":
        result = audit_history()
    elif args.step == "history":
        result = history()
    else:
        if not args.day or args.day >= datetime.now(timezone.utc).date():
            parser.error("Нужна дата завершённых суток UTC")
        funcs = {"clean": clean_day, "load": load_day, "publish": publish,
                 "check": quality, "compare": snapshot}
        result = funcs[args.step](args.day)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
