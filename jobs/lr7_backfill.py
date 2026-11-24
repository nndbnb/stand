"""Submit one Dagster backfill for the closed daily partitions with raw input."""
import argparse
import os
import subprocess
from contextlib import closing
from datetime import date, timedelta

import lr7


def select_days(partitions, raw):
    selected = []
    skipped = {}
    for key in partitions:
        day = date.fromisoformat(key)
        missing = [str(d) for d in (day, day + timedelta(days=1)) if d not in raw]
        if missing:
            skipped[key] = missing
        else:
            selected.append(key)
    return selected, skipped


def main():
    from pipeline import days

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", action="store_true", help="show dates without submitting runs")
    args = parser.parse_args()
    prefix = os.environ.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/")
    pattern = f"s3://{os.environ['S3_BUCKET']}/{prefix}/stream/dt=*/hour=*/*.jsonl.gz"
    with closing(lr7.duck()) as db:
        keys = db.execute("SELECT DISTINCT regexp_extract(file, '/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/hour=', 1) "
                          "FROM glob(?)", [pattern]).fetchall()
    raw = {date.fromisoformat(key) for key, in keys if key}
    selected, skipped = select_days(days.get_partition_keys(), raw)
    for key, missing in skipped.items():
        print(f"Пропуск {key}: нет закрытых raw-файлов за {', '.join(missing)}.")
    print(f"Готовы к пересчёту ({len(selected)}): {', '.join(selected)}", flush=True)
    if not selected:
        raise SystemExit("Нет доступных суток.")
    if not args.plan:
        subprocess.run(["dagster", "job", "backfill", "-w", "/opt/dagster/workspace.yaml",
                        "-j", "lr7_daily", "--partitions", ",".join(selected), "--noprompt"], check=True)


if __name__ == "__main__":
    main()
