#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
dir=data/lr7
mkdir -p "$dir"
case "${1:-}" in
  before)
    end=${2:-$(date -u -d yesterday +%F)}
    for i in 2 1 0; do date -u -d "$end $i days ago" +%F; done > "$dir/dates.txt"
    ;;
  after) test -s "$dir/before.jsonl" && test -s "$dir/dates.txt" || { echo 'Сначала снимок before'; exit 1; } ;;
  *) echo 'Использование: bash scripts/lr7_evidence.sh before|after' >&2; exit 2 ;;
esac
tmp=$(mktemp "$dir/compare.XXXXXX")
trap 'rm -f "$tmp"' EXIT
while read -r day; do
    docker compose exec -T orchestrator python /opt/jobs/lr7.py compare "$day" >> "$tmp"
done < "$dir/dates.txt"
python3 - "$tmp" <<'PY'
import json, sys
from pathlib import Path
rows = [json.loads(line) for line in Path(sys.argv[1]).read_text().splitlines()]
if len(rows) != 3 or not any(r["iceberg_rows"] for r in rows):
    raise SystemExit("Ожидались трое суток с данными хотя бы за одни сутки")
for r in rows:
    print(r["day"], "Iceberg =", r["iceberg_rows"], "ClickHouse =", r["mart_rows"], "событий =", r["events"])
PY
mv "$tmp" "$dir/$1.jsonl"
if [[ "$1" = after ]]; then
    diff -u "$dir/before.jsonl" "$dir/after.jsonl"
    echo 'Совпали число строк и содержимое всех групп за каждую из трёх дат.'
fi
