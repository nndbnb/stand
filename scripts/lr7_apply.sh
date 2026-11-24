#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
test -s .env && test -x .venv/bin/python || { echo 'Запусти из рабочей копии ЛР6 с .env и .venv'; exit 1; }
bash scripts/lr7_collector_ca.sh
mkdir -p data/lr7
test -e data/lr7/env.before-lr7 || install -m 600 .env data/lr7/env.before-lr7
.venv/bin/python - <<'PY'
from pathlib import Path
from dotenv import dotenv_values, set_key
env = dotenv_values(".env")
files = env["COMPOSE_FILE"].split(":")
if "compose.orchestrator-airflow.yaml" in files or "compose.broker-kafka.yaml" not in files:
    raise SystemExit("Ожидался профиль ЛР6 варианта 12 с Kafka")
overlay = "compose.orchestrator-dagster.yaml"
if overlay not in files:
    files.append(overlay)
for k, v in {"COMPOSE_FILE": ":".join(files), "ORCHESTRATOR": "dagster", "COLLECTOR_MODE": "record"}.items():
    set_key(".env", k, v, quote_mode="never")
for name in ("NOTES.md", "CATALOG.md"):
    path = Path(name)
    section = (Path("docs/lr7") / name).read_text()
    if section.splitlines()[0] not in path.read_text().splitlines():
        with path.open("a") as doc:
            doc.write("\n" + section)
PY
docker compose --profile lab7 config --quiet
for entry in 'orchestrator bigdata-stand/dagster:1' 'spark bigdata-stand/spark:2'; do
    read -r service tag <<< "$entry"
    docker image inspect "$tag" >/dev/null 2>&1 || docker compose build "$service"
done
docker compose --profile lab6 stop
docker compose --profile lab7 up -d
docker compose up -d --wait --wait-timeout 300 orchestrator warehouse
docker compose exec -T orchestrator python -c 'from pipeline import defs; from dagster import Definitions; Definitions.validate_loadable(defs); print("Dagster definitions OK")'
docker compose --profile lab7 ps
