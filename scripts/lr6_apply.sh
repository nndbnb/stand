#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
test -s .env || { echo 'Нужен заполненный .env из ЛР5'; exit 1; }
test -x .venv/bin/python || python3 -m venv .venv
.venv/bin/python -m pip install -r jobs/requirements-lr6.txt
test -e .env.before-lr6 || cp -p .env .env.before-lr6
.venv/bin/python jobs/lr6.py configure
docker compose --profile lab6 config --quiet
# Download and build before stopping the working profile.
docker compose --profile lab6 pull broker registry
docker compose build collector
docker compose --profile lab5 stop
docker compose --profile lab6 up -d
docker compose --profile lab6 up -d --wait --wait-timeout 300 registry
.venv/bin/python jobs/lr6.py registry
.venv/bin/python jobs/lr6.py produce
docker compose up -d collector
docker compose --profile lab6 ps
echo 'Следующий шаг в WSL: .venv/bin/python jobs/sink.py'
