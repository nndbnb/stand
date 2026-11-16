#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
python3 - <<'PY'
from pathlib import Path
p=Path('.env')
if not p.is_file():raise SystemExit('Нужен существующий .env стенда ЛР4')
lines=p.read_text().splitlines()
def set_value(key,value):
    found=[i for i,line in enumerate(lines) if line.startswith(key+'=')]
    if len(found)>1:raise SystemExit('Повторяющаяся переменная .env: '+key)
    if found:lines[found[0]]=key+'='+value
    else:lines.append(key+'='+value)
values={line.split('=',1)[0]:line.split('=',1)[1].strip().strip('"').strip("'") for line in lines if '=' in line and not line.startswith('#')}
if values.get('ENGINE')!='duckdb' or values.get('S3')!='seaweedfs':raise SystemExit('Ожидается ENGINE=duckdb, S3=seaweedfs')
files=values.get('COMPOSE_FILE','compose.yaml').split(':')
if any('compose.bi-' in name and name!='compose.bi-datalens.yaml' for name in files):raise SystemExit('В COMPOSE_FILE уже выбран другой BI')
if 'compose.bi-datalens.yaml' not in files:files.append('compose.bi-datalens.yaml')
set_value('BI','datalens');set_value('COMPOSE_FILE',':'.join(files))
backup=p.with_name('.env.before-lr5')
if not backup.exists():
    backup.write_bytes(p.read_bytes());backup.chmod(0o600)
p.write_text('\n'.join(lines)+'\n');p.chmod(0o600)
PY
mkdir -p data/lr5 data/duckdb/lr5
if [ ! -f data/duckdb/init.sql ]; then
  echo 'Нет data/duckdb/init.sql из ЛР4' >&2; exit 1
fi
if [ ! -x .venv/bin/python ]; then python3 -m venv .venv; fi
.venv/bin/python -m pip install -r jobs/requirements-lr5.txt
docker compose --profile lab5 config --quiet
docker compose --profile lab5 up -d
echo 'Ожидание ClickHouse и DataLens. Сборщик поднимается штатным профилем стенда.'
.venv/bin/python - <<'PY'
import sys,time
from urllib.request import urlopen
sys.path.insert(0,'jobs')
from lr5 import env_read
env=env_read()
for port,path in [(env.get('PORT_WAREHOUSE_HTTP','8123'),'/ping'),(env.get('PORT_BI','8088'),'/')]:
    for attempt in range(180):
        try:
            with urlopen('http://127.0.0.1:'+port+path,timeout=3) as response:
                if response.status==200:break
        except Exception:pass
        time.sleep(5)
    else:raise SystemExit('Сервис не готов на порту '+port+'; см. docker compose logs')
PY
.venv/bin/python jobs/lr5.py run --all
