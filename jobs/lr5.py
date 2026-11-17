#!/usr/bin/env python3
"""ЛР5: загрузка, проверка, сравнение трёх путей и изолированный опыт x10.

python jobs/lr5.py run --all
python jobs/lr5.py verify
python jobs/lr5.py benchmark
python jobs/lr5.py experiment --day YYYY-MM-DD
Данные и фактические результаты: data/lr5; секреты в отчёты не записываются.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import fcntl
import hashlib
import json
import os
import re
import statistics
import subprocess
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlsplit
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'data/lr5'
DUCK = ROOT / 'data/duckdb/lr5'
DAY_RE = re.compile(r'^clean/event_date=(\d{4}-\d{2}-\d{2})/part-00000\.parquet$')


def env_read():
    env = {}
    for line in (ROOT / '.env').read_text().splitlines():
        if '=' in line and not line.lstrip().startswith('#'):
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    env.update({k: v for k, v in os.environ.items() if k in env or k.startswith('LR5_')})
    return env


def quote(value):
    return "'" + str(value).replace('\\', '\\\\').replace("'", "''") + "'"


def statements(sql):
    """Split SQL outside quoted strings; templates use only line comments."""
    sql = re.sub(r'(?m)^\s*--.*$', '', sql)
    start, quoted, i = 0, False, 0
    while i < len(sql):
        if sql[i] == "'":
            if quoted and i + 1 < len(sql) and sql[i + 1] == "'":
                i += 2
                continue
            quoted = not quoted
        if sql[i] == ';' and not quoted:
            if sql[start:i].strip():
                yield sql[start:i].strip()
            start = i + 1
        i += 1
    if sql[start:].strip():
        yield sql[start:].strip()


def save(name, value):
    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / name
    tmp = target.with_suffix(target.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + '\n')
    tmp.replace(target)


class Warehouse:
    def __init__(self, env):
        self.env = env
        self.url = env.get('LR5_WAREHOUSE_URL', 'http://127.0.0.1:' + env.get('PORT_WAREHOUSE_HTTP', '8123'))

    def query(self, sql, params=None):
        args = {'max_threads': 2, 'max_memory_usage': 2500000000,
                'output_format_json_quote_64bit_integers': 0}
        args.update({'param_' + k: v for k, v in (params or {}).items()})
        token = base64.b64encode((self.env.get('WAREHOUSE_USER', 'student') + ':' + self.env.get('WAREHOUSE_PASSWORD', 'student')).encode()).decode()
        query = sql.rstrip(';')
        if re.match(r'(?is)^\s*(SELECT|WITH|SHOW|DESCRIBE|EXPLAIN)\b', query):
            query += ' FORMAT JSON'
        request = Request(self.url + '/?' + urlencode(args), data=query.encode(),
                          headers={'Authorization': 'Basic ' + token})
        try:
            with urlopen(request, timeout=1800) as response:
                data = response.read().decode()
        except HTTPError as exc:
            message=exc.read().decode('utf-8','replace')
            for key in ('S3_ACCESS_KEY','S3_SECRET_KEY','WAREHOUSE_PASSWORD'):
                if self.env.get(key):message=message.replace(self.env[key],'<redacted>')
            for key in ('s3_key','s3_secret','k','s'):
                if (params or {}).get(key):message=message.replace(str(params[key]),'<redacted>')
            raise RuntimeError(f'ClickHouse HTTP {exc.code}: {message[:1200]}') from None
        return json.loads(data) if data.strip() else {'data': []}

    def rows(self, sql, params=None):
        return self.query(sql, params)['data']

    def file(self, name, params=None):
        for sql in statements((ROOT / 'jobs' / name).read_text()):
            self.query(sql, params)


def s3_client(env):
    import boto3
    from botocore.config import Config
    endpoint = env.get('LR5_S3_URL', env.get('S3_ENDPOINT', 'http://s3:9000'))
    if urlsplit(endpoint).hostname == 's3':
        endpoint = 'http://127.0.0.1:' + env.get('PORT_S3', '9000')
    return boto3.client('s3', endpoint_url=endpoint, region_name=env.get('S3_REGION', 'stand'),
                        aws_access_key_id=env['S3_ACCESS_KEY'], aws_secret_access_key=env['S3_SECRET_KEY'],
                        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}))


def inventory(env):
    result = []
    client = s3_client(env)
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=env.get('S3_BUCKET', 'lake'), Prefix='clean/event_date='):
        for obj in page.get('Contents', []):
            match = DAY_RE.fullmatch(obj['Key'])
            if match:
                result.append({'day': match[1], 'key': obj['Key'], 'etag': obj['ETag'], 'bytes': obj['Size']})
    if not result:
        raise RuntimeError('В S3 нет clean/event_date=*/part-00000.parquet')
    return sorted(result, key=lambda item: item['day'])


def duck(sql, profile=False):
    DUCK.mkdir(parents=True, exist_ok=True)
    ident = uuid.uuid4().hex
    result_path, profile_path = DUCK / (ident + '.json'), DUCK / (ident + '-profile.json')
    container_result = '/data/duckdb/lr5/' + result_path.name
    container_profile = '/data/duckdb/lr5/' + profile_path.name
    pre = "SET TimeZone='UTC'; SET threads=2; SET enable_progress_bar=false;\n"
    if profile:
        pre += f"PRAGMA enable_profiling='json'; SET profiling_output={quote(container_profile)};\n"
    body = pre + f'COPY ({sql.rstrip().rstrip(";")}) TO {quote(container_result)} (FORMAT JSON, ARRAY true);\n'
    if profile:
        body += 'PRAGMA disable_profiling;\n'
    proc = subprocess.run(['docker', 'compose', 'run', '--rm', '-T', 'duckdb',
                           '-init', 'init.sql', '-bail', '-f', '/dev/stdin'],
                          cwd=ROOT, input=body, text=True, capture_output=True)
    if proc.returncode:
        raise RuntimeError('DuckDB завершился с ошибкой. Проверь data/duckdb/init.sql, каталог и S3 (вывод с секретами не сохраняется).')
    rows = json.loads(result_path.read_text())
    metrics = json.loads(profile_path.read_text()) if profile else None
    result_path.unlink()
    if profile:
        evidence=OUT/'profiles'/profile_path.name
        evidence.parent.mkdir(parents=True,exist_ok=True)
        evidence.write_text(json.dumps(metrics,ensure_ascii=False,indent=2)+'\n')
        metrics['_evidence_file']=str(evidence.relative_to(ROOT))
        profile_path.unlink()
    return rows, metrics


def duck_exec(sql):
    proc = subprocess.run(['docker', 'compose', 'run', '--rm', '-T', 'duckdb',
                           '-init', 'init.sql', '-bail', '-f', '/dev/stdin'],
                          cwd=ROOT, input="SET TimeZone='UTC';\n" + sql, text=True, capture_output=True)
    if proc.returncode:
        raise RuntimeError('Ошибка DDL/записи DuckDB; рабочая таблица не изменяется.')


def spark_exec(sql):
    proc=subprocess.run(['docker','compose','run','--rm','-T','spark','/opt/spark/bin/spark-sql','-S','-e',sql],
                        cwd=ROOT,text=True,capture_output=True)
    if proc.returncode:
        detail=proc.stdout+'\n'+proc.stderr
        for key,value in env_read().items():
            if value and any(word in key.upper() for word in ('PASSWORD','SECRET','ACCESS_KEY','TOKEN')):
                detail=detail.replace(value,'[hidden]')
        if len(detail)>6000:detail=detail[:3500]+'\n[...середина трассировки пропущена...]\n'+detail[-2500:]
        raise RuntimeError('Spark: SQL='+sql+'\n'+detail)
    return proc.stdout


def parameters(env, day):
    date.fromisoformat(day)
    return {'day': day, 'source_url': env.get('S3_ENDPOINT', 'http://s3:9000').rstrip('/') + '/' + env.get('S3_BUCKET', 'lake') + '/clean/event_date=' + day + '/part-00000.parquet',
            's3_key': env['S3_ACCESS_KEY'], 's3_secret': env['S3_SECRET_KEY']}


def load(env, days):
    ch = Warehouse(env)
    ch.file('warehouse_create.sql')
    for day in days:
        print('LOAD stg/core ' + day, flush=True)
        params=parameters(env,day)
        for sql in statements((ROOT/'jobs/stg_load.sql').read_text()):
            if sql.startswith('ALTER TABLE') and ch.rows('SELECT count() AS n FROM stg.events_load')[0]['n']==0:
                # Empty source is a valid replacement; there is no source partition to copy.
                sql=re.sub(r'REPLACE PARTITION .* FROM .*$', 'DROP PARTITION {day:Date}',sql)
            ch.query(sql,params)
    # Rebuild dimension and all facts: a corrected historical attribute may change
    # subsequent validity intervals / fact foreign keys.
    ch.file('security_history.sql')
    for row in ch.rows('SELECT DISTINCT event_date FROM core.events UNION DISTINCT SELECT DISTINCT date_key AS event_date FROM mart.fact_events ORDER BY event_date'):
        print('LOAD mart ' + row['event_date'], flush=True)
        params={'day':row['event_date']}
        if ch.rows('SELECT count() AS n FROM core.events WHERE event_date={day:Date}',params)[0]['n']:
            ch.file('mart_load.sql',params)
        else:
            ch.query('ALTER TABLE mart.fact_events DROP PARTITION {day:Date}',params)
            ch.query('ALTER TABLE mart.events_hourly DROP PARTITION {day:Date}',params)


def fingerprint(ch):
    return ch.rows('''SELECT date_key, count() AS rows, uniqExact(event_key) AS keys,
        sum(cityHash64(tuple(event_key,event_time,SECID,board_key,last_price,bid,offer,security_version_key))) AS digest
        FROM mart.fact_events GROUP BY date_key ORDER BY date_key''')


def verify(env, repeat_day=None):
    ch = Warehouse(env)
    source = inventory(env)
    before = fingerprint(ch)
    if repeat_day:
        load(env, [repeat_day])
    facts = fingerprint(ch)
    checks = {}
    checks['repeat_same_content'] = before == facts if repeat_day else None
    checks['unique_events'] = bool(facts) and all(r['rows'] == r['keys'] for r in facts)
    source_counts,_=duck("SELECT CAST(event_time AS DATE) AS event_date,count(*) AS n FROM read_parquet("+
                          quote('s3://'+env.get('S3_BUCKET','lake')+'/clean/event_date=*/part-00000.parquet')+
                          ",hive_partitioning=false,union_by_name=true) GROUP BY 1 ORDER BY 1")
    checks['source_day_counts_equal'] = [(r['date_key'],r['rows']) for r in facts] == [(str(r['event_date']),int(r['n'])) for r in source_counts]
    checks['three_weeks_span'] = bool(facts) and (date.fromisoformat(facts[-1]['date_key']) - date.fromisoformat(facts[0]['date_key'])).days + 1 >= 21
    counts = ch.rows('''SELECT 'stg' AS layer,count() AS rows FROM stg.events
        UNION ALL SELECT 'core',count() FROM core.events UNION ALL SELECT 'mart',count() FROM mart.fact_events''')
    checks['layer_counts_equal'] = len({r['rows'] for r in counts}) == 1
    mismatches = ch.rows('''SELECT count() AS n FROM
        (SELECT toStartOfHour(event_time) AS event_hour,SECID,count() AS expected FROM mart.fact_events GROUP BY event_hour,SECID) f
        FULL OUTER JOIN (SELECT event_hour,SECID,sum(events) AS actual FROM mart.events_hourly GROUP BY event_hour,SECID) a
        USING (event_hour,SECID) WHERE expected!=actual''')[0]['n']
    checks['mv_equals_facts'] = mismatches == 0
    checks['dimension_intervals_valid'] = ch.rows('''SELECT count() AS n FROM (
        SELECT *,lagInFrame(valid_to,1,toDateTime64('1970-01-01',3,'UTC')) OVER
        (PARTITION BY SECID ORDER BY valid_from ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS previous_end
        FROM mart.dim_security) WHERE valid_from>=valid_to OR previous_end>valid_from''')[0]['n'] == 0
    checks['dimension_keys_unique'] = ch.rows('SELECT count()=uniqExact(security_version_key) AS ok FROM mart.dim_security')[0]['ok'] == 1
    checks['fact_dimension_references'] = ch.rows('''SELECT count() AS n FROM mart.fact_events f
        LEFT JOIN mart.dim_security d USING (security_version_key)
        WHERE d.security_version_key='' OR f.event_time<d.valid_from OR f.event_time>=d.valid_to''')[0]['n'] == 0
    history = ch.rows('SELECT SECID,count() AS versions FROM mart.dim_security GROUP BY SECID ORDER BY versions DESC,SECID LIMIT 1')
    checks['history_has_changes'] = bool(history) and history[0]['versions'] > 1
    asof = []
    if history:
        secid = history[0]['SECID']
        first = ch.rows('SELECT min(valid_from) AS point FROM mart.dim_security WHERE SECID={s:String}', {'s': secid})[0]['point']
        params = {'secid': secid, 'past': first, 'today': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}
        sql = (ROOT / 'jobs/security_asof.sql').read_text()
        asof = ch.rows(next(statements(sql)), params)
        checks['asof_past_and_today'] = {r['point'] for r in asof} == {'past', 'today'}
    checks['source_unchanged'] = source == inventory(env)
    report = {'checked_at_utc': datetime.now(timezone.utc).isoformat(), 'checks': checks,
              'facts': facts, 'counts': counts, 'asof': asof, 'source': source,
              'scope': 'Real stand; missing calendar dates do not prove source delivery completeness.'}
    save('verification.json', report)
    print(json.dumps(checks, ensure_ascii=False, indent=2), flush=True)
    return report


def canonical(rows):
    return sorted((str(r['event_hour']).replace('T',' ')[:19],str(r['SECID']),int(r['events'])) for r in rows)


def queries(env, lake_source=None, iceberg='lake.clean.events', warehouse='mart.events_hourly'):
    lake_source = lake_source or 's3://' + env.get('S3_BUCKET', 'lake') + '/clean/event_date=*/part-00000.parquet'
    common = "SELECT hour(event_time) AS event_hour,SECID,count(*) AS events FROM {} GROUP BY 1,2 ORDER BY 1,2"
    return {'lake': common.format(f'read_parquet({quote(lake_source)},hive_partitioning=false,union_by_name=true)'),
            'lakehouse': common.format(iceberg),
            'warehouse': f'SELECT toHour(event_hour) AS event_hour,SECID,sum(events) AS events FROM {warehouse} GROUP BY event_hour,SECID ORDER BY event_hour,SECID'}


def measure(env, query_map, scale=1):
    ch = Warehouse(env)
    result, expected = {}, None
    for path, sql in query_map.items():
        runs = []
        for i in range(3):
            print(f'BENCH {path} {i+1}/3', flush=True)
            if path == 'warehouse':
                payload = ch.query(sql)
                rows, prof = payload['data'], payload['statistics']
                run = {'seconds': prof['elapsed'], 'read_bytes': prof['bytes_read'], 'read_rows': prof['rows_read']}
            else:
                rows, prof = duck(sql, profile=True)
                if 'total_bytes_read' not in prof or 'latency' not in prof:
                    raise RuntimeError('Профиль DuckDB не содержит latency/total_bytes_read; объём файлов не подставляется вместо чтения.')
                run = {'seconds': prof['latency'], 'read_bytes': prof['total_bytes_read'], 'profile_file':prof['_evidence_file']}
            answer = canonical(rows)
            if expected is None:
                expected = answer
            if answer != expected:
                raise RuntimeError(f'Ответ {path} не совпадает с другими путями; проверь полноту ЛР4. Замеры не объявлены успешными.')
            runs.append(run)
        if any(n%scale for _,_,n in expected):raise RuntimeError('Итоги групп не кратны масштабу опыта')
        digest=hashlib.sha256(json.dumps([(h,s,n//scale) for h,s,n in expected]).encode()).hexdigest()
        result[path] = {'query':sql,'runs': runs, 'median_seconds': statistics.median(r['seconds'] for r in runs),
                        'median_read_bytes': statistics.median(r['read_bytes'] for r in runs), 'answer_groups': len(expected or []),
                        'normalized_answer_sha256':digest,
                        'architecture': {'lake':'Data lake: Parquet/ZSTD in S3','lakehouse':'Lakehouse: Iceberg + REST catalog + Parquet in S3','warehouse':'DWH: ClickHouse SummingMergeTree, materialized hourly aggregate'}[path]}
        if path=='warehouse':result[path]['plan']=ch.rows('EXPLAIN indexes=1 '+sql)
    return result


def current_snapshot():
    # Spark metadata is authoritative; fail if it cannot be read.
    proc = subprocess.run(['docker','compose','run','--rm','-T','spark','/opt/spark/bin/spark-sql',
        '-S','-e', 'SELECT snapshot_id FROM lake.clean.events.refs WHERE name=\'main\''], cwd=ROOT, text=True, capture_output=True)
    ids = re.findall(r'(?m)^\s*(\d{8,})\s*$', proc.stdout)
    if proc.returncode or len(ids)!=1:
        raise RuntimeError('Не удалось зафиксировать current snapshot Iceberg через Spark')
    return ids[0]


def benchmark(env):
    source = inventory(env)
    snapshot_before = current_snapshot()
    result = measure(env, queries(env))
    if source != inventory(env) or snapshot_before != current_snapshot():
        raise RuntimeError('Данные изменились во время сравнения; повтори при остановленных загрузках.')
    report = {'checked_at_utc':datetime.now(timezone.utc).isoformat(),
              'question':'В какие часы суток UTC и по каким SECID поступает больше событий за всю историю', 'paths':result,
              'snapshot_id':snapshot_before, 'source':source, 'answers_equal':True,
              'read_bytes_note':'DuckDB: total_bytes_read из JSON-профиля (физическое чтение, cache может дать 0). ClickHouse: statistics.bytes_read (логические распакованные байты, как Processed). Метрики не тождественны.',
              'timing_note':'Медиана 3 последовательных запусков, 2 потока. DuckDB latency включает COPY малого ответа; запуск Docker исключён. Кэши ОС/сервисов не очищаются.'}
    save('benchmark.json',report)
    return report


def experiment(env, day):
    date.fromisoformat(day)
    source_inventory=inventory(env)
    if day not in {r['day'] for r in source_inventory}:
        raise RuntimeError('Для опыта нужны имеющиеся завершённые сутки clean')
    ch, client = Warehouse(env), s3_client(env)
    bucket = env.get('S3_BUCKET','lake')
    ident = 'lr5_exp_' + uuid.uuid4().hex[:12]
    prefix = 'lr5-experiment/' + ident + '/'
    source = f's3://{bucket}/clean/event_date={day}/part-00000.parquet'
    before = fingerprint(ch)
    snapshot_before=current_snapshot()
    results = {}
    cleanup = {'lakehouse':False,'warehouse':False,'s3':False}
    # Store the prediction BEFORE measurements; x10 checks direction, not x1000 itself.
    prediction = {
      'lake':'При x1000 объёма с прежним числом групп полного GROUP BY: чтение и CPU порядка x1000; пределы — сеть S3, CPU, spill. При x10 ожидается увеличение прочитанных байт и времени, возможен выигрыш кэша.',
      'lakehouse':'При x1000 фактов: чтение порядка x1000 для запроса без фильтра; дополнительно manifests/planning. При фильтре партиций рост может быть меньше. В опыте x10 нет отсечения — ожидается рост.',
      'warehouse':'При x1000 повторов в тех же часах/бумагах hourly содержит столько же групп: чтение и время близки к постоянным; загрузка/объём fact растут x1000. Если растёт и число часов/бумаг, растёт агрегат и время.'}
    save('experiment.json', {'id':ident,'day':day,'prediction_x1000':prediction,'status':'running','cleanup':cleanup})
    try:
        spark_exec(f'CREATE NAMESPACE lake.{ident};')
        ch.query(f'CREATE DATABASE {ident}')
        for factor in (1,10):
            table = f'lake.{ident}.events_{factor}'
            path = f's3://{bucket}/{prefix}x{factor}/events.parquet'
            select = f'''SELECT concat(event_key,'|lr5|',CAST(i AS VARCHAR)) AS event_key,event_time,SECID
                FROM read_parquet({quote(source)},hive_partitioning=false) CROSS JOIN range({factor}) r(i)'''
            duck_exec(f"COPY ({select}) TO {quote(path)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 32768);")
            spark_exec(f"CREATE TABLE {table} (event_key STRING,event_time TIMESTAMP,SECID STRING) USING iceberg;")
            duck_exec(f"INSERT INTO {table} SELECT event_key,event_time,SECID FROM read_parquet({quote(path)},hive_partitioning=false);")
            ch.query(f'''CREATE TABLE {ident}.fact_{factor} (event_key String,event_time DateTime64(3,'UTC'),SECID String)
                ENGINE=MergeTree PARTITION BY toDate(event_time) ORDER BY (SECID,event_time,event_key)''')
            ch.query(f'''CREATE TABLE {ident}.hourly_{factor} (event_hour DateTime('UTC'),SECID String,events UInt64)
                ENGINE=SummingMergeTree ORDER BY (event_hour,SECID)''')
            ch.query(f'''CREATE MATERIALIZED VIEW {ident}.mv_{factor} TO {ident}.hourly_{factor} AS
                SELECT toStartOfHour(event_time) AS event_hour,SECID,count() AS events FROM {ident}.fact_{factor} GROUP BY event_hour,SECID''')
            url = env.get('S3_ENDPOINT','http://s3:9000').rstrip('/') + '/' + bucket + '/' + prefix + f'x{factor}/events.parquet'
            ch.query(f"INSERT INTO {ident}.fact_{factor} SELECT * FROM s3({{u:String}},{{k:String}},{{s:String}},'Parquet')",
                     {'u':url,'k':env['S3_ACCESS_KEY'],'s':env['S3_SECRET_KEY']})
            control = ch.rows(f'SELECT count() AS rows,uniqExact(event_key) AS keys FROM {ident}.fact_{factor}')[0]
            if control['rows'] != control['keys']:
                raise RuntimeError('Коллизии ключей экспериментального набора')
            result = measure(env, queries(env,path,table,f'{ident}.hourly_{factor}'),scale=factor)
            results[str(factor)] = {'control':control,'paths':result}
        if results['10']['control']['rows'] != 10*results['1']['control']['rows']:
            raise RuntimeError('Набор не вырос ровно в 10 раз')
        if results['1']['paths']['lake']['normalized_answer_sha256']!=results['10']['paths']['lake']['normalized_answer_sha256']:
            raise RuntimeError('Итоги отдельных групп x10 не равны десяти итогам x1')
    finally:
        # Remove only exact resources owned by this run, including Iceberg data via PURGE.
        failures=[]
        lake_errors=[]
        for sql in [f'DROP TABLE IF EXISTS lake.{ident}.events_{factor} PURGE;' for factor in (1,10)]+[f'DROP NAMESPACE IF EXISTS lake.{ident};']:
            try:spark_exec(sql)
            except Exception as exc:lake_errors.append(str(exc))
        cleanup_warnings=[]
        if lake_errors:
            # A REST catalog may remove the entry before Spark's PURGE encounters
            # an already missing metadata file. Confirm absence; never ignore a
            # failed purge while its namespace is still present or unobservable.
            try:
                namespaces={line.strip() for line in spark_exec('SHOW NAMESPACES IN lake;').splitlines()}
                if ident not in namespaces:
                    cleanup_warnings=lake_errors
                    lake_errors=[]
            except Exception as exc:
                lake_errors.append(str(exc))
        cleanup['lakehouse']=not lake_errors
        failures.extend(lake_errors)
        try:
            ch.query(f'DROP DATABASE IF EXISTS {ident} SYNC')
            cleanup['warehouse']=True
        except Exception as exc:
            failures.append(str(exc))
        try:
            if not cleanup['lakehouse']:raise RuntimeError('S3-префикс оставлен до успешного удаления экспериментальных Iceberg-таблиц')
            keys=[]
            for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket,Prefix=prefix):
                keys.extend({'Key':o['Key']} for o in page.get('Contents',[]))
            for i in range(0,len(keys),1000):
                response=client.delete_objects(Bucket=bucket,Delete={'Objects':keys[i:i+1000]})
                if response.get('Errors'):raise RuntimeError('S3 cleanup returned errors')
            cleanup['s3']=client.list_objects_v2(Bucket=bucket,Prefix=prefix,MaxKeys=1).get('KeyCount',0)==0
        except Exception as exc:
            failures.append(str(exc))
        report={'id':ident,'day':day,'checked_at_utc':datetime.now(timezone.utc).isoformat(),
                'prediction_x1000':prediction,'measurements':results,'cleanup':cleanup,
                'working_fact_unchanged':before==fingerprint(ch),
                'working_clean_unchanged':source_inventory==inventory(env),
                'working_iceberg_unchanged':snapshot_before==current_snapshot(),'errors':failures,'cleanup_warnings':cleanup_warnings,
                'limit':'Проверка на x10 оценивает тенденцию. Реальное время x1000 не измерялось; линейная экстраполяция не гарантируется.'}
        if '1' in results and '10' in results:
            ratios={}
            for name in ('lake','lakehouse','warehouse'):
                a,b=results['1']['paths'][name],results['10']['paths'][name]
                ratios[name]={'time_x10_over_x1':b['median_seconds']/a['median_seconds'] if a['median_seconds'] else None,
                              'read_bytes_x10_over_x1':b['median_read_bytes']/a['median_read_bytes'] if a['median_read_bytes'] else None}
            report['ratios']=ratios
        save('experiment.json',report)
        if failures:print('Очистка не полностью завершена: см. data/lr5/experiment.json',file=sys.stderr)
    if not all(cleanup.values()) or not all(report[k] for k in ('working_fact_unchanged','working_clean_unchanged','working_iceberg_unchanged')):
        raise RuntimeError('Эксперимент не прошёл проверку очистки/изоляции')
    return report


def dashboard(env):
    payload={'env':{k:env[k] for k in ('WAREHOUSE_USER','WAREHOUSE_PASSWORD') if k in env},
             'adminPassword':env.get('LR5_BI_PASSWORD','student'),
             'template':json.loads((ROOT/'conf/datalens/ql_column.json').read_text()),
             'existing':json.loads((OUT/'dashboard.json').read_text()) if (OUT/'dashboard.json').exists() else None}
    code=(ROOT/'jobs/datalens_bootstrap.cjs').read_text()
    proc=subprocess.run(['docker','compose','exec','-T','bi','node','-e',code],cwd=ROOT,input=json.dumps(payload),text=True,capture_output=True)
    if proc.returncode:
        raise RuntimeError('DataLens: '+proc.stderr[-800:])
    result=json.loads(proc.stdout)
    result['url']='http://localhost:'+env.get('PORT_BI','8088')+'/dashboards/'+result['dashboardId']
    save('dashboard.json',result)
    print('DASHBOARD '+result['url'],flush=True)
    return result


def report_status():
    result={}
    for name in ('execution','verification','benchmark','experiment','dashboard'):
        path=OUT/(name+'.json')
        result[name]=json.loads(path.read_text()) if path.exists() else {'status':'not_run'}
    save('results.json',result)
    text=['# ЛР5 — результаты запуска','',
          'Фактические значения появляются после запуска на стенде. Времена — медианы трёх запросов; запуск контейнера не включён.','']
    verification=result['verification']
    if 'checks' in verification:
        text+=['## Проверки','', '| Проверка | Результат |','|---|---|']
        text += [f'| {k} | {"PASS" if v else "FAIL" if v is False else "не запускалась"} |' for k,v in verification['checks'].items()]
    bench=result['benchmark']
    if 'paths' in bench:
        text+=['','## Три пути за всю историю','', '| Путь | Архитектура | Медиана, с | Прочитано, байт |','|---|---|---:|---:|']
        text += [f'| {k} | {v["architecture"]} | {v["median_seconds"]:.6f} | {v["median_read_bytes"]} |' for k,v in bench['paths'].items()]
        text+=['',bench['read_bytes_note'],'',bench['timing_note']]
    exp=result['experiment']
    if 'prediction_x1000' in exp:
        text+=['','## Дополнительное задание','']
        text += [f'- **{k}:** {v}' for k,v in exp['prediction_x1000'].items()]
        if 'ratios' in exp:
            text+=['','| Набор | Путь | Медиана, с | Прочитано, байт |','|---|---|---:|---:|']
            for scale,item in exp['measurements'].items():
                text += [f'| x{scale} | {k} | {v["median_seconds"]:.6f} | {v["median_read_bytes"]} |' for k,v in item['paths'].items()]
            text+=['','| Путь | Время x10 / x1 | Чтение x10 / x1 |','|---|---:|---:|']
            text += [f'| {k} | {v["time_x10_over_x1"]} | {v["read_bytes_x10_over_x1"]} |' for k,v in exp['ratios'].items()]
            text+=['','Сопоставление с прогнозом:']
            for name,ratio in exp['ratios'].items():
                value=ratio['time_x10_over_x1']
                if value is None:verdict='базовое время равно нулю; коэффициент не определён'
                elif name=='warehouse':verdict='коэффициент сопоставляется с неизменным количеством групп агрегата, а не с числом исходных фактов'
                elif value>1:verdict='время выросло, направление прогноза подтверждено на x10; линейность и x1000 этим не доказаны'
                else:verdict='рост времени на x10 не подтвердился; на этом объёме кэш и накладные расходы могут перекрывать рост сканирования'
                text.append(f'- {name}: {verdict}.')
        text+=['',exp.get('limit',''),'',
               'Озеро: DuckDB перечисляет файлы, читает выбранные колонки Parquet, распаковывает и группирует факты. Lakehouse: тот же движок сначала разрешает снимок/манифесты Iceberg, затем сканирует Parquet; у запроса по всей истории нет выигрыша от отсечения дат. Витрина: ClickHouse читает уже посчитанные MV группы, а не все факты. Это перенос CPU на загрузку. Кэши и размер файлов влияют на коэффициенты; при x10 накладные расходы могут доминировать.',
               '',f'Удаление тестовых наборов: {exp.get("cleanup")}. Рабочие факты не изменились: {exp.get("working_fact_unchanged")}.']
    if 'url' in result['dashboard']:
        text+=['','## Дашборд','',result['dashboard']['url'], '',
               'API проверяет сохранённые объекты и выполняет запрос каждого чарта через DataLens. Внешний вид трёх графиков нужно проверить в браузере на стенде.']
    (OUT/'results.md').write_text('\n'.join(text)+'\n')
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command',choices=['run','load','verify','benchmark','experiment','dashboard'])
    group=parser.add_mutually_exclusive_group()
    group.add_argument('--all',action='store_true')
    group.add_argument('--day')
    parser.add_argument('--repeat-day',help='При verify реально повторить загрузку суток и сравнить содержимое')
    args=parser.parse_args()
    if args.command in ('run','load') and not args.all and not args.day:
        parser.error('Для загрузки укажи --all или --day')
    OUT.mkdir(parents=True,exist_ok=True)
    env=env_read()
    execution={'command':args.command,'started_at_utc':datetime.now(timezone.utc).isoformat(),'status':'running'}
    acquired=False
    try:
        # One writer per working copy; do not run other LR3/LR4 jobs concurrently.
        with (OUT/'run.lock').open('w') as lock:
            try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise RuntimeError('ЛР5 уже выполняется') from None
            acquired=True
            save('execution.json',execution)
            if args.command in ('run','load'):
                available=inventory(env)
                days=[r['day'] for r in available] if args.all else [args.day]
                if not set(days)<={r['day'] for r in available}:raise RuntimeError('Нет выбранных суток clean')
                load(env,days)
            if args.command in ('run','verify'):
                repeat=args.repeat_day or (days[0] if args.command=='run' else None)
                result=verify(env,repeat)
                failed=[k for k,v in result['checks'].items() if v is False]
                if failed:raise RuntimeError('Проверки не пройдены: '+', '.join(failed))
            if args.command in ('run','dashboard'):dashboard(env)
            if args.command in ('run','benchmark'):benchmark(env)
            if args.command in ('run','experiment'):
                day=args.day or inventory(env)[0]['day']
                experiment(env,day)
    except (Exception,KeyboardInterrupt) as exc:
        execution.update(status='failed',error=str(exc),finished_at_utc=datetime.now(timezone.utc).isoformat())
        if acquired:
            save('execution.json',execution)
            save('last_error.json',{'error':str(exc),'command':args.command,'at_utc':datetime.now(timezone.utc).isoformat()})
        print('ERROR: '+str(exc),file=sys.stderr)
        return 1
    finally:
        if acquired and execution['status']=='running':
            execution.update(status='completed',finished_at_utc=datetime.now(timezone.utc).isoformat())
            save('execution.json',execution)
            (OUT/'last_error.json').unlink(missing_ok=True)
        if acquired:report_status()
    print('OK: data/lr5/results.json',flush=True)
    return 0


if __name__=='__main__':
    raise SystemExit(main())

