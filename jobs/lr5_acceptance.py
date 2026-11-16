#!/usr/bin/env python3
"""Только чтение: показать один из 7 критериев уже выполненной ЛР5."""
import argparse
import json
from datetime import datetime, timezone
from lr5 import ROOT, OUT, Warehouse, env_read, statements


def show(value):
    print(json.dumps(value,ensure_ascii=False,indent=2,default=str))


def evidence(name):
    path=OUT/(name+'.json')
    if not path.exists():raise SystemExit('Нет результата: '+str(path))
    return json.loads(path.read_text())


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('criterion',type=int,choices=range(1,8))
    n=parser.parse_args().criterion
    ch=Warehouse(env_read())
    if n==1:
        show(ch.rows("SELECT name,engine FROM system.databases WHERE name IN ('stg','core','mart') ORDER BY name"))
        print('stg: приём очищенных Parquet; core: нормализованные события и изменения; mart: звезда и MV. Обоснование: NOTES.md, ЛР5.')
    elif n==2:
        show(ch.rows("SELECT name,engine,partition_key,sorting_key,create_table_query FROM system.tables WHERE database='mart' AND name IN ('fact_events','dim_security','dim_date','dim_board','mv_events_hourly','mv_events_hourly_load') ORDER BY name"))
        show(ch.rows('''SELECT f.event_key,f.event_time,d.SECID,d.attribute,b.board_key,c.date_key
          FROM mart.fact_events f INNER JOIN mart.dim_security d USING(security_version_key)
          INNER JOIN mart.dim_board b USING(board_key) INNER JOIN mart.dim_date c USING(date_key) LIMIT 5'''))
        print('Зерно: одно событие event_key. Ключи: security_version_key, date_key, board_key. Публикация партиций агрегата из MV загрузочной таблицы: jobs/mart_load.sql.')
        show({'mv_equals_facts':evidence('verification')['checks']['mv_equals_facts']})
    elif n==3:
        candidate=ch.rows('SELECT SECID,min(valid_from) AS past,count() AS versions FROM mart.dim_security GROUP BY SECID ORDER BY versions DESC,SECID LIMIT 1')[0]
        show(ch.rows('SELECT * FROM mart.dim_security WHERE SECID={s:String} ORDER BY valid_from',{'s':candidate['SECID']}))
        show(ch.rows(next(statements((ROOT/'jobs/security_asof.sql').read_text())),
                     {'secid':candidate['SECID'],'past':candidate['past'],'today':datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}))
    elif n==4:
        for table,day in [('stg.events','event_date'),('core.events','event_date'),('mart.fact_events','date_key')]:
            print(table)
            show(ch.rows(f'SELECT {day} AS day,count() AS rows,uniqExact(event_key) AS keys FROM {table} GROUP BY day ORDER BY day'))
        result=evidence('verification')
        show({k:result[k] for k in ('checked_at_utc','checks','counts')})
        print('S3 → s3(..., Parquet) → stg/core → mart; код jobs/stg_load.sql, jobs/mart_load.sql. Наборы и связи: CATALOG.md, ЛР5.')
    elif n==5:
        show(evidence('dashboard'))
        print('Открыть url, показать все три графика в DataLens. Логин admin. Пароль берётся из настройки стенда.')
    elif n==6:
        show(evidence('benchmark'))
        print('Полные профили DuckDB: data/lr5/profiles; итог: data/lr5/results.md.')
    else:
        result=evidence('experiment')
        show(result)
        show(ch.rows('SELECT name FROM system.databases WHERE name={name:String}',{'name':result['id']}))
        print('Пустой список подтверждает удаление экспериментальной базы. Удаление S3 и namespace Iceberg зафиксировано в cleanup. Объяснение: data/lr5/results.md; NOTES.md, ЛР5.')


if __name__=='__main__':main()
