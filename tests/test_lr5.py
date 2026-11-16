"""SQL integration on embedded ClickHouse (chdb), real Parquet/DuckDB fixtures.
No measurements from this fixture are presented as results of the student's stand.
python -m unittest discover -s tests -p 'test_lr5.py' -v
"""
import contextlib
import io
import json
import re
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'jobs'))
import lr5
import chdb
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


class Embedded(lr5.Warehouse):
    session=None
    sources={}

    def query(self,sql,params=None):
        if 'FROM s3(' in sql:
            sql=sql.replace("s3({source_url:String}, {s3_key:String}, {s3_secret:String}, 'Parquet')",
                            "file({source_url:String}, 'Parquet')")
            params=dict(params)
            params['source_url']=self.sources[params['day']]
        value=str(self.session.query(sql,fmt='JSON',params=params))
        return json.loads(value) if value.strip() else {'data':[]}


class WarehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory()
        Embedded.session=chdb.session.Session(cls.tmp.name+'/db')
        cls.ch=Embedded({})
        cls.ch.file('warehouse_create.sql')

    @classmethod
    def tearDownClass(cls):
        Embedded.session.close()
        cls.tmp.cleanup()

    def setUp(self):
        for database in ('stg','core','mart'):
            tables=self.ch.rows('SELECT name,engine FROM system.tables WHERE database={db:String}',{'db':database})
            for row in tables:
                if row['engine'] not in ('View','MaterializedView'):
                    self.ch.query(f'TRUNCATE TABLE {database}.{row["name"]}')
        Embedded.sources={}
        self.env={'S3_ACCESS_KEY':'fixture','S3_SECRET_KEY':'fixture'}
        self.patch=patch.object(lr5,'Warehouse',Embedded)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def write(self,day,n,status='T',duplicate=False):
        rows=[{'event_key':f'{day}-{i}','event_time':datetime.fromisoformat(day+'T10:00:00').replace(minute=i%60),
               'received_at':datetime.fromisoformat(day+'T10:01:00'), 'SECID':'SBER','BOARDID':'TQBR',
               'TRADINGSTATUS':status,'LAST':100.0+i,'BID':99.0+i,'OFFER':101.0+i} for i in range(n)]
        schema=pa.schema([('event_key',pa.string()),('event_time',pa.timestamp('ms')),('received_at',pa.timestamp('ms')),
                          ('SECID',pa.string()),('BOARDID',pa.string()),('TRADINGSTATUS',pa.string()),
                          ('LAST',pa.float64()),('BID',pa.float64()),('OFFER',pa.float64())])
        if duplicate:rows.append(rows[0])
        path=Path(self.tmp.name)/(day+'.parquet')
        pq.write_table(pa.Table.from_pylist(rows,schema=schema),path,compression='zstd')
        Embedded.sources[day]=str(path)

    def load(self,days):
        with contextlib.redirect_stdout(io.StringIO()):lr5.load(self.env,days)

    def test_repeat_day_preserves_facts_mv_and_neighbour_days(self):
        self.write('2026-10-12',2)
        self.write('2026-11-03',3,'N')
        self.load(['2026-10-12','2026-11-03'])
        before=lr5.fingerprint(self.ch)
        self.load(['2026-10-12'])
        self.assertEqual(before,lr5.fingerprint(self.ch))
        self.assertEqual(5,self.ch.rows('SELECT sum(events) AS n FROM mart.events_hourly')[0]['n'])
        self.assertEqual(2,self.ch.rows('SELECT count() AS n FROM mart.dim_security')[0]['n'])

    def test_duplicate_source_rejected_before_partition_replacement(self):
        self.write('2026-10-12',2)
        self.load(['2026-10-12'])
        before=lr5.fingerprint(self.ch)
        self.write('2026-10-12',2,duplicate=True)
        with self.assertRaises(Exception):self.load(['2026-10-12'])
        self.assertEqual(before,lr5.fingerprint(self.ch))
        self.assertEqual(2,self.ch.rows('SELECT count() AS n FROM core.events')[0]['n'])

    def test_scd2_half_open_boundary_and_asof(self):
        self.write('2026-10-12',2)
        self.write('2026-10-13',3,'N')
        self.load(['2026-10-12','2026-10-13'])
        sql=next(lr5.statements((lr5.ROOT/'jobs/security_asof.sql').read_text()))
        rows=self.ch.rows(sql,{'secid':'SBER','past':'2026-10-12 10:00:00','today':'2026-10-13 10:00:00'})
        self.assertEqual({'past':'T','today':'N'},{r['point']:r['attribute'] for r in rows})
        self.assertEqual(3,self.ch.rows("SELECT count() AS n FROM mart.fact_events WHERE security_version_key LIKE '%2026-10-13%'")[0]['n'])

    def test_constant_status_uses_measured_daily_activity(self):
        self.write('2026-10-12',2)
        self.write('2026-10-13',3)
        self.load(['2026-10-12','2026-10-13'])
        rows=self.ch.rows('SELECT attribute_kind,attribute FROM mart.dim_security ORDER BY valid_from')
        self.assertEqual(['daily_activity']*2,[r['attribute_kind'] for r in rows])
        self.assertEqual(['low:events=2','low:events=3'],[r['attribute'] for r in rows])

    def test_corrected_history_updates_existing_fact_foreign_keys(self):
        self.write('2026-10-12',2)
        self.write('2026-10-13',3,'N')
        self.load(['2026-10-12','2026-10-13'])
        self.write('2026-10-12',2,'N')
        self.load(['2026-10-12'])
        self.assertEqual(5,self.ch.rows('SELECT count() AS n FROM mart.fact_events f INNER JOIN mart.dim_security d USING(security_version_key) WHERE f.event_time>=d.valid_from AND f.event_time<d.valid_to')[0]['n'])

    def test_empty_replacement_removes_only_selected_partition(self):
        self.write('2026-10-12',2)
        self.write('2026-10-13',3,'N')
        self.load(['2026-10-12','2026-10-13'])
        self.write('2026-10-12',0)
        self.load(['2026-10-12'])
        self.assertEqual(3,self.ch.rows('SELECT count() AS n FROM mart.fact_events')[0]['n'])
        self.assertEqual(3,self.ch.rows('SELECT sum(events) AS n FROM mart.events_hourly')[0]['n'])

    def test_full_verification_and_broken_aggregate_detection(self):
        self.write('2026-10-12',2)
        self.write('2026-11-03',3,'N')
        self.load(['2026-10-12','2026-11-03'])
        inv=[{'day':d,'key':d} for d in sorted(Embedded.sources)]
        conn=duckdb.connect()
        def local_duck(sql,profile=False):
            glob=lr5.quote('s3://lake/clean/event_date=*/part-00000.parquet')
            paths='['+','.join(lr5.quote(p) for p in Embedded.sources.values())+']'
            cursor=conn.execute(sql.replace(glob,paths))
            names=[c[0] for c in cursor.description]
            return [dict(zip(names,r)) for r in cursor.fetchall()],None
        with patch.object(lr5,'OUT',Path(self.tmp.name)/'reports'),patch.object(lr5,'inventory',return_value=inv),patch.object(lr5,'duck',side_effect=local_duck),contextlib.redirect_stdout(io.StringIO()):
            result=lr5.verify(self.env,'2026-10-12')
            self.assertTrue(all(result['checks'].values()),result['checks'])
            self.ch.query('TRUNCATE TABLE mart.events_hourly')
            self.assertFalse(lr5.verify(self.env)['checks']['mv_equals_facts'])
        conn.close()

    def test_lake_and_materialized_answer_match_on_real_parquet(self):
        self.write('2026-10-12',12)
        self.write('2026-10-13',5,'N')
        self.load(['2026-10-12','2026-10-13'])
        query_map=lr5.queries(self.env,Embedded.sources['2026-10-12'])
        sql=query_map['lake'].replace(lr5.quote(Embedded.sources['2026-10-12']),
            '['+','.join(lr5.quote(p) for p in Embedded.sources.values())+']')
        cursor=duckdb.connect().execute(sql)
        lake=[dict(zip([c[0] for c in cursor.description],r)) for r in cursor.fetchall()]
        warehouse=self.ch.rows(query_map['warehouse'])
        self.assertEqual(lr5.canonical(lake),lr5.canonical(warehouse))


class ExperimentTests(unittest.TestCase):
    def test_failed_experiment_cleans_only_its_own_prefix_and_tables(self):
        calls=[]
        class Paginator:
            def paginate(self,**kwargs):
                calls.append(kwargs)
                return [{'Contents':[{'Key':kwargs['Prefix']+'x1/events.parquet'}]}]
        class S3:
            def get_paginator(self,*args):return Paginator()
            def delete_objects(self,**kwargs):calls.append(kwargs);return {}
            def list_objects_v2(self,**kwargs):return {'KeyCount':0}
        class CH:
            def __init__(self,env):pass
            def query(self,sql):calls.append(sql)
        with tempfile.TemporaryDirectory() as tmp,patch.object(lr5,'OUT',Path(tmp)),patch.object(lr5,'Warehouse',CH),patch.object(lr5,'s3_client',return_value=S3()),patch.object(lr5,'inventory',return_value=[{'day':'2026-10-12'}]),patch.object(lr5,'fingerprint',return_value=['stable']),patch.object(lr5,'current_snapshot',return_value='same'),patch.object(lr5,'duck_exec',side_effect=RuntimeError('fixture failure')),patch.object(lr5,'spark_exec',side_effect=lambda sql:calls.append(sql)):
            with self.assertRaisesRegex(RuntimeError,'fixture failure'):lr5.experiment({},'2026-10-12')
            result=json.loads((Path(tmp)/'experiment.json').read_text())
            self.assertTrue(all(result['cleanup'].values()))
            self.assertTrue(result['working_fact_unchanged'])
            self.assertIn('lr5_exp_',result['id'])
            self.assertTrue(any('PURGE' in c for c in calls if isinstance(c,str)))
            self.assertTrue(all(c.get('Prefix','lr5-experiment/').startswith('lr5-experiment/') for c in calls if isinstance(c,dict)))
            self.assertFalse(any('clean.events' in c for c in calls if isinstance(c,str)))


if __name__=='__main__':unittest.main()
