"""Cleanup tests without a live stand: no deletion after uncertain catalog state."""
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('lr5_cleanup_subject',ROOT/'jobs/lr5.py')
a=importlib.util.module_from_spec(spec);spec.loader.exec_module(a)
IDENT='lr5_exp_012345abcdef'


class CleanupTests(unittest.TestCase):
    def run_failure(self,namespace_present=False,show_fails=False,fail_during_insert=False):
        calls=[]
        class S3:
            def get_paginator(self,*args):return self
            def paginate(self,**kw):calls.append(kw);return [{'Contents':[]}]
            def list_objects_v2(self,**kw):return {'KeyCount':0}
        class CH:
            def __init__(self,*args):pass
            def query(self,sql):calls.append(sql)
        def spark(sql):
            calls.append(sql)
            if sql.startswith('DROP TABLE') and 'events_1 ' in sql:raise RuntimeError('NoSuchKey on purge')
            if sql.startswith('SHOW NAMESPACES'):
                if show_fails:raise RuntimeError('Catalog unreachable')
                return 'clean\n'+(IDENT+'\n' if namespace_present else '')
            return ''
        def duck(sql):
            calls.append(sql)
            if not fail_during_insert or sql.startswith('INSERT'):raise RuntimeError('fixture stopped before measurements')
        with tempfile.TemporaryDirectory() as td,patch.object(a,'OUT',Path(td)),patch.object(a,'Warehouse',CH),patch.object(a,'s3_client',return_value=S3()),patch.object(a,'inventory',return_value=[{'day':'2026-10-12'}]),patch.object(a,'fingerprint',return_value=['unchanged']),patch.object(a,'current_snapshot',return_value='same'),patch.object(a.uuid,'uuid4',return_value=SimpleNamespace(hex='012345abcdef0000')),patch.object(a,'spark_exec',side_effect=spark),patch.object(a,'duck_exec',side_effect=duck):
            with self.assertRaisesRegex(RuntimeError,'fixture stopped'):a.experiment({},'2026-10-12')
            report=json.loads((Path(td)/'experiment.json').read_text())
        return report,calls

    def test_absent_namespace_allows_exact_prefix_cleanup(self):
        r,calls=self.run_failure()
        self.assertTrue(all(r['cleanup'].values()));self.assertTrue(r['cleanup_warnings'])
        self.assertTrue(any(isinstance(x,dict) and x['Prefix']==f'lr5-experiment/{IDENT}/' for x in calls))

    def test_existing_namespace_prevents_s3_cleanup(self):
        r,calls=self.run_failure(namespace_present=True)
        self.assertFalse(r['cleanup']['s3']);self.assertFalse(r['cleanup']['lakehouse'])
        self.assertFalse(any(isinstance(x,dict) for x in calls))

    def test_unavailable_catalog_is_not_treated_as_absent(self):
        r,calls=self.run_failure(show_fails=True)
        self.assertFalse(r['cleanup']['s3']);self.assertTrue(r['errors'])

    def test_spark_creates_schema_and_duckdb_reads_parquet(self):
        r,calls=self.run_failure(fail_during_insert=True)
        create=next(x for x in calls if isinstance(x,str) and x.startswith('CREATE TABLE'))
        self.assertNotIn('AS SELECT',create);self.assertNotIn('LOCATION',create)
        self.assertTrue(any(isinstance(x,str) and x.startswith('INSERT INTO lake.') and 'read_parquet' in x for x in calls))
        self.assertTrue(r['working_fact_unchanged'])


if __name__=='__main__':unittest.main()
