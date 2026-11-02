#!/usr/bin/env python3
"""ЛР3: небольшие офлайн-тесты S3-синхронизации без сети и без SeaweedFS."""
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from botocore.exceptions import ClientError
from lake_sync import LakeSync


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.upload_calls = 0

    def head_object(self, Bucket, Key):
        if (Bucket, Key) not in self.objects:
            raise ClientError(
                {"Error": {"Code": "404"}, "ResponseMetadata": {"HTTPStatusCode": 404}},
                "HeadObject",
            )
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def upload_file(self, filename, bucket, key):
        self.upload_calls += 1
        self.objects[(bucket, key)] = Path(filename).read_bytes()


class LakeSyncTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.root = Path(self.dir.name)
        self.s3 = FakeS3()
        self.logs = []
        with patch.dict(os.environ, {"COLLECTOR_LAKE_PREFIX": "raw",
                                     "S3_BUCKET": "lake"}):
            with patch("lake_sync.make_s3_client", return_value=self.s3):
                self.sync = LakeSync(
                    self.root,
                    threading.Event(),
                    lambda level, message, **kw: self.logs.append((level, message, kw)),
                )

    def test_stream_and_history_idempotent(self):
        stream = self.root / "stream/dt=2026-10-26/hour=16/part-test.jsonl.gz"
        history = self.root / "history/dt=2026-10-02/candles-test.jsonl.gz"
        for path in (stream, history):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"test")
        wal = self.root / "state/wal/part-open.jsonl"
        wal.parent.mkdir(parents=True, exist_ok=True)
        wal.write_bytes(b"open")
        found, uploaded = self.sync.scan_once()
        self.assertEqual((found, uploaded), (2, 2))
        self.assertEqual(self.s3.upload_calls, 2)
        self.assertEqual(self.s3.objects[("lake", "raw/stream/dt=2026-10-26/hour=16/part-test.jsonl.gz")], b"test")
        self.assertEqual(self.s3.objects[("lake", "raw/history/dt=2026-10-02/candles-test.jsonl.gz")], b"test")
        self.assertEqual(self.sync.scan_once(), (2, 0))
        self.assertEqual(self.s3.upload_calls, 2)

    def test_remote_key_mismatch_no_overwrite(self):
        path = self.root / "stream/dt=2026-10-26/hour=16/part-test.jsonl.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"local")
        key = self.sync.object_key(path)
        self.s3.objects[("lake", key)] = b"other size"
        self.assertEqual(self.sync.scan_once(), (1, 0))
        self.assertEqual(self.s3.upload_calls, 0)
        self.assertTrue(any(msg == "lake_sync_failed" for _, msg, _ in self.logs))

    def test_existing_same_size_skipped(self):
        path = self.root / "stream/dt=2026-10-26/hour=16/part-test.jsonl.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"local")
        key = self.sync.object_key(path)
        self.s3.objects[("lake", key)] = b"local"
        self.assertEqual(self.sync.scan_once(), (1, 0))
        self.assertEqual(self.s3.upload_calls, 0)
        self.assertEqual(self.sync.scan_once(), (1, 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
