# ruff: noqa: E402
"""Requires jobs/requirements-lr6.txt and moto[s3]. Uses librdkafka's local test broker."""

import gzip
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import boto3
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "jobs"), str(ROOT / "collector/src")]
import broker_stream
import lr6
import sink


class StreamTest(unittest.TestCase):
    @mock_aws
    def test_replay_obeys_period_key_and_speed(self):
        import threading

        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="lake")
        rows = [
            dict(
                received_at=f"2026-10-12T12:00:{i:02d}Z", SECID="SBER", event_key=str(i)
            )
            for i in range(4)
        ]
        s3.put_object(
            Bucket="lake",
            Key="raw/stream/dt=2026-10-12/hour=12/a.jsonl.gz",
            Body=gzip.compress(b"".join(json.dumps(r).encode() + b"\n" for r in rows)),
        )
        captured = []

        class Capture:
            def send(self, events, key):
                captured.extend((row["event_key"], key) for row in events)

        stop = Mock(wraps=threading.Event())
        stop.wait.return_value = False
        env = dict(
            S3_BUCKET="lake",
            COLLECTOR_LAKE_PREFIX="raw",
            COLLECTOR_TOPIC="moex.tqbr.replay.test",
            COLLECTOR_REPLAY_SPEED="2",
            COLLECTOR_REPLAY_FROM="2026-10-12T12:00:01Z",
            COLLECTOR_REPLAY_TO="2026-10-12T12:00:03Z",
            LR6_REPLAY_KEY="SECID",
        )
        with (
            patch.dict(os.environ, env),
            patch("lake_sync.make_s3_client", return_value=s3),
            patch.object(broker_stream, "Publisher", return_value=Capture()),
            patch.object(broker_stream.time, "monotonic", return_value=42),
        ):
            broker_stream.replay(stop, Mock())
        self.assertEqual(captured, [("1", "SECID"), ("2", "SECID")])
        self.assertEqual([c.args[0] for c in stop.wait.call_args_list], [0, 0.5])

    def test_broker_outage_preserves_pending_across_restart(self):
        import threading

        failed, delivered = threading.Event(), threading.Event()
        sent = []

        def send(events, key):
            sent.extend(list(events))

        def log_failure(level, message, **fields):
            if message == "publish_retry":
                failed.set()

        def log_success(level, message, **fields):
            if message == "published":
                delivered.set()

        with (
            tempfile.TemporaryDirectory() as folder,
            patch.dict(os.environ, {"COLLECTOR_TOPIC": "test"}),
        ):
            stop = threading.Event()
            with patch.object(
                broker_stream, "Publisher", side_effect=OSError("offline")
            ):
                q = broker_stream.Outbox(Path(folder), stop, log_failure)
                q.append([{"event_key": "a"}])
                self.assertTrue(failed.wait(3))
                q.append([{"event_key": "b"}])
                stop.set()
                q.thread.join(2)
                q.db.close()
            stop = threading.Event()
            with patch.object(broker_stream, "Publisher") as pub:
                pub.return_value.send.side_effect = send
                q = broker_stream.Outbox(Path(folder), stop, log_success)
                self.assertTrue(delivered.wait(3))
                stop.set()
                q.thread.join(2)
                self.assertEqual(
                    q.db.execute("SELECT count(*) FROM pending").fetchone()[0], 0
                )
                q.db.close()
            self.assertEqual(sent, [{"event_key": "a"}, {"event_key": "b"}])

    @mock_aws
    def test_avro_transport_retry_and_lake_comparison(self):
        topic = sink.TOPIC
        owner = Producer({"test.mock.num.brokers": 1, "log_level": 0})
        node = next(iter(owner.list_topics(timeout=5).brokers.values()))
        addr = f"{node.host}:{node.port}"
        registry = SchemaRegistryClient.new_client({"url": "mock://lr6"})
        registry.register_schema(
            topic + "-value", Schema(json.dumps(sink.SCHEMA), "AVRO")
        )
        stamp = "2026-11-02T12:03:01.000Z"
        events = [
            dict(
                event_key=str(i),
                event_time=stamp,
                received_at=stamp,
                source="moex",
                SECID=sec,
                BOARDID="TQBR",
                SYSTIME="2026-11-02 15:03:01",
            )
            for i, sec in enumerate(("SBER", "GAZP", "SMCF"))
        ]
        with (
            patch.dict(
                os.environ, {"REGISTRY_URL": "mock://lr6", "BROKER_BOOTSTRAP": addr}
            ),
            patch.object(broker_stream, "SchemaRegistryClient", return_value=registry),
        ):
            broker_stream.Publisher(topic).send(events, "BOARDID")
        consumer = Consumer({"bootstrap.servers": addr, "group.id": "lr6-test"})
        try:
            parts = consumer.list_topics(topic, timeout=5).topics[topic].partitions
            consumer.assign([TopicPartition(topic, p, 0) for p in parts])
            messages = consumer.consume(3, timeout=10)
            self.assertEqual([m.key() for m in messages], [b"TQBR"] * 3)
            self.assertEqual(len({m.partition() for m in messages}), 1)
            s3 = boto3.client("s3", region_name="us-east-1")
            s3.create_bucket(Bucket=sink.BUCKET)
            file = "raw/stream/dt=2026-11-02/hour=12/part-test.jsonl.gz"
            s3.put_object(
                Bucket=sink.BUCKET,
                Key=file,
                Body=gzip.compress(
                    b"".join(json.dumps(e).encode() + b"\n" for e in events)
                ),
            )
            offsets = Mock()
            offsets.commit.side_effect = [RuntimeError("commit interrupted"), []]
            p = messages[0].partition()
            decode = AvroDeserializer(registry)
            args = (s3, offsets, p, messages, decode, sink.arrow_schema())
            with self.assertRaisesRegex(RuntimeError, "commit interrupted"):
                sink.persist(*args)
            sink.persist(s3, offsets, p, messages[:1], decode, sink.arrow_schema())
            self.assertEqual(
                offsets.seek.call_args.args[0].offset, messages[-1].offset() + 1
            )
            with (
                tempfile.TemporaryDirectory() as tmp,
                patch.object(lr6, "storage", return_value=s3),
                patch.object(lr6, "ROOT", Path(tmp)),
            ):
                lr6.compare("2026-11-02T12")
                result = json.loads((Path(tmp) / "data/lr6/compare.json").read_text())
                self.assertEqual(
                    (result["topic_rows"], result["stream_only"], result["topic_only"]),
                    (3, 0, 0),
                )
        finally:
            consumer.close()


if __name__ == "__main__":
    unittest.main()
