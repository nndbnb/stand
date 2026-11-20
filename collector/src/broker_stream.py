"""Kafka publication, restartable live queue and timed replay from raw.stream."""

import gzip
import json
import math
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


def utc(value):
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.tzinfo is None:
        raise ValueError("Expected timestamp with timezone: " + value)
    return stamp.astimezone(timezone.utc)


class Publisher:
    def __init__(self, topic):
        registry = SchemaRegistryClient({"url": os.environ["REGISTRY_URL"]})
        schema = registry.get_latest_version(topic + "-value").schema
        self.encode = AvroSerializer(
            registry, schema, conf={"auto.register.schemas": False}
        )
        self.context = SerializationContext(topic, MessageField.VALUE)
        self.topic = topic
        self.client = Producer(
            {
                "bootstrap.servers": os.environ["BROKER_BOOTSTRAP"],
                "enable.idempotence": True,
                "message.timeout.ms": 20000,
            }
        )

    def send(self, events, key):
        errors = []

        def delivered(error, message):
            if error:
                errors.append(str(error))

        for event in events:
            row = dict(event, event_time=utc(event["event_time"]))
            self.client.produce(
                self.topic,
                key=row[key].encode(),
                value=self.encode(row, self.context),
                on_delivery=delivered,
            )
            self.client.poll(0)
        if self.client.flush(25) or errors:
            raise RuntimeError("Kafka delivery failed: " + str(errors[:1]))


class Outbox:
    def __init__(self, directory, stop, log):
        self.path = directory / "lr6-outbox.sqlite"
        self.db = sqlite3.connect(self.path, timeout=10)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS pending (seq INTEGER PRIMARY KEY, event TEXT NOT NULL)"
        )
        self.stop, self.log = stop, log
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def append(self, events):
        with self.db:
            self.db.executemany(
                "INSERT INTO pending(event) VALUES (?)",
                ((json.dumps(event),) for event in events),
            )

    def run(self):
        db = sqlite3.connect(self.path, timeout=10)
        publisher = None
        try:
            while not self.stop.is_set():
                try:
                    batch = db.execute(
                        "SELECT seq,event FROM pending ORDER BY seq LIMIT 1000"
                    ).fetchall()
                    if not batch:
                        self.stop.wait(0.5)
                        continue
                    if publisher is None:
                        publisher = Publisher(os.environ["COLLECTOR_TOPIC"])
                    publisher.send((json.loads(row) for _, row in batch), "BOARDID")
                    with db:
                        db.execute(
                            "DELETE FROM pending WHERE seq <= ?", (batch[-1][0],)
                        )
                    self.log("info", "published", count=len(batch))
                except Exception as exc:
                    self.log("error", "publish_retry", error=str(exc))
                    publisher = None
                    self.stop.wait(5)
        finally:
            db.close()


def replay(stop, log):
    from lake_sync import make_s3_client

    start = utc(os.environ["COLLECTOR_REPLAY_FROM"])
    end = utc(os.environ["COLLECTOR_REPLAY_TO"])
    speed = float(os.environ["COLLECTOR_REPLAY_SPEED"])
    topic = os.environ["COLLECTOR_TOPIC"]
    if not topic.startswith("moex.tqbr.replay.") or not (
        end > start and math.isfinite(speed) and speed > 0
    ):
        raise ValueError(
            "Expected moex.tqbr.replay.* topic, positive interval and speed"
        )
    key = os.getenv("LR6_REPLAY_KEY", "BOARDID")
    if key not in {"SECID", "BOARDID"}:
        raise ValueError("Unknown replay key")
    s3, bucket = make_s3_client(), os.environ["S3_BUCKET"]
    publisher = Publisher(topic)
    hour = start.replace(minute=0, second=0, microsecond=0)
    origin = time.monotonic()
    batch, previous, count = [], None, 0
    while hour < end:
        prefix = os.environ["COLLECTOR_LAKE_PREFIX"].strip("/") + hour.strftime(
            "/stream/dt=%Y-%m-%d/hour=%H/"
        )
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix
        )
        for page in pages:
            for obj in sorted(page.get("Contents", []), key=lambda o: o["Key"]):
                if not obj["Key"].endswith(".jsonl.gz"):
                    continue
                body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"]
                try:
                    with gzip.GzipFile(fileobj=body) as stream:
                        for line in stream:
                            row = json.loads(line)
                            stamp = utc(row["received_at"])
                            if not start <= stamp < end:
                                continue
                            if stamp != previous or len(batch) >= 1000:
                                if batch:
                                    publisher.send(batch, key)
                                    batch.clear()
                                if stop.wait(
                                    max(
                                        0,
                                        origin
                                        + (stamp - start).total_seconds() / speed
                                        - time.monotonic(),
                                    )
                                ):
                                    raise InterruptedError("replay stopped")
                                previous = stamp
                            batch.append(row)
                            count += 1
                finally:
                    body.close()
        hour += timedelta(hours=1)
    if batch:
        publisher.send(batch, key)
    if not count:
        raise ValueError("No raw.stream events for this interval")
    log("info", "replay_complete", topic=topic, count=count, key=key)
