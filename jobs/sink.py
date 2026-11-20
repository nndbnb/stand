"""Consumer group sink. Run from the WSL virtual environment."""

import argparse
from collections import defaultdict
from datetime import datetime, timezone
from itertools import groupby

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from lr6_common import BROKER, BUCKET, RAW, REGISTRY, SCHEMA, TOPIC, storage


def arrow_schema():
    types = {"long": pa.int64(), "string": pa.string(), "double": pa.float64()}
    fields = []
    for field in SCHEMA["fields"]:
        t = field["type"]
        if isinstance(t, list):
            t = next(v for v in t if v != "null")
        typ = pa.timestamp("ms", tz="UTC") if isinstance(t, dict) else types[t]
        fields.append((field["name"], typ))
    return pa.schema(fields)


def hour(row):
    stamp = datetime.fromisoformat(row["received_at"].replace("Z", "+00:00"))
    return stamp.astimezone(timezone.utc).strftime("dt=%Y-%m-%d/hour=%H")


def persist(s3, consumer, part, messages, decode, schema):
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    rows = [(m.offset(), decode(m.value(), ctx)) for m in messages]
    for bucket_hour, chunk in groupby(rows, key=lambda pair: hour(pair[1])):
        batch = list(chunk)
        first, last = batch[0][0], batch[-1][0]
        path = f"{RAW}/topic/{bucket_hour}/p{part}-{first:020d}.parquet"
        try:
            saved = s3.head_object(Bucket=BUCKET, Key=path)
        except ClientError as error:
            if error.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            saved = None
        if saved is None:
            buf = pa.BufferOutputStream()
            pq.write_table(
                pa.Table.from_pylist([r for _, r in batch], schema=schema), buf
            )
            try:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=path,
                    Body=buf.getvalue().to_pybytes(),
                    IfNoneMatch="*",
                    Metadata={"next-offset": str(last + 1)},
                )
            except ClientError as error:
                if error.response["ResponseMetadata"]["HTTPStatusCode"] != 412:
                    raise
                saved = s3.head_object(Bucket=BUCKET, Key=path)
        # A previous writer may already have stored a different sized batch at first.
        next_offset = int(saved["Metadata"]["next-offset"]) if saved else last + 1
        if next_offset <= first:
            raise ValueError("Invalid next-offset in " + path)
        position = TopicPartition(TOPIC, part, next_offset)
        result = consumer.commit(offsets=[position], asynchronous=False)
        for item in result:
            if item.error:
                raise KafkaException(item.error)
        print(f"{path}  next-offset={next_offset}", flush=True)
        if next_offset != last + 1:
            consumer.seek(position)
            break


def run(idle_exit):
    consumer = Consumer(
        {
            "bootstrap.servers": BROKER,
            "group.id": "sink",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    decode = AvroDeserializer(SchemaRegistryClient({"url": REGISTRY}))
    s3, schema = storage(), arrow_schema()
    consumer.subscribe([TOPIC])
    try:
        while True:
            parts = defaultdict(list)
            for message in consumer.consume(2000, timeout=2):
                if message.error():
                    raise KafkaException(message.error())
                parts[message.partition()].append(message)
            for part, messages in parts.items():
                persist(s3, consumer, part, messages, decode, schema)
            if idle_exit and not parts and consumer.assignment():
                return
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--until-idle", action="store_true")
    run(parser.parse_args().until_idle)
