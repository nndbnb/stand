"""Commands for the variant 12 streaming lab. Run from repository root."""

import argparse
import copy
import gzip
import json
import subprocess
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import requests
from confluent_kafka import Consumer, TopicPartition
from dotenv import set_key

from lr6_common import (
    BROKER,
    BUCKET,
    ENV,
    RAW,
    REGISTRY,
    ROOT,
    SCHEMA,
    TOPIC,
    objects,
    storage,
)


def configure():
    files = ENV["COMPOSE_FILE"].split(":")
    if (
        "compose.broker-redpanda.yaml" in files
        or ENV.get("S3") != "seaweedfs"
        or not ENV.get("COLLECTOR_LAKE_PREFIX")
    ):
        raise ValueError("Expected SeaweedFS with raw lake sync and Kafka variant")
    if "compose.broker-kafka.yaml" not in files:
        files.append("compose.broker-kafka.yaml")
    for name, value in {
        "BROKER": "kafka",
        "COMPOSE_FILE": ":".join(files),
        "COLLECTOR_TOPIC": TOPIC,
    }.items():
        set_key(str(ROOT / ".env"), name, value, quote_mode="never")


def cli(program, *args):
    return subprocess.check_output(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "broker",
            "/opt/kafka/bin/kafka-" + program + ".sh",
            "--bootstrap-server",
            "localhost:9092",
            *args,
        ],
        text=True,
    )


def register(topic, negative=False):
    cli(
        "topics",
        "--create",
        "--if-not-exists",
        "--topic",
        topic,
        "--partitions",
        "3",
        "--replication-factor",
        "1",
    )
    subject = "/subjects/" + topic + "-value"

    def post(path, schema):
        return requests.post(
            REGISTRY + path, json={"schema": json.dumps(schema)}, timeout=20
        )

    good = post(subject + "/versions", SCHEMA)
    good.raise_for_status()
    if negative:
        mode = requests.get(
            REGISTRY + "/config/" + topic + "-value",
            params={"defaultToGlobal": "true"},
            timeout=15,
        )
        if mode.status_code == 404:
            mode = requests.get(REGISTRY + "/config", timeout=15)
        mode.raise_for_status()
        bad = copy.deepcopy(SCHEMA)
        next(f for f in bad["fields"] if f["name"] == "event_time")["type"] = "string"
        check = post("/compatibility" + subject + "/versions/latest", bad)
        check.raise_for_status()
        compatible = check.json()["is_compatible"]
        if compatible is not False:
            raise ValueError("Registry accepted incompatible schema in validation")
        refused = post(subject + "/versions", bad)
        report = dict(
            subject=topic + "-value",
            schema_id=good.json()["id"],
            mode=mode.json(),
            is_compatible=compatible,
            registration_status=refused.status_code,
        )
        print(json.dumps(report, ensure_ascii=False))
        if "BACKWARD" not in mode.json().values() or refused.status_code != 409:
            raise ValueError("Expected BACKWARD and 409")
        save("registry", report)


def save(name, data):
    folder = ROOT / "data/lr6"
    folder.mkdir(parents=True, exist_ok=True)
    (folder / (name + ".json")).write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n"
    )


def stats(topic):
    consumer = Consumer({"bootstrap.servers": BROKER, "group.id": "lr6-inspect"})
    try:
        meta = consumer.list_topics(topic, timeout=12).topics[topic]
        if meta.error or len(meta.partitions) != 3:
            raise ValueError("Expected three partitions for " + topic)
        logs = cli("log-dirs", "--describe", "--topic-list", topic)
        data = json.loads(logs[logs.index("{") :])
        sizes = {}
        for broker in data["brokers"]:
            for directory in broker["logDirs"]:
                for p in directory["partitions"]:
                    sizes[p["partition"]] = p["size"]
        result = []
        for part in sorted(meta.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, part), timeout=10
            )
            result.append(
                dict(
                    partition=part, messages=high - low, bytes=sizes[f"{topic}-{part}"]
                )
            )
        return result
    finally:
        consumer.close()


def latest_hour(layer):
    # List directory prefixes, without enumerating all objects in the lake.
    s3 = storage()
    prefix = RAW + "/" + layer + "/"

    def directories(root):
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix=root, Delimiter="/"
        )
        return sorted(
            (d["Prefix"] for p in pages for d in p.get("CommonPrefixes", [])),
            reverse=True,
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H")
    for day in directories(prefix):
        for path in directories(day):
            value = (
                day.rstrip("/").split("dt=")[-1]
                + "T"
                + path.rstrip("/").split("hour=")[-1]
            )
            if value >= now:
                continue
            rel = path[len(prefix) :]
            if next(objects(s3, RAW + "/stream/" + rel, ".jsonl.gz"), None):
                print(value)
                return
    raise ValueError("Нет завершённого часа для " + layer)


def parse_hour(value):
    return datetime.strptime(value, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)


def replay(value):
    start = parse_hour(value)
    end = start + timedelta(hours=1)
    if end > datetime.now(timezone.utc):
        raise ValueError("Select a closed hour in UTC")
    s3 = storage()
    prefix = RAW + start.strftime("/stream/dt=%Y-%m-%d/hour=%H/")
    count = 0
    for key in objects(s3, prefix, ".jsonl.gz"):
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"]
        try:
            with gzip.GzipFile(fileobj=body) as f:
                count += sum(1 for line in f if line.strip())
        finally:
            body.close()
    if not count:
        raise ValueError("No closed files in " + prefix)
    report = dict(hour=value, source_rows=count, live=stats(TOPIC), runs={})
    run = uuid.uuid4().hex[:12]
    for key in ("BOARDID", "SECID"):
        topic = f"moex.tqbr.replay.{run}.{key.lower()}"
        register(topic)
        subprocess.run(
            [
                "docker",
                "compose",
                "run",
                "--rm",
                "--no-deps",
                "-T",
                "-e",
                "COLLECTOR_TOPIC=" + topic,
                "-e",
                "LR6_REPLAY_KEY=" + key,
                "-e",
                "COLLECTOR_REPLAY_FROM=" + start.isoformat(),
                "-e",
                "COLLECTOR_REPLAY_TO=" + end.isoformat(),
                "-e",
                "COLLECTOR_REPLAY_SPEED=120",
                "collector",
                "replay",
            ],
            check=True,
        )
        rows = stats(topic)
        report["runs"][key] = dict(topic=topic, partitions=rows)
        if sum(p["messages"] for p in rows) != count:
            raise ValueError(f"Source has {count} events; Kafka: {rows}")
    print(json.dumps(report, indent=2))
    save("replay", report)
    if max(p["messages"] for p in report["runs"]["SECID"]["partitions"]) >= count:
        raise ValueError(
            "No improvement: choose an hour with multiple active securities"
        )


def compare(value):
    hour = parse_hour(value)
    if hour + timedelta(hours=1) > datetime.now(timezone.utc):
        raise ValueError("Select a closed hour in UTC")
    suffix = hour.strftime("/dt=%Y-%m-%d/hour=%H/")
    s3 = storage()
    with tempfile.TemporaryDirectory() as tmp, duckdb.connect() as db:
        for layer, ext, reader in (
            ("stream", ".jsonl.gz", "read_json"),
            ("topic", ".parquet", "read_parquet"),
        ):
            keys = list(objects(s3, RAW + "/" + layer + suffix, ext))
            if not keys:
                raise ValueError("No " + layer + " objects for " + value)
            for i, key in enumerate(keys):
                s3.download_file(BUCKET, key, str(Path(tmp) / f"{layer}{i}{ext}"))
            opts = (
                ",columns={event_key:'VARCHAR'},format='newline_delimited'"
                if layer == "stream"
                else ""
            )
            db.execute(
                f"CREATE TEMP TABLE {layer} AS SELECT event_key FROM {reader}(?{opts})",
                [str(Path(tmp) / (layer + "*" + ext))],
            )
        report = {"hour": value}
        for a, b in (("stream", "topic"), ("topic", "stream")):
            report[a + "_rows"] = db.execute(f"SELECT count(*) FROM {a}").fetchone()[0]
            diff = f"SELECT event_key FROM {a} EXCEPT SELECT event_key FROM {b}"
            report[a + "_only"] = db.execute(
                f"SELECT count(*) FROM ({diff})"
            ).fetchone()[0]
            report[a + "_examples"] = [
                x[0] for x in db.execute(diff + " LIMIT 5").fetchall()
            ]
        print(json.dumps(report, indent=2))
        save("compare", report)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in (
        "configure",
        "registry",
        "produce",
        "stats",
        "lag",
        "replay",
        "compare",
        "stream-hour",
        "common-hour",
    ):
        cmd = sub.add_parser(name)
        if name in ("replay", "compare"):
            cmd.add_argument("hour", help="UTC YYYY-MM-DDTHH")
    a = p.parse_args()
    if a.cmd == "configure":
        configure()
    elif a.cmd == "produce":
        set_key(str(ROOT / ".env"), "COLLECTOR_MODE", "produce", quote_mode="never")
    elif a.cmd == "registry":
        register(TOPIC, negative=True)
    elif a.cmd == "replay":
        replay(a.hour)
    elif a.cmd == "compare":
        compare(a.hour)
    elif a.cmd == "stream-hour":
        latest_hour("stream")
    elif a.cmd == "common-hour":
        latest_hour("topic")
    elif a.cmd == "stats":
        print(json.dumps(stats(TOPIC), indent=2))
    elif a.cmd == "lag":
        table = cli("consumer-groups", "--describe", "--group", "sink")
        stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
        entry = f"{stamp}\n{table}\n"
        folder = ROOT / "data/lr6"
        folder.mkdir(parents=True, exist_ok=True)
        with (folder / "lag.txt").open("a") as log:
            log.write(entry)
        print(entry)
