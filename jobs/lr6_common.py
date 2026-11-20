import json
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
ENV = dotenv_values(ROOT / ".env")
TOPIC = ENV.get("COLLECTOR_TOPIC") or "moex.tqbr.events"
REGISTRY = "http://localhost:" + (ENV.get("PORT_REGISTRY") or "8081")
BROKER = "localhost:" + (ENV.get("PORT_BROKER") or "9092")
BUCKET = ENV.get("S3_BUCKET") or "lake"
RAW = (ENV.get("COLLECTOR_LAKE_PREFIX") or "raw").strip("/")
SCHEMA = json.loads((ROOT / "schemas/event_v2.avsc").read_text())


def storage():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:" + (ENV.get("PORT_S3") or "9000"),
        region_name=ENV["S3_REGION"],
        aws_access_key_id=ENV["S3_ACCESS_KEY"],
        aws_secret_access_key=ENV["S3_SECRET_KEY"],
        config=Config(s3={"addressing_style": "path"}),
    )


def objects(s3, prefix, suffix):
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=BUCKET, Prefix=prefix
    ):
        yield from (
            o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(suffix)
        )
