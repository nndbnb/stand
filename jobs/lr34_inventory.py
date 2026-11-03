#!/usr/bin/env python3
"""Read-only S3 layer inventory for LR3/LR4; outputs JSON without credentials."""
import json
import re
from collections import defaultdict
from datetime import date

from iceberg_load import read_env, make_client


def main():
    env = read_env()
    bucket = env.get("S3_BUCKET", "lake")
    counts = defaultdict(lambda: {"objects": 0, "bytes": 0})
    daysets = defaultdict(set)
    client = make_client(env)
    raw_prefix = env.get("COLLECTOR_LAKE_PREFIX", "raw").strip("/")
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):
                continue
            if key.startswith(raw_prefix + "/stream/"):
                layer = "raw.stream"
            elif key.startswith(raw_prefix + "/history/"):
                layer = "raw.history"
            elif key.startswith("clean/history/"):
                layer = "clean.history"
            elif key.startswith("clean/event_date="):
                layer = "clean.events"
            else:
                layer = "iceberg_or_other"
            counts[layer]["objects"] += 1
            counts[layer]["bytes"] += int(obj["Size"])
            match = re.search(r"(?:dt|event_date)=(\d{4}-\d{2}-\d{2})(?:/|$)", key)
            if match:
                daysets[layer].add(match.group(1))
    for layer in counts:
        counts[layer]["dates"] = sorted(daysets[layer])
    print(json.dumps({"bucket": bucket, "layers": dict(counts)},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
