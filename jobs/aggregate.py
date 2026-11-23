"""Hourly MOEX aggregates for one closed UTC partition, or a measured join experiment."""
import argparse
import json
import os
import time
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from urllib.request import urlopen

from pyspark.sql import SparkSession, functions as F

EVENTS = "lake.clean.events"
HOURLY = "lake.clean.events_hourly"


def session():
    builder = (SparkSession.builder.appName("LR7 MOEX")
               .master("local[2]").config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.extensions",
                       "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.sql.adaptive.enabled", "false")
               .config("spark.sql.ui.explainMode", "formatted")
               .config("spark.redaction.regex", "(?i)secret|password|token|access[.]?key|access-key"))
    catalog = {"": "org.apache.iceberg.spark.SparkCatalog", ".type": "rest",
               ".io-impl": "org.apache.iceberg.aws.s3.S3FileIO", ".s3.path-style-access": "true"}
    for key, env in {".uri": "CATALOG_URI", ".warehouse": "CATALOG_WAREHOUSE",
                     ".s3.endpoint": "S3_ENDPOINT", ".client.region": "S3_REGION",
                     ".s3.access-key-id": "S3_ACCESS_KEY", ".s3.secret-access-key": "S3_SECRET_KEY"}.items():
        catalog[key] = os.environ[env]
    for suffix, value in catalog.items():
        builder = builder.config("spark.sql.catalog.lake" + suffix, value)
    return builder.getOrCreate()


def events(spark, start, end):
    return spark.table(EVENTS).where(
        (F.col("event_time") >= F.lit(str(start)).cast("timestamp_ntz")) &
        (F.col("event_time") < F.lit(str(end)).cast("timestamp_ntz")))


def plan(frame):
    buf = StringIO()
    with redirect_stdout(buf):
        frame.explain("formatted")
    print(buf.getvalue(), flush=True)
    return buf.getvalue()


def aggregate(spark, day, measure_files=False):
    source = events(spark, day, day + timedelta(days=1))
    grouped = source.groupBy(F.date_trunc("hour", "event_time").alias("event_hour"), "SECID").agg(
        F.count("*").alias("events"), F.countDistinct("BOARDID", "SECID").alias("entities"),
        F.coalesce(F.sum("QTY"), F.lit(0)).alias("qty_sum"))
    result = grouped.select(F.lit(day).alias("event_date"),
                            F.col("event_hour").cast("timestamp_ntz").alias("event_hour"),
                            "SECID", "events", "entities", "qty_sum")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {HOURLY} (event_date DATE, event_hour TIMESTAMP_NTZ,
                  SECID STRING, events BIGINT, entities BIGINT, qty_sum BIGINT)
                  USING iceberg PARTITIONED BY (event_date)""")
    physical = plan(result)
    seen = {s["stageId"] for s in rest(spark, "stages")}
    result.writeTo(HOURLY).overwrite(F.col("event_date") == F.lit(day))
    stages = [s for s in rest(spark, "stages") if s["stageId"] not in seen and s["status"] == "COMPLETE"]
    keys = ("stageId", "attemptId", "numCompleteTasks", "inputBytes", "shuffleReadBytes", "shuffleWriteBytes")
    measured = {}
    if measure_files:
        # File footprints are independent of Spark's task-level byte counters.
        touched = source.select(F.col("_file").alias("file_path")).distinct()
        files = spark.table(EVENTS + ".files").where("content = 0")
        footprint = files.join(touched, "file_path").agg(F.count("*"), F.sum("file_size_in_bytes")).first()
        measured = {"day_files": footprint[0], "day_file_bytes": footprint[1] or 0,
                    "history_files": files.count()}
    return {**measured, "day": str(day), "app": spark.sparkContext.applicationId, "plan": physical,
            "stages": [{k: s.get(k, 0) for k in keys} for s in stages],
            "input_bytes": sum(s["inputBytes"] for s in stages)}


def rest(spark, path):
    sc = spark.sparkContext
    port = sc.uiWebUrl.rsplit(":", 1)[1]
    with urlopen(f"http://127.0.0.1:{port}/api/v1/applications/{sc.applicationId}/{path}", timeout=30) as r:
        return json.load(r)


def partition_totals(rows):
    count = weight = 0
    for row in rows:
        count += 1
        weight += row.n
    yield count, weight


def join_trial(spark, events_df, dimension, broadcast):
    sc = spark.sparkContext
    label = "broadcast" if broadcast else "merge"
    sc.setJobGroup(label, "LR7 join " + label)
    right = F.broadcast(dimension) if broadcast else dimension.hint("merge")
    joined = events_df.join(right, "SECID").select("n")
    physical = plan(joined)
    # The terminal result stage consumes join output in the same tasks (no later exchange).
    totals = joined.rdd.mapPartitions(partition_totals).collect()
    jobs = [sc.statusTracker().getJobInfo(i) for i in sc.statusTracker().getJobIdsForGroup(label)]
    final_stage = max(s for job in jobs if job for s in job.stageIds)
    attempt = next(s for s in rest(spark, f"stages/{final_stage}") if s["status"] == "COMPLETE")
    tasks = rest(spark, f"stages/{final_stage}/{attempt['attemptId']}/taskList?length=100000")
    distribution = [{"partition": t["index"], "ms": t["duration"], "rows": totals[t["index"]][0]}
                    for t in tasks if t["status"] == "SUCCESS"]
    durations = sorted(t["ms"] for t in distribution)
    def stamp(key):
        return datetime.fromisoformat(attempt[key].replace("GMT", "+00:00").replace("Z", "+00:00"))
    elapsed = (stamp("completionTime") - stamp("submissionTime")).total_seconds()
    return {"plan": physical, "stage": final_stage, "stage_seconds": elapsed,
            "task_ms": durations, "tasks": distribution, "partition_rows": [v[0] for v in totals],
            "rows": sum(v[0] for v in totals), "weight": sum(v[1] for v in totals)}


def skew(spark, day, days):
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    start = day - timedelta(days=days - 1)
    source = events(spark, start, day + timedelta(days=1)).select("SECID").cache()
    count = source.count()
    if not count:
        raise ValueError("Пустой период эксперимента")
    dim = source.groupBy("SECID").agg(F.count("*").alias("n")).cache()
    keys = dim.count()
    top = dim.orderBy(F.desc("n"), "SECID").first()
    partitions = int(spark.conf.get("spark.sql.shuffle.partitions"))
    hot_partition = dim.where(F.col("SECID") == top.SECID).select(
        F.pmod(F.hash("SECID"), F.lit(partitions))).first()[0]
    before = join_trial(spark, source, dim, False)
    after = join_trial(spark, source, dim, True)
    if (before["rows"] != count or after["rows"] != count or before["weight"] != after["weight"]
            or "SortMergeJoin" not in before["plan"] or "BroadcastHashJoin" not in after["plan"]):
        raise ValueError("Планы или итоги соединений расходятся с условиями опыта")
    return {"start": str(start), "end": str(day), "keys": keys, "rows": count, "top": top.asDict(),
            "merge_hot_partition": hot_partition, "top_to_mean": top.n / (count / keys), "merge": before, "broadcast": after}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("day", type=date.fromisoformat)
    p.add_argument("--skew-days", type=int)
    p.add_argument("--hold", type=int, default=0, help="seconds to keep Spark UI alive")
    p.add_argument("--report", type=Path)
    a = p.parse_args()
    if a.day >= datetime.now(timezone.utc).date() or (a.skew_days is not None and a.skew_days < 1):
        p.error("Нужны закрытые сутки и положительная длина периода")
    spark = session()
    try:
        report = skew(spark, a.day, a.skew_days) if a.skew_days else aggregate(spark, a.day, a.report is not None)
        print(json.dumps(report, ensure_ascii=False, default=str), flush=True)
        if a.report:
            a.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        if a.hold:
            print(f"Spark UI открыт ещё {a.hold} с", flush=True)
            time.sleep(a.hold)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
