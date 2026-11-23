"""Variant 12. Each run owns exactly one closed UTC day."""
import json
import subprocess
import sys
from datetime import date

from dagster import (BackfillPolicy, DailyPartitionsDefinition, DefaultScheduleStatus, Definitions,
                     Field, MaterializeResult, asset, build_schedule_from_partitioned_job,
                     define_asset_job, in_process_executor)

import lr7


days = DailyPartitionsDefinition(start_date="2026-10-01", timezone="UTC")
daily = {"partitions_def": days, "backfill_policy": BackfillPolicy.multi_run(max_partitions_per_run=1)}


@asset(**daily)
def clean_parquet(context):
    return MaterializeResult(metadata=lr7.clean_day(date.fromisoformat(context.partition_key)))


@asset(deps=[clean_parquet], **daily)
def iceberg_events(context):
    return MaterializeResult(metadata=lr7.load_day(date.fromisoformat(context.partition_key)))


@asset(deps=[iceberg_events], **daily)
def iceberg_hourly(context):
    subprocess.run([sys.executable, "/opt/jobs/aggregate.py", context.partition_key], check=True)
    return MaterializeResult(metadata={"partition": context.partition_key})


@asset(deps=[iceberg_hourly], **daily)
def clickhouse_hourly(context):
    return MaterializeResult(metadata=lr7.publish(date.fromisoformat(context.partition_key)))


@asset(deps=[clickhouse_hourly], config_schema={"inject_missing": Field(bool, default_value=False)}, **daily)
def quality_checks(context):
    metrics = lr7.quality(date.fromisoformat(context.partition_key), context.op_config["inject_missing"])
    if metrics["freshness"] == "WARN":
        context.log.warning("Свежесть MOEX: " + json.dumps(metrics, ensure_ascii=False))
    return MaterializeResult(metadata=metrics)


assets = [clean_parquet, iceberg_events, iceberg_hourly, clickhouse_hourly, quality_checks]
job = define_asset_job("lr7_daily", selection=[a.key for a in assets],
                       executor_def=in_process_executor)
schedule = build_schedule_from_partitioned_job(job, name="lr7_0130_utc", hour_of_day=1,
                                              minute_of_hour=30,
                                              default_status=DefaultScheduleStatus.RUNNING)
defs = Definitions(assets=assets, jobs=[job], schedules=[schedule])
