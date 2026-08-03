"""Run, task and layer-level audit utilities for Databricks Jobs."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


PIPELINE_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("job_id", StringType(), True),
    StructField("job_name", StringType(), True),
    StructField("environment", StringType(), True),
    StructField("catalog", StringType(), True),
    StructField("trigger_type", StringType(), True),
    StructField("repair_count", LongType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("finished_at", TimestampType(), True),
    StructField("duration_seconds", LongType(), True),
    StructField("status", StringType(), False),
    StructField("tasks_total", LongType(), True),
    StructField("tasks_succeeded", LongType(), True),
    StructField("tasks_failed", LongType(), True),
    StructField("error_message", StringType(), True),
    StructField("updated_at", TimestampType(), False),
])

TASK_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("task_key", StringType(), False),
    StructField("task_run_id", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("source_name", StringType(), True),
    StructField("status", StringType(), False),
    StructField("execution_count", LongType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("finished_at", TimestampType(), True),
    StructField("duration_seconds", LongType(), True),
    StructField("error_code", StringType(), True),
    StructField("error_message", StringType(), True),
    StructField("updated_at", TimestampType(), False),
])

METRIC_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("source_name", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("rows_received", LongType(), True),
    StructField("rows_inserted", LongType(), True),
    StructField("rows_updated", LongType(), True),
    StructField("rows_rejected", LongType(), True),
    StructField("rows_unchanged", LongType(), True),
    StructField("rows_current", LongType(), True),
    StructField("min_event_time", TimestampType(), True),
    StructField("max_event_time", TimestampType(), True),
    StructField("lag_seconds", LongType(), True),
    StructField("status", StringType(), False),
    StructField("error_message", StringType(), True),
    StructField("collected_at", TimestampType(), False),
])

FAILED_STATES = {
    "FAILED", "CANCELED", "EVICTED", "TIMEDOUT", "UPSTREAM_CANCELED",
    "UPSTREAM_EVICTED", "UPSTREAM_FAILED", "INTERNAL_ERROR",
}


def widget(dbutils: Any, name: str, default: str = "") -> str:
    try:
        value = dbutils.widgets.get(name)
        return value if value is not None else default
    except Exception:
        return default


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _milliseconds(value: Any) -> datetime | None:
    if not value:
        return None
    return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).replace(tzinfo=None)


def _integer(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _enum(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "UNKNOWN").upper()


def _upsert(spark: Any, table: str, rows: list[tuple], schema: StructType, keys: Iterable[str]) -> None:
    if not rows:
        return
    source = spark.createDataFrame(rows, schema=schema)
    condition = " AND ".join(f"target.`{key}` <=> source.`{key}`" for key in keys)
    (
        DeltaTable.forName(spark, table)
        .alias("target")
        .merge(source.alias("source"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def start_pipeline_run(spark: Any, dbutils: Any, pipeline_table: str) -> str:
    now = datetime.utcnow()
    run_id = widget(dbutils, "run_id") or f"manual-{now:%Y%m%d%H%M%S}"
    started_at = _parse_timestamp(widget(dbutils, "job_start_time")) or now
    row = (
        run_id,
        widget(dbutils, "job_id"),
        widget(dbutils, "job_name"),
        widget(dbutils, "environment", "dev"),
        widget(dbutils, "catalog"),
        widget(dbutils, "trigger_type", "manual"),
        _integer(widget(dbutils, "repair_count", "0")),
        started_at,
        None,
        None,
        "RUNNING",
        None,
        None,
        None,
        None,
        now,
    )
    _upsert(spark, pipeline_table, [row], PIPELINE_SCHEMA, ["run_id"])
    return run_id


def _task_rows_from_api(job_run_id: str, run_id: str, task_specs: dict[str, tuple[str, str]]) -> dict[str, tuple]:
    try:
        from databricks.sdk import WorkspaceClient

        run = WorkspaceClient().jobs.get_run(_integer(job_run_id))
        rows = {}
        for task in run.tasks or []:
            key = task.task_key
            if key not in task_specs:
                continue
            layer, source_name = task_specs[key]
            started_at = _milliseconds(task.start_time)
            finished_at = _milliseconds(task.end_time)
            duration = _integer(task.execution_duration, 0) // 1000 or None
            state = _enum(getattr(task.state, "result_state", None))
            message = getattr(task.state, "state_message", None)
            rows[key] = (
                run_id, key, str(task.run_id or ""), layer, source_name, state,
                _integer(getattr(task, "attempt_number", None), 0) + 1,
                started_at, finished_at, duration, None, message, datetime.utcnow(),
            )
        return rows
    except Exception:
        return {}


def collect_task_rows(
    dbutils: Any,
    run_id: str,
    task_specs: dict[str, tuple[str, str]],
) -> list[tuple]:
    api_rows = _task_rows_from_api(widget(dbutils, "job_run_id"), run_id, task_specs)
    now = datetime.utcnow()
    rows = []
    for key, (layer, source_name) in task_specs.items():
        if key in api_rows:
            rows.append(api_rows[key])
            continue
        state = widget(dbutils, f"state_{key}", "UNKNOWN").upper()
        error_code = widget(dbutils, f"error_{key}") or None
        rows.append((
            run_id,
            key,
            widget(dbutils, f"task_run_{key}") or None,
            layer,
            source_name,
            state,
            _integer(widget(dbutils, f"attempt_{key}", "0"), 0),
            None,
            None,
            None,
            error_code,
            error_code,
            now,
        ))
    return rows


def _delta_metrics(spark: Any, table: str, started_at: datetime) -> dict[str, int]:
    totals: dict[str, int] = {}
    history = spark.sql(f"DESCRIBE HISTORY {table}").filter(F.col("timestamp") >= F.lit(started_at))
    for row in history.select("operationMetrics").collect():
        for key, value in (row["operationMetrics"] or {}).items():
            try:
                totals[key] = totals.get(key, 0) + int(value)
            except (TypeError, ValueError):
                continue
    return totals


def _table_profile(spark: Any, table: str) -> tuple[int, datetime | None, datetime | None]:
    df = spark.table(table)
    candidates = [
        name for name in (
            "fecha_hora", "fecha_inicio", "fecha", "valido_desde",
            "load_date", "run_date", "run_timestamp",
        ) if name in df.columns
    ]
    if not candidates:
        return df.count(), None, None
    date_column = candidates[0]
    row = df.agg(
        F.count("*").alias("rows_current"),
        F.min(F.col(date_column).cast("timestamp")).alias("min_event_time"),
        F.max(F.col(date_column).cast("timestamp")).alias("max_event_time"),
    ).first()
    return row["rows_current"], row["min_event_time"], row["max_event_time"]


def collect_delta_metric_rows(
    spark: Any,
    run_id: str,
    started_at: datetime,
    table_specs: Iterable[tuple[str, str, str]],
    rejected_by_table: dict[str, int] | None = None,
) -> list[tuple]:
    now = datetime.utcnow()
    rejected_by_table = rejected_by_table or {}
    rows = []
    for layer, source_name, table in table_specs:
        try:
            metrics = _delta_metrics(spark, table, started_at)
            current, minimum, maximum = _table_profile(spark, table)
            received = _integer(
                metrics.get("numSourceRows")
                or metrics.get("numInputRows")
                or metrics.get("numOutputRows"),
                0,
            )
            inserted = _integer(metrics.get("numTargetRowsInserted") or metrics.get("numOutputRows"), 0)
            updated = _integer(metrics.get("numTargetRowsUpdated"), 0)
            unchanged = max(received - inserted - updated, 0)
            lag = int((now - maximum).total_seconds()) if maximum else None
            rows.append((
                run_id, layer, source_name, table, received, inserted, updated,
                rejected_by_table.get(table, 0),
                unchanged, current, minimum, maximum, lag, "SUCCESS", None, now,
            ))
        except Exception as exc:
            rows.append((
                run_id, layer, source_name, table, None, None, None, None, None,
                None, None, None, None, "ERROR", str(exc)[:4000], now,
            ))
    return rows


def collect_landing_metric_rows(
    spark: Any,
    run_id: str,
    landing_files: dict[str, str],
) -> list[tuple]:
    now = datetime.utcnow()
    rows = []
    for source_name, path in landing_files.items():
        try:
            df = spark.read.json(path)
            date_candidates = [
                name for name in (
                    "FechaHora", "FechaInicio", "Fecha", "fecha_hora",
                    "fecha_inicio", "fecha",
                ) if name in df.columns
            ]
            if date_candidates:
                date_column = date_candidates[0]
                profile = df.agg(
                    F.count("*").alias("rows_current"),
                    F.min(F.col(date_column).cast("timestamp")).alias("min_event_time"),
                    F.max(F.col(date_column).cast("timestamp")).alias("max_event_time"),
                ).first()
                current = profile["rows_current"]
                minimum = profile["min_event_time"]
                maximum = profile["max_event_time"]
            else:
                current, minimum, maximum = df.count(), None, None
            lag = int((now - maximum).total_seconds()) if maximum else None
            rows.append((
                run_id, "LANDING", source_name, path, current, current, 0, 0, 0,
                current, minimum, maximum, lag, "SUCCESS", None, now,
            ))
        except Exception as exc:
            rows.append((
                run_id, "LANDING", source_name, path, None, None, None, None,
                None, None, None, None, None, "ERROR", str(exc)[:4000], now,
            ))
    return rows


def finish_pipeline_run(
    spark: Any,
    dbutils: Any,
    pipeline_table: str,
    task_table: str,
    metric_table: str,
    task_specs: dict[str, tuple[str, str]],
    table_specs: Iterable[tuple[str, str, str]],
    landing_files: dict[str, str],
    quarantine_table: str | None = None,
) -> str:
    now = datetime.utcnow()
    run_id = widget(dbutils, "run_id") or widget(dbutils, "job_run_id")
    started_at = _parse_timestamp(widget(dbutils, "job_start_time")) or now
    task_rows = collect_task_rows(dbutils, run_id, task_specs)
    _upsert(spark, task_table, task_rows, TASK_SCHEMA, ["run_id", "task_key"])
    rejected_by_table = {}
    if quarantine_table and spark.catalog.tableExists(quarantine_table):
        rejected_by_table = {
            row["source_table"]: int(row["rows_rejected"] or 0)
            for row in (
                spark.table(quarantine_table)
                .filter(F.col("run_id") == run_id)
                .filter(F.col("source_table").isNotNull())
                .groupBy("source_table")
                .agg(
                    F.sum(
                        F.get_json_object("payload_json", "$.error_count").cast("long")
                    ).alias("rows_rejected")
                )
                .collect()
            )
        }
    metric_rows = [
        *collect_landing_metric_rows(spark, run_id, landing_files),
        *collect_delta_metric_rows(
            spark, run_id, started_at, table_specs, rejected_by_table,
        ),
    ]
    _upsert(
        spark, metric_table, metric_rows, METRIC_SCHEMA,
        ["run_id", "layer", "source_name", "table_name"],
    )

    succeeded = sum(row[5] == "SUCCESS" for row in task_rows)
    failures = [row for row in task_rows if row[5] in FAILED_STATES]
    unknown = [row for row in task_rows if row[5] not in FAILED_STATES | {"SUCCESS"}]
    status = "FAILED" if failures else ("INCOMPLETE" if unknown else "SUCCESS")
    messages = [f"{row[1]}: {row[11] or row[10] or row[5]}" for row in failures]
    pipeline_row = (
        run_id,
        widget(dbutils, "job_id"),
        widget(dbutils, "job_name"),
        widget(dbutils, "environment", "dev"),
        widget(dbutils, "catalog"),
        widget(dbutils, "trigger_type", "manual"),
        _integer(widget(dbutils, "repair_count", "0")),
        started_at,
        now,
        max(int((now - started_at).total_seconds()), 0),
        status,
        len(task_rows),
        succeeded,
        len(failures),
        " | ".join(messages)[:4000] or None,
        now,
    )
    _upsert(spark, pipeline_table, [pipeline_row], PIPELINE_SCHEMA, ["run_id"])
    return status
