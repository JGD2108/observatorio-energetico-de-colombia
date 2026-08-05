# Databricks notebook source
"""Production-readiness gate and operational SLO evaluation for Phase 6."""

import sys

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import (  # noqa: E402
    AUDIT_TABLES,
    CATALOG,
    MAX_LAG_DAYS,
    MONITORING_TABLES,
    SERVING_VIEWS,
)
from operations.slo import evaluate  # noqa: E402


def parameter(name: str, default: str = "") -> str:
    try:
        return str(dbutils.widgets.get(name) or default)
    except Exception:
        return default


RUN_ID = parameter("run_id", "manual")
spark.sql(f"USE CATALOG `{CATALOG}`")

# COMMAND ----------

source_profile = (
    spark.table(SERVING_VIEWS["estado_fuentes"])
    .agg(
        F.max("dias_rezago").alias("max_lag_days"),
        F.sum(F.when(~F.col("cumple_sla_frescura"), 1).otherwise(0)).alias("stale_sources"),
    )
    .first()
)
latest_day = (
    spark.table(SERVING_VIEWS["kpi_sistema_diario"])
    .orderBy(F.col("fecha").desc())
    .select("fecha", "horas_con_datos", "completitud_horaria_pct")
    .first()
)
quality_profile = (
    spark.table(AUDIT_TABLES["data_quality_results"])
    .filter(F.col("run_id") == RUN_ID)
    .agg(
        F.count("*").alias("rules_evaluated"),
        F.sum(F.when((~F.col("passed")) & F.col("is_blocking"), 1).otherwise(0)).alias("blocking_failures"),
    )
    .first()
)
open_alerts = (
    spark.table(AUDIT_TABLES["data_quality_alerts"])
    .filter((F.col("status") == "OPEN") & F.col("severity").isin("CRITICAL", "HIGH"))
    .count()
)
recent_runs = (
    spark.table(AUDIT_TABLES["pipeline_runs"])
    .filter(
        (F.col("run_id") != RUN_ID)
        & F.col("finished_at").isNotNull()
        & F.col("status").isin("SUCCESS", "FAILED")
    )
    .orderBy(F.col("started_at").desc())
    .limit(10)
    .agg(
        F.count("*").alias("sample_size"),
        F.avg(F.when(F.col("status") == "SUCCESS", 100.0).otherwise(0.0)).alias("success_rate"),
    )
    .first()
)
views_ready = sum(spark.catalog.tableExists(name) for name in SERVING_VIEWS.values())

results = [
    evaluate("SLO_FRESHNESS", "max_source_lag_days", float(source_profile["max_lag_days"] or 9999), "<=", float(MAX_LAG_DAYS), detail=f"Fuentes fuera de SLA: {int(source_profile['stale_sources'] or 0)}"),
    evaluate("SLO_HOURLY_COMPLETENESS", "latest_day_completeness_pct", float(latest_day["completitud_horaria_pct"] if latest_day else 0), ">=", 100.0, detail=f"Ultimo dia disponible: {latest_day['fecha'] if latest_day else None}"),
    evaluate("SLO_QUALITY_EVIDENCE", "quality_rules_evaluated", float(quality_profile["rules_evaluated"] or 0), ">=", 1.0),
    evaluate("SLO_BLOCKING_QUALITY", "blocking_quality_failures", float(quality_profile["blocking_failures"] or 0), "<=", 0.0),
    evaluate("SLO_OPEN_ALERTS", "open_high_critical_alerts", float(open_alerts), "<=", 0.0),
    evaluate(
        "SLO_PIPELINE_SUCCESS",
        "last_10_runs_success_pct",
        float(recent_runs["success_rate"] if recent_runs["success_rate"] is not None else 100.0),
        ">=",
        95.0,
        blocking=False,
        detail=f"Ventana: {int(recent_runs['sample_size'] or 0)} ejecuciones historicas finalizadas; excluye la ejecucion actual",
    ),
    evaluate("SLO_SERVING_CONTRACT", "serving_views_ready", float(views_ready), "==", float(len(SERVING_VIEWS)), detail=f"Vistas esperadas: {len(SERVING_VIEWS)}"),
]

schema = StructType([
    StructField("slo_id", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("operator", StringType(), False),
    StructField("threshold", DoubleType(), False),
    StructField("passed", BooleanType(), False),
    StructField("blocking", BooleanType(), False),
    StructField("detail", StringType(), True),
])
results_df = spark.createDataFrame([
    (item.slo_id, item.metric_name, item.metric_value, item.operator, item.threshold, item.passed, item.blocking, item.detail)
    for item in results
], schema=schema).withColumn("run_id", F.lit(RUN_ID)).withColumn("measured_at", F.current_timestamp()).select(
    "run_id", "slo_id", "metric_name", "metric_value", "operator", "threshold", "passed", "blocking", "detail", "measured_at"
)
results_df.createOrReplaceTempView("phase6_slo_results")
spark.sql(f"""
MERGE INTO {MONITORING_TABLES['slo_results']} target
USING phase6_slo_results source
ON target.run_id = source.run_id AND target.slo_id = source.slo_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

spark.sql(f"""
MERGE INTO {MONITORING_TABLES['operational_alerts']} target
USING phase6_slo_results source
ON target.slo_id = source.slo_id AND target.status = 'OPEN'
WHEN MATCHED AND source.passed THEN UPDATE SET
  target.status = 'RESOLVED', target.resolved_at = current_timestamp()
""")

alerts_df = (
    results_df.filter(~F.col("passed"))
    .withColumn("alert_id", F.sha2(F.concat_ws("|", "run_id", "slo_id"), 256))
    .withColumn("severity", F.when(F.col("blocking"), F.lit("CRITICAL")).otherwise(F.lit("WARNING")))
    .withColumn("status", F.lit("OPEN"))
    .withColumn("message", F.concat_ws(": ", "metric_name", "detail"))
    .withColumn("created_at", F.current_timestamp())
    .withColumn("resolved_at", F.lit(None).cast("timestamp"))
    .select("alert_id", "run_id", "slo_id", "severity", "status", "metric_value", "threshold", "message", "created_at", "resolved_at")
)
alerts_df.createOrReplaceTempView("phase6_operational_alerts")
spark.sql(f"""
MERGE INTO {MONITORING_TABLES['operational_alerts']} target
USING phase6_operational_alerts source
ON target.alert_id = source.alert_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

summary = results_df.select("slo_id", "metric_value", "operator", "threshold", "passed", "blocking", "detail")
display(summary.orderBy("slo_id"))
blocking_failures = summary.filter((~F.col("passed")) & F.col("blocking")).count()
if blocking_failures:
    raise ValueError(f"Fase 6 no esta lista para produccion: {blocking_failures} SLO bloqueantes fallaron")

print("FASE 6 OPERATIVAMENTE APROBADA")
