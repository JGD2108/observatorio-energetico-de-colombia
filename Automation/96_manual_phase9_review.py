# Databricks notebook source
"""Read-only operational review for Phase 9. It does not create alerts or write tables."""

import sys

from pyspark.sql import functions as F

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import AUDIT_TABLES, CATALOG, SERVING_VIEWS  # noqa: E402
from operations.manual_review import assess, optional_float  # noqa: E402


spark.sql(f"USE CATALOG `{CATALOG}`")

# COMMAND ----------

source_profile = spark.table(SERVING_VIEWS["estado_fuentes"]).agg(
    F.count("*").alias("fuentes_revisadas"),
    F.max("dias_rezago").alias("maximo_rezago_dias"),
    F.sum(F.when(~F.col("cumple_sla_frescura"), 1).otherwise(0)).alias("fuentes_fuera_sla"),
).first()

latest_run = (
    spark.table(SERVING_VIEWS["pipeline_health"])
    .orderBy(F.col("started_at").desc())
    .select("run_id", "started_at", "finished_at", "status", "duration_seconds", "tasa_exito_ultimas_10_pct")
    .first()
)

open_quality_alerts = (
    spark.table(AUDIT_TABLES["data_quality_alerts"])
    .filter((F.col("status") == "OPEN") & F.col("severity").isin("HIGH", "CRITICAL"))
    .count()
)
serving_views_ready = sum(spark.catalog.tableExists(view) for view in SERVING_VIEWS.values())

review = assess(
    stale_sources=int(source_profile["fuentes_fuera_sla"] or 0),
    latest_pipeline_status=latest_run["status"] if latest_run else None,
    open_blocking_quality_alerts=int(open_quality_alerts),
    serving_views_ready=serving_views_ready,
    serving_views_expected=len(SERVING_VIEWS),
)

summary = spark.createDataFrame(
    [(
        review.estado,
        review.detalle,
        review.fuentes_fuera_sla,
        int(source_profile["maximo_rezago_dias"] or 0),
        review.ultima_corrida_exitosa,
        review.alertas_calidad_abiertas,
        f"{review.contratos_serving_listos}/{review.contratos_serving_esperados}",
        latest_run["run_id"] if latest_run else None,
        latest_run["finished_at"] if latest_run else None,
        optional_float(latest_run["tasa_exito_ultimas_10_pct"]) if latest_run else None,
    )],
    "estado string, detalle string, fuentes_fuera_sla int, maximo_rezago_dias int, "
    "ultima_corrida_exitosa boolean, alertas_calidad_abiertas int, contratos_serving string, "
    "ultima_corrida_id string, ultima_corrida_finalizada timestamp, tasa_exito_ultimas_10_pct double",
)
display(summary)

# COMMAND ----------

display(
    spark.table(SERVING_VIEWS["estado_fuentes"])
    .select("fuente", "fecha_maxima", "dias_rezago", "cumple_sla_frescura", "estado_operativo")
    .orderBy(F.col("dias_rezago").desc(), "fuente")
)

display(
    spark.table(SERVING_VIEWS["task_performance"])
    .select("task_key", "layer", "source_name", "duracion_p95_segundos", "duracion_maxima_segundos", "tasa_exito_pct", "ultima_ejecucion")
    .orderBy(F.col("duracion_p95_segundos").desc_nulls_last())
    .limit(10)
)

display(
    spark.table(SERVING_VIEWS["quality_alerts"])
    .filter((F.col("status") == "OPEN") & F.col("severity").isin("HIGH", "CRITICAL"))
    .orderBy(F.col("created_at").desc())
)

print(f"REVISION FASE 9: {review.estado} - {review.detalle}")
