# Databricks notebook source
"""Measure historical coverage after Bronze and Silver processing."""

import sys
from datetime import datetime
from pyspark.sql import functions as F

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backfill.runtime import _parameter, is_window_covered  # noqa: E402
from config.project_config import BRONZE_TABLES, CATALOG, MAX_LAG_DAYS  # noqa: E402

mode = _parameter(dbutils, "execution_mode", "AUTO").upper()
if mode != "BACKFILL":
    print("Validacion historica omitida: execution_mode no es BACKFILL")
    dbutils.notebook.exit("SKIPPED")

requested_start = datetime.strptime(_parameter(dbutils, "backfill_start_date"), "%Y-%m-%d").date()
requested_end = datetime.strptime(_parameter(dbutils, "backfill_end_date"), "%Y-%m-%d").date()
backfill_id = _parameter(dbutils, "backfill_id") or _parameter(dbutils, "run_id")
sources = {
    "demanda_real": "fecha_hora",
    "disponibilidad_plantas": "fecha_hora",
    "generacion_real": "fecha_hora",
    "niveles_embalses": "fecha_inicio",
    "precio_bolsa": "fecha_hora",
}

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{CATALOG}`.`audit`.`backfill_coverage` (
  backfill_id STRING NOT NULL,
  source_name STRING NOT NULL,
  requested_start DATE NOT NULL,
  requested_end DATE NOT NULL,
  actual_start DATE,
  actual_end DATE,
  row_count BIGINT,
  covered BOOLEAN NOT NULL,
  measured_at TIMESTAMP NOT NULL
) USING DELTA
""")

rows = []
for source_name, date_column in sources.items():
    table = BRONZE_TABLES[source_name]
    profile = (
        spark.table(table)
        .filter(F.to_date(date_column).between(F.lit(requested_start), F.lit(requested_end)))
        .agg(F.min(F.to_date(date_column)).alias("actual_start"), F.max(F.to_date(date_column)).alias("actual_end"), F.count("*").alias("row_count"))
        .first()
    )
    actual_start, actual_end = profile["actual_start"], profile["actual_end"]
    covered = is_window_covered(
        requested_start,
        requested_end,
        actual_start,
        actual_end,
        MAX_LAG_DAYS,
    )
    rows.append((backfill_id, source_name, requested_start, requested_end, actual_start, actual_end, int(profile["row_count"] or 0), covered))

coverage = spark.createDataFrame(rows, "backfill_id string, source_name string, requested_start date, requested_end date, actual_start date, actual_end date, row_count long, covered boolean").withColumn("measured_at", F.current_timestamp())
coverage.createOrReplaceTempView("phase5_coverage")
spark.sql(f"DELETE FROM `{CATALOG}`.`audit`.`backfill_coverage` WHERE backfill_id = '{backfill_id}'")
coverage.write.mode("append").saveAsTable(f"{CATALOG}.audit.backfill_coverage")
missing = [row["source_name"] for row in coverage.filter(~F.col("covered")).select("source_name").collect()]
if missing:
    raise ValueError(
        f"Backfill fuera de cobertura o del SLA de {MAX_LAG_DAYS} dias en: {missing}"
    )
print("Cobertura historica completa:", [row["source_name"] for row in coverage.select("source_name").collect()])
