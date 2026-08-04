# Databricks notebook source
"""Carga incremental optimizada del maestro de agentes."""

import sys
from pyspark.sql import functions as F

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import BRONZE_TABLES, CATALOG, SILVER_TABLES  # noqa: E402
from silver_runtime.core import (  # noqa: E402
    deduplicate, delta_operation, final_profile, incremental_source,
    input_profile, invalid_key_condition, merge_delta,
    require_table_contract, with_record_hash,
)

spark.sql(f"USE CATALOG `{CATALOG}`")
bronze_table = BRONZE_TABLES["agentes"]
silver_table = SILVER_TABLES["agentes"]
key = ["fecha", "codigo_duracion", "codigo_agente", "actividad_agente"]
metadata = ["source_file_name", "source_file_path", "ingestion_timestamp", "load_date"]
bronze_columns = ["fecha", "codigo_duracion", "codigo_sic_agente", "nombre_agente", "actividad_agente", *metadata]
silver_columns = [*key, "nombre_agente", *metadata, "silver_created_at", "silver_updated_at"]
require_table_contract(spark, bronze_table, bronze_columns)
require_table_contract(spark, silver_table, silver_columns)

# COMMAND ----------

bronze_df, watermark = incremental_source(spark, bronze_table, silver_table)
clean_text = lambda name: F.upper(F.trim(F.regexp_replace(F.col(name), r"\s+", " ")))
transformed = bronze_df.select(
    F.to_date("fecha").alias("fecha"),
    F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
    F.upper(F.trim("codigo_sic_agente")).alias("codigo_agente"),
    clean_text("nombre_agente").alias("nombre_agente"),
    clean_text("actividad_agente").alias("actividad_agente"),
    *metadata,
)
transformed = with_record_hash(transformed, [*key, "nombre_agente"])
invalid_key = invalid_key_condition(key, ["fecha"])
invalid_value = F.col("nombre_agente").isNull() | (F.col("nombre_agente") == "")
profile = input_profile(transformed, invalid_key, invalid_value)
print("Watermark:", watermark, "Perfil de entrada:", profile.asDict(recursive=True))
if int(profile["invalid_keys"] or 0) or int(profile["invalid_values"] or 0):
    display(transformed.filter(invalid_key | invalid_value).limit(100))
    raise ValueError("Agentes contiene llaves o nombres invalidos")

source = (
    deduplicate(transformed, key)
    .withColumn("silver_created_at", F.current_timestamp())
    .withColumn("silver_updated_at", F.current_timestamp())
)
mutable = ["nombre_agente", *metadata]
merge_delta(spark, silver_table, source, key, mutable, silver_columns)
print("Operacion Delta:", delta_operation(spark, silver_table))
print("Perfil final:", final_profile(spark, silver_table, key, "fecha"))
print("Carga Silver agentes completada sin cache ni persist.")
