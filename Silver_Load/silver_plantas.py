# Databricks notebook source
"""Carga optimizada del estado vigente del maestro de plantas."""

import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import BRONZE_TABLES, CATALOG, SILVER_TABLES  # noqa: E402
from silver_runtime.core import (  # noqa: E402
    delta_operation, final_profile, input_profile, invalid_key_condition,
    merge_delta, require_table_contract, with_record_hash,
)

spark.sql(f"USE CATALOG `{CATALOG}`")
bronze_table = BRONZE_TABLES["plantas"]
silver_table = SILVER_TABLES["plantas"]
key = ["codigo_planta"]
attributes = ["fecha", "codigo_duracion", "nombre_planta", "codigo_sic_agente", "cap_efectiva_neta", "fpo", "codigo_sub_area_operativa", "codigo_area_operativa", "tipo_despacho_recurso", "tipo_clasificacion", "tipo_generacion"]
metadata = ["source_file_name", "source_file_path", "ingestion_timestamp", "load_date"]
bronze_columns = ["fecha", "codigo_duracion", "codigo_planta", "nombre_planta", "codigo_sic_agente", "cap_efectiva_neta", "fpo", "codigo_sub_area_operativa", "codigo_area_operativa", "tipo_despacho_recurso", "tipo_clasificacion", "tipo_generacion", *metadata]
silver_columns = ["fecha", "codigo_duracion", "codigo_planta", "nombre_planta", "codigo_sic_agente", "cap_efectiva_neta", "fpo", "codigo_sub_area_operativa", "codigo_area_operativa", "tipo_despacho_recurso", "tipo_clasificacion", "tipo_generacion", *metadata, "silver_created_at", "silver_updated_at"]
require_table_contract(spark, bronze_table, bronze_columns)
require_table_contract(spark, silver_table, silver_columns)

# COMMAND ----------

upper = lambda name: F.upper(F.trim(F.col(name)))
clean = lambda name: F.upper(F.trim(F.regexp_replace(F.col(name), r"\s+", " ")))
transformed = spark.table(bronze_table).select(
    F.to_date("fecha").alias("fecha"), upper("codigo_duracion").alias("codigo_duracion"),
    upper("codigo_planta").alias("codigo_planta"), clean("nombre_planta").alias("nombre_planta"),
    upper("codigo_sic_agente").alias("codigo_sic_agente"),
    F.regexp_replace(F.trim("cap_efectiva_neta"), ",", ".").cast("double").alias("cap_efectiva_neta"),
    F.to_date("fpo").alias("fpo"), upper("codigo_sub_area_operativa").alias("codigo_sub_area_operativa"),
    upper("codigo_area_operativa").alias("codigo_area_operativa"), clean("tipo_despacho_recurso").alias("tipo_despacho_recurso"),
    clean("tipo_clasificacion").alias("tipo_clasificacion"), clean("tipo_generacion").alias("tipo_generacion"), *metadata,
)
transformed = with_record_hash(transformed, [*key, *attributes])
invalid_key = invalid_key_condition(key)
invalid_value = F.col("nombre_planta").isNull() | (F.col("nombre_planta") == "") | F.col("fecha").isNull()
profile = input_profile(transformed, invalid_key, invalid_value, "cap_efectiva_neta")
print("Perfil de entrada:", profile.asDict(recursive=True))
if int(profile["invalid_keys"] or 0) or int(profile["invalid_values"] or 0) or int(profile["negative_values"] or 0):
    display(transformed.filter(invalid_key | invalid_value | (F.col("cap_efectiva_neta") < 0)).limit(100))
    raise ValueError("Plantas contiene llaves, fechas, nombres o capacidades invalidas")

window = Window.partitionBy(*key).orderBy(F.col("fecha").desc_nulls_last(), F.col("ingestion_timestamp").desc_nulls_last(), F.col("load_date").desc_nulls_last(), F.col("record_hash").desc_nulls_last())
source = (
    transformed.withColumn("_row_number", F.row_number().over(window))
    .filter(F.col("_row_number") == 1).drop("_row_number", "record_hash")
    .withColumn("silver_created_at", F.current_timestamp()).withColumn("silver_updated_at", F.current_timestamp())
)
merge_delta(spark, silver_table, source, key, [*attributes, *metadata], silver_columns)
print("Operacion Delta:", delta_operation(spark, silver_table))
print("Perfil final:", final_profile(spark, silver_table, key, "fecha"))
print("Carga Silver plantas completada sin cache ni persist.")
