# Databricks notebook source
"""Carga incremental optimizada de demanda real."""

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
SOURCE_NAME = "demanda_real"
bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]
agents_table = SILVER_TABLES["agentes"]
key = ["codigo_variable", "fecha_hora", "codigo_agente", "tipo_mercado", "version", "unidad_medida", "codigo_duracion"]
metadata = ["source_file_name", "source_file_path", "ingestion_timestamp", "load_date"]
bronze_columns = ["codigo_variable", "fecha_hora", "codigo_sic_agente", "tipo_mercado", "version", "valor", "unidad_medida", "codigo_duracion", *metadata]
silver_columns = [*key, "demanda_real_kwh", "es_demanda_cero", "agente_encontrado", *metadata, "silver_created_at", "silver_updated_at"]

for table, columns in [(bronze_table, bronze_columns), (silver_table, silver_columns), (agents_table, ["codigo_agente"])]:
    require_table_contract(spark, table, columns)

# COMMAND ----------

bronze_df, watermark = incremental_source(spark, bronze_table, silver_table)
transformed = bronze_df.select(
    F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
    F.to_timestamp("fecha_hora").alias("fecha_hora"),
    F.upper(F.trim("codigo_sic_agente")).alias("codigo_agente"),
    F.upper(F.trim("tipo_mercado")).alias("tipo_mercado"),
    F.upper(F.trim("version")).alias("version"),
    F.regexp_replace(F.trim("valor"), ",", ".").cast("double").alias("demanda_real_kwh"),
    F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
    F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
    *metadata,
)
transformed = with_record_hash(transformed, [*key, "demanda_real_kwh"])
invalid_key = invalid_key_condition(key, ["fecha_hora"])
invalid_value = F.col("demanda_real_kwh").isNull()
profile = input_profile(transformed, invalid_key, invalid_value, "demanda_real_kwh")
print("Watermark:", watermark, "Perfil de entrada:", profile.asDict(recursive=True))
if int(profile["invalid_keys"] or 0) or int(profile["invalid_values"] or 0) or int(profile["negative_values"] or 0):
    display(transformed.filter(invalid_key | invalid_value | (F.col("demanda_real_kwh") < 0)).limit(100))
    raise ValueError("Demanda contiene llaves, valores invalidos o valores negativos")

agents = spark.table(agents_table).select("codigo_agente").distinct()
source = (
    deduplicate(transformed, key).alias("demand")
    .join(F.broadcast(agents).alias("agent"), F.col("demand.codigo_agente") == F.col("agent.codigo_agente"), "left")
    .select("demand.*", (F.col("demanda_real_kwh") == 0).alias("es_demanda_cero"), F.col("agent.codigo_agente").isNotNull().alias("agente_encontrado"))
    .withColumn("silver_created_at", F.current_timestamp())
    .withColumn("silver_updated_at", F.current_timestamp())
)
mutable = ["demanda_real_kwh", "es_demanda_cero", "agente_encontrado", *metadata]
merge_delta(spark, silver_table, source, key, mutable, silver_columns)
print("Operacion Delta:", delta_operation(spark, silver_table))
print("Perfil final:", final_profile(spark, silver_table, key, "fecha_hora"))
print("Carga Silver demanda completada sin cache ni persist.")
