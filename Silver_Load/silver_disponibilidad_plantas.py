# Databricks notebook source
"""Carga incremental optimizada de disponibilidad de plantas."""

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
SOURCE_NAME = "disponibilidad_plantas"
bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]
plants_table = SILVER_TABLES["plantas"]
key = ["fecha_hora", "codigo_planta", "codigo_variable", "codigo_duracion", "unidad_medida", "version"]
metadata = ["source_file_name", "source_file_path", "ingestion_timestamp", "load_date"]
bronze_columns = ["codigo_variable", "fecha_hora", "codigo_duracion", "unidad_medida", "codigo_planta", "version", "valor", *metadata]
silver_columns = [*key, "valor", "planta_encontrada", *metadata, "silver_created_at", "silver_updated_at"]

for table, columns in [(bronze_table, bronze_columns), (silver_table, silver_columns), (plants_table, ["codigo_planta"])]:
    require_table_contract(spark, table, columns)

# COMMAND ----------

bronze_df, watermark = incremental_source(spark, bronze_table, silver_table)
transformed = bronze_df.select(
    F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
    F.to_timestamp("fecha_hora").alias("fecha_hora"),
    F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
    F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
    F.upper(F.trim("codigo_planta")).alias("codigo_planta"),
    F.upper(F.trim("version")).alias("version"),
    F.regexp_replace(F.trim("valor"), ",", ".").cast("double").alias("valor"),
    *metadata,
)
transformed = with_record_hash(transformed, [*key, "valor"])
invalid_key = invalid_key_condition(key, ["fecha_hora"])
invalid_value = F.col("valor").isNull()
profile = input_profile(transformed, invalid_key, invalid_value, "valor")
print("Watermark:", watermark, "Perfil de entrada:", profile.asDict(recursive=True))
if int(profile["invalid_keys"] or 0) or int(profile["invalid_values"] or 0):
    display(transformed.filter(invalid_key | invalid_value).limit(100))
    raise ValueError("Disponibilidad contiene llaves o valores invalidos")

plants = spark.table(plants_table).select("codigo_planta").distinct()
source = (
    deduplicate(transformed, key).alias("availability")
    .join(F.broadcast(plants).alias("plant"), F.col("availability.codigo_planta") == F.col("plant.codigo_planta"), "left")
    .select("availability.*", F.col("plant.codigo_planta").isNotNull().alias("planta_encontrada"))
    .withColumn("silver_created_at", F.current_timestamp())
    .withColumn("silver_updated_at", F.current_timestamp())
)
mutable = ["valor", "planta_encontrada", *metadata]
merge_delta(spark, silver_table, source, key, mutable, silver_columns)
print("Operacion Delta:", delta_operation(spark, silver_table))
print("Perfil final:", final_profile(spark, silver_table, key, "fecha_hora"))
print("Carga Silver disponibilidad completada sin cache ni persist.")
