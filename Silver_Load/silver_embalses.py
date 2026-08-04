# Databricks notebook source
"""Carga optimizada del maestro georreferenciado de embalses."""

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
bronze_table = BRONZE_TABLES["embalses"]
silver_table = SILVER_TABLES["embalses"]
key = ["codigo_embalse"]
attributes = ["nombre_embalse", "latitud", "longitud", "tipo_coordenada", "fuente_coordenada", "estado_geocodificacion", "consulta_geocodificacion", "coordenadas_validas", "requiere_revision_manual"]
metadata = ["source_file_name", "source_file_path", "ingestion_timestamp", "load_date"]
bronze_columns = ["codigo_embalse", "nombre_embalse", "latitud", "longitud", "tipo_coordenada", "coordinate_source", "geocoding_status", "geocoding_query", *metadata]
silver_columns = [*key, *attributes, *metadata, "silver_created_at", "silver_updated_at"]
require_table_contract(spark, bronze_table, bronze_columns)
require_table_contract(spark, silver_table, silver_columns)

# COMMAND ----------

transformed = spark.table(bronze_table).select(
    F.upper(F.trim("codigo_embalse")).alias("codigo_embalse"),
    F.upper(F.trim(F.regexp_replace(F.col("nombre_embalse"), r"\s+", " "))).alias("nombre_embalse"),
    F.col("latitud").cast("double").alias("latitud"), F.col("longitud").cast("double").alias("longitud"),
    F.lower(F.trim("tipo_coordenada")).alias("tipo_coordenada"),
    F.lower(F.trim("coordinate_source")).alias("fuente_coordenada"),
    F.lower(F.trim("geocoding_status")).alias("estado_geocodificacion"),
    F.trim("geocoding_query").alias("consulta_geocodificacion"), *metadata,
)
valid_coordinates = F.col("latitud").between(-90.0, 90.0) & F.col("longitud").between(-180.0, 180.0)
transformed = (
    transformed.withColumn("coordenadas_validas", valid_coordinates)
    .withColumn("requiere_revision_manual", (~F.col("coordenadas_validas")) | F.col("fuente_coordenada").isNull() | ((F.col("fuente_coordenada") == "nominatim") & (F.col("estado_geocodificacion") != "found")) | ((F.col("fuente_coordenada") == "fallback_manual") & (F.col("tipo_coordenada") == "approximate")))
)
transformed = with_record_hash(transformed, [*key, *attributes])
invalid_key = invalid_key_condition(key)
invalid_value = F.col("nombre_embalse").isNull() | (F.col("nombre_embalse") == "")
profile = input_profile(transformed, invalid_key, invalid_value)
print("Perfil de entrada:", profile.asDict(recursive=True))
if int(profile["invalid_keys"] or 0) or int(profile["invalid_values"] or 0):
    display(transformed.filter(invalid_key | invalid_value).limit(100))
    raise ValueError("Embalses contiene codigos o nombres invalidos")

window = Window.partitionBy(*key).orderBy(F.col("ingestion_timestamp").desc_nulls_last(), F.col("load_date").desc_nulls_last(), F.col("coordenadas_validas").desc(), F.col("record_hash").desc_nulls_last())
source = (
    transformed.withColumn("_row_number", F.row_number().over(window))
    .filter(F.col("_row_number") == 1).drop("_row_number", "record_hash")
    .withColumn("silver_created_at", F.current_timestamp()).withColumn("silver_updated_at", F.current_timestamp())
)
merge_delta(spark, silver_table, source, key, [*attributes, *metadata], silver_columns)
print("Operacion Delta:", delta_operation(spark, silver_table))
print("Perfil final:", final_profile(spark, silver_table, key, "ingestion_timestamp"))
print("Carga Silver embalses completada sin cache ni persist.")
