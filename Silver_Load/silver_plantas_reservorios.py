# Databricks notebook source
"""Carga snapshot optimizada de relaciones planta-reservorio."""

import sys
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import BRONZE_TABLES, CATALOG, GOVERNANCE_TABLES, SILVER_TABLES  # noqa: E402
from silver_runtime.core import delta_operation, final_profile, input_profile, require_table_contract, with_record_hash  # noqa: E402

spark.sql(f"USE CATALOG `{CATALOG}`")
bronze_table = BRONZE_TABLES["plantas_reservorios"]
silver_table = SILVER_TABLES["plantas_reservorios"]
plants_table = SILVER_TABLES["plantas"]
reservoirs_table = SILVER_TABLES["embalses"]
alias_table = GOVERNANCE_TABLES["ref_entity_alias"]
key = ["nombre_planta", "nombre_reservorio"]
metadata = ["source_file_name", "source_file_path", "ingestion_timestamp", "load_date"]
bronze_columns = ["region", *key, "tipo_relacion", "es_principal", "permite_atribucion", "fuente_relacion", "estado_validacion", "valido_desde", "valido_hasta", *metadata]
silver_columns = ["region", *key, "tipo_relacion", "es_principal", "permite_atribucion", "fuente_relacion", "estado_validacion", "valido_desde", "valido_hasta", "codigo_planta", "codigo_embalse", "planta_encontrada", "embalse_encontrado", "relacion_completa", "requiere_revision_manual", *metadata, "silver_created_at", "silver_updated_at", "activo", "fecha_retiro"]
for table, columns in [(bronze_table, bronze_columns), (silver_table, silver_columns), (plants_table, ["codigo_planta", "nombre_planta"]), (reservoirs_table, ["codigo_embalse", "nombre_embalse"]), (alias_table, ["entity_type", "status", "valid_from", "valid_to", "alias_normalized", "canonical_code"])]:
    require_table_contract(spark, table, columns)

# COMMAND ----------

history = spark.table(bronze_table)
snapshot_timestamp = history.agg(F.max("ingestion_timestamp").alias("value")).first()["value"]
bronze_df = history.filter(F.col("ingestion_timestamp") == F.lit(snapshot_timestamp))
clean = lambda name: F.upper(F.trim(F.regexp_replace(F.col(name), r"\s+", " ")))
transformed = bronze_df.select(
    clean("region").alias("region"), clean("nombre_planta").alias("nombre_planta"), clean("nombre_reservorio").alias("nombre_reservorio"),
    F.lower(F.trim("tipo_relacion")).alias("tipo_relacion"), F.coalesce(F.col("es_principal").cast("boolean"), F.lit(False)).alias("es_principal"),
    F.coalesce(F.col("permite_atribucion").cast("boolean"), F.lit(False)).alias("permite_atribucion"), F.lower(F.trim("fuente_relacion")).alias("fuente_relacion"),
    F.lower(F.trim("estado_validacion")).alias("estado_validacion"), F.col("valido_desde").cast("date").alias("valido_desde"), F.col("valido_hasta").cast("date").alias("valido_hasta"), *metadata,
)
transformed = with_record_hash(transformed, [*key, "tipo_relacion", "estado_validacion"])
invalid = F.col("region").isNull() | (F.col("region") == "") | F.col("nombre_planta").isNull() | (F.col("nombre_planta") == "") | F.col("nombre_reservorio").isNull() | (F.col("nombre_reservorio") == "")
profile = input_profile(transformed, invalid)
print("Snapshot:", snapshot_timestamp, "Perfil de entrada:", profile.asDict(recursive=True))
if int(profile["invalid_keys"] or 0):
    display(transformed.filter(invalid).limit(100))
    raise ValueError("Existen relaciones sin region, planta o reservorio")

window = Window.partitionBy(*key).orderBy(F.col("ingestion_timestamp").desc_nulls_last(), F.col("load_date").desc_nulls_last(), F.col("record_hash").desc_nulls_last())
relationships = transformed.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn", "record_hash")

def normalize_name(column):
    return F.upper(F.trim(F.regexp_replace(F.translate(column, "ÁÉÍÓÚÜÑ", "AEIOUUN"), r"[^A-Z0-9]+", "")))

plants = spark.table(plants_table).select("codigo_planta", normalize_name(F.col("nombre_planta")).alias("plant_name")).dropDuplicates(["codigo_planta"])
reservoirs = spark.table(reservoirs_table).select("codigo_embalse", normalize_name(F.col("nombre_embalse")).alias("reservoir_name")).dropDuplicates(["codigo_embalse"])
aliases = spark.table(alias_table).filter((F.col("entity_type") == "EMBALSE") & (F.col("status") == "APPROVED") & (F.col("valid_from") <= F.current_date()) & (F.col("valid_to").isNull() | (F.col("valid_to") >= F.current_date()))).select(F.col("alias_normalized").alias("reservoir_name"), F.col("canonical_code").alias("alias_code"))

named = relationships.withColumn("plant_name", normalize_name(F.col("nombre_planta"))).withColumn("reservoir_name", normalize_name(F.col("nombre_reservorio")))
enriched = (
    named.alias("r").join(plants.alias("p"), F.col("r.plant_name") == F.col("p.plant_name"), "left")
    .join(aliases.alias("a"), F.col("r.reservoir_name") == F.col("a.reservoir_name"), "left")
    .join(reservoirs.alias("e"), (F.col("r.reservoir_name") == F.col("e.reservoir_name")) | (F.col("a.alias_code") == F.col("e.codigo_embalse")), "left")
    .select("r.*", F.col("p.codigo_planta"), F.col("e.codigo_embalse"))
)
match_profile = enriched.groupBy(*key).agg(F.countDistinct("codigo_planta").alias("plants"), F.countDistinct("codigo_embalse").alias("reservoirs")).agg(F.sum(F.when((F.col("plants") > 1) | (F.col("reservoirs") > 1), 1).otherwise(0)).alias("ambiguous")).first()
if int(match_profile["ambiguous"] or 0):
    raise ValueError(f"Existen {int(match_profile['ambiguous']):,} relaciones ambiguas")

source = (
    enriched.dropDuplicates(key)
    .withColumn("planta_encontrada", F.col("codigo_planta").isNotNull()).withColumn("embalse_encontrado", F.col("codigo_embalse").isNotNull())
    .withColumn("relacion_completa", F.col("planta_encontrada") & F.col("embalse_encontrado"))
    .withColumn("requiere_revision_manual", (~F.col("relacion_completa")) | (F.col("estado_validacion") != "validated"))
    .drop("plant_name", "reservoir_name")
    .withColumn("silver_created_at", F.current_timestamp()).withColumn("silver_updated_at", F.current_timestamp())
    .withColumn("activo", F.lit(True)).withColumn("fecha_retiro", F.lit(None).cast("timestamp"))
)

# COMMAND ----------

mutable = [column for column in silver_columns if column not in [*key, "silver_created_at"]]
change = " OR ".join(f"NOT (target.{column} <=> source.{column})" for column in mutable if column != "silver_updated_at")
target = DeltaTable.forName(spark, silver_table)
(
    target.alias("target").merge(source.alias("source"), " AND ".join(f"target.{column} = source.{column}" for column in key))
    .whenMatchedUpdate(condition=change, set={column: f"source.{column}" for column in mutable})
    .whenNotMatchedInsert(values={column: f"source.{column}" for column in silver_columns})
    .whenNotMatchedBySourceUpdate(condition="target.activo = true", set={"activo": "false", "fecha_retiro": "current_timestamp()", "silver_updated_at": "current_timestamp()"})
    .execute()
)
print("Operacion Delta:", delta_operation(spark, silver_table))
print("Perfil final:", final_profile(spark, silver_table, key, "ingestion_timestamp"))
print("Carga Silver plantas-reservorios completada sin cache ni persist.")
