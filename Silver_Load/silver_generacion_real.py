# Databricks notebook source
"""Carga incremental optimizada de generación real en Silver."""

import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import BRONZE_TABLES, CATALOG, SILVER_TABLES  # noqa: E402

spark.sql(f"USE CATALOG `{CATALOG}`")

SOURCE_NAME = "generacion_real"
bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]
plants_table = SILVER_TABLES["plantas"]
agents_table = SILVER_TABLES["agentes"]

generation_key = [
    "fecha_hora", "codigo_agente", "codigo_planta", "codigo_variable",
    "codigo_duracion", "unidad_medida", "version",
]
required_bronze_columns = {
    "codigo_variable", "fecha_hora", "codigo_duracion", "unidad_medida",
    "codigo_sic_agente", "codigo_planta", "version", "valor",
    "source_file_name", "source_file_path", "ingestion_timestamp", "load_date",
}
required_silver_columns = {
    "codigo_variable", "fecha_hora", "codigo_duracion", "unidad_medida",
    "codigo_agente", "codigo_planta", "version", "valor",
    "planta_encontrada", "agente_encontrado", "source_file_name",
    "source_file_path", "ingestion_timestamp", "load_date",
    "silver_created_at", "silver_updated_at",
}


# COMMAND ----------

required_tables = [bronze_table, silver_table, plants_table, agents_table]
missing_tables = [table for table in required_tables if not spark.catalog.tableExists(table)]
if missing_tables:
    raise ValueError(f"No existen las tablas requeridas: {missing_tables}")

missing_bronze = required_bronze_columns - set(spark.table(bronze_table).columns)
missing_silver = required_silver_columns - set(spark.table(silver_table).columns)
if missing_bronze:
    raise ValueError(
        f"La tabla {bronze_table} no contiene las columnas requeridas: "
        f"{sorted(missing_bronze)}"
    )
if missing_silver:
    raise ValueError(
        f"La tabla {silver_table} no contiene las columnas requeridas: "
        f"{sorted(missing_silver)}"
    )


# COMMAND ----------

last_ingestion_timestamp = (
    spark.table(silver_table)
    .agg(F.max("ingestion_timestamp").alias("watermark"))
    .first()["watermark"]
)
bronze_df = spark.table(bronze_table)
if last_ingestion_timestamp is not None:
    bronze_df = bronze_df.filter(
        F.col("ingestion_timestamp") >= F.lit(last_ingestion_timestamp)
    )

transformed_df = (
    bronze_df
    .select(
        F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
        F.to_timestamp("fecha_hora").alias("fecha_hora"),
        F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
        F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
        F.upper(F.trim("codigo_sic_agente")).alias("codigo_agente"),
        F.upper(F.trim("codigo_planta")).alias("codigo_planta"),
        F.upper(F.trim("version")).alias("version"),
        F.regexp_replace(F.trim("valor"), ",", ".").cast("double").alias("valor"),
        "source_file_name", "source_file_path", "ingestion_timestamp", "load_date",
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(column).cast("string"), F.lit(""))
                    for column in generation_key + ["valor"]
                ],
            ),
            256,
        ),
    )
)

invalid_key_condition = F.lit(False)
for column in generation_key:
    invalid_key_condition = invalid_key_condition | F.col(column).isNull()
    if column != "fecha_hora":
        invalid_key_condition = invalid_key_condition | (F.col(column) == "")

input_profile = (
    transformed_df
    .agg(
        F.count("*").alias("rows_received"),
        F.sum(F.when(invalid_key_condition, 1).otherwise(0)).alias("invalid_keys"),
        F.sum(F.when(F.col("valor").isNull(), 1).otherwise(0)).alias("invalid_values"),
        F.sum(F.when(F.col("valor") < 0, 1).otherwise(0)).alias("negative_values"),
        F.sum(F.when(F.col("valor") == 0, 1).otherwise(0)).alias("zero_values"),
    )
    .first()
)
invalid_key_rows = int(input_profile["invalid_keys"] or 0)
invalid_value_rows = int(input_profile["invalid_values"] or 0)
print("Watermark Silver:", last_ingestion_timestamp)
print("Registros recibidos:", int(input_profile["rows_received"] or 0))
print("Llaves inválidas:", invalid_key_rows)
print("Valores no convertibles:", invalid_value_rows)
print("Valores negativos:", int(input_profile["negative_values"] or 0))
print("Valores cero:", int(input_profile["zero_values"] or 0))

if invalid_key_rows or invalid_value_rows:
    display(
        transformed_df
        .filter(invalid_key_condition | F.col("valor").isNull())
        .limit(100)
    )
    raise ValueError(
        "Generación contiene registros inválidos: "
        f"llaves={invalid_key_rows:,}, valores={invalid_value_rows:,}"
    )


# COMMAND ----------

deduplication_window = Window.partitionBy(*generation_key).orderBy(
    F.col("ingestion_timestamp").desc_nulls_last(),
    F.col("load_date").desc_nulls_last(),
    F.col("record_hash").desc_nulls_last(),
)
plants_reference_df = spark.table(plants_table).select("codigo_planta").distinct()
agents_reference_df = spark.table(agents_table).select("codigo_agente").distinct()

merge_source_df = (
    transformed_df
    .withColumn("row_number", F.row_number().over(deduplication_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number", "record_hash")
    .alias("generation")
    .join(
        F.broadcast(plants_reference_df).alias("plant"),
        F.col("generation.codigo_planta") == F.col("plant.codigo_planta"),
        "left",
    )
    .join(
        F.broadcast(agents_reference_df).alias("agent"),
        F.col("generation.codigo_agente") == F.col("agent.codigo_agente"),
        "left",
    )
    .select(
        "generation.*",
        F.col("plant.codigo_planta").isNotNull().alias("planta_encontrada"),
        F.col("agent.codigo_agente").isNotNull().alias("agente_encontrado"),
    )
    .withColumn("silver_created_at", F.current_timestamp())
    .withColumn("silver_updated_at", F.current_timestamp())
)

target = DeltaTable.forName(spark, silver_table)
(
    target.alias("target")
    .merge(
        merge_source_df.alias("source"),
        " AND ".join(
            f"target.{column} = source.{column}" for column in generation_key
        ),
    )
    .whenMatchedUpdate(
        condition="""
            NOT (target.valor <=> source.valor)
            OR NOT (target.source_file_name <=> source.source_file_name)
            OR NOT (target.source_file_path <=> source.source_file_path)
            OR NOT (target.ingestion_timestamp <=> source.ingestion_timestamp)
            OR NOT (target.load_date <=> source.load_date)
            OR NOT (target.planta_encontrada <=> source.planta_encontrada)
            OR NOT (target.agente_encontrado <=> source.agente_encontrado)
        """,
        set={
            "valor": "source.valor",
            "source_file_name": "source.source_file_name",
            "source_file_path": "source.source_file_path",
            "ingestion_timestamp": "source.ingestion_timestamp",
            "load_date": "source.load_date",
            "planta_encontrada": "source.planta_encontrada",
            "agente_encontrado": "source.agente_encontrado",
            "silver_updated_at": "source.silver_updated_at",
        },
    )
    .whenNotMatchedInsert(values={
        column: f"source.{column}"
        for column in [
            *generation_key, "valor", "planta_encontrada", "agente_encontrado",
            "source_file_name", "source_file_path", "ingestion_timestamp",
            "load_date", "silver_created_at", "silver_updated_at",
        ]
    })
    .execute()
)


# COMMAND ----------

history_row = (
    spark.sql(f"DESCRIBE HISTORY {silver_table}")
    .select("version", "timestamp", "operation", "operationMetrics")
    .limit(1)
    .first()
)
print("Operación Delta:", history_row.asDict(recursive=True))

silver_validation_df = spark.table(silver_table)
final_profile = (
    silver_validation_df
    .agg(
        F.count("*").alias("total_rows"),
        F.countDistinct(F.struct(*[F.col(column) for column in generation_key])).alias(
            "distinct_keys"
        ),
        F.min("fecha_hora").alias("min_event_time"),
        F.max("fecha_hora").alias("max_event_time"),
        F.countDistinct("codigo_planta").alias("plants"),
        F.countDistinct("codigo_agente").alias("agents"),
        F.sum(F.when(~F.col("planta_encontrada"), 1).otherwise(0)).alias(
            "rows_without_plant"
        ),
        F.sum(F.when(~F.col("agente_encontrado"), 1).otherwise(0)).alias(
            "rows_without_agent"
        ),
        F.sum(F.when(F.col("valor") < 0, 1).otherwise(0)).alias("negative_values"),
    )
    .first()
)
duplicate_rows = int(final_profile["total_rows"] or 0) - int(
    final_profile["distinct_keys"] or 0
)
print("Perfil final Silver:", final_profile.asDict(recursive=True))
if duplicate_rows:
    raise ValueError(
        f"{silver_table} contiene {duplicate_rows:,} llaves duplicadas."
    )

print("Carga Silver generación completada sin cache ni persist.")
