# Databricks notebook source
from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys


NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from config.project_config import (
    BRONZE_TABLES,
    SILVER_TABLES,
)


SOURCE_NAME = "agentes"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]


print("Tabla Bronze:", bronze_table)
print("Tabla Silver:", silver_table)

# COMMAND ----------

if not spark.catalog.tableExists(
    bronze_table
):
    raise ValueError(
        f"No existe la tabla Bronze: {bronze_table}"
    )


if not spark.catalog.tableExists(
    silver_table
):
    raise ValueError(
        f"No existe la tabla Silver: {silver_table}. "
        "Ejecuta primero el DDL de Silver."
    )


print("Tablas Bronze y Silver encontradas.")

# COMMAND ----------

required_bronze_columns = {
    "fecha",
    "codigo_duracion",
    "codigo_sic_agente",
    "nombre_agente",
    "actividad_agente",
    "source_file_name",
    "source_file_path",
    "ingestion_timestamp",
    "load_date",
}


bronze_columns = set(
    spark.table(bronze_table).columns
)


missing_bronze_columns = (
    required_bronze_columns
    - bronze_columns
)


if missing_bronze_columns:
    raise ValueError(
        f"La tabla {bronze_table} no contiene "
        "las columnas requeridas: "
        f"{sorted(missing_bronze_columns)}"
    )


required_silver_columns = {
    "fecha",
    "codigo_duracion",
    "codigo_agente",
    "nombre_agente",
    "actividad_agente",
    "source_file_name",
    "source_file_path",
    "ingestion_timestamp",
    "load_date",
    "silver_created_at",
    "silver_updated_at",
}


silver_columns = set(
    spark.table(silver_table).columns
)


missing_silver_columns = (
    required_silver_columns
    - silver_columns
)


if missing_silver_columns:
    raise ValueError(
        f"La tabla {silver_table} no contiene "
        "las columnas requeridas: "
        f"{sorted(missing_silver_columns)}"
    )


print("Esquemas validados correctamente.")

# COMMAND ----------

last_ingestion_timestamp = (
    spark.table(silver_table)
    .agg(
        F.max(
            "ingestion_timestamp"
        ).alias(
            "last_ingestion_timestamp"
        )
    )
    .first()[
        "last_ingestion_timestamp"
    ]
)


print(
    "Último ingestion_timestamp en Silver:",
    last_ingestion_timestamp,
)

# COMMAND ----------

bronze_df = spark.table(
    bronze_table
)


if last_ingestion_timestamp is not None:
    bronze_df = bronze_df.filter(
        F.col("ingestion_timestamp")
        >= F.lit(last_ingestion_timestamp)
    )




bronze_rows = bronze_df.count()


print(
    "Registros Bronze a procesar:",
    f"{bronze_rows:,}",
)

# COMMAND ----------

silver_transformed_df = (
    bronze_df
    .select(
        F.to_date(
            F.col("fecha")
        ).alias("fecha"),

        F.upper(
            F.trim(
                F.col("codigo_duracion")
            )
        ).alias("codigo_duracion"),

        F.upper(
            F.trim(
                F.col("codigo_sic_agente")
            )
        ).alias("codigo_agente"),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("nombre_agente"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("nombre_agente"),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("actividad_agente"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("actividad_agente"),

        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col("fecha").cast("string"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("codigo_duracion"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("codigo_agente"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("nombre_agente"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("actividad_agente"),
                    F.lit(""),
                ),
            ),
            256,
        ),
    )
)

# COMMAND ----------

invalid_condition = (
    F.col("fecha").isNull()
    | F.col("codigo_duracion").isNull()
    | F.col("codigo_agente").isNull()
    | F.col("nombre_agente").isNull()
    | F.col("actividad_agente").isNull()
    | (F.col("codigo_duracion") == "")
    | (F.col("codigo_agente") == "")
    | (F.col("nombre_agente") == "")
    | (F.col("actividad_agente") == "")
)


invalid_df = (
    silver_transformed_df
    .filter(invalid_condition)
)


valid_df = (
    silver_transformed_df
    .filter(~invalid_condition)
)


invalid_rows = invalid_df.count()
valid_rows_before_deduplication = valid_df.count()


print(
    "Registros válidos antes de deduplicar:",
    f"{valid_rows_before_deduplication:,}",
)

print(
    "Registros inválidos:",
    f"{invalid_rows:,}",
)

# COMMAND ----------

agent_key = [
    "fecha",
    "codigo_duracion",
    "codigo_agente",
    "actividad_agente",
]


deduplication_window = (
    Window
    .partitionBy(*agent_key)
    .orderBy(
        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col(
            "record_hash"
        ).desc_nulls_last(),
    )
)

silver_df = (
    valid_df
    .withColumn(
        "row_number",
        F.row_number().over(
            deduplication_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop(
        "row_number",
        "record_hash",
    )
    .withColumn(
        "silver_created_at",
        F.current_timestamp(),
    )
    .withColumn(
        "silver_updated_at",
        F.current_timestamp(),
    )
)


silver_rows = silver_df.count()


print(
    "Registros Silver después de deduplicar:",
    f"{silver_rows:,}",
)

print(
    "Duplicados eliminados:",
    f"{valid_rows_before_deduplication - silver_rows:,}",
)

# COMMAND ----------

duplicate_source_keys = (
    silver_df
    .groupBy(*agent_key)
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_source_key_count = (
    duplicate_source_keys.count()
)


if duplicate_source_key_count > 0:
    display(duplicate_source_keys)

    raise ValueError(
        "El DataFrame que será enviado al MERGE "
        "todavía tiene llaves duplicadas. "
        f"Grupos duplicados: "
        f"{duplicate_source_key_count:,}"
    )


print(
    "Fuente del MERGE única por llave de negocio."
)

# COMMAND ----------

if silver_rows == 0:
    print(
        "No existen registros válidos para cargar."
    )

else:
    target = DeltaTable.forName(
        spark,
        silver_table,
    )

    (
        target.alias("target")
        .merge(
            silver_df.alias("source"),
            """
            target.fecha = source.fecha
            AND target.codigo_duracion =
                source.codigo_duracion
            AND target.codigo_agente =
                source.codigo_agente
            AND target.actividad_agente =
                source.actividad_agente
            """
        )
        .whenMatchedUpdate(
            condition="""
                NOT (
                    target.nombre_agente
                    <=> source.nombre_agente
                )
                OR NOT (
                    target.source_file_name
                    <=> source.source_file_name
                )
                OR NOT (
                    target.source_file_path
                    <=> source.source_file_path
                )
                OR NOT (
                    target.ingestion_timestamp
                    <=> source.ingestion_timestamp
                )
                OR NOT (
                    target.load_date
                    <=> source.load_date
                )
            """,
            set={
                "nombre_agente":
                    "source.nombre_agente",

                "source_file_name":
                    "source.source_file_name",

                "source_file_path":
                    "source.source_file_path",

                "ingestion_timestamp":
                    "source.ingestion_timestamp",

                "load_date":
                    "source.load_date",

                "silver_updated_at":
                    "source.silver_updated_at",
            },
        )
        .whenNotMatchedInsert(
            values={
                "fecha":
                    "source.fecha",

                "codigo_duracion":
                    "source.codigo_duracion",

                "codigo_agente":
                    "source.codigo_agente",

                "nombre_agente":
                    "source.nombre_agente",

                "actividad_agente":
                    "source.actividad_agente",

                "source_file_name":
                    "source.source_file_name",

                "source_file_path":
                    "source.source_file_path",

                "ingestion_timestamp":
                    "source.ingestion_timestamp",

                "load_date":
                    "source.load_date",

                "silver_created_at":
                    "source.silver_created_at",

                "silver_updated_at":
                    "source.silver_updated_at",
            },
        )
        .execute()
    )


    print(
        "MERGE de agentes ejecutado correctamente."
    )

# COMMAND ----------

merge_history = (
    spark.sql(
        f"DESCRIBE HISTORY {silver_table}"
    )
    .select(
        "version",
        "timestamp",
        "operation",
        "operationMetrics",
    )
    .limit(1)
)


display(merge_history)

# COMMAND ----------

silver_total_rows = (
    spark.table(silver_table).count()
)


silver_distinct_keys = (
    spark.table(silver_table)
    .select(*agent_key)
    .distinct()
    .count()
)


print(
    "Total Silver:",
    f"{silver_total_rows:,}",
)

print(
    "Llaves distintas:",
    f"{silver_distinct_keys:,}",
)

print(
    "Duplicados:",
    f"{silver_total_rows - silver_distinct_keys:,}",
)


if silver_total_rows != silver_distinct_keys:
    raise ValueError(
        "La tabla Silver de agentes contiene "
        "llaves de negocio duplicadas."
    )

# COMMAND ----------

quality_summary = (
    spark.table(silver_table)
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.countDistinct(
            "codigo_agente"
        ).alias(
            "agentes_distintos"
        ),

        F.countDistinct(
            "actividad_agente"
        ).alias(
            "actividades_distintas"
        ),

        F.min("fecha").alias(
            "fecha_minima"
        ),

        F.max("fecha").alias(
            "fecha_maxima"
        ),

        F.sum(
            F.when(
                F.col("codigo_agente").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "codigo_agente_nulos"
        ),

        F.sum(
            F.when(
                F.col("nombre_agente").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "nombre_agente_nulos"
        ),
    )
)


display(quality_summary)

# COMMAND ----------

display(
    spark.table(silver_table)
    .groupBy(
        "actividad_agente"
    )
    .agg(
        F.count("*").alias(
            "registros"
        ),

        F.countDistinct(
            "codigo_agente"
        ).alias(
            "agentes_distintos"
        ),
    )
    .orderBy(
        F.desc("agentes_distintos")
    )
)

# COMMAND ----------

display(
    spark.table(silver_table)
    .groupBy(
        "codigo_agente"
    )
    .agg(
        F.countDistinct(
            "nombre_agente"
        ).alias(
            "nombres_distintos"
        ),

        F.collect_set(
            "nombre_agente"
        ).alias(
            "nombres_encontrados"
        ),
    )
    .filter(
        F.col("nombres_distintos") > 1
    )
    .orderBy(
        F.desc("nombres_distintos")
    )
)

# COMMAND ----------

print("Procesamiento Silver de agentes finalizado.")
