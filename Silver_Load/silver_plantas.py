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


SOURCE_NAME = "plantas"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]


print("Tabla Bronze:", bronze_table)
print("Tabla Silver:", silver_table)

# COMMAND ----------

if not spark.catalog.tableExists(bronze_table):
    raise ValueError(
        f"No existe la tabla Bronze: {bronze_table}"
    )


if not spark.catalog.tableExists(silver_table):
    raise ValueError(
        f"No existe la tabla Silver: {silver_table}. "
        "Ejecuta primero el DDL de Silver."
    )


print("Tablas Bronze y Silver encontradas.")

# COMMAND ----------

required_bronze_columns = {
    "fecha",
    "codigo_duracion",
    "codigo_planta",
    "nombre_planta",
    "codigo_sic_agente",
    "cap_efectiva_neta",
    "fpo",
    "codigo_sub_area_operativa",
    "codigo_area_operativa",
    "tipo_despacho_recurso",
    "tipo_clasificacion",
    "tipo_generacion",
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
    "codigo_planta",
    "nombre_planta",
    "codigo_sic_agente",
    "cap_efectiva_neta",
    "fpo",
    "codigo_sub_area_operativa",
    "codigo_area_operativa",
    "tipo_despacho_recurso",
    "tipo_clasificacion",
    "tipo_generacion",
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

bronze_df = spark.table(
    bronze_table
)


bronze_rows = bronze_df.count()


print(
    "Registros Bronze:",
    f"{bronze_rows:,}",
)

# COMMAND ----------

transformed_df = (
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
                F.col("codigo_planta")
            )
        ).alias("codigo_planta"),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("nombre_planta"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("nombre_planta"),

        F.upper(
            F.trim(
                F.col("codigo_sic_agente")
            )
        ).alias("codigo_sic_agente"),

        F.regexp_replace(
            F.trim(
                F.col("cap_efectiva_neta")
            ),
            ",",
            ".",
        )
        .cast("double")
        .alias("cap_efectiva_neta"),

        F.to_date(
            F.col("fpo")
        ).alias("fpo"),

        F.upper(
            F.trim(
                F.col(
                    "codigo_sub_area_operativa"
                )
            )
        ).alias(
            "codigo_sub_area_operativa"
        ),

        F.upper(
            F.trim(
                F.col(
                    "codigo_area_operativa"
                )
            )
        ).alias(
            "codigo_area_operativa"
        ),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col(
                        "tipo_despacho_recurso"
                    ),
                    r"\s+",
                    " ",
                )
            )
        ).alias(
            "tipo_despacho_recurso"
        ),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col(
                        "tipo_clasificacion"
                    ),
                    r"\s+",
                    " ",
                )
            )
        ).alias(
            "tipo_clasificacion"
        ),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("tipo_generacion"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("tipo_generacion"),

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
                    F.col("codigo_planta"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("fecha").cast("string"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("nombre_planta"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("codigo_sic_agente"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col(
                        "cap_efectiva_neta"
                    ).cast("string"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("tipo_generacion"),
                    F.lit(""),
                ),
            ),
            256,
        ),
    )
)

# COMMAND ----------

invalid_condition = (
    F.col("codigo_planta").isNull()
    | (F.col("codigo_planta") == "")
    | F.col("fecha").isNull()
)


invalid_df = transformed_df.filter(
    invalid_condition
)


valid_df = transformed_df.filter(
    ~invalid_condition
)


invalid_rows = invalid_df.count()
valid_rows = valid_df.count()


print(
    "Registros válidos:",
    f"{valid_rows:,}",
)

print(
    "Registros inválidos:",
    f"{invalid_rows:,}",
)

if invalid_rows > 0:
    display(
        invalid_df.select(
            "fecha",
            "codigo_planta",
            "nombre_planta",
            "source_file_name",
            "ingestion_timestamp",
        )
    )

    raise ValueError(
        "Se encontraron plantas sin código "
        "o sin fecha válida. "
        f"Cantidad: {invalid_rows:,}"
    )

# COMMAND ----------

capacity_quality = (
    valid_df
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.sum(
            F.when(
                F.col(
                    "cap_efectiva_neta"
                ).isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "capacidad_nula"
        ),

        F.sum(
            F.when(
                F.col(
                    "cap_efectiva_neta"
                ) < 0,
                1,
            ).otherwise(0)
        ).alias(
            "capacidad_negativa"
        ),

        F.min(
            "cap_efectiva_neta"
        ).alias(
            "capacidad_minima"
        ),

        F.max(
            "cap_efectiva_neta"
        ).alias(
            "capacidad_maxima"
        ),
    )
)


display(capacity_quality)

negative_capacity_rows = (
    valid_df
    .filter(
        F.col("cap_efectiva_neta") < 0
    )
    .count()
)


if negative_capacity_rows > 0:
    raise ValueError(
        "Se encontraron capacidades negativas. "
        f"Cantidad: {negative_capacity_rows:,}"
    )

# COMMAND ----------

latest_plant_window = (
    Window
    .partitionBy(
        "codigo_planta"
    )
    .orderBy(
        F.col(
            "fecha"
        ).desc_nulls_last(),

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
            latest_plant_window
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
    "Plantas vigentes para Silver:",
    f"{silver_rows:,}",
)

print(
    "Registros históricos descartados:",
    f"{valid_rows - silver_rows:,}",
)

# COMMAND ----------

duplicate_source_keys = (
    silver_df
    .groupBy(
        "codigo_planta"
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_groups = (
    duplicate_source_keys.count()
)


if duplicate_groups > 0:
    display(duplicate_source_keys)

    raise ValueError(
        "La fuente del MERGE todavía tiene "
        "más de una fila por codigo_planta. "
        f"Grupos duplicados: {duplicate_groups:,}"
    )


print(
    "Fuente del MERGE única por codigo_planta."
)

# COMMAND ----------

if silver_rows == 0:
    print(
        "No existen plantas válidas para procesar."
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
            target.codigo_planta =
                source.codigo_planta
            """
        )
        .whenMatchedUpdate(
            condition="""
                NOT (
                    target.fecha
                    <=> source.fecha
                )
                OR NOT (
                    target.codigo_duracion
                    <=> source.codigo_duracion
                )
                OR NOT (
                    target.nombre_planta
                    <=> source.nombre_planta
                )
                OR NOT (
                    target.codigo_sic_agente
                    <=> source.codigo_sic_agente
                )
                OR NOT (
                    target.cap_efectiva_neta
                    <=> source.cap_efectiva_neta
                )
                OR NOT (
                    target.fpo
                    <=> source.fpo
                )
                OR NOT (
                    target.codigo_sub_area_operativa
                    <=> source.codigo_sub_area_operativa
                )
                OR NOT (
                    target.codigo_area_operativa
                    <=> source.codigo_area_operativa
                )
                OR NOT (
                    target.tipo_despacho_recurso
                    <=> source.tipo_despacho_recurso
                )
                OR NOT (
                    target.tipo_clasificacion
                    <=> source.tipo_clasificacion
                )
                OR NOT (
                    target.tipo_generacion
                    <=> source.tipo_generacion
                )
                OR NOT (
                    target.ingestion_timestamp
                    <=> source.ingestion_timestamp
                )
            """,
            set={
                "fecha":
                    "source.fecha",

                "codigo_duracion":
                    "source.codigo_duracion",

                "nombre_planta":
                    "source.nombre_planta",

                "codigo_sic_agente":
                    "source.codigo_sic_agente",

                "cap_efectiva_neta":
                    "source.cap_efectiva_neta",

                "fpo":
                    "source.fpo",

                "codigo_sub_area_operativa":
                    "source.codigo_sub_area_operativa",

                "codigo_area_operativa":
                    "source.codigo_area_operativa",

                "tipo_despacho_recurso":
                    "source.tipo_despacho_recurso",

                "tipo_clasificacion":
                    "source.tipo_clasificacion",

                "tipo_generacion":
                    "source.tipo_generacion",

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

                "codigo_planta":
                    "source.codigo_planta",

                "nombre_planta":
                    "source.nombre_planta",

                "codigo_sic_agente":
                    "source.codigo_sic_agente",

                "cap_efectiva_neta":
                    "source.cap_efectiva_neta",

                "fpo":
                    "source.fpo",

                "codigo_sub_area_operativa":
                    "source.codigo_sub_area_operativa",

                "codigo_area_operativa":
                    "source.codigo_area_operativa",

                "tipo_despacho_recurso":
                    "source.tipo_despacho_recurso",

                "tipo_clasificacion":
                    "source.tipo_clasificacion",

                "tipo_generacion":
                    "source.tipo_generacion",

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
        "MERGE de plantas ejecutado correctamente."
    )

# COMMAND ----------

current_silver_duplicates = (
    spark.table(silver_table)
    .groupBy("codigo_planta")
    .count()
    .filter(F.col("count") > 1)
)


print(
    "Plantas con varias filas actualmente en Silver:",
    current_silver_duplicates.count(),
)

# COMMAND ----------

display(
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

# COMMAND ----------

silver_validation_df = spark.table(
    silver_table
)


total_rows = (
    silver_validation_df.count()
)


distinct_plants = (
    silver_validation_df
    .select("codigo_planta")
    .distinct()
    .count()
)


duplicate_rows = (
    total_rows
    - distinct_plants
)


print(
    "Total Silver:",
    f"{total_rows:,}",
)

print(
    "Plantas distintas:",
    f"{distinct_plants:,}",
)

print(
    "Duplicados:",
    f"{duplicate_rows:,}",
)


if duplicate_rows > 0:
    raise ValueError(
        "Silver plantas contiene más de una "
        "fila por codigo_planta."
    )

# COMMAND ----------

display(
    silver_validation_df
    .groupBy(
        "tipo_generacion"
    )
    .agg(
        F.count("*").alias(
            "plantas"
        ),

        F.sum(
            "cap_efectiva_neta"
        ).alias(
            "capacidad_total"
        ),
    )
    .orderBy(
        F.desc("plantas")
    )
)

# COMMAND ----------

agent_reference_quality = (
    silver_validation_df.alias("plant")
    .join(
        spark.table(
            SILVER_TABLES["agentes"]
        )
        .select(
            F.col("codigo_agente")
        )
        .distinct()
        .alias("agent"),
        F.col(
            "plant.codigo_sic_agente"
        )
        ==
        F.col(
            "agent.codigo_agente"
        ),
        "left",
    )
    .agg(
        F.count("*").alias(
            "total_plantas"
        ),

        F.sum(
            F.when(
                F.col(
                    "agent.codigo_agente"
                ).isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "plantas_sin_agente"
        ),
    )
)


display(agent_reference_quality)

# COMMAND ----------

spark.table(
    silver_table
).groupBy(
    "codigo_planta"
).count().filter(
    F.col("count") > 1
).show()
