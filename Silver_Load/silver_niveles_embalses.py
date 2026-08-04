# Databricks notebook source
from delta.tables import DeltaTable

from pyspark.sql import functions as F
from pyspark.sql.window import Window

import sys


spark.sql("SET TIME ZONE 'UTC'")


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


SOURCE_NAME = "niveles_embalses"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]
plants_table = SILVER_TABLES["plantas"]


print("Tabla Bronze:", bronze_table)
print("Tabla Silver:", silver_table)
print("Maestro de plantas:", plants_table)

# COMMAND ----------

required_tables = [
    bronze_table,
    silver_table,
    plants_table,
]


missing_tables = [
    table_name
    for table_name in required_tables
    if not spark.catalog.tableExists(table_name)
]


if missing_tables:
    raise ValueError(
        "No existen las tablas requeridas: "
        f"{missing_tables}"
    )


print("Todas las tablas requeridas existen.")

# COMMAND ----------

required_bronze_columns = {
    "codigo_variable",
    "fecha_inicio",
    "codigo_duracion",
    "unidad_medida",
    "codigo_planta",
    "version",
    "valor",
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
    "codigo_variable",
    "fecha_inicio",
    "codigo_duracion",
    "unidad_medida",
    "codigo_planta",
    "version",
    "valor",
    "es_valor_cero",
    "es_valor_negativo",
    "planta_encontrada",
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


print("Watermark Silver:", last_ingestion_timestamp)

# COMMAND ----------

transformed_df = (
    bronze_df
    .select(
        F.upper(
            F.trim(
                F.col("codigo_variable")
            )
        ).alias("codigo_variable"),

        F.to_timestamp(
            F.col("fecha_inicio")
        ).alias("fecha_inicio"),

        F.upper(
            F.trim(
                F.col("codigo_duracion")
            )
        ).alias("codigo_duracion"),

        F.upper(
            F.trim(
                F.col("unidad_medida")
            )
        ).alias("unidad_medida"),

        F.upper(
            F.trim(
                F.col("codigo_planta")
            )
        ).alias("codigo_planta"),

        F.upper(
            F.trim(
                F.col("version")
            )
        ).alias("version"),

        F.regexp_replace(
            F.trim(
                F.col("valor").cast("string")
            ),
            ",",
            ".",
        )
        .cast("double")
        .alias("valor"),

        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
    .withColumn(
        "es_valor_cero",
        F.col("valor") == F.lit(0),
    )
    .withColumn(
        "es_valor_negativo",
        F.col("valor") < F.lit(0),
    )
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col("codigo_variable"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("fecha_inicio").cast("string"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("codigo_duracion"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("unidad_medida"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("codigo_planta"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("version"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("valor").cast("string"),
                    F.lit(""),
                ),
            ),
            256,
        ),
    )
)

# COMMAND ----------

invalid_key_condition = (
    F.col("codigo_variable").isNull()
    | (F.col("codigo_variable") == "")
    | F.col("fecha_inicio").isNull()
    | F.col("codigo_duracion").isNull()
    | (F.col("codigo_duracion") == "")
    | F.col("unidad_medida").isNull()
    | (F.col("unidad_medida") == "")
    | F.col("codigo_planta").isNull()
    | (F.col("codigo_planta") == "")
    | F.col("version").isNull()
    | (F.col("version") == "")
)


invalid_keys_df = transformed_df.filter(
    invalid_key_condition
)


invalid_values_df = transformed_df.filter(
    F.col("valor").isNull()
)


validation_stats = transformed_df.agg(
    F.sum(F.when(invalid_key_condition, 1).otherwise(0)).alias("invalid_key_rows"),
    F.sum(F.when(F.col("valor").isNull(), 1).otherwise(0)).alias("invalid_value_rows"),
    F.sum(
        F.when(~invalid_key_condition & F.col("valor").isNotNull(), 1).otherwise(0)
    ).alias("valid_rows"),
    F.sum(F.when(F.col("es_valor_negativo"), 1).otherwise(0)).alias("valores_negativos"),
    F.sum(F.when(F.col("es_valor_cero"), 1).otherwise(0)).alias("valores_cero"),
    F.min("valor").alias("valor_minimo"),
    F.max("valor").alias("valor_maximo"),
    F.avg("valor").alias("valor_promedio"),
).first()

invalid_key_rows = validation_stats["invalid_key_rows"] or 0
invalid_value_rows = validation_stats["invalid_value_rows"] or 0
valid_rows = validation_stats["valid_rows"] or 0


print(
    "Registros con llaves inválidas:",
    f"{invalid_key_rows:,}",
)

print(
    "Valores no convertibles:",
    f"{invalid_value_rows:,}",
)


if invalid_key_rows > 0:
    display(
        invalid_keys_df.limit(100)
    )

    raise ValueError(
        "Existen registros con llaves "
        "obligatorias inválidas. "
        f"Cantidad: {invalid_key_rows:,}"
    )


if invalid_value_rows > 0:
    display(
        invalid_values_df.limit(100)
    )

    raise ValueError(
        "Existen valores que no pudieron "
        "convertirse a DOUBLE. "
        f"Cantidad: {invalid_value_rows:,}"
    )


valid_df = transformed_df.filter(
    ~invalid_key_condition
    & F.col("valor").isNotNull()
)


print(
    "Registros válidos:",
    f"{valid_rows:,}",
)

# COMMAND ----------

print(
    "Calidad de valores:",
    {
        "valores_negativos": validation_stats["valores_negativos"] or 0,
        "valores_cero": validation_stats["valores_cero"] or 0,
        "valor_minimo": validation_stats["valor_minimo"],
        "valor_maximo": validation_stats["valor_maximo"],
        "valor_promedio": validation_stats["valor_promedio"],
    },
)

# COMMAND ----------

reservoir_level_key = [
    "codigo_variable",
    "fecha_inicio",
    "codigo_planta",
    "codigo_duracion",
    "unidad_medida",
    "version",
]


deduplication_window = (
    Window
    .partitionBy(*reservoir_level_key)
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


print(
    "La deduplicación se materializará una sola vez durante el MERGE."
)

# COMMAND ----------

print(
    "Fuente única por llave de niveles_embalses; "
    "garantizada por row_number = 1."
)

# COMMAND ----------

plants_reference_df = (
    spark.table(plants_table)
    .select("codigo_planta")
    .distinct()
)


silver_df = (
    silver_df.alias("level")
    .join(
        F.broadcast(plants_reference_df).alias("plant"),
        F.col(
            "level.codigo_planta"
        )
        ==
        F.col(
            "plant.codigo_planta"
        ),
        "left",
    )
    .select(
        "level.*",

        F.col(
            "plant.codigo_planta"
        ).isNotNull().alias(
            "planta_encontrada"
        ),
    )
)


print(
    "Enriquecimiento con el maestro de plantas preparado."
)

# COMMAND ----------

print("La calidad referencial se calcula una sola vez en la validación final.")

# COMMAND ----------

print(
    "El detalle por variable y versión se consulta bajo demanda; "
    "no se materializa durante la carga diaria."
)

# COMMAND ----------

if valid_rows == 0:
    print(
        "No existen registros válidos "
        "para cargar en Silver."
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
            target.codigo_variable =
                source.codigo_variable
            AND target.fecha_inicio =
                source.fecha_inicio
            AND target.codigo_planta =
                source.codigo_planta
            AND target.codigo_duracion =
                source.codigo_duracion
            AND target.unidad_medida =
                source.unidad_medida
            AND target.version =
                source.version
            """
        )
        .whenMatchedUpdate(
            condition="""
                NOT (
                    target.valor
                    <=> source.valor
                )
                OR NOT (
                    target.es_valor_cero
                    <=> source.es_valor_cero
                )
                OR NOT (
                    target.es_valor_negativo
                    <=> source.es_valor_negativo
                )
                OR NOT (
                    target.planta_encontrada
                    <=> source.planta_encontrada
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
                "valor":
                    "source.valor",

                "es_valor_cero":
                    "source.es_valor_cero",

                "es_valor_negativo":
                    "source.es_valor_negativo",

                "planta_encontrada":
                    "source.planta_encontrada",

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
                "codigo_variable":
                    "source.codigo_variable",

                "fecha_inicio":
                    "source.fecha_inicio",

                "codigo_duracion":
                    "source.codigo_duracion",

                "unidad_medida":
                    "source.unidad_medida",

                "codigo_planta":
                    "source.codigo_planta",

                "version":
                    "source.version",

                "valor":
                    "source.valor",

                "es_valor_cero":
                    "source.es_valor_cero",

                "es_valor_negativo":
                    "source.es_valor_negativo",

                "planta_encontrada":
                    "source.planta_encontrada",

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
        "MERGE de niveles_embalses ejecutado."
    )

# COMMAND ----------

history_row = (
    spark.sql(f"DESCRIBE HISTORY {silver_table}")
    .select("version", "timestamp", "operation", "operationMetrics")
    .limit(1)
    .first()
)
print("Operacion Delta:", history_row.asDict(recursive=True))

# COMMAND ----------

silver_validation_df = spark.table(
    silver_table
)


validation_summary = silver_validation_df.agg(
    F.count("*").alias("total_registros"),
    F.countDistinct(*[F.col(column) for column in reservoir_level_key]).alias(
        "llaves_distintas"
    ),
    F.min("fecha_inicio").alias("fecha_minima"),
    F.max("fecha_inicio").alias("fecha_maxima"),
    F.countDistinct("codigo_planta").alias("plantas_distintas"),
    F.countDistinct("codigo_variable").alias("variables_distintas"),
    F.countDistinct("version").alias("versiones_distintas"),
    F.sum(F.when(F.col("es_valor_negativo"), 1).otherwise(0)).alias(
        "valores_negativos"
    ),
    F.sum(F.when(F.col("es_valor_cero"), 1).otherwise(0)).alias("valores_cero"),
    F.sum(F.when(F.col("planta_encontrada").isNull(), 1).otherwise(0)).alias(
        "planta_encontrada_nula"
    ),
    F.sum(F.when(~F.col("planta_encontrada"), 1).otherwise(0)).alias(
        "registros_sin_planta"
    ),
    F.countDistinct(
        F.when(~F.col("planta_encontrada"), F.col("codigo_planta"))
    ).alias("codigos_planta_sin_maestro"),
    F.min("valor").alias("valor_minimo"),
    F.max("valor").alias("valor_maximo"),
).first()

total_rows = validation_summary["total_registros"]
distinct_keys = validation_summary["llaves_distintas"]
duplicate_rows = total_rows - distinct_keys


print(
    "Total Silver:",
    f"{total_rows:,}",
)

print(
    "Llaves distintas:",
    f"{distinct_keys:,}",
)

print(
    "Duplicados:",
    f"{duplicate_rows:,}",
)


if duplicate_rows > 0:
    raise ValueError(
        "Silver niveles_embalses contiene "
        "llaves duplicadas."
    )

print("Resumen final:", validation_summary.asDict())
