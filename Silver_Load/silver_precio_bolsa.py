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


SOURCE_NAME = "precio_bolsa"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]


print("Tabla Bronze:", bronze_table)
print("Tabla Silver:", silver_table)

# COMMAND ----------

required_tables = [
    bronze_table,
    silver_table,
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
    "fecha_hora",
    "codigo_duracion",
    "unidad_medida",
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
    "fecha_hora",
    "codigo_duracion",
    "unidad_medida",
    "version",
    "valor",
    "es_precio_cero",
    "es_precio_negativo",
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

transformed_df = (
    bronze_df
    .select(
        F.upper(
            F.trim(
                F.col("codigo_variable")
            )
        ).alias("codigo_variable"),

        F.to_timestamp(
            F.col("fecha_hora")
        ).alias("fecha_hora"),

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
        "es_precio_cero",
        F.col("valor") == F.lit(0),
    )
    .withColumn(
        "es_precio_negativo",
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
                    F.col("fecha_hora").cast("string"),
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
    | F.col("fecha_hora").isNull()
    | F.col("codigo_duracion").isNull()
    | (F.col("codigo_duracion") == "")
    | F.col("unidad_medida").isNull()
    | (F.col("unidad_medida") == "")
    | F.col("version").isNull()
    | (F.col("version") == "")
)


invalid_keys_df = transformed_df.filter(
    invalid_key_condition
)


invalid_values_df = transformed_df.filter(
    F.col("valor").isNull()
)


invalid_key_rows = invalid_keys_df.count()
invalid_value_rows = invalid_values_df.count()


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
        "Existen registros de precio con "
        "llaves obligatorias inválidas. "
        f"Cantidad: {invalid_key_rows:,}"
    )


if invalid_value_rows > 0:
    display(
        invalid_values_df.limit(100)
    )

    raise ValueError(
        "Existen valores de precio que no "
        "pudieron convertirse a DOUBLE. "
        f"Cantidad: {invalid_value_rows:,}"
    )


valid_df = transformed_df.filter(
    ~invalid_key_condition
    & F.col("valor").isNotNull()
)


valid_rows = valid_df.count()


print(
    "Registros válidos:",
    f"{valid_rows:,}",
)

# COMMAND ----------

display(
    valid_df
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.sum(
            F.when(
                F.col("es_precio_negativo"),
                1,
            ).otherwise(0)
        ).alias(
            "precios_negativos"
        ),

        F.sum(
            F.when(
                F.col("es_precio_cero"),
                1,
            ).otherwise(0)
        ).alias(
            "precios_cero"
        ),

        F.min("valor").alias(
            "precio_minimo"
        ),

        F.max("valor").alias(
            "precio_maximo"
        ),

        F.avg("valor").alias(
            "precio_promedio"
        ),
    )
)

# COMMAND ----------

price_key = [
    "codigo_variable",
    "fecha_hora",
    "codigo_duracion",
    "unidad_medida",
    "version",
]


deduplication_window = (
    Window
    .partitionBy(*price_key)
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
    "Registros después de deduplicar:",
    f"{silver_rows:,}",
)

print(
    "Duplicados descartados:",
    f"{valid_rows - silver_rows:,}",
)

# COMMAND ----------

duplicate_source_keys = (
    silver_df
    .groupBy(*price_key)
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_groups = (
    duplicate_source_keys.count()
)


if duplicate_groups > 0:
    display(
        duplicate_source_keys.limit(100)
    )

    raise ValueError(
        "La fuente del MERGE contiene "
        "llaves de precio duplicadas. "
        f"Grupos: {duplicate_groups:,}"
    )


print(
    "Fuente única por llave de precio."
)

# COMMAND ----------

display(
    silver_df
    .groupBy(
        "codigo_variable",
        "codigo_duracion",
        "unidad_medida",
        "version",
    )
    .agg(
        F.count("*").alias(
            "registros"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.min("valor").alias(
            "precio_minimo"
        ),

        F.max("valor").alias(
            "precio_maximo"
        ),
    )
    .orderBy(
        "codigo_variable",
        "version",
    )
)

# COMMAND ----------

if silver_rows == 0:
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
            AND target.fecha_hora =
                source.fecha_hora
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
                    target.es_precio_cero
                    <=> source.es_precio_cero
                )
                OR NOT (
                    target.es_precio_negativo
                    <=> source.es_precio_negativo
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

                "es_precio_cero":
                    "source.es_precio_cero",

                "es_precio_negativo":
                    "source.es_precio_negativo",

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

                "fecha_hora":
                    "source.fecha_hora",

                "codigo_duracion":
                    "source.codigo_duracion",

                "unidad_medida":
                    "source.unidad_medida",

                "version":
                    "source.version",

                "valor":
                    "source.valor",

                "es_precio_cero":
                    "source.es_precio_cero",

                "es_precio_negativo":
                    "source.es_precio_negativo",

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
        "MERGE de precio de bolsa ejecutado."
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


total_rows = silver_validation_df.count()


distinct_keys = (
    silver_validation_df
    .select(*price_key)
    .distinct()
    .count()
)


duplicate_rows = (
    total_rows
    - distinct_keys
)


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
        "Silver precio_bolsa contiene "
        "llaves duplicadas."
    )

# COMMAND ----------

display(
    silver_validation_df
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.countDistinct(
            "codigo_variable"
        ).alias(
            "variables_distintas"
        ),

        F.countDistinct(
            "version"
        ).alias(
            "versiones_distintas"
        ),

        F.sum(
            F.when(
                F.col("es_precio_negativo"),
                1,
            ).otherwise(0)
        ).alias(
            "precios_negativos"
        ),

        F.sum(
            F.when(
                F.col("es_precio_cero"),
                1,
            ).otherwise(0)
        ).alias(
            "precios_cero"
        ),

        F.min("valor").alias(
            "precio_minimo"
        ),

        F.max("valor").alias(
            "precio_maximo"
        ),

        F.avg("valor").alias(
            "precio_promedio"
        ),
    )
)
