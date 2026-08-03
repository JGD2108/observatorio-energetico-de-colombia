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


SOURCE_NAME = "embalses"

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
        "Ejecuta primero el DDL correspondiente."
    )


print("Tablas Bronze y Silver encontradas.")

# COMMAND ----------

required_bronze_columns = {
    "codigo_embalse",
    "nombre_embalse",
    "latitud",
    "longitud",
    "tipo_coordenada",
    "coordinate_source",
    "geocoding_status",
    "geocoding_query",
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
    "codigo_embalse",
    "nombre_embalse",
    "latitud",
    "longitud",
    "tipo_coordenada",
    "fuente_coordenada",
    "estado_geocodificacion",
    "consulta_geocodificacion",
    "coordenadas_validas",
    "requiere_revision_manual",
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
        F.upper(
            F.trim(
                F.col("codigo_embalse")
            )
        ).alias("codigo_embalse"),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("nombre_embalse"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("nombre_embalse"),

        F.col("latitud")
        .cast("double")
        .alias("latitud"),

        F.col("longitud")
        .cast("double")
        .alias("longitud"),

        F.lower(
            F.trim(
                F.col("tipo_coordenada")
            )
        ).alias("tipo_coordenada"),

        F.lower(
            F.trim(
                F.col("coordinate_source")
            )
        ).alias("fuente_coordenada"),

        F.lower(
            F.trim(
                F.col("geocoding_status")
            )
        ).alias("estado_geocodificacion"),

        F.trim(
            F.col("geocoding_query")
        ).alias("consulta_geocodificacion"),

        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
)

# COMMAND ----------

transformed_df = (
    transformed_df
    .withColumn(
        "coordenadas_validas",
        (
            F.col("latitud").isNotNull()
            & F.col("longitud").isNotNull()
            & F.col("latitud").between(
                -90.0,
                90.0,
            )
            & F.col("longitud").between(
                -180.0,
                180.0,
            )
        ),
    )
    .withColumn(
    "requiere_revision_manual",
    (
        ~F.col("coordenadas_validas")
        | F.col("fuente_coordenada").isNull()

        | (
            (F.col("fuente_coordenada") == "nominatim")
            & (
                F.col("estado_geocodificacion")
                != "found"
            )
        )

        | (
            (F.col("fuente_coordenada") == "fallback_manual")
            & (
                F.col("tipo_coordenada")
                == "approximate"
            )
        )
    ),
)
    .withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(
                    F.col("codigo_embalse"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("nombre_embalse"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("latitud").cast("string"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("longitud").cast("string"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("fuente_coordenada"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("estado_geocodificacion"),
                    F.lit(""),
                ),
            ),
            256,
        ),
    )
)

# COMMAND ----------

invalid_condition = (
    F.col("codigo_embalse").isNull()
    | (F.col("codigo_embalse") == "")
    | F.col("nombre_embalse").isNull()
    | (F.col("nombre_embalse") == "")
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
    display(invalid_df)

    raise ValueError(
        "Se encontraron embalses sin código "
        "o nombre válido. "
        f"Cantidad: {invalid_rows:,}"
    )

# COMMAND ----------

latest_reservoir_window = (
    Window
    .partitionBy(
        "codigo_embalse"
    )
    .orderBy(
        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col(
            "coordenadas_validas"
        ).desc(),

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
            latest_reservoir_window
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
    "Embalses vigentes para Silver:",
    f"{silver_rows:,}",
)

print(
    "Versiones descartadas:",
    f"{valid_rows - silver_rows:,}",
)

# COMMAND ----------

duplicate_source_keys = (
    silver_df
    .groupBy(
        "codigo_embalse"
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
        "La fuente del MERGE contiene más "
        "de una fila por codigo_embalse. "
        f"Grupos duplicados: {duplicate_groups:,}"
    )


print(
    "Fuente del MERGE única por codigo_embalse."
)

# COMMAND ----------

if silver_rows == 0:
    print(
        "No existen embalses válidos para cargar."
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
            target.codigo_embalse =
                source.codigo_embalse
            """
        )
        .whenMatchedUpdate(
            condition="""
                NOT (
                    target.nombre_embalse
                    <=> source.nombre_embalse
                )
                OR NOT (
                    target.latitud
                    <=> source.latitud
                )
                OR NOT (
                    target.longitud
                    <=> source.longitud
                )
                OR NOT (
                    target.tipo_coordenada
                    <=> source.tipo_coordenada
                )
                OR NOT (
                    target.fuente_coordenada
                    <=> source.fuente_coordenada
                )
                OR NOT (
                    target.estado_geocodificacion
                    <=> source.estado_geocodificacion
                )
                OR NOT (
                    target.consulta_geocodificacion
                    <=> source.consulta_geocodificacion
                )
                OR NOT (
                    target.coordenadas_validas
                    <=> source.coordenadas_validas
                )
                OR NOT (
                    target.requiere_revision_manual
                    <=> source.requiere_revision_manual
                )
                OR NOT (
                    target.ingestion_timestamp
                    <=> source.ingestion_timestamp
                )
            """,
            set={
                "nombre_embalse":
                    "source.nombre_embalse",

                "latitud":
                    "source.latitud",

                "longitud":
                    "source.longitud",

                "tipo_coordenada":
                    "source.tipo_coordenada",

                "fuente_coordenada":
                    "source.fuente_coordenada",

                "estado_geocodificacion":
                    "source.estado_geocodificacion",

                "consulta_geocodificacion":
                    "source.consulta_geocodificacion",

                "coordenadas_validas":
                    "source.coordenadas_validas",

                "requiere_revision_manual":
                    "source.requiere_revision_manual",

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
                "codigo_embalse":
                    "source.codigo_embalse",

                "nombre_embalse":
                    "source.nombre_embalse",

                "latitud":
                    "source.latitud",

                "longitud":
                    "source.longitud",

                "tipo_coordenada":
                    "source.tipo_coordenada",

                "fuente_coordenada":
                    "source.fuente_coordenada",

                "estado_geocodificacion":
                    "source.estado_geocodificacion",

                "consulta_geocodificacion":
                    "source.consulta_geocodificacion",

                "coordenadas_validas":
                    "source.coordenadas_validas",

                "requiere_revision_manual":
                    "source.requiere_revision_manual",

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
        "MERGE de embalses ejecutado correctamente."
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


distinct_reservoirs = (
    silver_validation_df
    .select("codigo_embalse")
    .distinct()
    .count()
)


print(
    "Total Silver:",
    f"{total_rows:,}",
)

print(
    "Embalses distintos:",
    f"{distinct_reservoirs:,}",
)

print(
    "Duplicados:",
    f"{total_rows - distinct_reservoirs:,}",
)


if total_rows != distinct_reservoirs:
    raise ValueError(
        "Silver embalses contiene más de "
        "una fila por codigo_embalse."
    )

# COMMAND ----------

display(
    silver_validation_df
    .agg(
        F.count("*").alias(
            "total_embalses"
        ),

        F.sum(
            F.when(
                F.col("coordenadas_validas"),
                1,
            ).otherwise(0)
        ).alias(
            "coordenadas_validas"
        ),

        F.sum(
            F.when(
                ~F.col("coordenadas_validas"),
                1,
            ).otherwise(0)
        ).alias(
            "coordenadas_invalidas"
        ),

        F.sum(
            F.when(
                F.col(
                    "requiere_revision_manual"
                ),
                1,
            ).otherwise(0)
        ).alias(
            "requieren_revision"
        ),

        F.sum(
            F.when(
                F.col("latitud").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "latitudes_nulas"
        ),

        F.sum(
            F.when(
                F.col("longitud").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "longitudes_nulas"
        ),
    )
)

# COMMAND ----------

display(
    silver_validation_df
    .groupBy(
        "fuente_coordenada",
        "estado_geocodificacion",
        "tipo_coordenada",
    )
    .agg(
        F.count("*").alias(
            "embalses"
        )
    )
    .orderBy(
        F.desc("embalses")
    )
)

# COMMAND ----------

display(
    silver_validation_df
    .filter(
        F.col(
            "requiere_revision_manual"
        )
    )
    .select(
        "codigo_embalse",
        "nombre_embalse",
        "latitud",
        "longitud",
        "tipo_coordenada",
        "fuente_coordenada",
        "estado_geocodificacion",
        "consulta_geocodificacion",
    )
    .orderBy(
        "codigo_embalse"
    )
)
