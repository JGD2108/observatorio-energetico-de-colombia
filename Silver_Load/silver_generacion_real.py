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
    CATALOG,
    SILVER_TABLES,
)

spark.sql(f"USE CATALOG `{CATALOG}`")


SOURCE_NAME = "generacion_real"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]

plants_table = SILVER_TABLES["plantas"]
agents_table = SILVER_TABLES["agentes"]


print("Tabla Bronze:", bronze_table)
print("Tabla Silver:", silver_table)
print("Maestro plantas:", plants_table)
print("Maestro agentes:", agents_table)

# COMMAND ----------

required_tables = [
    bronze_table,
    silver_table,
    plants_table,
    agents_table,
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
    "codigo_sic_agente",
    "codigo_planta",
    "version",
    "valor",
    "planta_encontrada",
    "agente_encontrado",
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
    "codigo_agente",
    "codigo_planta",
    "version",
    "valor",
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
                F.col("codigo_sic_agente")
            )
        ).alias("codigo_agente"),

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
                F.col("valor")
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
                    F.col("codigo_agente"),
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
    | F.col("fecha_hora").isNull()
    | F.col("codigo_duracion").isNull()
    | (F.col("codigo_duracion") == "")
    | F.col("unidad_medida").isNull()
    | (F.col("unidad_medida") == "")
    | F.col("codigo_agente").isNull()
    | (F.col("codigo_agente") == "")
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


invalid_key_rows = invalid_keys_df.count()
invalid_value_rows = invalid_values_df.count()


print(
    "Registros con llaves inválidas:",
    f"{invalid_key_rows:,}",
)

print(
    "Registros con valor no convertible:",
    f"{invalid_value_rows:,}",
)

if invalid_key_rows > 0:
    display(
        invalid_keys_df.limit(100)
    )

    raise ValueError(
        "Existen registros de generación "
        "con llaves obligatorias inválidas. "
        f"Cantidad: {invalid_key_rows:,}"
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

if invalid_key_rows > 0:
    display(
        invalid_keys_df.limit(100)
    )

    raise ValueError(
        "Existen registros de generación "
        "con llaves obligatorias inválidas. "
        f"Cantidad: {invalid_key_rows:,}"
    )


if invalid_value_rows > 0:
    display(
        invalid_values_df.limit(100)
    )

    raise ValueError(
        "Existen valores de generación que "
        "no pudieron convertirse a DOUBLE. "
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

value_quality_df = (
    valid_df
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.sum(
            F.when(
                F.col("valor") < 0,
                1,
            ).otherwise(0)
        ).alias(
            "valores_negativos"
        ),

        F.sum(
            F.when(
                F.col("valor") == 0,
                1,
            ).otherwise(0)
        ).alias(
            "valores_cero"
        ),

        F.min("valor").alias(
            "valor_minimo"
        ),

        F.max("valor").alias(
            "valor_maximo"
        ),
    )
)


display(value_quality_df)

# COMMAND ----------

generation_key = [
    "fecha_hora",
    "codigo_agente",
    "codigo_planta",
    "codigo_variable",
    "codigo_duracion",
    "unidad_medida",
    "version",
]


deduplication_window = (
    Window
    .partitionBy(*generation_key)
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
    .groupBy(*generation_key)
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
        "La fuente del MERGE todavía contiene "
        "llaves duplicadas. "
        f"Grupos: {duplicate_groups:,}"
    )


print(
    "Fuente del MERGE única por llave de generación."
)

# COMMAND ----------

plants_reference_df = (
    spark.table(plants_table)
    .select("codigo_planta")
    .distinct()
)


agents_reference_df = (
    spark.table(agents_table)
    .select(
        F.col("codigo_agente")
    )
    .distinct()
)

plant_reference_quality = (
    silver_df.alias("generation")
    .join(
        plants_reference_df.alias("plant"),
        F.col(
            "generation.codigo_planta"
        )
        ==
        F.col(
            "plant.codigo_planta"
        ),
        "left",
    )
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.sum(
            F.when(
                F.col(
                    "plant.codigo_planta"
                ).isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "registros_sin_planta"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "plant.codigo_planta"
                ).isNull(),
                F.col(
                    "generation.codigo_planta"
                ),
            )
        ).alias(
            "codigos_planta_sin_maestro"
        ),
    )
)


display(plant_reference_quality)

agent_reference_quality = (
    silver_df.alias("generation")
    .join(
        agents_reference_df.alias("agent"),
        F.col(
            "generation.codigo_agente"
        )
        ==
        F.col(
            "agent.codigo_agente"
        ),
        "left",
    )
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.sum(
            F.when(
                F.col(
                    "agent.codigo_agente"
                ).isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "registros_sin_agente"
        ),

        F.countDistinct(
            F.when(
                F.col(
                    "agent.codigo_agente"
                ).isNull(),
                F.col(
                    "generation.codigo_agente"
                ),
            )
        ).alias(
            "codigos_agente_sin_maestro"
        ),
    )
)


display(agent_reference_quality)

# COMMAND ----------

plants_reference_df = (
    spark.table(plants_table)
    .select("codigo_planta")
    .distinct()
)


agents_reference_df = (
    spark.table(agents_table)
    .select("codigo_agente")
    .distinct()
)


silver_enriched_df = (
    silver_df.alias("generation")
    .join(
        plants_reference_df.alias("plant"),
        F.col("generation.codigo_planta")
        ==
        F.col("plant.codigo_planta"),
        "left",
    )
    .join(
        agents_reference_df.alias("agent"),
        F.col("generation.codigo_agente")
        ==
        F.col("agent.codigo_agente"),
        "left",
    )
    .select(
        "generation.*",

        F.col(
            "plant.codigo_planta"
        ).isNotNull().alias(
            "planta_encontrada"
        ),

        F.col(
            "agent.codigo_agente"
        ).isNotNull().alias(
            "agente_encontrado"
        ),
    )
)


silver_df = silver_enriched_df


print(
    "Registros enriquecidos:",
    f"{silver_df.count():,}",
)

# COMMAND ----------

plants_reference_df
agents_reference_df

# COMMAND ----------

current_silver_rows = (
    spark.table(silver_table).count()
)


current_distinct_keys = (
    spark.table(silver_table)
    .select(*generation_key)
    .distinct()
    .count()
)


print(
    "Total actual Silver:",
    f"{current_silver_rows:,}",
)

print(
    "Llaves distintas actuales:",
    f"{current_distinct_keys:,}",
)

print(
    "Duplicados actuales:",
    f"{current_silver_rows - current_distinct_keys:,}",
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
            target.fecha_hora =
                source.fecha_hora
            AND target.codigo_agente =
                source.codigo_agente
            AND target.codigo_planta =
                source.codigo_planta
            AND target.codigo_variable =
                source.codigo_variable
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
                OR NOT (
                    target.planta_encontrada
                    <=> source.planta_encontrada
                )
                OR NOT (
                    target.agente_encontrado
                    <=> source.agente_encontrado
)
            """,
            set={
                "valor":
                    "source.valor",

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

                "planta_encontrada":
                    "source.planta_encontrada",

                "agente_encontrado":
                    "source.agente_encontrado",
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

                "codigo_agente":
                    "source.codigo_agente",

                "codigo_planta":
                    "source.codigo_planta",

                "version":
                    "source.version",

                "planta_encontrada":
                    "source.planta_encontrada",

                "agente_encontrado":
                    "source.agente_encontrado",

                "valor":
                    "source.valor",

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
        "MERGE de generación real "
        "ejecutado correctamente."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC UPDATE silver.generacion_real AS generation
# MAGIC SET planta_encontrada = EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM silver.plantas AS plant
# MAGIC     WHERE plant.codigo_planta = generation.codigo_planta
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC UPDATE silver.generacion_real AS generation
# MAGIC SET agente_encontrado = EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM silver.agentes AS agent
# MAGIC     WHERE agent.codigo_agente = generation.codigo_agente
# MAGIC );

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
    .select(*generation_key)
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
        "Silver generacion_real contiene "
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
            "codigo_planta"
        ).alias(
            "plantas_distintas"
        ),

        F.countDistinct(
            "codigo_agente"
        ).alias(
            "agentes_distintos"
        ),

        F.countDistinct(
            "version"
        ).alias(
            "versiones_distintas"
        ),

        F.sum(
            F.when(
                F.col("valor") < 0,
                1,
            ).otherwise(0)
        ).alias(
            "valores_negativos"
        ),

        F.sum(
            F.when(
                F.col("valor") == 0,
                1,
            ).otherwise(0)
        ).alias(
            "valores_cero"
        ),
    )
)

# COMMAND ----------

missing_plants_df = (
    silver_df.alias("generation")
    .join(
        plants_reference_df.alias("plant"),
        F.col("generation.codigo_planta")
        ==
        F.col("plant.codigo_planta"),
        "left_anti",
    )
    .groupBy(
        "codigo_planta"
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

        F.countDistinct(
            "codigo_agente"
        ).alias(
            "agentes_distintos"
        ),

        F.collect_set(
            "codigo_agente"
        ).alias(
            "codigos_agente"
        ),
    )
    .orderBy(
        F.desc("registros")
    )
)


display(missing_plants_df)

# COMMAND ----------

missing_plant_codes_df = (
    missing_plants_df
    .select("codigo_planta")
)


bronze_plant_history_df = (
    spark.table(
        BRONZE_TABLES["plantas"]
    )
    .select(
        F.upper(
            F.trim(
                F.col("codigo_planta")
            )
        ).alias("codigo_planta"),
        "nombre_planta",
        "fecha",
        "codigo_sic_agente",
    )
)


missing_plant_history_df = (
    missing_plant_codes_df.alias("missing")
    .join(
        bronze_plant_history_df.alias("history"),
        F.col("missing.codigo_planta")
        ==
        F.col("history.codigo_planta"),
        "left",
    )
    .groupBy(
        F.col(
            "missing.codigo_planta"
        ).alias("codigo_planta")
    )
    .agg(
        F.count(
            "history.codigo_planta"
        ).alias(
            "apariciones_en_bronze_plantas"
        ),

        F.max(
            F.to_date(
                F.col("history.fecha")
            )
        ).alias(
            "ultima_fecha_en_maestro"
        ),

        F.collect_set(
            "history.nombre_planta"
        ).alias(
            "nombres_encontrados"
        ),
    )
    .orderBy(
        F.desc(
            "apariciones_en_bronze_plantas"
        )
    )
)


display(missing_plant_history_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_registros,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN planta_encontrada THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS registros_con_planta,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN NOT planta_encontrada THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS registros_sin_planta,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN agente_encontrado THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS registros_con_agente,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN NOT agente_encontrado THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS registros_sin_agente
# MAGIC
# MAGIC FROM silver.generacion_real;
