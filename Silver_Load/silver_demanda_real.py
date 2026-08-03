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
    CATALOG,
    SILVER_TABLES,
)

spark.sql(f"USE CATALOG `{CATALOG}`")


SOURCE_NAME = "demanda_real"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]
agents_table = SILVER_TABLES["agentes"]


print("Tabla Bronze:", bronze_table)
print("Tabla Silver:", silver_table)
print("Maestro de agentes:", agents_table)

# COMMAND ----------

required_tables = [
    bronze_table,
    silver_table,
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

# MAGIC %md
# MAGIC ALTER TABLE silver.demanda_real
# MAGIC ADD COLUMNS (
# MAGIC     agente_encontrado BOOLEAN
# MAGIC );

# COMMAND ----------

required_bronze_columns = {
    "codigo_variable",
    "fecha_hora",
    "codigo_sic_agente",
    "tipo_mercado",
    "version",
    "valor",
    "unidad_medida",
    "codigo_duracion",
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
    "codigo_agente",
    "tipo_mercado",
    "version",
    "demanda_real_kwh",
    "unidad_medida",
    "codigo_duracion",
    "es_demanda_cero",
    "agente_encontrado",
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
                F.col("codigo_sic_agente")
            )
        ).alias("codigo_agente"),

        F.upper(
            F.trim(
                F.regexp_replace(
                    F.col("tipo_mercado"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("tipo_mercado"),

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
        .alias("demanda_real_kwh"),

        F.upper(
            F.trim(
                F.col("unidad_medida")
            )
        ).alias("unidad_medida"),

        F.upper(
            F.trim(
                F.col("codigo_duracion")
            )
        ).alias("codigo_duracion"),

        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
    .withColumn(
        "es_demanda_cero",
        F.col("demanda_real_kwh") == F.lit(0),
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
                    F.col("codigo_agente"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("tipo_mercado"),
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
                    F.col("demanda_real_kwh").cast("string"),
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
    | F.col("codigo_agente").isNull()
    | (F.col("codigo_agente") == "")
    | F.col("tipo_mercado").isNull()
    | (F.col("tipo_mercado") == "")
    | F.col("version").isNull()
    | (F.col("version") == "")
    | F.col("unidad_medida").isNull()
    | (F.col("unidad_medida") == "")
    | F.col("codigo_duracion").isNull()
    | (F.col("codigo_duracion") == "")
)


invalid_keys_df = transformed_df.filter(
    invalid_key_condition
)


invalid_values_df = transformed_df.filter(
    F.col("demanda_real_kwh").isNull()
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
        "Existen registros de demanda con "
        "llaves obligatorias inválidas. "
        f"Cantidad: {invalid_key_rows:,}"
    )


if invalid_value_rows > 0:
    display(
        invalid_values_df.limit(100)
    )

    raise ValueError(
        "Existen valores de demanda que no "
        "pudieron convertirse a DOUBLE. "
        f"Cantidad: {invalid_value_rows:,}"
    )


valid_df = transformed_df.filter(
    ~invalid_key_condition
    & F.col("demanda_real_kwh").isNotNull()
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
                F.col("demanda_real_kwh") < 0,
                1,
            ).otherwise(0)
        ).alias(
            "valores_negativos"
        ),

        F.sum(
            F.when(
                F.col("demanda_real_kwh") == 0,
                1,
            ).otherwise(0)
        ).alias(
            "valores_cero"
        ),

        F.min(
            "demanda_real_kwh"
        ).alias(
            "valor_minimo"
        ),

        F.max(
            "demanda_real_kwh"
        ).alias(
            "valor_maximo"
        ),
    )
)


display(value_quality_df)

negative_rows = (
    valid_df
    .filter(
        F.col("demanda_real_kwh") < 0
    )
    .count()
)


if negative_rows > 0:
    raise ValueError(
        "Se encontraron valores negativos "
        "en demanda real. "
        f"Cantidad: {negative_rows:,}"
    )

# COMMAND ----------

demand_key = [
    "codigo_variable",
    "fecha_hora",
    "codigo_agente",
    "tipo_mercado",
    "codigo_duracion",
    "unidad_medida",
    "version",
]


deduplication_window = (
    Window
    .partitionBy(*demand_key)
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
    .groupBy(*demand_key)
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
        "llaves duplicadas. "
        f"Grupos: {duplicate_groups:,}"
    )


print(
    "Fuente del MERGE única por llave de demanda."
)

# COMMAND ----------

agents_reference_df = (
    spark.table(agents_table)
    .select("codigo_agente")
    .distinct()
)


silver_df = (
    silver_df.alias("demand")
    .join(
        agents_reference_df.alias("agent"),
        F.col(
            "demand.codigo_agente"
        )
        ==
        F.col(
            "agent.codigo_agente"
        ),
        "left",
    )
    .select(
        "demand.*",

        F.col(
            "agent.codigo_agente"
        ).isNotNull().alias(
            "agente_encontrado"
        ),
    )
)


print(
    "Registros enriquecidos:",
    f"{silver_df.count():,}",
)

# COMMAND ----------

display(
    silver_df
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.sum(
            F.when(
                F.col("agente_encontrado") == True,
                1,
            ).otherwise(0)
        ).alias(
            "registros_con_agente"
        ),

        F.sum(
            F.when(
                F.col("agente_encontrado") == False,
                1,
            ).otherwise(0)
        ).alias(
            "registros_sin_agente"
        ),

        F.countDistinct(
            F.when(
                F.col("agente_encontrado") == False,
                F.col("codigo_agente"),
            )
        ).alias(
            "codigos_agente_sin_maestro"
        ),
    )
)

# COMMAND ----------

current_silver_df = spark.table(
    silver_table
)


current_silver_rows = (
    current_silver_df.count()
)


current_distinct_keys = (
    current_silver_df
    .select(*demand_key)
    .distinct()
    .count()
)


current_duplicate_rows = (
    current_silver_rows
    - current_distinct_keys
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
    f"{current_duplicate_rows:,}",
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
            AND target.codigo_agente =
                source.codigo_agente
            AND target.tipo_mercado =
                source.tipo_mercado
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
                    target.demanda_real_kwh
                    <=> source.demanda_real_kwh
                )
                OR NOT (
                    target.es_demanda_cero
                    <=> source.es_demanda_cero
                )
                OR NOT (
                    target.agente_encontrado
                    <=> source.agente_encontrado
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
                "demanda_real_kwh":
                    "source.demanda_real_kwh",

                "es_demanda_cero":
                    "source.es_demanda_cero",

                "agente_encontrado":
                    "source.agente_encontrado",

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

                "codigo_agente":
                    "source.codigo_agente",

                "tipo_mercado":
                    "source.tipo_mercado",

                "version":
                    "source.version",

                "demanda_real_kwh":
                    "source.demanda_real_kwh",

                "unidad_medida":
                    "source.unidad_medida",

                "codigo_duracion":
                    "source.codigo_duracion",

                "es_demanda_cero":
                    "source.es_demanda_cero",

                "agente_encontrado":
                    "source.agente_encontrado",

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
        "MERGE de demanda real ejecutado."
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


silver_validation_df = spark.table(
    silver_table
)


total_rows = silver_validation_df.count()


distinct_keys = (
    silver_validation_df
    .select(*demand_key)
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
        "Silver demanda_real contiene "
        "llaves duplicadas."
    )

# COMMAND ----------

display(
    spark.table(silver_table)
    .agg(
        F.count("*").alias(
            "total_registros"
        ),

        F.sum(
            F.when(
                F.col("agente_encontrado").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "agente_encontrado_nulo"
        ),

        F.sum(
            F.when(
                F.col("agente_encontrado") == True,
                1,
            ).otherwise(0)
        ).alias(
            "registros_con_agente"
        ),

        F.sum(
            F.when(
                F.col("agente_encontrado") == False,
                1,
            ).otherwise(0)
        ).alias(
            "registros_sin_agente"
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC UPDATE silver.demanda_real AS demand
# MAGIC SET agente_encontrado = EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM silver.agentes AS agent
# MAGIC     WHERE agent.codigo_agente = demand.codigo_agente
# MAGIC );

# COMMAND ----------

display(
    spark.table(silver_table)
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
            "codigo_agente"
        ).alias(
            "agentes_distintos"
        ),

        F.countDistinct(
            "tipo_mercado"
        ).alias(
            "mercados_distintos"
        ),

        F.countDistinct(
            "version"
        ).alias(
            "versiones_distintas"
        ),

        F.sum(
            F.when(
                F.col("demanda_real_kwh") < 0,
                1,
            ).otherwise(0)
        ).alias(
            "valores_negativos"
        ),

        F.sum(
            F.when(
                F.col("es_demanda_cero") == True,
                1,
            ).otherwise(0)
        ).alias(
            "valores_cero"
        ),

        F.sum(
            F.when(
                F.col("agente_encontrado").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "agente_encontrado_nulo"
        ),

        F.sum(
            F.when(
                F.col("agente_encontrado") == False,
                1,
            ).otherwise(0)
        ).alias(
            "registros_sin_agente"
        ),

        F.countDistinct(
            F.when(
                F.col("agente_encontrado") == False,
                F.col("codigo_agente"),
            )
        ).alias(
            "codigos_agente_sin_maestro"
        ),
    )
)
