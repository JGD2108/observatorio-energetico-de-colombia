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
    GOVERNANCE_TABLES,
    SILVER_TABLES,
)

spark.sql(f"USE CATALOG `{CATALOG}`")


SOURCE_NAME = "plantas_reservorios"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
silver_table = SILVER_TABLES[SOURCE_NAME]

plants_table = SILVER_TABLES["plantas"]
reservoirs_table = SILVER_TABLES["embalses"]


print("Tabla Bronze:", bronze_table)
print("Tabla Silver:", silver_table)
print("Maestro plantas:", plants_table)
print("Maestro embalses:", reservoirs_table)

# COMMAND ----------

required_tables = [
    bronze_table,
    silver_table,
    plants_table,
    reservoirs_table,
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
    "region",
    "nombre_planta",
    "nombre_reservorio",
    "tipo_relacion",
    "es_principal",
    "permite_atribucion",
    "fuente_relacion",
    "estado_validacion",
    "valido_desde",
    "valido_hasta",
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
    "region",
    "nombre_planta",
    "nombre_reservorio",
    "tipo_relacion",
    "es_principal",
    "permite_atribucion",
    "fuente_relacion",
    "estado_validacion",
    "valido_desde",
    "valido_hasta",
    "codigo_planta",
    "codigo_embalse",
    "planta_encontrada",
    "embalse_encontrado",
    "relacion_completa",
    "requiere_revision_manual",
    "source_file_name",
    "source_file_path",
    "ingestion_timestamp",
    "load_date",
    "silver_created_at",
    "silver_updated_at",
    "activo",
    "fecha_retiro",
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

bronze_history_df = spark.table(bronze_table)
latest_snapshot_timestamp = (
    bronze_history_df
    .agg(F.max("ingestion_timestamp").alias("snapshot_timestamp"))
    .first()["snapshot_timestamp"]
)
bronze_df = bronze_history_df.filter(
    F.col("ingestion_timestamp") == F.lit(latest_snapshot_timestamp)
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
                F.regexp_replace(
                    F.col("region"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("region"),

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
                F.regexp_replace(
                    F.col("nombre_reservorio"),
                    r"\s+",
                    " ",
                )
            )
        ).alias("nombre_reservorio"),

        F.lower(
            F.trim(
                F.col("tipo_relacion")
            )
        ).alias("tipo_relacion"),

        F.coalesce(
            F.col("es_principal").cast("boolean"),
            F.lit(False),
        ).alias("es_principal"),

        F.coalesce(
            F.col(
                "permite_atribucion"
            ).cast("boolean"),
            F.lit(False),
        ).alias("permite_atribucion"),

        F.lower(
            F.trim(
                F.col("fuente_relacion")
            )
        ).alias("fuente_relacion"),

        F.lower(
            F.trim(
                F.col("estado_validacion")
            )
        ).alias("estado_validacion"),

        F.col("valido_desde")
        .cast("date")
        .alias("valido_desde"),

        F.col("valido_hasta")
        .cast("date")
        .alias("valido_hasta"),

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
                    F.col("nombre_planta"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("nombre_reservorio"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col("tipo_relacion"),
                    F.lit(""),
                ),
                F.coalesce(
                    F.col(
                        "estado_validacion"
                    ),
                    F.lit(""),
                ),
            ),
            256,
        ),
    )
)

# COMMAND ----------

invalid_condition = (
    F.col("region").isNull()
    | (F.col("region") == "")
    | F.col("nombre_planta").isNull()
    | (F.col("nombre_planta") == "")
    | F.col("nombre_reservorio").isNull()
    | (F.col("nombre_reservorio") == "")
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
        "Existen relaciones sin región, planta "
        "o reservorio. "
        f"Cantidad: {invalid_rows:,}"
    )

# COMMAND ----------

relationship_key = [
    "nombre_planta",
    "nombre_reservorio",
]


latest_relationship_window = (
    Window
    .partitionBy(*relationship_key)
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


deduplicated_df = (
    valid_df
    .withColumn(
        "row_number",
        F.row_number().over(
            latest_relationship_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop(
        "row_number",
        "record_hash",
    )
)


deduplicated_rows = deduplicated_df.count()


print(
    "Relaciones después de deduplicar:",
    f"{deduplicated_rows:,}",
)

print(
    "Duplicados descartados:",
    f"{valid_rows - deduplicated_rows:,}",
)

# COMMAND ----------

def normalize_name(column):
    return F.upper(
        F.trim(
            F.regexp_replace(
                F.translate(
                    column,
                    "ÁÉÍÓÚÜÑ",
                    "AEIOUUN",
                ),
                r"[^A-Z0-9]+",
                "",
            )
        )
    )

plants_reference_df = (
    spark.table(plants_table)
    .select(
        "codigo_planta",
        "nombre_planta",
    )
    .withColumn(
        "nombre_planta_normalizado",
        normalize_name(
            F.col("nombre_planta")
        ),
    )
    .dropDuplicates(
        ["codigo_planta"]
    )
)

reservoirs_reference_df = (
    spark.table(reservoirs_table)
    .select(
        "codigo_embalse",
        "nombre_embalse",
    )
    .withColumn(
        "nombre_embalse_normalizado",
        normalize_name(
            F.col("nombre_embalse")
        ),
    )
    .dropDuplicates(
        ["codigo_embalse"]
    )
)

reservoir_alias_df = (
    spark.table(GOVERNANCE_TABLES["ref_entity_alias"])
    .filter(
        (F.col("entity_type") == "EMBALSE")
        & (F.col("status") == "APPROVED")
        & (F.col("valid_from") <= F.current_date())
        & (F.col("valid_to").isNull() | (F.col("valid_to") >= F.current_date()))
    )
    .select(
        F.col("alias_normalized").alias("nombre_reservorio_normalizado"),
        F.col("canonical_code").alias("codigo_embalse_alias"),
    )
)


# COMMAND ----------

relationship_df = (
    deduplicated_df
    .withColumn(
        "nombre_planta_normalizado",
        normalize_name(
            F.col("nombre_planta")
        ),
    )
    .withColumn(
        "nombre_embalse_normalizado",
        normalize_name(
            F.col("nombre_reservorio")
        ),
    )
)

# COMMAND ----------

relationship_with_plant_df = (
    relationship_df.alias("relationship")
    .join(
        plants_reference_df.alias("plant"),
        F.col(
            "relationship.nombre_planta_normalizado"
        )
        ==
        F.col(
            "plant.nombre_planta_normalizado"
        ),
        "left",
    )
    .select(
        "relationship.*",

        F.col(
            "plant.codigo_planta"
        ).alias(
            "codigo_planta_encontrado"
        ),
    )
)

# COMMAND ----------

relationship_with_alias_df = (
    relationship_with_plant_df.alias(
        "relationship"
    )
    .join(
        reservoir_alias_df.alias("alias"),
        F.col(
            "relationship.nombre_embalse_normalizado"
        )
        ==
        F.col(
            "alias.nombre_reservorio_normalizado"
        ),
        "left",
    )
    .select(
        "relationship.*",
        F.col(
            "alias.codigo_embalse_alias"
        ).alias(
            "codigo_embalse_desde_alias"
        ),
    )
)

# COMMAND ----------

enriched_df = (
    relationship_with_alias_df.alias(
        "relationship"
    )
    .join(
        reservoirs_reference_df.alias(
            "reservoir"
        ),
        (
            F.col(
                "relationship.nombre_embalse_normalizado"
            )
            ==
            F.col(
                "reservoir.nombre_embalse_normalizado"
            )
        )
        |
        (
            F.col(
                "relationship.codigo_embalse_desde_alias"
            )
            ==
            F.col(
                "reservoir.codigo_embalse"
            )
        ),
        "left",
    )
    .select(
        "relationship.*",

        F.col(
            "reservoir.codigo_embalse"
        ).alias(
            "codigo_embalse_encontrado"
        ),
    )
)

# COMMAND ----------

multiple_matches = (
    enriched_df
    .groupBy(
        "nombre_planta",
        "nombre_reservorio",
    )
    .agg(
        F.countDistinct(
            "codigo_planta_encontrado"
        ).alias(
            "codigos_planta_encontrados"
        ),

        F.countDistinct(
            "codigo_embalse_encontrado"
        ).alias(
            "codigos_embalse_encontrados"
        ),
    )
    .filter(
        (F.col(
            "codigos_planta_encontrados"
        ) > 1)
        |
        (F.col(
            "codigos_embalse_encontrados"
        ) > 1)
    )
)


multiple_match_count = (
    multiple_matches.count()
)


if multiple_match_count > 0:
    display(multiple_matches)

    raise ValueError(
        "Se encontraron relaciones con múltiples "
        "códigos posibles. "
        f"Cantidad: {multiple_match_count:,}"
    )


print("No existen coincidencias ambiguas.")

# COMMAND ----------

silver_df = (
    enriched_df
    .select(
        "region",
        "nombre_planta",
        "nombre_reservorio",
        "tipo_relacion",
        "es_principal",
        "permite_atribucion",
        "fuente_relacion",
        "estado_validacion",
        "valido_desde",
        "valido_hasta",

        F.col(
            "codigo_planta_encontrado"
        ).alias("codigo_planta"),

        F.col(
            "codigo_embalse_encontrado"
        ).alias("codigo_embalse"),

        F.col(
            "codigo_planta_encontrado"
        ).isNotNull().alias(
            "planta_encontrada"
        ),

        F.col(
            "codigo_embalse_encontrado"
        ).isNotNull().alias(
            "embalse_encontrado"
        ),

        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
    .withColumn(
        "relacion_completa",
        (
            F.col("planta_encontrada")
            & F.col("embalse_encontrado")
        ),
    )
    .withColumn(
        "requiere_revision_manual",
        (
            ~F.col("relacion_completa")
            | (
                F.col("estado_validacion")
                != "validated"
            )
        ),
    )
    .withColumn(
        "silver_created_at",
        F.current_timestamp(),
    )
    .withColumn(
        "silver_updated_at",
        F.current_timestamp(),
    )
    .withColumn("activo", F.lit(True))
    .withColumn("fecha_retiro", F.lit(None).cast("timestamp"))
)

# COMMAND ----------

silver_rows = silver_df.count()


duplicate_source_keys = (
    silver_df
    .groupBy(*relationship_key)
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_groups = (
    duplicate_source_keys.count()
)


print(
    "Relaciones para Silver:",
    f"{silver_rows:,}",
)

print(
    "Grupos duplicados:",
    f"{duplicate_groups:,}",
)


if duplicate_groups > 0:
    display(duplicate_source_keys)

    raise ValueError(
        "La fuente del MERGE contiene "
        "relaciones duplicadas."
    )

# COMMAND ----------

display(
    silver_df
    .select(
        "region",
        "nombre_planta",
        "codigo_planta",
        "planta_encontrada",
        "nombre_reservorio",
        "codigo_embalse",
        "embalse_encontrado",
        "estado_validacion",
        "requiere_revision_manual",
    )
    .orderBy(
        "nombre_planta",
        "nombre_reservorio",
    )
)

# COMMAND ----------

if silver_rows == 0:
    print(
        "No existen relaciones válidas para cargar."
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
        target.nombre_planta =
            source.nombre_planta
        AND target.nombre_reservorio =
            source.nombre_reservorio
        """
    )
    .whenMatchedUpdate(
        condition="""
          NOT (target.region <=> source.region)
          OR NOT (target.tipo_relacion <=> source.tipo_relacion)
          OR NOT (target.es_principal <=> source.es_principal)
          OR NOT (target.permite_atribucion <=> source.permite_atribucion)
          OR NOT (target.fuente_relacion <=> source.fuente_relacion)
          OR NOT (target.estado_validacion <=> source.estado_validacion)
          OR NOT (target.valido_desde <=> source.valido_desde)
          OR NOT (target.valido_hasta <=> source.valido_hasta)
          OR NOT (target.codigo_planta <=> source.codigo_planta)
          OR NOT (target.codigo_embalse <=> source.codigo_embalse)
          OR NOT (target.activo <=> true)
        """,
        set={
            column: f"source.{column}"
            for column in [
                "region", "tipo_relacion", "es_principal", "permite_atribucion",
                "fuente_relacion", "estado_validacion", "valido_desde", "valido_hasta",
                "codigo_planta", "codigo_embalse", "planta_encontrada",
                "embalse_encontrado", "relacion_completa", "requiere_revision_manual",
                "source_file_name", "source_file_path", "ingestion_timestamp", "load_date",
                "silver_updated_at", "activo", "fecha_retiro",
            ]
        },
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceUpdate(
        condition="target.activo = true",
        set={
            "activo": "false",
            "fecha_retiro": "current_timestamp()",
            "silver_updated_at": "current_timestamp()",
        },
    )
    .execute()
)


    print(
        "MERGE de plantas-reservorios "
        "ejecutado correctamente."
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


display(
    silver_validation_df
    .agg(
        F.count("*").alias(
            "total_relaciones"
        ),

        F.sum(
            F.when(
                F.col("planta_encontrada"),
                1,
            ).otherwise(0)
        ).alias(
            "plantas_encontradas"
        ),

        F.sum(
            F.when(
                F.col("embalse_encontrado"),
                1,
            ).otherwise(0)
        ).alias(
            "embalses_encontrados"
        ),

        F.sum(
            F.when(
                F.col("relacion_completa"),
                1,
            ).otherwise(0)
        ).alias(
            "relaciones_completas"
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
    )
)

# COMMAND ----------

display(
    silver_validation_df
    .filter(
        ~F.col("relacion_completa")
    )
    .select(
        "region",
        "nombre_planta",
        "codigo_planta",
        "nombre_reservorio",
        "codigo_embalse",
        "planta_encontrada",
        "embalse_encontrado",
    )
    .orderBy(
        "nombre_planta",
        "nombre_reservorio",
    )
)

# COMMAND ----------

display(
    spark.table(
        SILVER_TABLES["embalses"]
    )
    .filter(
        F.col("codigo_embalse").isin(
            "CALIMA",
            "PORCE2",
            "PORCE3",
            "SOGAMOSO",
            "TOPOCORO",
            "URRA1",
        )
        |
        F.col("nombre_embalse").contains("CALIMA")
        |
        F.col("nombre_embalse").contains("PORCE")
        |
        F.col("nombre_embalse").contains("TOPO")
        |
        F.col("nombre_embalse").contains("URRA")
    )
    .select(
        "codigo_embalse",
        "nombre_embalse",
    )
    .orderBy(
        "codigo_embalse"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     nombre_planta,
# MAGIC     nombre_reservorio
# MAGIC FROM bronze.plantas_reservorios
# MAGIC WHERE UPPER(nombre_planta) = 'SOGAMOSO';
