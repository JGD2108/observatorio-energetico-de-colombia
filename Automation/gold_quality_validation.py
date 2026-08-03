# Databricks notebook source
# NO ACTIVO: validador conservado como referencia histórica.
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

CATALOG = "observatorio_dev"

SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA = f"{CATALOG}.gold"


SILVER_TABLES = {
    "agentes":
        f"{SILVER_SCHEMA}.agentes",

    "plantas":
        f"{SILVER_SCHEMA}.plantas",

    "embalses":
        f"{SILVER_SCHEMA}.embalses",

    "plantas_reservorios":
        f"{SILVER_SCHEMA}.plantas_reservorios",

    "generacion_real":
        f"{SILVER_SCHEMA}.generacion_real",

    "disponibilidad_plantas":
        f"{SILVER_SCHEMA}.disponibilidad_plantas",

    "demanda_real":
        f"{SILVER_SCHEMA}.demanda_real",

    "precio_bolsa":
        f"{SILVER_SCHEMA}.precio_bolsa",

    "niveles_embalses":
        f"{SILVER_SCHEMA}.niveles_embalses",
}


GOLD_TABLES = {
    "dim_fecha":
        f"{GOLD_SCHEMA}.dim_fecha",

    "dim_periodo":
        f"{GOLD_SCHEMA}.dim_periodo",

    "dim_agente":
        f"{GOLD_SCHEMA}.dim_agente",

    "dim_planta":
        f"{GOLD_SCHEMA}.dim_planta",

    "dim_embalse":
        f"{GOLD_SCHEMA}.dim_embalse",

    "bridge_planta_embalse":
        f"{GOLD_SCHEMA}.bridge_planta_embalse",

    "fact_generacion_real":
        f"{GOLD_SCHEMA}.fact_generacion_real",

    "fact_disponibilidad_planta":
        f"{GOLD_SCHEMA}.fact_disponibilidad_planta",

    "fact_demanda_real":
        f"{GOLD_SCHEMA}.fact_demanda_real",

    "fact_precio_bolsa":
        f"{GOLD_SCHEMA}.fact_precio_bolsa",

    "fact_energia_embalsada_planta":
        f"{GOLD_SCHEMA}.fact_energia_embalsada_planta",
}


print("Configuración cargada.")
print("Catálogo:", CATALOG)
print("Esquema Silver:", SILVER_SCHEMA)
print("Esquema Gold:", GOLD_SCHEMA)

# COMMAND ----------

all_tables = {
    **{
        f"silver.{name}": table_name
        for name, table_name
        in SILVER_TABLES.items()
    },
    **{
        f"gold.{name}": table_name
        for name, table_name
        in GOLD_TABLES.items()
    },
}


table_existence_results = []


for logical_name, physical_name in all_tables.items():
    exists = spark.catalog.tableExists(
        physical_name
    )

    table_existence_results.append(
        (
            logical_name,
            physical_name,
            exists,
        )
    )


table_existence_df = spark.createDataFrame(
    table_existence_results,
    [
        "nombre_logico",
        "nombre_fisico",
        "existe",
    ],
)


display(
    table_existence_df
    .orderBy(
        "nombre_logico"
    )
)


missing_tables = (
    table_existence_df
    .filter(
        ~F.col("existe")
    )
    .count()
)


print(
    "Tablas esperadas:",
    len(all_tables),
)

print(
    "Tablas inexistentes:",
    missing_tables,
)


if missing_tables > 0:
    raise ValueError(
        "Faltan tablas necesarias para ejecutar "
        "la validación integral."
    )


print(
    "Existencia de tablas aprobada."
)

# COMMAND ----------

def validate_unique_key(
    table_name,
    key_columns,
    validation_name,
):
    source_df = spark.table(
        table_name
    )

    total_rows = source_df.count()

    distinct_rows = (
        source_df
        .select(
            *key_columns
        )
        .distinct()
        .count()
    )

    null_condition = None

    for column_name in key_columns:
        current_condition = (
            F.col(column_name).isNull()
        )

        null_condition = (
            current_condition
            if null_condition is None
            else null_condition
            | current_condition
        )

    null_key_rows = (
        source_df
        .filter(null_condition)
        .count()
    )

    duplicate_rows = (
        total_rows
        - distinct_rows
    )

    approved = (
        duplicate_rows == 0
        and null_key_rows == 0
    )

    return {
        "validacion": validation_name,
        "tabla": table_name,
        "filas": total_rows,
        "claves_distintas": distinct_rows,
        "duplicados": duplicate_rows,
        "claves_nulas": null_key_rows,
        "aprobada": approved,
    }

# COMMAND ----------

dimension_key_validations = [
    validate_unique_key(
        GOLD_TABLES["dim_fecha"],
        ["fecha_key"],
        "PK dim_fecha",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_fecha"],
        ["fecha"],
        "Natural key dim_fecha",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_periodo"],
        ["periodo_key"],
        "PK dim_periodo",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_periodo"],
        ["numero_periodo"],
        "Natural key dim_periodo",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_agente"],
        ["agente_key"],
        "PK dim_agente",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_agente"],
        [
            "codigo_agente",
            "fecha_inicio",
        ],
        "Natural SCD2 dim_agente",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_planta"],
        ["planta_key"],
        "PK dim_planta",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_planta"],
        ["codigo_planta"],
        "Natural key dim_planta",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_embalse"],
        ["embalse_key"],
        "PK dim_embalse",
    ),

    validate_unique_key(
        GOLD_TABLES["dim_embalse"],
        ["codigo_embalse"],
        "Natural key dim_embalse",
    ),

    validate_unique_key(
        GOLD_TABLES[
            "bridge_planta_embalse"
        ],
        ["planta_embalse_key"],
        "PK bridge_planta_embalse",
    ),

    validate_unique_key(
        GOLD_TABLES[
            "bridge_planta_embalse"
        ],
        [
            "codigo_planta",
            "codigo_embalse",
        ],
        "Natural key bridge",
    ),
]


dimension_key_validation_df = (
    spark.createDataFrame(
        dimension_key_validations
    )
)


display(
    dimension_key_validation_df
    .select(
        "validacion",
        "tabla",
        "filas",
        "claves_distintas",
        "duplicados",
        "claves_nulas",
        "aprobada",
    )
)


failed_dimension_keys = (
    dimension_key_validation_df
    .filter(
        ~F.col("aprobada")
    )
    .count()
)


print(
    "Validaciones de claves ejecutadas:",
    len(dimension_key_validations),
)

print(
    "Validaciones fallidas:",
    failed_dimension_keys,
)


if failed_dimension_keys > 0:
    raise ValueError(
        "Existen dimensiones o bridges "
        "con claves duplicadas o nulas."
    )


print(
    "Claves de dimensiones aprobadas."
)

# COMMAND ----------

fact_key_validations = [
    validate_unique_key(
        GOLD_TABLES[
            "fact_generacion_real"
        ],
        ["generacion_key"],
        "PK fact_generacion_real",
    ),

    validate_unique_key(
        GOLD_TABLES[
            "fact_disponibilidad_planta"
        ],
        ["disponibilidad_key"],
        "PK fact_disponibilidad_planta",
    ),

    validate_unique_key(
        GOLD_TABLES[
            "fact_demanda_real"
        ],
        ["demanda_key"],
        "PK fact_demanda_real",
    ),

    validate_unique_key(
        GOLD_TABLES[
            "fact_precio_bolsa"
        ],
        ["precio_bolsa_key"],
        "PK fact_precio_bolsa",
    ),

    validate_unique_key(
        GOLD_TABLES[
            "fact_energia_embalsada_planta"
        ],
        ["energia_embalsada_key"],
        "PK fact_energia_embalsada_planta",
    ),
]


fact_key_validation_df = (
    spark.createDataFrame(
        fact_key_validations
    )
)


display(
    fact_key_validation_df
    .select(
        "validacion",
        "tabla",
        "filas",
        "claves_distintas",
        "duplicados",
        "claves_nulas",
        "aprobada",
    )
)


failed_fact_keys = (
    fact_key_validation_df
    .filter(
        ~F.col("aprobada")
    )
    .count()
)


print(
    "Validaciones de hechos ejecutadas:",
    len(fact_key_validations),
)

print(
    "Validaciones fallidas:",
    failed_fact_keys,
)


if failed_fact_keys > 0:
    raise ValueError(
        "Existen hechos con claves "
        "duplicadas o nulas."
    )


print(
    "Claves de hechos aprobadas."
)

# COMMAND ----------

def validate_foreign_key(
    source_table,
    source_key,
    dimension_table,
    dimension_key,
    validation_name,
):
    source_keys_df = (
        spark.table(source_table)
        .select(
            F.col(source_key).alias(
                "source_key"
            )
        )
        .distinct()
    )

    dimension_keys_df = (
        spark.table(dimension_table)
        .select(
            F.col(dimension_key).alias(
                "dimension_key"
            )
        )
        .distinct()
    )

    null_source_keys = (
        source_keys_df
        .filter(
            F.col("source_key").isNull()
        )
        .count()
    )

    orphan_keys_df = (
        source_keys_df
        .filter(
            F.col("source_key").isNotNull()
        )
        .join(
            dimension_keys_df,
            F.col("source_key")
            ==
            F.col("dimension_key"),
            "left_anti",
        )
    )

    orphan_keys = (
        orphan_keys_df.count()
    )

    approved = (
        null_source_keys == 0
        and orphan_keys == 0
    )

    return {
        "validacion": validation_name,
        "tabla_origen": source_table,
        "columna_origen": source_key,
        "tabla_dimension": dimension_table,
        "columna_dimension": dimension_key,
        "claves_nulas": null_source_keys,
        "claves_huerfanas": orphan_keys,
        "aprobada": approved,
    }

# COMMAND ----------

foreign_key_validations = [
    validate_foreign_key(
        GOLD_TABLES[
            "fact_generacion_real"
        ],
        "fecha_key",
        GOLD_TABLES["dim_fecha"],
        "fecha_key",
        "Generación → fecha",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_generacion_real"
        ],
        "periodo_key",
        GOLD_TABLES["dim_periodo"],
        "periodo_key",
        "Generación → periodo",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_generacion_real"
        ],
        "planta_key",
        GOLD_TABLES["dim_planta"],
        "planta_key",
        "Generación → planta",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_generacion_real"
        ],
        "agente_key",
        GOLD_TABLES["dim_agente"],
        "agente_key",
        "Generación → agente",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_disponibilidad_planta"
        ],
        "fecha_key",
        GOLD_TABLES["dim_fecha"],
        "fecha_key",
        "Disponibilidad → fecha",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_disponibilidad_planta"
        ],
        "periodo_key",
        GOLD_TABLES["dim_periodo"],
        "periodo_key",
        "Disponibilidad → periodo",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_disponibilidad_planta"
        ],
        "planta_key",
        GOLD_TABLES["dim_planta"],
        "planta_key",
        "Disponibilidad → planta",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_demanda_real"
        ],
        "fecha_key",
        GOLD_TABLES["dim_fecha"],
        "fecha_key",
        "Demanda → fecha",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_demanda_real"
        ],
        "periodo_key",
        GOLD_TABLES["dim_periodo"],
        "periodo_key",
        "Demanda → periodo",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_demanda_real"
        ],
        "agente_key",
        GOLD_TABLES["dim_agente"],
        "agente_key",
        "Demanda → agente",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_precio_bolsa"
        ],
        "fecha_key",
        GOLD_TABLES["dim_fecha"],
        "fecha_key",
        "Precio → fecha",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_precio_bolsa"
        ],
        "periodo_key",
        GOLD_TABLES["dim_periodo"],
        "periodo_key",
        "Precio → periodo",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_energia_embalsada_planta"
        ],
        "fecha_key",
        GOLD_TABLES["dim_fecha"],
        "fecha_key",
        "Energía embalsada → fecha",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "fact_energia_embalsada_planta"
        ],
        "planta_key",
        GOLD_TABLES["dim_planta"],
        "planta_key",
        "Energía embalsada → planta",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "bridge_planta_embalse"
        ],
        "planta_key",
        GOLD_TABLES["dim_planta"],
        "planta_key",
        "Bridge → planta",
    ),

    validate_foreign_key(
        GOLD_TABLES[
            "bridge_planta_embalse"
        ],
        "embalse_key",
        GOLD_TABLES["dim_embalse"],
        "embalse_key",
        "Bridge → embalse",
    ),
]


foreign_key_validation_df = (
    spark.createDataFrame(
        foreign_key_validations
    )
)


display(
    foreign_key_validation_df
    .select(
        "validacion",
        "claves_nulas",
        "claves_huerfanas",
        "aprobada",
    )
)


failed_foreign_keys = (
    foreign_key_validation_df
    .filter(
        ~F.col("aprobada")
    )
    .count()
)


print(
    "Relaciones evaluadas:",
    len(foreign_key_validations),
)

print(
    "Relaciones fallidas:",
    failed_foreign_keys,
)


if failed_foreign_keys > 0:
    raise ValueError(
        "La capa Gold contiene claves "
        "foráneas nulas o huérfanas."
    )


print(
    "Integridad referencial aprobada."
)

# COMMAND ----------

dim_agent_df = spark.table(
    GOLD_TABLES["dim_agente"]
)


agents_with_invalid_current_state = (
    dim_agent_df
    .groupBy("codigo_agente")
    .agg(
        F.sum(
            F.when(
                F.col("es_actual"),
                1,
            ).otherwise(0)
        ).alias(
            "versiones_actuales"
        )
    )
    .filter(
        F.col("versiones_actuales") != 1
    )
    .count()
)


invalid_agent_ranges = (
    dim_agent_df
    .filter(
        F.col("fecha_fin")
        <
        F.col("fecha_inicio")
    )
    .count()
)


overlapping_agent_versions = (
    dim_agent_df.alias("a")
    .join(
        dim_agent_df.alias("b"),
        (
            F.col("a.codigo_agente")
            ==
            F.col("b.codigo_agente")
        )
        & (
            F.col("a.agente_key")
            <
            F.col("b.agente_key")
        )
        & (
            F.col("a.fecha_inicio")
            <=
            F.col("b.fecha_fin")
        )
        & (
            F.col("b.fecha_inicio")
            <=
            F.col("a.fecha_fin")
        ),
        "inner",
    )
    .count()
)


current_versions_not_open = (
    dim_agent_df
    .filter(
        F.col("es_actual")
        &
        (
            F.col("fecha_fin")
            != F.to_date(
                F.lit("9999-12-31")
            )
        )
    )
    .count()
)


historical_versions_open = (
    dim_agent_df
    .filter(
        (~F.col("es_actual"))
        &
        (
            F.col("fecha_fin")
            ==
            F.to_date(
                F.lit("9999-12-31")
            )
        )
    )
    .count()
)


print(
    "Agentes con estado actual inválido:",
    agents_with_invalid_current_state,
)

print(
    "Rangos inválidos:",
    invalid_agent_ranges,
)

print(
    "Solapamientos:",
    overlapping_agent_versions,
)

print(
    "Versiones actuales sin fecha abierta:",
    current_versions_not_open,
)

print(
    "Versiones históricas con fecha abierta:",
    historical_versions_open,
)


if (
    agents_with_invalid_current_state > 0
    or invalid_agent_ranges > 0
    or overlapping_agent_versions > 0
    or current_versions_not_open > 0
    or historical_versions_open > 0
):
    raise ValueError(
        "dim_agente no cumple las reglas "
        "SCD Tipo 2."
    )


print(
    "SCD Tipo 2 de agentes aprobada."
)

# COMMAND ----------

dim_plant_df = spark.table(
    GOLD_TABLES["dim_planta"]
)


invalid_inferred_official_plants = (
    dim_plant_df
    .filter(
        F.col("es_registro_inferido")
        &
        F.col("esta_en_maestro_actual")
    )
    .count()
)


official_plants_marked_inferred = (
    dim_plant_df
    .filter(
        F.col("esta_en_maestro_actual")
        &
        F.col("es_registro_inferido")
    )
    .count()
)


inferred_without_observation_dates = (
    dim_plant_df
    .filter(
        F.col("es_registro_inferido")
        &
        (
            F.col(
                "fecha_primera_observacion"
            ).isNull()
            | F.col(
                "fecha_ultima_observacion"
            ).isNull()
        )
    )
    .count()
)


invalid_plant_observation_ranges = (
    dim_plant_df
    .filter(
        F.col("fecha_ultima_observacion")
        <
        F.col("fecha_primera_observacion")
    )
    .count()
)


print(
    "Inferidas y oficiales simultáneamente:",
    invalid_inferred_official_plants,
)

print(
    "Oficiales marcadas como inferidas:",
    official_plants_marked_inferred,
)

print(
    "Inferidas sin fechas de observación:",
    inferred_without_observation_dates,
)

print(
    "Rangos de observación inválidos:",
    invalid_plant_observation_ranges,
)


if (
    invalid_inferred_official_plants > 0
    or official_plants_marked_inferred > 0
    or inferred_without_observation_dates > 0
    or invalid_plant_observation_ranges > 0
):
    raise ValueError(
        "dim_planta contiene estados "
        "de gobierno inconsistentes."
    )


print(
    "Gobierno de dim_planta aprobado."
)

# COMMAND ----------

structural_summary = [
    (
        "Existencia de tablas",
        missing_tables == 0,
        missing_tables,
    ),

    (
        "Claves dimensiones",
        failed_dimension_keys == 0,
        failed_dimension_keys,
    ),

    (
        "Claves hechos",
        failed_fact_keys == 0,
        failed_fact_keys,
    ),

    (
        "Integridad referencial",
        failed_foreign_keys == 0,
        failed_foreign_keys,
    ),

    (
        "SCD2 agentes",
        (
            agents_with_invalid_current_state == 0
            and invalid_agent_ranges == 0
            and overlapping_agent_versions == 0
            and current_versions_not_open == 0
            and historical_versions_open == 0
        ),
        (
            agents_with_invalid_current_state
            + invalid_agent_ranges
            + overlapping_agent_versions
            + current_versions_not_open
            + historical_versions_open
        ),
    ),

    (
        "Gobierno de plantas",
        (
            invalid_inferred_official_plants == 0
            and inferred_without_observation_dates == 0
            and invalid_plant_observation_ranges == 0
        ),
        (
            invalid_inferred_official_plants
            + inferred_without_observation_dates
            + invalid_plant_observation_ranges
        ),
    ),
]


structural_summary_df = (
    spark.createDataFrame(
        structural_summary,
        [
            "componente",
            "aprobado",
            "errores",
        ],
    )
)


display(
    structural_summary_df
)


structural_failures = (
    structural_summary_df
    .filter(
        ~F.col("aprobado")
    )
    .count()
)


if structural_failures > 0:
    raise ValueError(
        "La validación estructural integral "
        "de Gold no fue aprobada."
    )


print(
    "VALIDACIÓN ESTRUCTURAL GOLD APROBADA."
)

# COMMAND ----------

def add_tx_priority(
    source_df,
    version_column="version",
):
    return (
        source_df
        .withColumn(
            "_numero_tx",
            F.when(
                F.col(version_column).rlike(
                    r"^TX[0-9]+$"
                ),
                F.regexp_extract(
                    F.col(version_column),
                    r"^TX([0-9]+)$",
                    1,
                ).cast("int"),
            ),
        )
        .withColumn(
            "_prioridad_version",
            F.when(
                F.col(version_column) == "TXF",
                F.lit(10000),
            )
            .when(
                F.col(version_column) == "TXR",
                F.lit(9000),
            )
            .when(
                F.col("_numero_tx").isNotNull(),
                F.col("_numero_tx") * 100,
            )
            .otherwise(
                F.lit(0)
            ),
        )
        .drop("_numero_tx")
    )

# COMMAND ----------

def add_tx_priority(
    source_df,
    version_column="version",
):
    return (
        source_df
        .withColumn(
            "_numero_tx",
            F.when(
                F.col(version_column).rlike(
                    r"^TX[0-9]+$"
                ),
                F.regexp_extract(
                    F.col(version_column),
                    r"^TX([0-9]+)$",
                    1,
                ).cast("int"),
            ),
        )
        .withColumn(
            "_prioridad_version",
            F.when(
                F.col(version_column) == "TXF",
                F.lit(10000),
            )
            .when(
                F.col(version_column) == "TXR",
                F.lit(9000),
            )
            .when(
                F.col("_numero_tx").isNotNull(),
                F.col("_numero_tx") * 100,
            )
            .otherwise(
                F.lit(0)
            ),
        )
        .drop("_numero_tx")
    )

# COMMAND ----------

def detect_first_existing_column(
    dataframe,
    candidates,
    logical_name,
):
    available_columns = set(
        dataframe.columns
    )

    selected_column = next(
        (
            candidate
            for candidate in candidates
            if candidate in available_columns
        ),
        None,
    )

    if selected_column is None:
        raise ValueError(
            f"No se encontró la columna para "
            f"{logical_name}. "
            f"Columnas disponibles: "
            f"{sorted(available_columns)}"
        )

    return selected_column

silver_generation_raw_df = spark.table(
    SILVER_TABLES["generacion_real"]
)


generation_value_column = (
    detect_first_existing_column(
        silver_generation_raw_df,
        [
            "generacion_real_kwh",
            "generacion_kwh",
            "valor_generacion",
            "valor",
        ],
        "generación real",
    )
)


generation_silver_prepared_df = (
    silver_generation_raw_df
    .select(
        F.to_timestamp(
            "fecha_hora"
        ).alias("fecha_hora"),

        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.upper(
            F.trim("codigo_agente")
        ).alias("codigo_agente"),

        F.upper(
            F.trim("codigo_variable")
        ).alias("codigo_variable"),

        F.upper(
            F.trim("codigo_duracion")
        ).alias("codigo_duracion"),

        F.upper(
            F.trim("unidad_medida")
        ).alias("unidad_medida"),

        F.upper(
            F.trim("version")
        ).alias("version"),

        F.col(generation_value_column)
        .cast("decimal(24,6)")
        .alias("valor"),

        (
            F.col("silver_updated_at")
            if "silver_updated_at"
            in silver_generation_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp"
            in silver_generation_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date"
            in silver_generation_raw_df.columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        (F.col("codigo_variable") == "GREAL")
        & (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)


generation_prioritized_validation_df = (
    add_tx_priority(
        generation_silver_prepared_df
    )
)


generation_validation_window = (
    Window
    .partitionBy(
        "fecha_hora",
        "codigo_planta",
        "codigo_agente",
        "codigo_variable",
        "codigo_duracion",
        "unidad_medida",
    )
    .orderBy(
        F.col(
            "_prioridad_version"
        ).desc(),

        F.col(
            "silver_updated_at"
        ).desc_nulls_last(),

        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col("version").desc(),
    )
)


generation_expected_df = (
    generation_prioritized_validation_df
    .withColumn(
        "_rn",
        F.row_number().over(
            generation_validation_window
        ),
    )
    .filter(
        F.col("_rn") == 1
    )
    .select(
        "fecha_hora",
        "codigo_planta",
        "codigo_agente",

        F.col("valor").alias(
            "valor_silver"
        ),

        F.col("version").alias(
            "version_silver"
        ),

        F.col(
            "_prioridad_version"
        ).alias(
            "prioridad_silver"
        ),
    )
)


generation_gold_comparable_df = (
    spark.table(
        GOLD_TABLES[
            "fact_generacion_real"
        ]
    )
    .alias("fact")
    .join(
        spark.table(
            GOLD_TABLES["dim_planta"]
        )
        .select(
            "planta_key",
            "codigo_planta",
        )
        .alias("plant"),
        "planta_key",
        "inner",
    )
    .join(
        spark.table(
            GOLD_TABLES["dim_agente"]
        )
        .select(
            "agente_key",
            "codigo_agente",
        )
        .alias("agent"),
        "agente_key",
        "inner",
    )
    .select(
        F.col("fact.fecha_hora"),
        F.col("plant.codigo_planta"),
        F.col("agent.codigo_agente"),

        F.col(
            "fact.generacion_real_kwh"
        ).alias(
            "valor_gold"
        ),

        F.col(
            "fact.version_seleccionada"
        ).alias(
            "version_gold"
        ),

        F.col(
            "fact.prioridad_version"
        ).alias(
            "prioridad_gold"
        ),
    )
)

generation_reconciliation_df = (
    generation_expected_df.alias("silver")
    .join(
        generation_gold_comparable_df.alias("gold"),
        [
            "fecha_hora",
            "codigo_planta",
            "codigo_agente",
        ],
        "full",
    )
    .withColumn(
        "estado",
        F.when(
            F.col("silver.valor_silver").isNull(),
            F.lit("SOLO_GOLD"),
        )
        .when(
            F.col("gold.valor_gold").isNull(),
            F.lit("SOLO_SILVER"),
        )
        .when(
            ~(
                F.col("silver.valor_silver").eqNullSafe(
                    F.col("gold.valor_gold")
                )
            ),
            F.lit("VALOR_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.version_silver").eqNullSafe(
                    F.col("gold.version_gold")
                )
            ),
            F.lit("VERSION_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.prioridad_silver").eqNullSafe(
                    F.col("gold.prioridad_gold")
                )
            ),
            F.lit("PRIORIDAD_DIFERENTE"),
        )
        .otherwise(
            F.lit("OK")
        ),
    )
)


generation_reconciliation_summary_df = (
    generation_reconciliation_df
    .groupBy("estado")
    .count()
    .orderBy("estado")
)


display(
    generation_reconciliation_summary_df
)


generation_reconciliation_errors = (
    generation_reconciliation_df
    .filter(
        F.col("estado") != "OK"
    )
    .count()
)


print(
    "Errores reconciliación generación:",
    generation_reconciliation_errors,
)


if generation_reconciliation_errors > 0:
    display(
        generation_reconciliation_df
        .filter(
            F.col("estado") != "OK"
        )
        .limit(100)
    )

    raise ValueError(
        "La reconciliación Silver–Gold "
        "de generación falló."
    )


print(
    "Reconciliación de generación aprobada."
)

# COMMAND ----------

silver_availability_raw_df = spark.table(
    SILVER_TABLES[
        "disponibilidad_plantas"
    ]
)


availability_value_column = (
    detect_first_existing_column(
        silver_availability_raw_df,
        [
            "disponibilidad_real_kwh",
            "disponibilidad_kwh",
            "valor_disponibilidad",
            "valor",
        ],
        "disponibilidad real",
    )
)


availability_silver_prepared_df = (
    silver_availability_raw_df
    .select(
        F.to_timestamp(
            "fecha_hora"
        ).alias("fecha_hora"),

        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.upper(
            F.trim("codigo_variable")
        ).alias("codigo_variable"),

        F.upper(
            F.trim("codigo_duracion")
        ).alias("codigo_duracion"),

        F.upper(
            F.trim("unidad_medida")
        ).alias("unidad_medida"),

        F.upper(
            F.trim("version")
        ).alias("version"),

        F.col(availability_value_column)
        .cast("decimal(24,6)")
        .alias("valor"),

        (
            F.col("silver_updated_at")
            if "silver_updated_at"
            in silver_availability_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp"
            in silver_availability_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date"
            in silver_availability_raw_df.columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        (F.col("codigo_variable") == "DISPREAL")
        & (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)


availability_prioritized_validation_df = (
    add_tx_priority(
        availability_silver_prepared_df
    )
)


availability_validation_window = (
    Window
    .partitionBy(
        "fecha_hora",
        "codigo_planta",
        "codigo_variable",
        "codigo_duracion",
        "unidad_medida",
    )
    .orderBy(
        F.col(
            "_prioridad_version"
        ).desc(),

        F.col(
            "silver_updated_at"
        ).desc_nulls_last(),

        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col("version").desc(),
    )
)


availability_expected_df = (
    availability_prioritized_validation_df
    .withColumn(
        "_rn",
        F.row_number().over(
            availability_validation_window
        ),
    )
    .filter(
        F.col("_rn") == 1
    )
    .select(
        "fecha_hora",
        "codigo_planta",

        F.col("valor").alias(
            "valor_silver"
        ),

        F.col("version").alias(
            "version_silver"
        ),

        F.col(
            "_prioridad_version"
        ).alias(
            "prioridad_silver"
        ),
    )
)


availability_gold_comparable_df = (
    spark.table(
        GOLD_TABLES[
            "fact_disponibilidad_planta"
        ]
    )
    .alias("fact")
    .join(
        spark.table(
            GOLD_TABLES["dim_planta"]
        )
        .select(
            "planta_key",
            "codigo_planta",
        )
        .alias("plant"),
        "planta_key",
        "inner",
    )
    .select(
        F.col("fact.fecha_hora"),
        F.col("plant.codigo_planta"),

        F.col(
            "fact.disponibilidad_real_kwh"
        ).alias(
            "valor_gold"
        ),

        F.col(
            "fact.version_seleccionada"
        ).alias(
            "version_gold"
        ),

        F.col(
            "fact.prioridad_version"
        ).alias(
            "prioridad_gold"
        ),
    )
)


availability_reconciliation_df = (
    availability_expected_df.alias("silver")
    .join(
        availability_gold_comparable_df.alias("gold"),
        [
            "fecha_hora",
            "codigo_planta",
        ],
        "full",
    )
    .withColumn(
        "estado",
        F.when(
            F.col("silver.valor_silver").isNull(),
            F.lit("SOLO_GOLD"),
        )
        .when(
            F.col("gold.valor_gold").isNull(),
            F.lit("SOLO_SILVER"),
        )
        .when(
            ~(
                F.col("silver.valor_silver").eqNullSafe(
                    F.col("gold.valor_gold")
                )
            ),
            F.lit("VALOR_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.version_silver").eqNullSafe(
                    F.col("gold.version_gold")
                )
            ),
            F.lit("VERSION_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.prioridad_silver").eqNullSafe(
                    F.col("gold.prioridad_gold")
                )
            ),
            F.lit("PRIORIDAD_DIFERENTE"),
        )
        .otherwise(
            F.lit("OK")
        ),
    )
)


display(
    availability_reconciliation_df
    .groupBy("estado")
    .count()
    .orderBy("estado")
)


availability_reconciliation_errors = (
    availability_reconciliation_df
    .filter(
        F.col("estado") != "OK"
    )
    .count()
)


print(
    "Errores reconciliación disponibilidad:",
    availability_reconciliation_errors,
)


if availability_reconciliation_errors > 0:
    display(
        availability_reconciliation_df
        .filter(
            F.col("estado") != "OK"
        )
        .limit(100)
    )

    raise ValueError(
        "La reconciliación Silver–Gold "
        "de disponibilidad falló."
    )


print(
    "Reconciliación de disponibilidad aprobada."
)

# COMMAND ----------

reconciliation_partial_summary = [
    (
        "Generación",
        generation_reconciliation_errors,
        generation_reconciliation_errors == 0,
    ),
    (
        "Disponibilidad",
        availability_reconciliation_errors,
        availability_reconciliation_errors == 0,
    ),
]


reconciliation_partial_summary_df = (
    spark.createDataFrame(
        reconciliation_partial_summary,
        [
            "componente",
            "errores",
            "aprobado",
        ],
    )
)


display(
    reconciliation_partial_summary_df
)


partial_reconciliation_failures = (
    reconciliation_partial_summary_df
    .filter(
        ~F.col("aprobado")
    )
    .count()
)


if partial_reconciliation_failures > 0:
    raise ValueError(
        "La reconciliación parcial "
        "Silver–Gold falló."
    )


print(
    "RECONCILIACIÓN PARCIAL APROBADA."
)

# COMMAND ----------

silver_demand_raw_df = spark.table(
    SILVER_TABLES["demanda_real"]
)


demand_value_column = (
    detect_first_existing_column(
        silver_demand_raw_df,
        [
            "demanda_real_kwh",
            "demanda_kwh",
            "valor_demanda",
            "valor",
        ],
        "demanda real",
    )
)


demand_silver_prepared_df = (
    silver_demand_raw_df
    .select(
        F.to_timestamp(
            "fecha_hora"
        ).alias("fecha_hora"),

        F.upper(
            F.trim("codigo_agente")
        ).alias("codigo_agente"),

        F.upper(
            F.trim("tipo_mercado")
        ).alias("tipo_mercado"),

        F.upper(
            F.trim("codigo_variable")
        ).alias("codigo_variable"),

        F.upper(
            F.trim("codigo_duracion")
        ).alias("codigo_duracion"),

        F.upper(
            F.trim("unidad_medida")
        ).alias("unidad_medida"),

        F.upper(
            F.trim("version")
        ).alias("version"),

        F.col(demand_value_column)
        .cast("decimal(24,6)")
        .alias("valor"),

        (
            F.col("silver_updated_at")
            if "silver_updated_at"
            in silver_demand_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp"
            in silver_demand_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date"
            in silver_demand_raw_df.columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)


demand_prioritized_validation_df = (
    add_tx_priority(
        demand_silver_prepared_df
    )
)


demand_validation_window = (
    Window
    .partitionBy(
        "fecha_hora",
        "codigo_agente",
        "tipo_mercado",
        "codigo_variable",
        "codigo_duracion",
        "unidad_medida",
    )
    .orderBy(
        F.col(
            "_prioridad_version"
        ).desc(),

        F.col(
            "silver_updated_at"
        ).desc_nulls_last(),

        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col("version").desc(),
    )
)


demand_expected_df = (
    demand_prioritized_validation_df
    .withColumn(
        "_rn",
        F.row_number().over(
            demand_validation_window
        ),
    )
    .filter(
        F.col("_rn") == 1
    )
    .select(
        "fecha_hora",
        "codigo_agente",
        "tipo_mercado",

        F.col("valor").alias(
            "valor_silver"
        ),

        F.col("version").alias(
            "version_silver"
        ),

        F.col(
            "_prioridad_version"
        ).alias(
            "prioridad_silver"
        ),
    )
)

demand_gold_comparable_df = (
    spark.table(
        GOLD_TABLES[
            "fact_demanda_real"
        ]
    )
    .alias("fact")
    .join(
        spark.table(
            GOLD_TABLES["dim_agente"]
        )
        .select(
            "agente_key",
            "codigo_agente",
        )
        .alias("agent"),
        "agente_key",
        "inner",
    )
    .select(
        F.col("fact.fecha_hora"),
        F.col("agent.codigo_agente"),
        F.col("fact.tipo_mercado"),

        F.col(
            "fact.demanda_real_kwh"
        ).alias(
            "valor_gold"
        ),

        F.col(
            "fact.version_seleccionada"
        ).alias(
            "version_gold"
        ),

        F.col(
            "fact.prioridad_version"
        ).alias(
            "prioridad_gold"
        ),
    )
)

demand_reconciliation_df = (
    demand_expected_df.alias("silver")
    .join(
        demand_gold_comparable_df.alias("gold"),
        [
            "fecha_hora",
            "codigo_agente",
            "tipo_mercado",
        ],
        "full",
    )
    .withColumn(
        "estado",
        F.when(
            F.col("silver.valor_silver").isNull(),
            F.lit("SOLO_GOLD"),
        )
        .when(
            F.col("gold.valor_gold").isNull(),
            F.lit("SOLO_SILVER"),
        )
        .when(
            ~(
                F.col("silver.valor_silver").eqNullSafe(
                    F.col("gold.valor_gold")
                )
            ),
            F.lit("VALOR_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.version_silver").eqNullSafe(
                    F.col("gold.version_gold")
                )
            ),
            F.lit("VERSION_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.prioridad_silver").eqNullSafe(
                    F.col("gold.prioridad_gold")
                )
            ),
            F.lit("PRIORIDAD_DIFERENTE"),
        )
        .otherwise(
            F.lit("OK")
        ),
    )
)


display(
    demand_reconciliation_df
    .groupBy("estado")
    .count()
    .orderBy("estado")
)


demand_reconciliation_errors = (
    demand_reconciliation_df
    .filter(
        F.col("estado") != "OK"
    )
    .count()
)


print(
    "Errores reconciliación demanda:",
    demand_reconciliation_errors,
)


if demand_reconciliation_errors > 0:
    display(
        demand_reconciliation_df
        .filter(
            F.col("estado") != "OK"
        )
        .limit(100)
    )

    raise ValueError(
        "La reconciliación Silver–Gold "
        "de demanda falló."
    )


print(
    "Reconciliación de demanda aprobada."
)

# COMMAND ----------

silver_price_raw_df = spark.table(
    SILVER_TABLES["precio_bolsa"]
)


price_value_column = (
    detect_first_existing_column(
        silver_price_raw_df,
        [
            "precio_bolsa",
            "precio_kwh",
            "valor_precio",
            "precio",
            "valor",
        ],
        "precio de bolsa",
    )
)


price_silver_prepared_df = (
    silver_price_raw_df
    .select(
        F.to_timestamp(
            "fecha_hora"
        ).alias("fecha_hora"),

        F.upper(
            F.trim("codigo_variable")
        ).alias("codigo_variable"),

        F.upper(
            F.trim("codigo_duracion")
        ).alias("codigo_duracion"),

        F.upper(
            F.trim("unidad_medida")
        ).alias("unidad_medida"),

        F.upper(
            F.trim("version")
        ).alias("version"),

        F.col(price_value_column)
        .cast("decimal(24,6)")
        .alias("valor"),

        (
            F.col("silver_updated_at")
            if "silver_updated_at"
            in silver_price_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp"
            in silver_price_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date"
            in silver_price_raw_df.columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        F.col("codigo_variable").isin(
            "PB_INT",
            "PB_NAL",
            "PB_TIE",
        )
        & (
            F.col("codigo_duracion")
            == "PT1H"
        )
    )
)


price_prioritized_validation_df = (
    add_tx_priority(
        price_silver_prepared_df
    )
)


price_validation_window = (
    Window
    .partitionBy(
        "fecha_hora",
        "codigo_variable",
        "codigo_duracion",
        "unidad_medida",
    )
    .orderBy(
        F.col(
            "_prioridad_version"
        ).desc(),

        F.col(
            "silver_updated_at"
        ).desc_nulls_last(),

        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col("version").desc(),
    )
)


price_expected_long_df = (
    price_prioritized_validation_df
    .withColumn(
        "_rn",
        F.row_number().over(
            price_validation_window
        ),
    )
    .filter(
        F.col("_rn") == 1
    )
)


price_expected_df = (
    price_expected_long_df
    .groupBy("fecha_hora")
    .agg(
        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_INT",
                F.col("valor"),
            )
        ).alias(
            "pb_int_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_NAL",
                F.col("valor"),
            )
        ).alias(
            "pb_nal_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_TIE",
                F.col("valor"),
            )
        ).alias(
            "pb_tie_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_INT",
                F.col("version"),
            )
        ).alias(
            "version_pb_int_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_NAL",
                F.col("version"),
            )
        ).alias(
            "version_pb_nal_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_TIE",
                F.col("version"),
            )
        ).alias(
            "version_pb_tie_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_INT",
                F.col("_prioridad_version"),
            )
        ).alias(
            "prioridad_pb_int_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_NAL",
                F.col("_prioridad_version"),
            )
        ).alias(
            "prioridad_pb_nal_silver"
        ),

        F.max(
            F.when(
                F.col("codigo_variable")
                == "PB_TIE",
                F.col("_prioridad_version"),
            )
        ).alias(
            "prioridad_pb_tie_silver"
        ),
    )
)

price_gold_comparable_df = (
    spark.table(
        GOLD_TABLES[
            "fact_precio_bolsa"
        ]
    )
    .select(
        "fecha_hora",

        F.col(
            "precio_bolsa_internacional_cop_kwh"
        ).alias(
            "pb_int_gold"
        ),

        F.col(
            "precio_bolsa_nacional_cop_kwh"
        ).alias(
            "pb_nal_gold"
        ),

        F.col(
            "precio_bolsa_tie_cop_kwh"
        ).alias(
            "pb_tie_gold"
        ),

        F.col("version_pb_int").alias(
            "version_pb_int_gold"
        ),

        F.col("version_pb_nal").alias(
            "version_pb_nal_gold"
        ),

        F.col("version_pb_tie").alias(
            "version_pb_tie_gold"
        ),

        F.col("prioridad_pb_int").alias(
            "prioridad_pb_int_gold"
        ),

        F.col("prioridad_pb_nal").alias(
            "prioridad_pb_nal_gold"
        ),

        F.col("prioridad_pb_tie").alias(
            "prioridad_pb_tie_gold"
        ),
    )
)

price_reconciliation_df = (
    price_expected_df.alias("silver")
    .join(
        price_gold_comparable_df.alias("gold"),
        ["fecha_hora"],
        "full",
    )
    .withColumn(
        "estado",
        F.when(
            F.col("silver.pb_int_silver").isNull()
            & F.col("silver.pb_nal_silver").isNull()
            & F.col("silver.pb_tie_silver").isNull(),
            F.lit("SOLO_GOLD"),
        )
        .when(
            F.col("gold.pb_int_gold").isNull()
            & F.col("gold.pb_nal_gold").isNull()
            & F.col("gold.pb_tie_gold").isNull(),
            F.lit("SOLO_SILVER"),
        )
        .when(
            ~(
                F.col("silver.pb_int_silver").eqNullSafe(
                    F.col("gold.pb_int_gold")
                )
            )
            |
            ~(
                F.col("silver.pb_nal_silver").eqNullSafe(
                    F.col("gold.pb_nal_gold")
                )
            )
            |
            ~(
                F.col("silver.pb_tie_silver").eqNullSafe(
                    F.col("gold.pb_tie_gold")
                )
            ),
            F.lit("VALOR_DIFERENTE"),
        )
        .when(
            ~(
                F.col(
                    "silver.version_pb_int_silver"
                ).eqNullSafe(
                    F.col(
                        "gold.version_pb_int_gold"
                    )
                )
            )
            |
            ~(
                F.col(
                    "silver.version_pb_nal_silver"
                ).eqNullSafe(
                    F.col(
                        "gold.version_pb_nal_gold"
                    )
                )
            )
            |
            ~(
                F.col(
                    "silver.version_pb_tie_silver"
                ).eqNullSafe(
                    F.col(
                        "gold.version_pb_tie_gold"
                    )
                )
            ),
            F.lit("VERSION_DIFERENTE"),
        )
        .when(
            ~(
                F.col(
                    "silver.prioridad_pb_int_silver"
                ).eqNullSafe(
                    F.col(
                        "gold.prioridad_pb_int_gold"
                    )
                )
            )
            |
            ~(
                F.col(
                    "silver.prioridad_pb_nal_silver"
                ).eqNullSafe(
                    F.col(
                        "gold.prioridad_pb_nal_gold"
                    )
                )
            )
            |
            ~(
                F.col(
                    "silver.prioridad_pb_tie_silver"
                ).eqNullSafe(
                    F.col(
                        "gold.prioridad_pb_tie_gold"
                    )
                )
            ),
            F.lit("PRIORIDAD_DIFERENTE"),
        )
        .otherwise(
            F.lit("OK")
        ),
    )
)


display(
    price_reconciliation_df
    .groupBy("estado")
    .count()
    .orderBy("estado")
)


price_reconciliation_errors = (
    price_reconciliation_df
    .filter(
        F.col("estado") != "OK"
    )
    .count()
)


print(
    "Errores reconciliación precio:",
    price_reconciliation_errors,
)


if price_reconciliation_errors > 0:
    display(
        price_reconciliation_df
        .filter(
            F.col("estado") != "OK"
        )
        .limit(100)
    )

    raise ValueError(
        "La reconciliación Silver–Gold "
        "de precio de bolsa falló."
    )


print(
    "Reconciliación de precio aprobada."
)

# COMMAND ----------

silver_reservoir_raw_df = spark.table(
    SILVER_TABLES[
        "niveles_embalses"
    ]
)


reservoir_value_column = (
    detect_first_existing_column(
        silver_reservoir_raw_df,
        [
            "energia_embalsada_kwh",
            "energia_embalsada",
            "nivel_embalse_kwh",
            "valor_nem",
            "valor",
        ],
        "energía embalsada",
    )
)


reservoir_silver_prepared_df = (
    silver_reservoir_raw_df
    .select(
        F.to_date(
            "fecha_inicio"
        ).alias(
            "fecha_medicion"
        ),

        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.upper(
            F.trim("codigo_variable")
        ).alias("codigo_variable"),

        F.upper(
            F.trim("codigo_duracion")
        ).alias("codigo_duracion"),

        F.upper(
            F.trim("unidad_medida")
        ).alias("unidad_medida"),

        F.upper(
            F.trim("version")
        ).alias("version"),

        F.col(reservoir_value_column)
        .cast("decimal(24,6)")
        .alias("valor"),

        (
            F.col("silver_updated_at")
            if "silver_updated_at"
            in silver_reservoir_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp"
            in silver_reservoir_raw_df.columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date"
            in silver_reservoir_raw_df.columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        (F.col("codigo_variable") == "NEM")
        & (F.col("codigo_duracion") == "P1D")
        & (F.col("unidad_medida") == "KWH")
    )
)


reservoir_prioritized_validation_df = (
    add_tx_priority(
        reservoir_silver_prepared_df
    )
)


reservoir_validation_window = (
    Window
    .partitionBy(
        "fecha_medicion",
        "codigo_planta",
        "codigo_variable",
        "codigo_duracion",
        "unidad_medida",
    )
    .orderBy(
        F.col(
            "_prioridad_version"
        ).desc(),

        F.col(
            "silver_updated_at"
        ).desc_nulls_last(),

        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col("version").desc(),
    )
)


reservoir_expected_df = (
    reservoir_prioritized_validation_df
    .withColumn(
        "_rn",
        F.row_number().over(
            reservoir_validation_window
        ),
    )
    .filter(
        F.col("_rn") == 1
    )
    .select(
        "fecha_medicion",
        "codigo_planta",

        F.col("valor").alias(
            "valor_silver"
        ),

        F.col("version").alias(
            "version_silver"
        ),

        F.col(
            "_prioridad_version"
        ).alias(
            "prioridad_silver"
        ),
    )
)

reservoir_gold_comparable_df = (
    spark.table(
        GOLD_TABLES[
            "fact_energia_embalsada_planta"
        ]
    )
    .alias("fact")
    .join(
        spark.table(
            GOLD_TABLES["dim_planta"]
        )
        .select(
            "planta_key",
            "codigo_planta",
        )
        .alias("plant"),
        "planta_key",
        "inner",
    )
    .select(
        F.col("fact.fecha_medicion"),
        F.col("plant.codigo_planta"),

        F.col(
            "fact.energia_embalsada_kwh"
        ).alias(
            "valor_gold"
        ),

        F.col(
            "fact.version_seleccionada"
        ).alias(
            "version_gold"
        ),

        F.col(
            "fact.prioridad_version"
        ).alias(
            "prioridad_gold"
        ),
    )
)

reservoir_reconciliation_df = (
    reservoir_expected_df.alias("silver")
    .join(
        reservoir_gold_comparable_df.alias("gold"),
        [
            "fecha_medicion",
            "codigo_planta",
        ],
        "full",
    )
    .withColumn(
        "estado",
        F.when(
            F.col("silver.valor_silver").isNull(),
            F.lit("SOLO_GOLD"),
        )
        .when(
            F.col("gold.valor_gold").isNull(),
            F.lit("SOLO_SILVER"),
        )
        .when(
            ~(
                F.col("silver.valor_silver").eqNullSafe(
                    F.col("gold.valor_gold")
                )
            ),
            F.lit("VALOR_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.version_silver").eqNullSafe(
                    F.col("gold.version_gold")
                )
            ),
            F.lit("VERSION_DIFERENTE"),
        )
        .when(
            ~(
                F.col("silver.prioridad_silver").eqNullSafe(
                    F.col("gold.prioridad_gold")
                )
            ),
            F.lit("PRIORIDAD_DIFERENTE"),
        )
        .otherwise(
            F.lit("OK")
        ),
    )
)


display(
    reservoir_reconciliation_df
    .groupBy("estado")
    .count()
    .orderBy("estado")
)


reservoir_reconciliation_errors = (
    reservoir_reconciliation_df
    .filter(
        F.col("estado") != "OK"
    )
    .count()
)


print(
    "Errores reconciliación energía embalsada:",
    reservoir_reconciliation_errors,
)


if reservoir_reconciliation_errors > 0:
    display(
        reservoir_reconciliation_df
        .filter(
            F.col("estado") != "OK"
        )
        .limit(100)
    )

    raise ValueError(
        "La reconciliación Silver–Gold "
        "de energía embalsada falló."
    )


print(
    "Reconciliación de energía embalsada aprobada."
)

# COMMAND ----------

reconciliation_summary = [
    (
        "Generación",
        generation_reconciliation_errors,
        generation_reconciliation_errors == 0,
    ),

    (
        "Disponibilidad",
        availability_reconciliation_errors,
        availability_reconciliation_errors == 0,
    ),

    (
        "Demanda",
        demand_reconciliation_errors,
        demand_reconciliation_errors == 0,
    ),

    (
        "Precio de bolsa",
        price_reconciliation_errors,
        price_reconciliation_errors == 0,
    ),

    (
        "Energía embalsada",
        reservoir_reconciliation_errors,
        reservoir_reconciliation_errors == 0,
    ),
]


reconciliation_summary_df = (
    spark.createDataFrame(
        reconciliation_summary,
        [
            "componente",
            "errores",
            "aprobado",
        ],
    )
)


display(
    reconciliation_summary_df
)


reconciliation_failures = (
    reconciliation_summary_df
    .filter(
        ~F.col("aprobado")
    )
    .count()
)


if reconciliation_failures > 0:
    raise ValueError(
        "La reconciliación integral "
        "Silver–Gold falló."
    )


print(
    "RECONCILIACIÓN INTEGRAL SILVER–GOLD APROBADA."
)

# COMMAND ----------

def build_gold_fingerprint():
    results = []

    configurations = [
        {
            "table": "fact_generacion_real",
            "key": "generacion_key",
            "date": "fecha_hora",
            "measure": "generacion_real_kwh",
        },
        {
            "table": "fact_disponibilidad_planta",
            "key": "disponibilidad_key",
            "date": "fecha_hora",
            "measure": "disponibilidad_real_kwh",
        },
        {
            "table": "fact_demanda_real",
            "key": "demanda_key",
            "date": "fecha_hora",
            "measure": "demanda_real_kwh",
        },
        {
            "table": "fact_precio_bolsa",
            "key": "precio_bolsa_key",
            "date": "fecha_hora",
            "measure": "precio_bolsa_nacional_cop_kwh",
        },
        {
            "table": "fact_energia_embalsada_planta",
            "key": "energia_embalsada_key",
            "date": "fecha_medicion",
            "measure": "energia_embalsada_kwh",
        },
    ]

    for configuration in configurations:
        logical_name = configuration["table"]
        physical_name = GOLD_TABLES[logical_name]

        metrics = (
            spark.table(physical_name)
            .agg(
                F.count("*").alias("filas"),

                F.countDistinct(
                    configuration["key"]
                ).alias("claves_distintas"),

                F.min(
                    configuration["date"]
                ).cast("string").alias(
                    "fecha_minima"
                ),

                F.max(
                    configuration["date"]
                ).cast("string").alias(
                    "fecha_maxima"
                ),

                F.sum(
                    configuration["measure"]
                ).cast("decimal(38,6)").alias(
                    "total_medida"
                ),
            )
            .first()
        )

        results.append(
            (
                logical_name,
                metrics["filas"],
                metrics["claves_distintas"],
                metrics["fecha_minima"],
                metrics["fecha_maxima"],
                metrics["total_medida"],
            )
        )

    return spark.createDataFrame(
        results,
        [
            "tabla",
            "filas",
            "claves_distintas",
            "fecha_minima",
            "fecha_maxima",
            "total_medida",
        ],
    )

# COMMAND ----------

gold_fingerprint_before_df = (
    build_gold_fingerprint()
)


display(
    gold_fingerprint_before_df
    .orderBy("tabla")
)

delta_versions_before = []


for logical_name in [
    "fact_generacion_real",
    "fact_disponibilidad_planta",
    "fact_demanda_real",
    "fact_precio_bolsa",
    "fact_energia_embalsada_planta",
]:
    physical_name = GOLD_TABLES[
        logical_name
    ]

    latest_history = (
        spark.sql(
            f"DESCRIBE HISTORY {physical_name}"
        )
        .select(
            "version",
            "timestamp",
            "operation",
        )
        .orderBy(
            F.desc("version")
        )
        .first()
    )

    delta_versions_before.append(
        (
            logical_name,
            latest_history["version"],
            str(latest_history["timestamp"]),
            latest_history["operation"],
        )
    )


delta_versions_before_df = (
    spark.createDataFrame(
        delta_versions_before,
        [
            "tabla",
            "version_delta_anterior",
            "timestamp_anterior",
            "ultima_operacion_anterior",
        ],
    )
)


display(
    delta_versions_before_df
    .orderBy("tabla")
)

# COMMAND ----------

gold_fingerprint_after_df = (
    build_gold_fingerprint()
)


display(
    gold_fingerprint_after_df
    .orderBy("tabla")
)

# COMMAND ----------

idempotency_comparison_df = (
    gold_fingerprint_before_df.alias("before")
    .join(
        gold_fingerprint_after_df.alias("after"),
        ["tabla"],
        "full",
    )
    .select(
        "tabla",

        F.col("before.filas").alias(
            "filas_antes"
        ),

        F.col("after.filas").alias(
            "filas_despues"
        ),

        F.col(
            "before.claves_distintas"
        ).alias(
            "claves_antes"
        ),

        F.col(
            "after.claves_distintas"
        ).alias(
            "claves_despues"
        ),

        F.col(
            "before.fecha_minima"
        ).alias(
            "fecha_minima_antes"
        ),

        F.col(
            "after.fecha_minima"
        ).alias(
            "fecha_minima_despues"
        ),

        F.col(
            "before.fecha_maxima"
        ).alias(
            "fecha_maxima_antes"
        ),

        F.col(
            "after.fecha_maxima"
        ).alias(
            "fecha_maxima_despues"
        ),

        F.col(
            "before.total_medida"
        ).alias(
            "total_antes"
        ),

        F.col(
            "after.total_medida"
        ).alias(
            "total_despues"
        ),
    )
    .withColumn(
        "filas_iguales",
        F.col("filas_antes").eqNullSafe(
            F.col("filas_despues")
        ),
    )
    .withColumn(
        "claves_iguales",
        F.col("claves_antes").eqNullSafe(
            F.col("claves_despues")
        ),
    )
    .withColumn(
        "rango_fechas_igual",
        (
            F.col("fecha_minima_antes").eqNullSafe(
                F.col("fecha_minima_despues")
            )
        )
        &
        (
            F.col("fecha_maxima_antes").eqNullSafe(
                F.col("fecha_maxima_despues")
            )
        ),
    )
    .withColumn(
        "total_medida_igual",
        F.col("total_antes").eqNullSafe(
            F.col("total_despues")
        ),
    )
    .withColumn(
        "idempotente",
        F.col("filas_iguales")
        & F.col("claves_iguales")
        & F.col("rango_fechas_igual")
        & F.col("total_medida_igual"),
    )
)


display(
    idempotency_comparison_df
    .orderBy("tabla")
)

# COMMAND ----------

non_idempotent_tables = (
    idempotency_comparison_df
    .filter(
        ~F.col("idempotente")
    )
    .count()
)


print(
    "Tablas evaluadas:",
    idempotency_comparison_df.count(),
)

print(
    "Tablas no idempotentes:",
    non_idempotent_tables,
)


if non_idempotent_tables > 0:
    display(
        idempotency_comparison_df
        .filter(
            ~F.col("idempotente")
        )
    )

    raise ValueError(
        "La carga Gold no es idempotente."
    )


print(
    "IDEMPOTENCIA GOLD APROBADA."
)
