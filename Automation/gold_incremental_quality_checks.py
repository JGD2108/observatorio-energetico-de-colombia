# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Incremental Quality Checks
# MAGIC
# MAGIC Control de calidad incremental para ejecutar después de la carga Gold.
# MAGIC
# MAGIC Parámetros del job:
# MAGIC - `lookback_days = 45`
# MAGIC - `max_lag_days = 45`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 1 — Importaciones y configuración

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import timedelta
import sys
import uuid

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import (  # noqa: E402
    CATALOG, GOLD_TABLES, MONITORING_SCHEMA, SCHEMAS, SILVER_TABLES,
)

SILVER_SCHEMA = f"{CATALOG}.{SCHEMAS['silver']}"
GOLD_SCHEMA = f"{CATALOG}.{SCHEMAS['gold']}"

QUALITY_RESULTS_TABLE = f"{MONITORING_SCHEMA}.gold_incremental_quality_results"

print("Configuración cargada.")
print("Silver:", SILVER_SCHEMA)
print("Gold:", GOLD_SCHEMA)
print("Monitoring:", MONITORING_SCHEMA)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 2 — Parámetros

# COMMAND ----------

try:
    dbutils.widgets.text("lookback_days", "45", "Días de validación")
    dbutils.widgets.text("max_lag_days", "45", "Rezago máximo permitido")
except Exception:
    pass

try:
    LOOKBACK_DAYS = int(dbutils.widgets.get("lookback_days"))
except Exception:
    LOOKBACK_DAYS = 45

try:
    MAX_LAG_DAYS = int(dbutils.widgets.get("max_lag_days"))
except Exception:
    MAX_LAG_DAYS = 45

if LOOKBACK_DAYS <= 0:
    raise ValueError("lookback_days debe ser mayor que cero.")
if MAX_LAG_DAYS < 0:
    raise ValueError("max_lag_days no puede ser negativo.")

RUN_ID = str(uuid.uuid4())
RUN_DATE = spark.sql("SELECT current_date() AS run_date").first()["run_date"]
WINDOW_END_DATE = RUN_DATE
WINDOW_START_DATE = RUN_DATE - timedelta(days=LOOKBACK_DAYS - 1)

print("Run ID:", RUN_ID)
print("Fecha de ejecución:", RUN_DATE)
print("Ventana:", WINDOW_START_DATE, "→", WINDOW_END_DATE)
print("Días evaluados:", LOOKBACK_DAYS)
print("Rezago máximo:", MAX_LAG_DAYS)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 3 — Esquema de monitoreo y existencia de tablas

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {MONITORING_SCHEMA}")

required_tables = {
    **{f"silver.{k}": v for k, v in SILVER_TABLES.items()},
    **{f"gold.{k}": v for k, v in GOLD_TABLES.items()},
}

table_existence_results = [
    (logical, physical, spark.catalog.tableExists(physical))
    for logical, physical in required_tables.items()
]

table_existence_df = spark.createDataFrame(
    table_existence_results,
    ["nombre_logico", "nombre_fisico", "existe"],
)

display(table_existence_df.orderBy("nombre_logico"))

missing_tables = table_existence_df.filter(~F.col("existe")).count()
print("Tablas inexistentes:", missing_tables)

if missing_tables > 0:
    raise ValueError("Faltan tablas necesarias para ejecutar los controles incrementales.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 4 — Funciones auxiliares

# COMMAND ----------

quality_results = []

def add_quality_result(component, validation, error_count, detail):
    error_count = int(error_count)
    quality_results.append({
        "componente": component,
        "validacion": validation,
        "errores": error_count,
        "aprobado": error_count == 0,
        "detalle": str(detail),
    })

def filter_validation_window(dataframe, date_column):
    return dataframe.filter(
        F.to_date(F.col(date_column)).between(
            F.lit(WINDOW_START_DATE),
            F.lit(WINDOW_END_DATE),
        )
    )

def detect_first_existing_column(dataframe, candidates, logical_name):
    available = set(dataframe.columns)
    selected = next((x for x in candidates if x in available), None)
    if selected is None:
        raise ValueError(
            f"No se encontró la columna de {logical_name}. "
            f"Columnas disponibles: {sorted(available)}"
        )
    return selected

def tx_priority_expression(version_column):
    numeric_tx = F.regexp_extract(
        F.col(version_column), r"^TX([0-9]+)$", 1
    ).cast("int")
    return (
        F.when(F.col(version_column) == "TXF", F.lit(10000))
        .when(F.col(version_column) == "TXR", F.lit(9000))
        .when(F.col(version_column).rlike(r"^TX[0-9]+$"), numeric_tx * 100)
        .otherwise(F.lit(0))
    )

def add_tx_priority(dataframe, version_column="version"):
    return dataframe.withColumn(
        "_prioridad_esperada",
        tx_priority_expression(version_column),
    )

def latest_tx_records(dataframe, business_key):
    order_columns = [F.col("_prioridad_esperada").desc()]
    for metadata_column in [
        "silver_updated_at",
        "ingestion_timestamp",
        "load_date",
    ]:
        if metadata_column in dataframe.columns:
            order_columns.append(F.col(metadata_column).desc_nulls_last())
    order_columns.append(F.col("version").desc())

    selection_window = Window.partitionBy(*business_key).orderBy(*order_columns)

    return (
        dataframe
        .withColumn("_row_number", F.row_number().over(selection_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 5 — Configuración de hechos

# COMMAND ----------

FACT_CONFIGURATIONS = [
    {
        "component": "Generación",
        "table": GOLD_TABLES["fact_generacion_real"],
        "key": "generacion_key",
        "date": "fecha_hora",
        "dimensions": [
            ("fecha_key", GOLD_TABLES["dim_fecha"], "fecha_key"),
            ("periodo_key", GOLD_TABLES["dim_periodo"], "periodo_key"),
            ("planta_key", GOLD_TABLES["dim_planta"], "planta_key"),
            ("agente_key", GOLD_TABLES["dim_agente"], "agente_key"),
        ],
    },
    {
        "component": "Disponibilidad",
        "table": GOLD_TABLES["fact_disponibilidad_planta"],
        "key": "disponibilidad_key",
        "date": "fecha_hora",
        "dimensions": [
            ("fecha_key", GOLD_TABLES["dim_fecha"], "fecha_key"),
            ("periodo_key", GOLD_TABLES["dim_periodo"], "periodo_key"),
            ("planta_key", GOLD_TABLES["dim_planta"], "planta_key"),
        ],
    },
    {
        "component": "Demanda",
        "table": GOLD_TABLES["fact_demanda_real"],
        "key": "demanda_key",
        "date": "fecha_hora",
        "dimensions": [
            ("fecha_key", GOLD_TABLES["dim_fecha"], "fecha_key"),
            ("periodo_key", GOLD_TABLES["dim_periodo"], "periodo_key"),
            ("agente_key", GOLD_TABLES["dim_agente"], "agente_key"),
        ],
    },
    {
        "component": "Precio de bolsa",
        "table": GOLD_TABLES["fact_precio_bolsa"],
        "key": "precio_bolsa_key",
        "date": "fecha_hora",
        "dimensions": [
            ("fecha_key", GOLD_TABLES["dim_fecha"], "fecha_key"),
            ("periodo_key", GOLD_TABLES["dim_periodo"], "periodo_key"),
        ],
    },
    {
        "component": "Energía embalsada",
        "table": GOLD_TABLES["fact_energia_embalsada_planta"],
        "key": "energia_embalsada_key",
        "date": "fecha_medicion",
        "dimensions": [
            ("fecha_key", GOLD_TABLES["dim_fecha"], "fecha_key"),
            ("planta_key", GOLD_TABLES["dim_planta"], "planta_key"),
        ],
    },
]


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 6 — Volumen, duplicados y claves nulas

# COMMAND ----------

window_statistics = []

for cfg in FACT_CONFIGURATIONS:
    component = cfg["component"]
    window_df = filter_validation_window(spark.table(cfg["table"]), cfg["date"])
    total_rows = window_df.count()
    distinct_keys = window_df.select(cfg["key"]).distinct().count()
    duplicate_rows = total_rows - distinct_keys
    null_keys = window_df.filter(F.col(cfg["key"]).isNull()).count()

    window_statistics.append(
        (component, total_rows, distinct_keys, duplicate_rows, null_keys)
    )

    add_quality_result(
        component,
        "Existencia de datos en ventana",
        0 if total_rows > 0 else 1,
        f"Filas encontradas: {total_rows:,}",
    )
    add_quality_result(
        component,
        "Claves duplicadas",
        duplicate_rows,
        f"Filas: {total_rows:,}; claves distintas: {distinct_keys:,}",
    )
    add_quality_result(
        component,
        "Claves primarias nulas",
        null_keys,
        f"Claves nulas: {null_keys:,}",
    )

window_statistics_df = spark.createDataFrame(
    window_statistics,
    ["componente", "filas_ventana", "claves_distintas", "duplicados", "claves_nulas"],
)
display(window_statistics_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 7 — Integridad referencial

# COMMAND ----------

foreign_key_details = []

for cfg in FACT_CONFIGURATIONS:
    component = cfg["component"]
    fact_window_df = filter_validation_window(
        spark.table(cfg["table"]),
        cfg["date"],
    )

    for fact_key, dimension_table, dimension_key in cfg["dimensions"]:
        null_keys = fact_window_df.filter(F.col(fact_key).isNull()).count()

        dimension_keys_df = (
            spark.table(dimension_table)
            .select(F.col(dimension_key).alias("_dimension_key"))
            .distinct()
        )

        orphan_keys = (
            fact_window_df
            .select(F.col(fact_key).alias("_fact_key"))
            .filter(F.col("_fact_key").isNotNull())
            .distinct()
            .join(
                dimension_keys_df,
                F.col("_fact_key") == F.col("_dimension_key"),
                "left_anti",
            )
            .count()
        )

        total_errors = null_keys + orphan_keys
        foreign_key_details.append(
            (component, fact_key, null_keys, orphan_keys, total_errors == 0)
        )

        add_quality_result(
            component,
            f"Integridad referencial {fact_key}",
            total_errors,
            f"Nulas: {null_keys:,}; huérfanas: {orphan_keys:,}",
        )

foreign_key_details_df = spark.createDataFrame(
    foreign_key_details,
    ["componente", "clave_foranea", "claves_nulas", "claves_huerfanas", "aprobado"],
)
display(foreign_key_details_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 8 — Medidas nulas o negativas

# COMMAND ----------

generation_window_df = filter_validation_window(
    spark.table(GOLD_TABLES["fact_generacion_real"]), "fecha_hora"
)
availability_window_df = filter_validation_window(
    spark.table(GOLD_TABLES["fact_disponibilidad_planta"]), "fecha_hora"
)
demand_window_df = filter_validation_window(
    spark.table(GOLD_TABLES["fact_demanda_real"]), "fecha_hora"
)
price_window_df = filter_validation_window(
    spark.table(GOLD_TABLES["fact_precio_bolsa"]), "fecha_hora"
)
reservoir_window_df = filter_validation_window(
    spark.table(GOLD_TABLES["fact_energia_embalsada_planta"]), "fecha_medicion"
)

generation_invalid_values = generation_window_df.filter(
    F.col("generacion_real_kwh").isNull() | (F.col("generacion_real_kwh") < 0)
).count()
availability_invalid_values = availability_window_df.filter(
    F.col("disponibilidad_real_kwh").isNull()
    | (F.col("disponibilidad_real_kwh") < 0)
).count()
demand_invalid_values = demand_window_df.filter(
    F.col("demanda_real_kwh").isNull() | (F.col("demanda_real_kwh") < 0)
).count()
price_invalid_values = price_window_df.filter(
    F.col("precio_bolsa_internacional_cop_kwh").isNull()
    | F.col("precio_bolsa_nacional_cop_kwh").isNull()
    | F.col("precio_bolsa_tie_cop_kwh").isNull()
    | (F.col("precio_bolsa_internacional_cop_kwh") < 0)
    | (F.col("precio_bolsa_nacional_cop_kwh") < 0)
    | (F.col("precio_bolsa_tie_cop_kwh") < 0)
).count()
reservoir_invalid_values = reservoir_window_df.filter(
    F.col("energia_embalsada_kwh").isNull()
    | (F.col("energia_embalsada_kwh") < 0)
).count()

add_quality_result("Generación", "Medidas nulas o negativas", generation_invalid_values, "generacion_real_kwh")
add_quality_result("Disponibilidad", "Medidas nulas o negativas", availability_invalid_values, "disponibilidad_real_kwh")
add_quality_result("Demanda", "Medidas nulas o negativas", demand_invalid_values, "demanda_real_kwh")
add_quality_result("Precio de bolsa", "Medidas nulas o negativas", price_invalid_values, "PB_INT, PB_NAL y PB_TIE")
add_quality_result("Energía embalsada", "Medidas nulas o negativas", reservoir_invalid_values, "energia_embalsada_kwh")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 9 — Consistencia de versiones TX

# COMMAND ----------

def validate_single_tx_fact(component, dataframe, version_column, priority_column):
    invalid_versions = (
        dataframe
        .withColumn(
            "_prioridad_calculada",
            tx_priority_expression(version_column),
        )
        .filter(
            F.col(version_column).isNull()
            | (F.col("_prioridad_calculada") == 0)
            | ~(F.col(priority_column).eqNullSafe(F.col("_prioridad_calculada")))
        )
        .count()
    )

    add_quality_result(
        component,
        "Consistencia de versión TX",
        invalid_versions,
        f"Versión: {version_column}; prioridad: {priority_column}",
    )

validate_single_tx_fact("Generación", generation_window_df, "version_seleccionada", "prioridad_version")
validate_single_tx_fact("Disponibilidad", availability_window_df, "version_seleccionada", "prioridad_version")
validate_single_tx_fact("Demanda", demand_window_df, "version_seleccionada", "prioridad_version")
validate_single_tx_fact("Energía embalsada", reservoir_window_df, "version_seleccionada", "prioridad_version")

price_tx_invalid_rows = (
    price_window_df
    .withColumn("_expected_pb_int", tx_priority_expression("version_pb_int"))
    .withColumn("_expected_pb_nal", tx_priority_expression("version_pb_nal"))
    .withColumn("_expected_pb_tie", tx_priority_expression("version_pb_tie"))
    .filter(
        F.col("version_pb_int").isNull()
        | F.col("version_pb_nal").isNull()
        | F.col("version_pb_tie").isNull()
        | (F.col("_expected_pb_int") == 0)
        | (F.col("_expected_pb_nal") == 0)
        | (F.col("_expected_pb_tie") == 0)
        | ~(F.col("prioridad_pb_int").eqNullSafe(F.col("_expected_pb_int")))
        | ~(F.col("prioridad_pb_nal").eqNullSafe(F.col("_expected_pb_nal")))
        | ~(F.col("prioridad_pb_tie").eqNullSafe(F.col("_expected_pb_tie")))
    )
    .count()
)

add_quality_result(
    "Precio de bolsa",
    "Consistencia de versiones TX",
    price_tx_invalid_rows,
    "Versiones y prioridades de PB_INT, PB_NAL y PB_TIE",
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 10 — Frescura

# COMMAND ----------

freshness_results = []

for cfg in FACT_CONFIGURATIONS:
    maximum_date = (
        spark.table(cfg["table"])
        .agg(F.max(F.to_date(F.col(cfg["date"]))).alias("fecha_maxima"))
        .first()["fecha_maxima"]
    )

    if maximum_date is None:
        lag_days = None
        freshness_errors = 1
    else:
        lag_days = (RUN_DATE - maximum_date).days
        freshness_errors = 0 if lag_days <= MAX_LAG_DAYS else 1

    freshness_results.append(
        (cfg["component"], maximum_date, lag_days, MAX_LAG_DAYS, freshness_errors == 0)
    )

    add_quality_result(
        cfg["component"],
        "Frescura del dato",
        freshness_errors,
        f"Fecha máxima: {maximum_date}; rezago: {lag_days}; máximo: {MAX_LAG_DAYS}",
    )

freshness_results_df = spark.createDataFrame(
    freshness_results,
    ["componente", "fecha_maxima", "dias_rezago", "rezago_maximo", "aprobado"],
)
display(freshness_results_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 11 — Generación esperada desde Silver

# COMMAND ----------

silver_generation_raw_df = spark.table(SILVER_TABLES["generacion_real"])
generation_value_column = detect_first_existing_column(
    silver_generation_raw_df,
    ["generacion_real_kwh", "generacion_kwh", "valor_generacion", "valor"],
    "generación real",
)

generation_silver_df = (
    silver_generation_raw_df
    .select(
        F.to_timestamp("fecha_hora").alias("fecha_hora"),
        F.upper(F.trim("codigo_planta")).alias("codigo_planta"),
        F.upper(F.trim("codigo_agente")).alias("codigo_agente"),
        F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
        F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
        F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
        F.upper(F.trim("version")).alias("version"),
        F.col(generation_value_column).cast("decimal(24,6)").alias("valor"),
        *[
            F.col(x)
            for x in ["silver_updated_at", "ingestion_timestamp", "load_date"]
            if x in silver_generation_raw_df.columns
        ],
    )
    .filter(
        (F.col("codigo_variable") == "GREAL")
        & (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)

generation_silver_df = filter_validation_window(generation_silver_df, "fecha_hora")
generation_expected_df = latest_tx_records(
    add_tx_priority(generation_silver_df),
    [
        "fecha_hora", "codigo_planta", "codigo_agente",
        "codigo_variable", "codigo_duracion", "unidad_medida",
    ],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 12 — Disponibilidad esperada desde Silver

# COMMAND ----------

silver_availability_raw_df = spark.table(SILVER_TABLES["disponibilidad_plantas"])
availability_value_column = detect_first_existing_column(
    silver_availability_raw_df,
    ["disponibilidad_real_kwh", "disponibilidad_kwh", "valor_disponibilidad", "valor"],
    "disponibilidad real",
)

availability_silver_df = (
    silver_availability_raw_df
    .select(
        F.to_timestamp("fecha_hora").alias("fecha_hora"),
        F.upper(F.trim("codigo_planta")).alias("codigo_planta"),
        F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
        F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
        F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
        F.upper(F.trim("version")).alias("version"),
        F.col(availability_value_column).cast("decimal(24,6)").alias("valor"),
        *[
            F.col(x)
            for x in ["silver_updated_at", "ingestion_timestamp", "load_date"]
            if x in silver_availability_raw_df.columns
        ],
    )
    .filter(
        (F.col("codigo_variable") == "DISPREAL")
        & (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)

availability_silver_df = filter_validation_window(availability_silver_df, "fecha_hora")
availability_expected_df = latest_tx_records(
    add_tx_priority(availability_silver_df),
    [
        "fecha_hora", "codigo_planta",
        "codigo_variable", "codigo_duracion", "unidad_medida",
    ],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 13 — Demanda esperada desde Silver

# COMMAND ----------

silver_demand_raw_df = spark.table(SILVER_TABLES["demanda_real"])
demand_value_column = detect_first_existing_column(
    silver_demand_raw_df,
    ["demanda_real_kwh", "demanda_kwh", "valor_demanda", "valor"],
    "demanda real",
)

demand_silver_df = (
    silver_demand_raw_df
    .select(
        F.to_timestamp("fecha_hora").alias("fecha_hora"),
        F.upper(F.trim("codigo_agente")).alias("codigo_agente"),
        F.upper(F.trim("tipo_mercado")).alias("tipo_mercado"),
        F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
        F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
        F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
        F.upper(F.trim("version")).alias("version"),
        F.col(demand_value_column).cast("decimal(24,6)").alias("valor"),
        *[
            F.col(x)
            for x in ["silver_updated_at", "ingestion_timestamp", "load_date"]
            if x in silver_demand_raw_df.columns
        ],
    )
    .filter(
        (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)

demand_silver_df = filter_validation_window(demand_silver_df, "fecha_hora")
demand_expected_df = latest_tx_records(
    add_tx_priority(demand_silver_df),
    [
        "fecha_hora", "codigo_agente", "tipo_mercado",
        "codigo_variable", "codigo_duracion", "unidad_medida",
    ],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 14 — Precio esperado desde Silver

# COMMAND ----------

silver_price_raw_df = spark.table(SILVER_TABLES["precio_bolsa"])
price_value_column = detect_first_existing_column(
    silver_price_raw_df,
    ["precio_bolsa", "precio_kwh", "valor_precio", "precio", "valor"],
    "precio de bolsa",
)

price_silver_long_df = (
    silver_price_raw_df
    .select(
        F.to_timestamp("fecha_hora").alias("fecha_hora"),
        F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
        F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
        F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
        F.upper(F.trim("version")).alias("version"),
        F.col(price_value_column).cast("decimal(24,6)").alias("valor"),
        *[
            F.col(x)
            for x in ["silver_updated_at", "ingestion_timestamp", "load_date"]
            if x in silver_price_raw_df.columns
        ],
    )
    .filter(
        F.col("codigo_variable").isin("PB_INT", "PB_NAL", "PB_TIE")
        & (F.col("codigo_duracion") == "PT1H")
    )
)

price_silver_long_df = filter_validation_window(price_silver_long_df, "fecha_hora")
price_selected_long_df = latest_tx_records(
    add_tx_priority(price_silver_long_df),
    ["fecha_hora", "codigo_variable", "codigo_duracion", "unidad_medida"],
)

price_expected_df = (
    price_selected_long_df
    .groupBy("fecha_hora")
    .agg(
        F.max(F.when(F.col("codigo_variable") == "PB_INT", F.col("valor"))).alias("pb_int"),
        F.max(F.when(F.col("codigo_variable") == "PB_NAL", F.col("valor"))).alias("pb_nal"),
        F.max(F.when(F.col("codigo_variable") == "PB_TIE", F.col("valor"))).alias("pb_tie"),
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 15 — Energía embalsada esperada desde Silver

# COMMAND ----------

silver_reservoir_raw_df = spark.table(SILVER_TABLES["niveles_embalses"])
reservoir_value_column = detect_first_existing_column(
    silver_reservoir_raw_df,
    ["energia_embalsada_kwh", "energia_embalsada", "nivel_embalse_kwh", "valor_nem", "valor"],
    "energía embalsada",
)

reservoir_silver_df = (
    silver_reservoir_raw_df
    .select(
        F.to_date("fecha_inicio").alias("fecha_medicion"),
        F.upper(F.trim("codigo_planta")).alias("codigo_planta"),
        F.upper(F.trim("codigo_variable")).alias("codigo_variable"),
        F.upper(F.trim("codigo_duracion")).alias("codigo_duracion"),
        F.upper(F.trim("unidad_medida")).alias("unidad_medida"),
        F.upper(F.trim("version")).alias("version"),
        F.col(reservoir_value_column).cast("decimal(24,6)").alias("valor"),
        *[
            F.col(x)
            for x in ["silver_updated_at", "ingestion_timestamp", "load_date"]
            if x in silver_reservoir_raw_df.columns
        ],
    )
    .filter(
        (F.col("codigo_variable") == "NEM")
        & (F.col("codigo_duracion") == "P1D")
        & (F.col("unidad_medida") == "KWH")
    )
)

reservoir_silver_df = filter_validation_window(reservoir_silver_df, "fecha_medicion")
reservoir_expected_df = latest_tx_records(
    add_tx_priority(reservoir_silver_df),
    [
        "fecha_medicion", "codigo_planta",
        "codigo_variable", "codigo_duracion", "unidad_medida",
    ],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 16 — Función de reconciliación diaria

# COMMAND ----------

def reconcile_daily_metrics(
    component,
    silver_daily_df,
    gold_daily_df,
    date_column,
    measure_columns,
    tolerance=0.001,
):
    comparison_df = silver_daily_df.alias("silver").join(
        gold_daily_df.alias("gold"),
        [date_column],
        "full",
    )

    mismatch_condition = ~(
        F.col("silver.registros").eqNullSafe(F.col("gold.registros"))
    )

    for measure_column in measure_columns:
        silver_measure = F.coalesce(
            F.col(f"silver.{measure_column}"),
            F.lit(0),
        )
        gold_measure = F.coalesce(
            F.col(f"gold.{measure_column}"),
            F.lit(0),
        )
        mismatch_condition = mismatch_condition | (
            F.abs(silver_measure - gold_measure) > F.lit(tolerance)
        )

    mismatched_days_df = comparison_df.filter(mismatch_condition)
    mismatched_days = mismatched_days_df.count()

    add_quality_result(
        component,
        "Reconciliación diaria Silver–Gold",
        mismatched_days,
        f"Días diferentes: {mismatched_days:,}",
    )

    return mismatched_days_df


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 17 — Reconciliar generación y disponibilidad

# COMMAND ----------

generation_silver_daily_df = (
    generation_expected_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("valor").alias("total_generacion"),
    )
)
generation_gold_daily_df = (
    generation_window_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("generacion_real_kwh").alias("total_generacion"),
    )
)
generation_daily_differences_df = reconcile_daily_metrics(
    "Generación",
    generation_silver_daily_df,
    generation_gold_daily_df,
    "fecha",
    ["total_generacion"],
)

availability_silver_daily_df = (
    availability_expected_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("valor").alias("total_disponibilidad"),
    )
)
availability_gold_daily_df = (
    availability_window_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("disponibilidad_real_kwh").alias("total_disponibilidad"),
    )
)
availability_daily_differences_df = reconcile_daily_metrics(
    "Disponibilidad",
    availability_silver_daily_df,
    availability_gold_daily_df,
    "fecha",
    ["total_disponibilidad"],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 18 — Reconciliar demanda

# COMMAND ----------

demand_silver_daily_df = (
    demand_expected_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("valor").alias("total_demanda"),
    )
)
demand_gold_daily_df = (
    demand_window_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("demanda_real_kwh").alias("total_demanda"),
    )
)
demand_daily_differences_df = reconcile_daily_metrics(
    "Demanda",
    demand_silver_daily_df,
    demand_gold_daily_df,
    "fecha",
    ["total_demanda"],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 19 — Reconciliar precio de bolsa

# COMMAND ----------

price_silver_daily_df = (
    price_expected_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("pb_int").alias("total_pb_int"),
        F.sum("pb_nal").alias("total_pb_nal"),
        F.sum("pb_tie").alias("total_pb_tie"),
    )
)
price_gold_daily_df = (
    price_window_df
    .groupBy(F.to_date("fecha_hora").alias("fecha"))
    .agg(
        F.count("*").alias("registros"),
        F.sum("precio_bolsa_internacional_cop_kwh").alias("total_pb_int"),
        F.sum("precio_bolsa_nacional_cop_kwh").alias("total_pb_nal"),
        F.sum("precio_bolsa_tie_cop_kwh").alias("total_pb_tie"),
    )
)
price_daily_differences_df = reconcile_daily_metrics(
    "Precio de bolsa",
    price_silver_daily_df,
    price_gold_daily_df,
    "fecha",
    ["total_pb_int", "total_pb_nal", "total_pb_tie"],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 20 — Reconciliar energía embalsada

# COMMAND ----------

reservoir_silver_daily_df = (
    reservoir_expected_df
    .groupBy("fecha_medicion")
    .agg(
        F.count("*").alias("registros"),
        F.sum("valor").alias("total_energia_embalsada"),
    )
)
reservoir_gold_daily_df = (
    reservoir_window_df
    .groupBy("fecha_medicion")
    .agg(
        F.count("*").alias("registros"),
        F.sum("energia_embalsada_kwh").alias("total_energia_embalsada"),
    )
)
reservoir_daily_differences_df = reconcile_daily_metrics(
    "Energía embalsada",
    reservoir_silver_daily_df,
    reservoir_gold_daily_df,
    "fecha_medicion",
    ["total_energia_embalsada"],
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 21 — Mostrar diferencias

# COMMAND ----------

difference_dataframes = [
    ("Generación", generation_daily_differences_df),
    ("Disponibilidad", availability_daily_differences_df),
    ("Demanda", demand_daily_differences_df),
    ("Precio de bolsa", price_daily_differences_df),
    ("Energía embalsada", reservoir_daily_differences_df),
]

for component, difference_df in difference_dataframes:
    difference_count = difference_df.count()
    if difference_count > 0:
        print(f"Diferencias en {component}: {difference_count:,}")
        display(difference_df.limit(100))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 22 — Resumen general

# COMMAND ----------

quality_results_df = (
    spark.createDataFrame(quality_results)
    .select(
        "componente",
        "validacion",
        F.col("errores").cast("long"),
        F.col("aprobado").cast("boolean"),
        "detalle",
    )
)

display(quality_results_df.orderBy("componente", "validacion"))

quality_summary_df = (
    quality_results_df
    .groupBy("componente")
    .agg(
        F.count("*").alias("validaciones"),
        F.sum(F.when(F.col("aprobado"), 1).otherwise(0)).alias("validaciones_aprobadas"),
        F.sum(F.when(~F.col("aprobado"), 1).otherwise(0)).alias("validaciones_fallidas"),
        F.sum("errores").alias("errores_totales"),
    )
    .withColumn("aprobado", F.col("validaciones_fallidas") == 0)
)

display(quality_summary_df.orderBy("componente"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 23 — Guardar resultados históricos

# COMMAND ----------

quality_results_to_write_df = (
    quality_results_df
    .withColumn("run_id", F.lit(RUN_ID))
    .withColumn("run_timestamp", F.current_timestamp())
    .withColumn("run_date", F.lit(RUN_DATE).cast("date"))
    .withColumn("window_start_date", F.lit(WINDOW_START_DATE).cast("date"))
    .withColumn("window_end_date", F.lit(WINDOW_END_DATE).cast("date"))
    .withColumn("lookback_days", F.lit(LOOKBACK_DAYS).cast("int"))
    .withColumn("max_lag_days", F.lit(MAX_LAG_DAYS).cast("int"))
    .select(
        "run_id",
        "run_timestamp",
        "run_date",
        "window_start_date",
        "window_end_date",
        "lookback_days",
        "max_lag_days",
        "componente",
        "validacion",
        "errores",
        "aprobado",
        "detalle",
    )
)

(
    quality_results_to_write_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable(QUALITY_RESULTS_TABLE)
)

print("Resultados guardados en:", QUALITY_RESULTS_TABLE)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Celda 24 — Resultado final y fallo controlado

# COMMAND ----------

total_validations = quality_results_df.count()
failed_validations = quality_results_df.filter(~F.col("aprobado")).count()
total_errors = (
    quality_results_df
    .agg(F.coalesce(F.sum("errores"), F.lit(0)).alias("total_errores"))
    .first()["total_errores"]
)

print("=" * 80)
print("RESULTADO DE CALIDAD GOLD INCREMENTAL")
print("=" * 80)
print("Run ID:", RUN_ID)
print("Ventana:", WINDOW_START_DATE, "→", WINDOW_END_DATE)
print("Validaciones ejecutadas:", total_validations)
print("Validaciones fallidas:", failed_validations)
print("Errores detectados:", total_errors)

if failed_validations > 0:
    display(
        quality_results_df
        .filter(~F.col("aprobado"))
        .orderBy("componente", "validacion")
    )
    raise ValueError(
        "CONTROL DE CALIDAD GOLD INCREMENTAL NO APROBADO."
    )

print("CONTROL DE CALIDAD GOLD INCREMENTAL APROBADO.")
