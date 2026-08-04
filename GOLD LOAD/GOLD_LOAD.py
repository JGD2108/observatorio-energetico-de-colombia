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
    CATALOG,
    GOLD_TABLES,
    GOVERNANCE_TABLES,
    SCHEMAS,
    SILVER_TABLES,
)
from governance.rules import load_tx_policy, tx_priority_expression


GOLD_SCHEMA = f"{CATALOG}.{SCHEMAS['gold']}"
TX_POLICY = load_tx_policy(spark, GOVERNANCE_TABLES["ref_version_tx"])

DIM_FECHA_TABLE = GOLD_TABLES["dim_fecha"]
DIM_PERIODO_TABLE = GOLD_TABLES["dim_periodo"]

DIM_AGENTE_TABLE = GOLD_TABLES["dim_agente"]
DIM_PLANTA_TABLE = GOLD_TABLES["dim_planta"]
DIM_EMBALSE_TABLE = GOLD_TABLES["dim_embalse"]

BRIDGE_PLANTA_EMBALSE_TABLE = GOLD_TABLES["bridge_planta_embalse"]

FACT_GENERACION_TABLE = GOLD_TABLES["fact_generacion_real"]

FACT_DISPONIBILIDAD_TABLE = GOLD_TABLES["fact_disponibilidad_planta"]

FACT_DEMANDA_TABLE = GOLD_TABLES["fact_demanda_real"]

FACT_PRECIO_TABLE = GOLD_TABLES["fact_precio_bolsa"]

FACT_ENERGIA_EMBALSADA_TABLE = GOLD_TABLES["fact_energia_embalsada_planta"]

FACT_PRECIO_BOLSA_TABLE = GOLD_TABLES["fact_precio_bolsa"]



print("Gold schema:", GOLD_SCHEMA)
print("Dim fecha:", DIM_FECHA_TABLE)
print("Dim periodo:", DIM_PERIODO_TABLE)

# COMMAND ----------

required_gold_tables = [
    DIM_FECHA_TABLE,
    DIM_PERIODO_TABLE,
    DIM_AGENTE_TABLE,
    DIM_PLANTA_TABLE,
    DIM_EMBALSE_TABLE,
    BRIDGE_PLANTA_EMBALSE_TABLE,
    FACT_GENERACION_TABLE,
    FACT_DISPONIBILIDAD_TABLE,
    FACT_DEMANDA_TABLE,
    FACT_PRECIO_TABLE,
    FACT_ENERGIA_EMBALSADA_TABLE,
]


missing_gold_tables = [
    table_name
    for table_name in required_gold_tables
    if not spark.catalog.tableExists(table_name)
]


if missing_gold_tables:
    raise ValueError(
        "Faltan tablas Gold requeridas: "
        f"{missing_gold_tables}"
    )


print("Las 11 tablas Gold existen.")

# COMMAND ----------

# MAGIC %md
# MAGIC Calcular rango Calendario

# COMMAND ----------

date_sources = [
    (
        spark.table(
            SILVER_TABLES["agentes"]
        )
        .select(
            F.col("fecha").cast("date").alias("fecha")
        )
    ),

    (
        spark.table(
            SILVER_TABLES["generacion_real"]
        )
        .select(
            F.to_date("fecha_hora").alias("fecha")
        )
    ),

    (
        spark.table(
            SILVER_TABLES["disponibilidad_plantas"]
        )
        .select(
            F.to_date("fecha_hora").alias("fecha")
        )
    ),

    (
        spark.table(
            SILVER_TABLES["demanda_real"]
        )
        .select(
            F.to_date("fecha_hora").alias("fecha")
        )
    ),

    (
        spark.table(
            SILVER_TABLES["precio_bolsa"]
        )
        .select(
            F.to_date("fecha_hora").alias("fecha")
        )
    ),

    (
        spark.table(
            SILVER_TABLES["niveles_embalses"]
        )
        .select(
            F.to_date("fecha_inicio").alias("fecha")
        )
    ),
]


all_dates_df = date_sources[0]

for source_df in date_sources[1:]:
    all_dates_df = all_dates_df.unionByName(
        source_df
    )


date_limits = (
    all_dates_df
    .filter(
        F.col("fecha").isNotNull()
    )
    .agg(
        F.min("fecha").alias("fecha_minima"),
        F.max("fecha").alias("fecha_maxima"),
    )
    .first()
)


source_min_date = date_limits["fecha_minima"]
source_max_date = date_limits["fecha_maxima"]


if source_min_date is None or source_max_date is None:
    raise ValueError(
        "No fue posible determinar el rango "
        "de fechas desde Silver."
    )


calendar_start_date = source_min_date.replace(
    month=1,
    day=1,
)


calendar_end_date = source_max_date.replace(
    year=source_max_date.year + 2,
    month=12,
    day=31,
)


print("Fecha mínima en Silver:", source_min_date)
print("Fecha máxima en Silver:", source_max_date)
print("Inicio dimensión:", calendar_start_date)
print("Fin dimensión:", calendar_end_date)

# COMMAND ----------

# MAGIC %md
# MAGIC Construir Staging Fecha

# COMMAND ----------

dim_fecha_source_df = (
    spark.sql(
        f"""
        SELECT EXPLODE(
            SEQUENCE(
                DATE('{calendar_start_date}'),
                DATE('{calendar_end_date}'),
                INTERVAL 1 DAY
            )
        ) AS fecha
        """
    )
    .select(
        F.date_format(
            "fecha",
            "yyyyMMdd",
        ).cast("int").alias(
            "fecha_key"
        ),

        F.col("fecha"),

        F.year("fecha")
        .cast("smallint")
        .alias("anio"),

        F.when(
            F.month("fecha") <= 6,
            F.lit(1),
        )
        .otherwise(
            F.lit(2)
        )
        .cast("tinyint")
        .alias("semestre"),

        F.quarter("fecha")
        .cast("tinyint")
        .alias("trimestre"),

        F.month("fecha")
        .cast("tinyint")
        .alias("mes_numero"),

        F.date_format(
            "fecha",
            "MMMM",
        ).alias("mes_nombre"),

        F.date_format(
            "fecha",
            "MMM",
        ).alias("mes_nombre_corto"),

        F.date_format(
            "fecha",
            "yyyyMM",
        ).cast("int").alias(
            "anio_mes"
        ),

        F.date_format(
            "fecha",
            "yyyy-MM",
        ).alias(
            "anio_mes_nombre"
        ),

        F.weekofyear("fecha")
        .cast("tinyint")
        .alias("semana_anio"),

        F.dayofyear("fecha")
        .cast("smallint")
        .alias("dia_anio"),

        F.dayofmonth("fecha")
        .cast("tinyint")
        .alias("dia_mes"),

        (
            (
                F.dayofweek("fecha")
                + F.lit(5)
            )
            % F.lit(7)
            + F.lit(1)
        )
        .cast("tinyint")
        .alias(
            "dia_semana_numero"
        ),

        F.date_format(
            "fecha",
            "EEEE",
        ).alias(
            "dia_semana_nombre"
        ),

        (
            (
                (
                    F.dayofweek("fecha")
                    + F.lit(5)
                )
                % F.lit(7)
                + F.lit(1)
            )
            >= F.lit(6)
        ).alias(
            "es_fin_semana"
        ),

        (
            F.dayofmonth("fecha")
            == F.lit(1)
        ).alias(
            "es_inicio_mes"
        ),

        (
            F.col("fecha")
            == F.last_day("fecha")
        ).alias(
            "es_fin_mes"
        ),

        F.current_timestamp().alias(
            "fecha_creacion"
        ),

        F.current_timestamp().alias(
            "fecha_actualizacion"
        ),
    )
)


print(
    "Fechas preparadas:",
    f"{dim_fecha_source_df.count():,}",
)

# COMMAND ----------

date_source_rows = (
    dim_fecha_source_df.count()
)


date_distinct_keys = (
    dim_fecha_source_df
    .select("fecha_key")
    .distinct()
    .count()
)


date_distinct_dates = (
    dim_fecha_source_df
    .select("fecha")
    .distinct()
    .count()
)


print(
    "Filas fuente:",
    f"{date_source_rows:,}",
)

print(
    "fecha_key distintas:",
    f"{date_distinct_keys:,}",
)

print(
    "Fechas distintas:",
    f"{date_distinct_dates:,}",
)


if not (
    date_source_rows
    == date_distinct_keys
    == date_distinct_dates
):
    raise ValueError(
        "La fuente de dim_fecha contiene "
        "duplicados."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Merge Dim Fecha

# COMMAND ----------

dim_fecha_target = DeltaTable.forName(
    spark,
    DIM_FECHA_TABLE,
)


(
    dim_fecha_target.alias("target")
    .merge(
        dim_fecha_source_df.alias("source"),
        """
        target.fecha_key =
            source.fecha_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.fecha
                <=> source.fecha
            )
            OR NOT (
                target.anio
                <=> source.anio
            )
            OR NOT (
                target.semestre
                <=> source.semestre
            )
            OR NOT (
                target.trimestre
                <=> source.trimestre
            )
            OR NOT (
                target.mes_numero
                <=> source.mes_numero
            )
            OR NOT (
                target.mes_nombre
                <=> source.mes_nombre
            )
            OR NOT (
                target.mes_nombre_corto
                <=> source.mes_nombre_corto
            )
            OR NOT (
                target.anio_mes
                <=> source.anio_mes
            )
            OR NOT (
                target.anio_mes_nombre
                <=> source.anio_mes_nombre
            )
            OR NOT (
                target.semana_anio
                <=> source.semana_anio
            )
            OR NOT (
                target.dia_anio
                <=> source.dia_anio
            )
            OR NOT (
                target.dia_mes
                <=> source.dia_mes
            )
            OR NOT (
                target.dia_semana_numero
                <=> source.dia_semana_numero
            )
            OR NOT (
                target.dia_semana_nombre
                <=> source.dia_semana_nombre
            )
            OR NOT (
                target.es_fin_semana
                <=> source.es_fin_semana
            )
            OR NOT (
                target.es_inicio_mes
                <=> source.es_inicio_mes
            )
            OR NOT (
                target.es_fin_mes
                <=> source.es_fin_mes
            )
        """,
        set={
            "fecha":
                "source.fecha",

            "anio":
                "source.anio",

            "semestre":
                "source.semestre",

            "trimestre":
                "source.trimestre",

            "mes_numero":
                "source.mes_numero",

            "mes_nombre":
                "source.mes_nombre",

            "mes_nombre_corto":
                "source.mes_nombre_corto",

            "anio_mes":
                "source.anio_mes",

            "anio_mes_nombre":
                "source.anio_mes_nombre",

            "semana_anio":
                "source.semana_anio",

            "dia_anio":
                "source.dia_anio",

            "dia_mes":
                "source.dia_mes",

            "dia_semana_numero":
                "source.dia_semana_numero",

            "dia_semana_nombre":
                "source.dia_semana_nombre",

            "es_fin_semana":
                "source.es_fin_semana",

            "es_inicio_mes":
                "source.es_inicio_mes",

            "es_fin_mes":
                "source.es_fin_mes",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print("MERGE de dim_fecha completado.")

# COMMAND ----------

# MAGIC %md
# MAGIC Validar Dim Fecha

# COMMAND ----------

dim_fecha_validation_df = spark.table(
    DIM_FECHA_TABLE
)


display(
    dim_fecha_validation_df
    .agg(
        F.count("*").alias(
            "total_fechas"
        ),

        F.countDistinct(
            "fecha_key"
        ).alias(
            "fecha_keys_distintas"
        ),

        F.countDistinct(
            "fecha"
        ).alias(
            "fechas_distintas"
        ),

        F.min("fecha").alias(
            "fecha_minima"
        ),

        F.max("fecha").alias(
            "fecha_maxima"
        ),

        F.sum(
            F.when(
                F.col("fecha_key").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "fecha_key_nula"
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Dim Periodo

# COMMAND ----------

dim_periodo_source_df = (
    spark.range(1, 25)
    .select(
        F.col("id")
        .cast("tinyint")
        .alias("periodo_key"),

        F.col("id")
        .cast("tinyint")
        .alias("numero_periodo"),

        (
            F.col("id") - F.lit(1)
        )
        .cast("tinyint")
        .alias("hora_inicio"),

        (
            F.col("id") % F.lit(24)
        )
        .cast("tinyint")
        .alias("hora_fin"),
    )
    .withColumn(
        "hora_inicio_etiqueta",
        F.format_string(
            "%02d:00",
            F.col("hora_inicio"),
        ),
    )
    .withColumn(
        "hora_fin_etiqueta",
        F.format_string(
            "%02d:00",
            F.col("hora_fin"),
        ),
    )
    .withColumn(
        "periodo_etiqueta",
        F.concat(
            F.lit("Periodo "),
            F.col("numero_periodo")
            .cast("string"),
        ),
    )
    .withColumn(
        "rango_horario",
        F.concat_ws(
            " - ",
            F.col("hora_inicio_etiqueta"),
            F.col("hora_fin_etiqueta"),
        ),
    )
    .withColumn(
        "fecha_creacion",
        F.current_timestamp(),
    )
    .withColumn(
        "fecha_actualizacion",
        F.current_timestamp(),
    )
)


display(dim_periodo_source_df)

# COMMAND ----------

period_source_rows = (
    dim_periodo_source_df.count()
)


period_distinct_keys = (
    dim_periodo_source_df
    .select("periodo_key")
    .distinct()
    .count()
)


period_min = (
    dim_periodo_source_df
    .agg(
        F.min("periodo_key").alias("minimo")
    )
    .first()["minimo"]
)


period_max = (
    dim_periodo_source_df
    .agg(
        F.max("periodo_key").alias("maximo")
    )
    .first()["maximo"]
)


print("Total periodos:", period_source_rows)
print("Claves distintas:", period_distinct_keys)
print("Periodo mínimo:", period_min)
print("Periodo máximo:", period_max)


if (
    period_source_rows != 24
    or period_distinct_keys != 24
    or period_min != 1
    or period_max != 24
):
    raise ValueError(
        "La fuente de dim_periodo no contiene "
        "exactamente los periodos 1 a 24."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Merge Dim Periodo

# COMMAND ----------

dim_periodo_target = DeltaTable.forName(
    spark,
    DIM_PERIODO_TABLE,
)


(
    dim_periodo_target.alias("target")
    .merge(
        dim_periodo_source_df.alias("source"),
        """
        target.periodo_key =
            source.periodo_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.numero_periodo
                <=> source.numero_periodo
            )
            OR NOT (
                target.hora_inicio
                <=> source.hora_inicio
            )
            OR NOT (
                target.hora_fin
                <=> source.hora_fin
            )
            OR NOT (
                target.hora_inicio_etiqueta
                <=> source.hora_inicio_etiqueta
            )
            OR NOT (
                target.hora_fin_etiqueta
                <=> source.hora_fin_etiqueta
            )
            OR NOT (
                target.periodo_etiqueta
                <=> source.periodo_etiqueta
            )
            OR NOT (
                target.rango_horario
                <=> source.rango_horario
            )
        """,
        set={
            "numero_periodo":
                "source.numero_periodo",

            "hora_inicio":
                "source.hora_inicio",

            "hora_fin":
                "source.hora_fin",

            "hora_inicio_etiqueta":
                "source.hora_inicio_etiqueta",

            "hora_fin_etiqueta":
                "source.hora_fin_etiqueta",

            "periodo_etiqueta":
                "source.periodo_etiqueta",

            "rango_horario":
                "source.rango_horario",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print("MERGE de dim_periodo completado.")

# COMMAND ----------

# MAGIC %md
# MAGIC Validar ambas dimensiones

# COMMAND ----------

dim_fecha_df = spark.table(
    DIM_FECHA_TABLE
)


dim_periodo_df = spark.table(
    DIM_PERIODO_TABLE
)


fecha_total = dim_fecha_df.count()
fecha_distinct = (
    dim_fecha_df
    .select("fecha_key")
    .distinct()
    .count()
)


periodo_total = dim_periodo_df.count()
periodo_distinct = (
    dim_periodo_df
    .select("periodo_key")
    .distinct()
    .count()
)


print("dim_fecha filas:", fecha_total)
print("dim_fecha claves distintas:", fecha_distinct)
print("dim_periodo filas:", periodo_total)
print(
    "dim_periodo claves distintas:",
    periodo_distinct,
)


if fecha_total != fecha_distinct:
    raise ValueError(
        "dim_fecha contiene claves duplicadas."
    )


if (
    periodo_total != 24
    or periodo_distinct != 24
):
    raise ValueError(
        "dim_periodo no contiene exactamente "
        "24 periodos únicos."
    )


print(
    "dim_fecha y dim_periodo aprobadas."
)

# COMMAND ----------

silver_agents_table = SILVER_TABLES["agentes"]


agents_bronze_df = (
    spark.table(silver_agents_table)
    .select(
        F.to_date("fecha").alias("fecha"),

        F.upper(
            F.trim("codigo_agente")
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

        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
)


invalid_agents_df = (
    agents_bronze_df
    .filter(
        F.col("fecha").isNull()
        | F.col("codigo_agente").isNull()
        | (F.col("codigo_agente") == "")
        | F.col("nombre_agente").isNull()
        | (F.col("nombre_agente") == "")
        | F.col("actividad_agente").isNull()
        | (F.col("actividad_agente") == "")
    )
)


invalid_agents_count = invalid_agents_df.count()


print(
    "Registros Silver agentes:",
    f"{agents_bronze_df.count():,}",
)

print(
    "Registros inválidos:",
    f"{invalid_agents_count:,}",
)


if invalid_agents_count > 0:
    display(
        invalid_agents_df.limit(100)
    )

    raise ValueError(
        "Existen agentes con fecha, código, "
        "nombre o actividad inválidos."
    )

# COMMAND ----------

agent_daily_state_validation_df = (
    agents_bronze_df
    .groupBy(
        "fecha",
        "codigo_agente",
    )
    .agg(
        F.countDistinct(
            F.struct(
                "nombre_agente",
                "actividad_agente",
            )
        ).alias(
            "estados_distintos"
        )
    )
    .filter(
        F.col("estados_distintos") > 1
    )
)


ambiguous_agent_days = (
    agent_daily_state_validation_df.count()
)


print(
    "Agentes con más de un estado el mismo día:",
    f"{ambiguous_agent_days:,}",
)


if ambiguous_agent_days > 0:
    display(
        agent_daily_state_validation_df.limit(100)
    )

    raise ValueError(
        "No se puede construir dim_agente: "
        "existen agentes con múltiples estados "
        "distintos para una misma fecha."
    )


agent_daily_window = (
    Window
    .partitionBy(
        "fecha",
        "codigo_agente",
    )
    .orderBy(
        F.col(
            "ingestion_timestamp"
        ).desc_nulls_last(),

        F.col(
            "load_date"
        ).desc_nulls_last(),

        F.col(
            "nombre_agente"
        ).desc_nulls_last(),

        F.col(
            "actividad_agente"
        ).desc_nulls_last(),
    )
)


agent_daily_df = (
    agents_bronze_df
    .withColumn(
        "row_number",
        F.row_number().over(
            agent_daily_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


print(
    "Snapshots diarios únicos:",
    f"{agent_daily_df.count():,}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC normalizar textos y detectar cambios

# COMMAND ----------

def normalize_dimension_text(column):
    return F.upper(
        F.trim(
            F.regexp_replace(
                F.translate(
                    column,
                    "ÁÉÍÓÚÜÑ",
                    "AEIOUUN",
                ),
                r"\s+",
                " ",
            )
        )
    )


agent_daily_df = (
    agent_daily_df
    .withColumn(
        "nombre_agente_normalizado",
        normalize_dimension_text(
            F.col("nombre_agente")
        ),
    )
    .withColumn(
        "actividad_normalizada",
        normalize_dimension_text(
            F.col("actividad_agente")
        ),
    )
)


agent_history_window = (
    Window
    .partitionBy("codigo_agente")
    .orderBy("fecha")
)


agent_changes_df = (
    agent_daily_df
    .withColumn(
        "nombre_anterior",
        F.lag(
            "nombre_agente_normalizado"
        ).over(agent_history_window),
    )
    .withColumn(
        "actividad_anterior",
        F.lag(
            "actividad_normalizada"
        ).over(agent_history_window),
    )
    .withColumn(
        "inicia_nueva_version",
        F.when(
            F.col("nombre_anterior").isNull(),
            F.lit(1),
        )
        .when(
            ~(
                F.col(
                    "nombre_agente_normalizado"
                ).eqNullSafe(
                    F.col("nombre_anterior")
                )
            )
            |
            ~(
                F.col(
                    "actividad_normalizada"
                ).eqNullSafe(
                    F.col("actividad_anterior")
                )
            ),
            F.lit(1),
        )
        .otherwise(F.lit(0)),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Construir versiones SCD tipo 2

# COMMAND ----------

agent_version_window = (
    Window
    .partitionBy("codigo_agente")
    .orderBy("fecha")
    .rowsBetween(
        Window.unboundedPreceding,
        Window.currentRow,
    )
)


agent_versioned_daily_df = (
    agent_changes_df
    .withColumn(
        "numero_version",
        F.sum(
            "inicia_nueva_version"
        ).over(agent_version_window),
    )
)


agent_version_start_df = (
    agent_versioned_daily_df
    .filter(
        F.col("inicia_nueva_version") == 1
    )
    .select(
        "codigo_agente",
        "nombre_agente",
        "nombre_agente_normalizado",
        "actividad_agente",
        "actividad_normalizada",

        F.col(
            "numero_version"
        ).cast("int").alias(
            "numero_version"
        ),

        F.col("fecha").alias(
            "fecha_inicio"
        ),
    )
)


version_boundary_window = (
    Window
    .partitionBy("codigo_agente")
    .orderBy("fecha_inicio")
)


dim_agente_source_df = (
    agent_version_start_df
    .withColumn(
        "siguiente_fecha_inicio",
        F.lead(
            "fecha_inicio"
        ).over(version_boundary_window),
    )
    .withColumn(
        "fecha_fin",
        F.when(
            F.col(
                "siguiente_fecha_inicio"
            ).isNull(),
            F.to_date(
                F.lit("9999-12-31")
            ),
        )
        .otherwise(
            F.date_sub(
                F.col(
                    "siguiente_fecha_inicio"
                ),
                1,
            )
        ),
    )
    .withColumn(
        "es_actual",
        F.col(
            "siguiente_fecha_inicio"
        ).isNull(),
    )
    .withColumn(
        "fecha_creacion",
        F.current_timestamp(),
    )
    .withColumn(
        "fecha_actualizacion",
        F.current_timestamp(),
    )
    .drop(
        "siguiente_fecha_inicio"
    )
)


print(
    "Versiones SCD2 preparadas:",
    f"{dim_agente_source_df.count():,}",
)

print(
    "Agentes naturales distintos:",
    f"{dim_agente_source_df.select('codigo_agente').distinct().count():,}",
)

# COMMAND ----------

source_agent_rows = (
    dim_agente_source_df.count()
)


source_agent_keys = (
    dim_agente_source_df
    .select(
        "codigo_agente",
        "fecha_inicio",
    )
    .distinct()
    .count()
)


invalid_date_ranges = (
    dim_agente_source_df
    .filter(
        F.col("fecha_fin")
        < F.col("fecha_inicio")
    )
    .count()
)


multiple_current_agents = (
    dim_agente_source_df
    .filter(F.col("es_actual"))
    .groupBy("codigo_agente")
    .count()
    .filter(F.col("count") > 1)
    .count()
)


agents_without_current = (
    dim_agente_source_df
    .groupBy("codigo_agente")
    .agg(
        F.sum(
            F.when(
                F.col("es_actual"),
                1,
            ).otherwise(0)
        ).alias("versiones_actuales")
    )
    .filter(
        F.col("versiones_actuales") != 1
    )
    .count()
)


print("Versiones fuente:", source_agent_rows)
print("Claves SCD2 distintas:", source_agent_keys)
print("Rangos inválidos:", invalid_date_ranges)
print(
    "Agentes con múltiples versiones actuales:",
    multiple_current_agents,
)
print(
    "Agentes sin exactamente una versión actual:",
    agents_without_current,
)


if source_agent_rows != source_agent_keys:
    raise ValueError(
        "La fuente de dim_agente contiene "
        "duplicados por codigo_agente y fecha_inicio."
    )


if invalid_date_ranges > 0:
    raise ValueError(
        "Existen versiones con fecha_fin anterior "
        "a fecha_inicio."
    )


if (
    multiple_current_agents > 0
    or agents_without_current > 0
):
    raise ValueError(
        "Cada agente debe tener exactamente "
        "una versión actual."
    )


print("Fuente SCD2 de agentes aprobada.")

# COMMAND ----------

# MAGIC %md
# MAGIC Merge DIM Agente

# COMMAND ----------

dim_agente_target = DeltaTable.forName(
    spark,
    DIM_AGENTE_TABLE,
)


(
    dim_agente_target.alias("target")
    .merge(
        dim_agente_source_df.alias("source"),
        """
        target.codigo_agente =
            source.codigo_agente
        AND target.fecha_inicio =
            source.fecha_inicio
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.nombre_agente
                <=> source.nombre_agente
            )
            OR NOT (
                target.nombre_agente_normalizado
                <=> source.nombre_agente_normalizado
            )
            OR NOT (
                target.actividad_agente
                <=> source.actividad_agente
            )
            OR NOT (
                target.actividad_normalizada
                <=> source.actividad_normalizada
            )
            OR NOT (
                target.numero_version
                <=> source.numero_version
            )
            OR NOT (
                target.fecha_fin
                <=> source.fecha_fin
            )
            OR NOT (
                target.es_actual
                <=> source.es_actual
            )
        """,
        set={
            "nombre_agente":
                "source.nombre_agente",

            "nombre_agente_normalizado":
                "source.nombre_agente_normalizado",

            "actividad_agente":
                "source.actividad_agente",

            "actividad_normalizada":
                "source.actividad_normalizada",

            "numero_version":
                "source.numero_version",

            "fecha_fin":
                "source.fecha_fin",

            "es_actual":
                "source.es_actual",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsert(
        values={
            "codigo_agente":
                "source.codigo_agente",

            "nombre_agente":
                "source.nombre_agente",

            "nombre_agente_normalizado":
                "source.nombre_agente_normalizado",

            "actividad_agente":
                "source.actividad_agente",

            "actividad_normalizada":
                "source.actividad_normalizada",

            "numero_version":
                "source.numero_version",

            "fecha_inicio":
                "source.fecha_inicio",

            "fecha_fin":
                "source.fecha_fin",

            "es_actual":
                "source.es_actual",

            "fecha_creacion":
                "source.fecha_creacion",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedBySourceDelete()
    .execute()
)


print("MERGE de dim_agente completado.")

# COMMAND ----------

dim_agente_validation_df = spark.table(
    DIM_AGENTE_TABLE
)


total_agent_versions = (
    dim_agente_validation_df.count()
)


distinct_agent_version_keys = (
    dim_agente_validation_df
    .select(
        "codigo_agente",
        "fecha_inicio",
    )
    .distinct()
    .count()
)


distinct_agents = (
    dim_agente_validation_df
    .select("codigo_agente")
    .distinct()
    .count()
)


current_agent_rows = (
    dim_agente_validation_df
    .filter("es_actual = true")
    .count()
)


invalid_current_agents = (
    dim_agente_validation_df
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


overlapping_agent_versions = (
    dim_agente_validation_df.alias("a")
    .join(
        dim_agente_validation_df.alias("b"),
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


print(
    "Versiones en dim_agente:",
    f"{total_agent_versions:,}",
)

print(
    "Claves codigo+inicio distintas:",
    f"{distinct_agent_version_keys:,}",
)

print(
    "Agentes distintos:",
    f"{distinct_agents:,}",
)

print(
    "Versiones actuales:",
    f"{current_agent_rows:,}",
)

print(
    "Agentes con estado actual inválido:",
    f"{invalid_current_agents:,}",
)

print(
    "Solapamientos de vigencia:",
    f"{overlapping_agent_versions:,}",
)


if total_agent_versions != distinct_agent_version_keys:
    raise ValueError(
        "dim_agente contiene claves SCD2 duplicadas."
    )


if current_agent_rows != distinct_agents:
    raise ValueError(
        "La cantidad de versiones actuales no "
        "coincide con los agentes distintos."
    )


if invalid_current_agents > 0:
    raise ValueError(
        "Existen agentes sin exactamente una "
        "versión actual."
    )


if overlapping_agent_versions > 0:
    raise ValueError(
        "Existen vigencias solapadas en dim_agente."
    )


display(
    dim_agente_validation_df
    .orderBy(
        "codigo_agente",
        "fecha_inicio",
    )
    .limit(30)
)


print("dim_agente aprobada.")

# COMMAND ----------

# MAGIC %md
# MAGIC Preparar maestro oficial de plantas

# COMMAND ----------

silver_plants_table = SILVER_TABLES["plantas"]


plant_master_df = (
    spark.table(silver_plants_table)
    .select(
        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.trim(
            F.regexp_replace(
                F.col("nombre_planta"),
                r"\s+",
                " ",
            )
        ).alias("nombre_planta"),

        F.upper(
            F.trim("codigo_sic_agente")
        ).alias("codigo_sic_agente"),

        F.col("cap_efectiva_neta")
        .cast("decimal(24,6)")
        .alias("cap_efectiva_neta"),

        F.to_date("fpo").alias("fpo"),

        F.upper(
            F.trim("codigo_sub_area_operativa")
        ).alias("codigo_sub_area_operativa"),

        F.upper(
            F.trim("codigo_area_operativa")
        ).alias("codigo_area_operativa"),

        F.upper(
            F.trim("tipo_despacho_recurso")
        ).alias("tipo_despacho_recurso"),

        F.upper(
            F.trim("tipo_clasificacion")
        ).alias("tipo_clasificacion"),

        F.upper(
            F.trim("tipo_generacion")
        ).alias("tipo_generacion"),

        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
)


invalid_master_plants_df = (
    plant_master_df
    .filter(
        F.col("codigo_planta").isNull()
        | (F.col("codigo_planta") == "")
        | F.col("nombre_planta").isNull()
        | (F.col("nombre_planta") == "")
    )
)


invalid_master_plants = (
    invalid_master_plants_df.count()
)


print(
    "Plantas en maestro Silver:",
    f"{plant_master_df.count():,}",
)

print(
    "Plantas inválidas:",
    f"{invalid_master_plants:,}",
)


if invalid_master_plants > 0:
    display(
        invalid_master_plants_df.limit(100)
    )

    raise ValueError(
        "Existen plantas del maestro con "
        "código o nombre inválido."
    )

# COMMAND ----------

duplicate_master_plants_df = (
    plant_master_df
    .groupBy("codigo_planta")
    .count()
    .filter(F.col("count") > 1)
)


duplicate_master_plants = (
    duplicate_master_plants_df.count()
)


print(
    "Códigos duplicados en maestro:",
    f"{duplicate_master_plants:,}",
)


if duplicate_master_plants > 0:
    display(
        duplicate_master_plants_df.limit(100)
    )

    raise ValueError(
        "silver.plantas debe contener una sola "
        "fila vigente por codigo_planta."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC obtener codigos operativos

# COMMAND ----------

generation_plant_codes_df = (
    spark.table(
        SILVER_TABLES["generacion_real"]
    )
    .select(
        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.to_date("fecha_hora").alias(
            "fecha_observacion"
        ),
    )
    .filter(
        F.col("codigo_planta").isNotNull()
        & (F.col("codigo_planta") != "")
    )
    .withColumn(
        "origen_observacion",
        F.lit("GENERACION_REAL"),
    )
)


availability_plant_codes_df = (
    spark.table(
        SILVER_TABLES["disponibilidad_plantas"]
    )
    .select(
        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.to_date("fecha_hora").alias(
            "fecha_observacion"
        ),
    )
    .filter(
        F.col("codigo_planta").isNotNull()
        & (F.col("codigo_planta") != "")
    )
    .withColumn(
        "origen_observacion",
        F.lit("DISPONIBILIDAD_PLANTAS"),
    )
)


reservoir_plant_codes_df = (
    spark.table(
        SILVER_TABLES["niveles_embalses"]
    )
    .select(
        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.to_date("fecha_inicio").alias(
            "fecha_observacion"
        ),
    )
    .filter(
        F.col("codigo_planta").isNotNull()
        & (F.col("codigo_planta") != "")
    )
    .withColumn(
        "origen_observacion",
        F.lit("NIVELES_EMBALSES"),
    )
)


operational_plant_observations_df = (
    generation_plant_codes_df
    .unionByName(
        availability_plant_codes_df
    )
    .unionByName(
        reservoir_plant_codes_df
    )
)


print(
    "Observaciones operativas de plantas:",
    f"{operational_plant_observations_df.count():,}",
)

# COMMAND ----------

operational_plant_summary_df = (
    operational_plant_observations_df
    .groupBy("codigo_planta")
    .agg(
        F.min("fecha_observacion").alias(
            "fecha_primera_observacion"
        ),

        F.max("fecha_observacion").alias(
            "fecha_ultima_observacion"
        ),

        F.array_sort(
            F.collect_set(
                "origen_observacion"
            )
        ).alias(
            "origenes_observacion"
        ),
    )
    .withColumn(
        "origen_registro",
        F.concat_ws(
            ",",
            F.col("origenes_observacion"),
        ),
    )
    .drop("origenes_observacion")
)


print(
    "Códigos operativos distintos:",
    f"{operational_plant_summary_df.count():,}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC preparar plantas oficiales

# COMMAND ----------

official_plants_df = (
    plant_master_df.alias("master")
    .join(
        operational_plant_summary_df.alias("ops"),
        F.col("master.codigo_planta")
        ==
        F.col("ops.codigo_planta"),
        "left",
    )
    .select(
        F.col(
            "master.codigo_planta"
        ).alias(
            "codigo_planta"
        ),

        F.col(
            "master.nombre_planta"
        ).alias(
            "nombre_planta"
        ),

        F.col(
            "master.codigo_sic_agente"
        ).alias(
            "codigo_sic_agente"
        ),

        F.col(
            "master.cap_efectiva_neta"
        ).alias(
            "cap_efectiva_neta"
        ),

        F.col(
            "master.fpo"
        ).alias("fpo"),

        F.col(
            "master.codigo_sub_area_operativa"
        ).alias(
            "codigo_sub_area_operativa"
        ),

        F.col(
            "master.codigo_area_operativa"
        ).alias(
            "codigo_area_operativa"
        ),

        F.col(
            "master.tipo_despacho_recurso"
        ).alias(
            "tipo_despacho_recurso"
        ),

        F.col(
            "master.tipo_clasificacion"
        ).alias(
            "tipo_clasificacion"
        ),

        F.col(
            "master.tipo_generacion"
        ).alias(
            "tipo_generacion"
        ),

        F.lit(False).alias(
            "es_registro_inferido"
        ),

        F.lit("MAESTRO_PLANTAS").alias(
            "origen_registro"
        ),

        F.lit(True).alias(
            "esta_en_maestro_actual"
        ),

        F.coalesce(
            F.col(
                "ops.fecha_primera_observacion"
            ),
            F.col(
                "master.load_date"
            ),
        ).alias(
            "fecha_primera_observacion"
        ),

        F.coalesce(
            F.col(
                "ops.fecha_ultima_observacion"
            ),
            F.col(
                "master.load_date"
            ),
        ).alias(
            "fecha_ultima_observacion"
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC preparar miembros inferidos

# COMMAND ----------

official_plants_df = (
    plant_master_df.alias("master")
    .join(
        operational_plant_summary_df.alias("ops"),
        F.col("master.codigo_planta")
        ==
        F.col("ops.codigo_planta"),
        "left",
    )
    .select(
        F.col(
            "master.codigo_planta"
        ).alias(
            "codigo_planta"
        ),

        F.col(
            "master.nombre_planta"
        ).alias(
            "nombre_planta"
        ),

        F.col(
            "master.codigo_sic_agente"
        ).alias(
            "codigo_sic_agente"
        ),

        F.col(
            "master.cap_efectiva_neta"
        ).alias(
            "cap_efectiva_neta"
        ),

        F.col(
            "master.fpo"
        ).alias("fpo"),

        F.col(
            "master.codigo_sub_area_operativa"
        ).alias(
            "codigo_sub_area_operativa"
        ),

        F.col(
            "master.codigo_area_operativa"
        ).alias(
            "codigo_area_operativa"
        ),

        F.col(
            "master.tipo_despacho_recurso"
        ).alias(
            "tipo_despacho_recurso"
        ),

        F.col(
            "master.tipo_clasificacion"
        ).alias(
            "tipo_clasificacion"
        ),

        F.col(
            "master.tipo_generacion"
        ).alias(
            "tipo_generacion"
        ),

        F.lit(False).alias(
            "es_registro_inferido"
        ),

        F.lit("MAESTRO_PLANTAS").alias(
            "origen_registro"
        ),

        F.lit(True).alias(
            "esta_en_maestro_actual"
        ),

        F.coalesce(
            F.col(
                "ops.fecha_primera_observacion"
            ),
            F.col(
                "master.load_date"
            ),
        ).alias(
            "fecha_primera_observacion"
        ),

        F.coalesce(
            F.col(
                "ops.fecha_ultima_observacion"
            ),
            F.col(
                "master.load_date"
            ),
        ).alias(
            "fecha_ultima_observacion"
        ),
    )
)

# COMMAND ----------

master_plant_codes_df = (
    plant_master_df
    .select("codigo_planta")
    .distinct()
)


inferred_plants_df = (
    operational_plant_summary_df.alias("ops")
    .join(
        master_plant_codes_df.alias("master"),
        F.col("ops.codigo_planta")
        ==
        F.col("master.codigo_planta"),
        "left_anti",
    )
    .select(
        F.col("ops.codigo_planta"),

        F.concat(
            F.lit("RECURSO SIN MAESTRO - "),
            F.col("ops.codigo_planta"),
        ).alias("nombre_planta"),

        F.lit(None)
        .cast("string")
        .alias("codigo_sic_agente"),

        F.lit(None)
        .cast("decimal(24,6)")
        .alias("cap_efectiva_neta"),

        F.lit(None)
        .cast("date")
        .alias("fpo"),

        F.lit(None)
        .cast("string")
        .alias("codigo_sub_area_operativa"),

        F.lit(None)
        .cast("string")
        .alias("codigo_area_operativa"),

        F.lit(None)
        .cast("string")
        .alias("tipo_despacho_recurso"),

        F.lit(None)
        .cast("string")
        .alias("tipo_clasificacion"),

        F.lit(None)
        .cast("string")
        .alias("tipo_generacion"),

        F.lit(True).alias(
            "es_registro_inferido"
        ),

        F.col(
            "ops.origen_registro"
        ).alias(
            "origen_registro"
        ),

        F.lit(False).alias(
            "esta_en_maestro_actual"
        ),

        F.col(
            "ops.fecha_primera_observacion"
        ),

        F.col(
            "ops.fecha_ultima_observacion"
        ),
    )
)


print(
    "Miembros inferidos preparados:",
    f"{inferred_plants_df.count():,}",
)

# COMMAND ----------

# MAGIC %md
# MAGIC construir fuente completa dim planta

# COMMAND ----------

dim_planta_source_df = (
    official_plants_df
    .unionByName(
        inferred_plants_df
    )
    .withColumn(
        "fecha_creacion",
        F.current_timestamp(),
    )
    .withColumn(
        "fecha_actualizacion",
        F.current_timestamp(),
    )
)


total_source_plants = (
    dim_planta_source_df.count()
)


distinct_source_plants = (
    dim_planta_source_df
    .select("codigo_planta")
    .distinct()
    .count()
)


inferred_source_plants = (
    dim_planta_source_df
    .filter("es_registro_inferido = true")
    .count()
)


official_source_plants = (
    dim_planta_source_df
    .filter(
        "esta_en_maestro_actual = true"
    )
    .count()
)


print(
    "Total plantas fuente:",
    f"{total_source_plants:,}",
)

print(
    "Códigos distintos:",
    f"{distinct_source_plants:,}",
)

print(
    "Plantas oficiales:",
    f"{official_source_plants:,}",
)

print(
    "Miembros inferidos:",
    f"{inferred_source_plants:,}",
)


if (
    total_source_plants
    != distinct_source_plants
):
    raise ValueError(
        "La fuente de dim_planta contiene "
        "codigo_planta duplicados."
    )

# COMMAND ----------

invalid_inferred_plants = (
    dim_planta_source_df
    .filter(
        F.col("es_registro_inferido")
        &
        (
            F.col("esta_en_maestro_actual")
            |
            F.col("nombre_planta").isNull()
            |
            F.col(
                "fecha_primera_observacion"
            ).isNull()
            |
            F.col(
                "fecha_ultima_observacion"
            ).isNull()
        )
    )
    .count()
)


invalid_official_plants = (
    dim_planta_source_df
    .filter(
        (~F.col("es_registro_inferido"))
        &
        (
            ~F.col("esta_en_maestro_actual")
            |
            F.col("nombre_planta").isNull()
        )
    )
    .count()
)


invalid_observation_ranges = (
    dim_planta_source_df
    .filter(
        F.col("fecha_ultima_observacion")
        <
        F.col("fecha_primera_observacion")
    )
    .count()
)


print(
    "Inferidas inválidas:",
    invalid_inferred_plants,
)

print(
    "Oficiales inválidas:",
    invalid_official_plants,
)

print(
    "Rangos de observación inválidos:",
    invalid_observation_ranges,
)


if (
    invalid_inferred_plants > 0
    or invalid_official_plants > 0
    or invalid_observation_ranges > 0
):
    raise ValueError(
        "La fuente de dim_planta no superó "
        "las validaciones de calidad."
    )


print("Fuente de dim_planta aprobada.")

# COMMAND ----------

dim_planta_target = DeltaTable.forName(
    spark,
    DIM_PLANTA_TABLE,
)


(
    dim_planta_target.alias("target")
    .merge(
        dim_planta_source_df.alias("source"),
        """
        target.codigo_planta =
            source.codigo_planta
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
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
                target.es_registro_inferido
                <=> source.es_registro_inferido
            )
            OR NOT (
                target.origen_registro
                <=> source.origen_registro
            )
            OR NOT (
                target.esta_en_maestro_actual
                <=> source.esta_en_maestro_actual
            )
            OR NOT (
                target.fecha_primera_observacion
                <=> source.fecha_primera_observacion
            )
            OR NOT (
                target.fecha_ultima_observacion
                <=> source.fecha_ultima_observacion
            )
        """,
        set={
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

            "es_registro_inferido":
                "source.es_registro_inferido",

            "origen_registro":
                """
                CASE
                  WHEN target.es_registro_inferido = true
                   AND source.es_registro_inferido = false
                  THEN concat(
                    'INFERIDO:', coalesce(target.origen_registro, 'DESCONOCIDO'),
                    '->', source.origen_registro
                  )
                  ELSE source.origen_registro
                END
                """,

            "esta_en_maestro_actual":
                "source.esta_en_maestro_actual",

            "fecha_primera_observacion":
                "source.fecha_primera_observacion",

            "fecha_ultima_observacion":
                "source.fecha_ultima_observacion",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsert(
        values={
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

            "es_registro_inferido":
                "source.es_registro_inferido",

            "origen_registro":
                "source.origen_registro",

            "esta_en_maestro_actual":
                "source.esta_en_maestro_actual",

            "fecha_primera_observacion":
                "source.fecha_primera_observacion",

            "fecha_ultima_observacion":
                "source.fecha_ultima_observacion",

            "fecha_creacion":
                "source.fecha_creacion",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .execute()
)


print("MERGE de dim_planta completado.")

# COMMAND ----------

dim_planta_validation_df = spark.table(
    DIM_PLANTA_TABLE
)


missing_operational_plants_df = (
    operational_plant_summary_df.alias("ops")
    .join(
        dim_planta_validation_df.alias("dim"),
        F.col("ops.codigo_planta")
        ==
        F.col("dim.codigo_planta"),
        "left_anti",
    )
)


missing_operational_plants = (
    missing_operational_plants_df.count()
)


print(
    "Códigos operativos sin dim_planta:",
    missing_operational_plants,
)


if missing_operational_plants > 0:
    display(
        missing_operational_plants_df.limit(100)
    )

    raise ValueError(
        "Existen códigos operativos sin "
        "correspondencia en dim_planta."
    )

# COMMAND ----------

total_dim_plants = (
    dim_planta_validation_df.count()
)


distinct_dim_plants = (
    dim_planta_validation_df
    .select("codigo_planta")
    .distinct()
    .count()
)


official_dim_plants = (
    dim_planta_validation_df
    .filter(
        "esta_en_maestro_actual = true"
    )
    .count()
)


inferred_dim_plants = (
    dim_planta_validation_df
    .filter(
        "es_registro_inferido = true"
    )
    .count()
)


invalid_inferred_official_state = (
    dim_planta_validation_df
    .filter(
        F.col("es_registro_inferido")
        &
        F.col("esta_en_maestro_actual")
    )
    .count()
)


null_plant_keys = (
    dim_planta_validation_df
    .filter(
        F.col("planta_key").isNull()
    )
    .count()
)


print(
    "Plantas en dim_planta:",
    f"{total_dim_plants:,}",
)

print(
    "Códigos distintos:",
    f"{distinct_dim_plants:,}",
)

print(
    "Plantas oficiales:",
    f"{official_dim_plants:,}",
)

print(
    "Miembros inferidos:",
    f"{inferred_dim_plants:,}",
)

print(
    "Inferidas marcadas como oficiales:",
    invalid_inferred_official_state,
)

print(
    "planta_key nulas:",
    null_plant_keys,
)

print(
    "Códigos operativos sin dimensión:",
    missing_operational_plants,
)


if total_dim_plants != distinct_dim_plants:
    raise ValueError(
        "dim_planta contiene códigos duplicados."
    )


if invalid_inferred_official_state > 0:
    raise ValueError(
        "Hay plantas marcadas simultáneamente "
        "como inferidas y oficiales."
    )


if null_plant_keys > 0:
    raise ValueError(
        "Existen plantas sin planta_key."
    )


if missing_operational_plants > 0:
    raise ValueError(
        "La cobertura operativa de dim_planta "
        "no es completa."
    )


display(
    dim_planta_validation_df
    .groupBy(
        "es_registro_inferido",
        "origen_registro",
    )
    .count()
    .orderBy(
        "es_registro_inferido",
        F.desc("count"),
    )
)


print("dim_planta aprobada.")

# COMMAND ----------

# MAGIC %md
# MAGIC Preparar fuente Embalses

# COMMAND ----------

silver_reservoirs_table = SILVER_TABLES["embalses"]


dim_embalse_source_df = (
    spark.table(silver_reservoirs_table)
    .select(
        F.upper(
            F.trim("codigo_embalse")
        ).alias("codigo_embalse"),

        F.trim(
            F.regexp_replace(
                F.col("nombre_embalse"),
                r"\s+",
                " ",
            )
        ).alias("nombre_embalse"),

        F.col("latitud")
        .cast("decimal(10,7)")
        .alias("latitud"),

        F.col("longitud")
        .cast("decimal(11,7)")
        .alias("longitud"),

        F.upper(
            F.trim("tipo_coordenada")
        ).alias("tipo_coordenada"),

        F.trim(
            F.col("fuente_coordenada")
        ).alias("fuente_coordenada"),

        F.upper(
            F.trim("estado_geocodificacion")
        ).alias("estado_geocodificacion"),

        F.trim(
            F.col("consulta_geocodificacion")
        ).alias("consulta_geocodificacion"),

        F.coalesce(
            F.col("coordenadas_validas"),
            F.lit(False),
        ).alias("coordenadas_validas"),

        F.coalesce(
            F.col("requiere_revision_manual"),
            F.lit(True),
        ).alias("requiere_revision_manual"),

        F.col("source_file_name"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp"),

        F.col("load_date").alias(
            "silver_load_date"
        ),
    )
    .withColumn(
        "nombre_embalse_normalizado",
        normalize_dimension_text(
            F.col("nombre_embalse")
        ),
    )
    .withColumn(
        "fecha_creacion",
        F.current_timestamp(),
    )
    .withColumn(
        "fecha_actualizacion",
        F.current_timestamp(),
    )
)


print(
    "Embalses Silver preparados:",
    f"{dim_embalse_source_df.count():,}",
)

# COMMAND ----------

invalid_reservoirs_df = (
    dim_embalse_source_df
    .filter(
        F.col("codigo_embalse").isNull()
        | (F.col("codigo_embalse") == "")
        | F.col("nombre_embalse").isNull()
        | (F.col("nombre_embalse") == "")
        | F.col(
            "nombre_embalse_normalizado"
        ).isNull()
        | (
            F.col(
                "nombre_embalse_normalizado"
            )
            == ""
        )
    )
)


invalid_reservoirs = (
    invalid_reservoirs_df.count()
)


print(
    "Embalses con código o nombre inválido:",
    invalid_reservoirs,
)


if invalid_reservoirs > 0:
    display(
        invalid_reservoirs_df.limit(100)
    )

    raise ValueError(
        "Existen embalses con código o nombre "
        "obligatorio inválido."
    )

# COMMAND ----------

reservoir_source_rows = (
    dim_embalse_source_df.count()
)


reservoir_source_keys = (
    dim_embalse_source_df
    .select("codigo_embalse")
    .distinct()
    .count()
)


duplicate_reservoirs_df = (
    dim_embalse_source_df
    .groupBy("codigo_embalse")
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_reservoirs = (
    duplicate_reservoirs_df.count()
)


print(
    "Embalses fuente:",
    reservoir_source_rows,
)

print(
    "Códigos distintos:",
    reservoir_source_keys,
)

print(
    "Códigos duplicados:",
    duplicate_reservoirs,
)


if (
    reservoir_source_rows
    != reservoir_source_keys
):
    display(
        duplicate_reservoirs_df
    )

    raise ValueError(
        "La fuente de dim_embalse contiene "
        "codigo_embalse duplicados."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC validar coordenadas

# COMMAND ----------

invalid_coordinate_range_df = (
    dim_embalse_source_df
    .filter(
        (
            F.col("latitud").isNotNull()
            &
            (
                (F.col("latitud") < -90)
                | (F.col("latitud") > 90)
            )
        )
        |
        (
            F.col("longitud").isNotNull()
            &
            (
                (F.col("longitud") < -180)
                | (F.col("longitud") > 180)
            )
        )
    )
)


invalid_coordinate_ranges = (
    invalid_coordinate_range_df.count()
)


inconsistent_valid_coordinates_df = (
    dim_embalse_source_df
    .filter(
        F.col("coordenadas_validas")
        &
        (
            F.col("latitud").isNull()
            | F.col("longitud").isNull()
            | (F.col("latitud") < -90)
            | (F.col("latitud") > 90)
            | (F.col("longitud") < -180)
            | (F.col("longitud") > 180)
        )
    )
)


inconsistent_valid_coordinates = (
    inconsistent_valid_coordinates_df.count()
)


valid_coordinates_requiring_review = (
    dim_embalse_source_df
    .filter(
        F.col("coordenadas_validas")
        &
        F.col("requiere_revision_manual")
    )
    .count()
)


print(
    "Coordenadas fuera de rango:",
    invalid_coordinate_ranges,
)

print(
    "Marcadas válidas pero inconsistentes:",
    inconsistent_valid_coordinates,
)

print(
    "Coordenadas válidas que requieren revisión:",
    valid_coordinates_requiring_review,
)


if invalid_coordinate_ranges > 0:
    display(
        invalid_coordinate_range_df
    )

    raise ValueError(
        "Existen coordenadas fuera del rango "
        "geográfico permitido."
    )


if inconsistent_valid_coordinates > 0:
    display(
        inconsistent_valid_coordinates_df
    )

    raise ValueError(
        "Existen embalses marcados con coordenadas "
        "válidas, pero sus coordenadas son nulas "
        "o están fuera de rango."
    )

# COMMAND ----------

display(
    dim_embalse_source_df
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
            "requieren_revision_manual"
        ),

        F.sum(
            F.when(
                F.col("latitud").isNull()
                | F.col("longitud").isNull(),
                1,
            ).otherwise(0)
        ).alias(
            "coordenadas_nulas"
        ),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge Dim Embalse

# COMMAND ----------

dim_embalse_target = DeltaTable.forName(
    spark,
    DIM_EMBALSE_TABLE,
)


(
    dim_embalse_target.alias("target")
    .merge(
        dim_embalse_source_df.alias("source"),
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
                target.nombre_embalse_normalizado
                <=> source.nombre_embalse_normalizado
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
                target.silver_load_date
                <=> source.silver_load_date
            )
        """,
        set={
            "nombre_embalse":
                "source.nombre_embalse",

            "nombre_embalse_normalizado":
                "source.nombre_embalse_normalizado",

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

            "silver_load_date":
                "source.silver_load_date",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsert(
        values={
            "codigo_embalse":
                "source.codigo_embalse",

            "nombre_embalse":
                "source.nombre_embalse",

            "nombre_embalse_normalizado":
                "source.nombre_embalse_normalizado",

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

            "silver_load_date":
                "source.silver_load_date",

            "fecha_creacion":
                "source.fecha_creacion",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .execute()
)


print("MERGE de dim_embalse completado.")

# COMMAND ----------

dim_embalse_validation_df = spark.table(
    DIM_EMBALSE_TABLE
)


total_dim_reservoirs = (
    dim_embalse_validation_df.count()
)


distinct_dim_reservoirs = (
    dim_embalse_validation_df
    .select("codigo_embalse")
    .distinct()
    .count()
)


null_reservoir_keys = (
    dim_embalse_validation_df
    .filter(
        F.col("embalse_key").isNull()
    )
    .count()
)


valid_dim_coordinates = (
    dim_embalse_validation_df
    .filter(
        F.col("coordenadas_validas")
    )
    .count()
)


manual_review_reservoirs = (
    dim_embalse_validation_df
    .filter(
        F.col("requiere_revision_manual")
    )
    .count()
)


invalid_dim_coordinate_ranges = (
    dim_embalse_validation_df
    .filter(
        (
            F.col("latitud").isNotNull()
            &
            (
                (F.col("latitud") < -90)
                | (F.col("latitud") > 90)
            )
        )
        |
        (
            F.col("longitud").isNotNull()
            &
            (
                (F.col("longitud") < -180)
                | (F.col("longitud") > 180)
            )
        )
    )
    .count()
)


print(
    "Embalses en dim_embalse:",
    total_dim_reservoirs,
)

print(
    "Códigos distintos:",
    distinct_dim_reservoirs,
)

print(
    "embalse_key nulas:",
    null_reservoir_keys,
)

print(
    "Coordenadas válidas:",
    valid_dim_coordinates,
)

print(
    "Requieren revisión manual:",
    manual_review_reservoirs,
)

print(
    "Coordenadas fuera de rango:",
    invalid_dim_coordinate_ranges,
)


if (
    total_dim_reservoirs
    != distinct_dim_reservoirs
):
    raise ValueError(
        "dim_embalse contiene códigos duplicados."
    )


if null_reservoir_keys > 0:
    raise ValueError(
        "Existen embalses sin embalse_key."
    )


if invalid_dim_coordinate_ranges > 0:
    raise ValueError(
        "dim_embalse contiene coordenadas "
        "fuera de rango."
    )


display(
    dim_embalse_validation_df
    .orderBy("codigo_embalse")
)


print("dim_embalse aprobada.")

# COMMAND ----------

# MAGIC %md
# MAGIC Preparar fuente bridge

# COMMAND ----------

silver_bridge_table = SILVER_TABLES[
    "plantas_reservorios"
]


silver_bridge_df = (
    spark.table(silver_bridge_table)
    .filter(F.col("activo"))
    .select(
        F.upper(
            F.trim("codigo_planta")
        ).alias("codigo_planta"),

        F.upper(
            F.trim("codigo_embalse")
        ).alias("codigo_embalse"),

        F.trim(
            F.col("region")
        ).alias("region"),

        F.trim(
            F.col("nombre_planta")
        ).alias(
            "nombre_planta_fuente"
        ),

        F.trim(
            F.col("nombre_reservorio")
        ).alias(
            "nombre_reservorio_fuente"
        ),

        F.upper(
            F.trim("tipo_relacion")
        ).alias("tipo_relacion"),

        F.coalesce(
            F.col("es_principal"),
            F.lit(False),
        ).alias("es_principal"),

        F.coalesce(
            F.col("permite_atribucion"),
            F.lit(False),
        ).alias("permite_atribucion"),

        F.upper(
            F.trim("fuente_relacion")
        ).alias("fuente_relacion"),

        F.upper(
            F.trim("estado_validacion")
        ).alias("estado_validacion"),

        F.to_date(
            "valido_desde"
        ).alias("valido_desde"),

        F.to_date(
            "valido_hasta"
        ).alias("valido_hasta"),

        F.coalesce(
            F.col(
                "requiere_revision_manual"
            ),
            F.lit(True),
        ).alias(
            "requiere_revision_manual"
        ),

        F.coalesce(
            F.col("relacion_completa"),
            F.lit(False),
        ).alias(
            "relacion_completa"
        ),
    )
)


print(
    "Relaciones Silver disponibles:",
    silver_bridge_df.count(),
)

# COMMAND ----------

invalid_bridge_rows_df = (
    silver_bridge_df
    .filter(
        F.col("codigo_planta").isNull()
        | (F.col("codigo_planta") == "")
        | F.col("codigo_embalse").isNull()
        | (F.col("codigo_embalse") == "")
        | (~F.col("relacion_completa"))
        | F.col("requiere_revision_manual")
    )
)


invalid_bridge_rows = (
    invalid_bridge_rows_df.count()
)


print(
    "Relaciones incompletas o pendientes:",
    invalid_bridge_rows,
)


if invalid_bridge_rows > 0:
    display(
        invalid_bridge_rows_df
    )

    raise ValueError(
        "Existen relaciones planta-embalse "
        "incompletas o pendientes de revisión."
    )

# COMMAND ----------

bridge_with_dimensions_df = (
    silver_bridge_df.alias("source")
    .join(
        spark.table(
            DIM_PLANTA_TABLE
        )
        .select(
            "planta_key",
            "codigo_planta",
        )
        .alias("plant"),
        F.col("source.codigo_planta")
        ==
        F.col("plant.codigo_planta"),
        "left",
    )
    .join(
        spark.table(
            DIM_EMBALSE_TABLE
        )
        .select(
            "embalse_key",
            "codigo_embalse",
        )
        .alias("reservoir"),
        F.col("source.codigo_embalse")
        ==
        F.col("reservoir.codigo_embalse"),
        "left",
    )
    .select(
        F.col("source.*"),
        F.col("plant.planta_key"),
        F.col("reservoir.embalse_key"),
    )
)


missing_bridge_dimensions_df = (
    bridge_with_dimensions_df
    .filter(
        F.col("planta_key").isNull()
        | F.col("embalse_key").isNull()
    )
)


missing_bridge_dimensions = (
    missing_bridge_dimensions_df.count()
)


print(
    "Relaciones sin dimensión:",
    missing_bridge_dimensions,
)


if missing_bridge_dimensions > 0:
    display(
        missing_bridge_dimensions_df
    )

    raise ValueError(
        "Existen relaciones sin planta_key "
        "o embalse_key."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC calcular cardinalidad por planta

# COMMAND ----------

reservoir_count_by_plant_df = (
    bridge_with_dimensions_df
    .groupBy(
        "planta_key",
        "codigo_planta",
    )
    .agg(
        F.countDistinct(
            "embalse_key"
        ).cast("int").alias(
            "cantidad_embalses_planta"
        )
    )
)


bridge_source_df = (
    bridge_with_dimensions_df.alias("relation")
    .join(
        reservoir_count_by_plant_df.alias("count"),
        (
            F.col("relation.planta_key")
            ==
            F.col("count.planta_key")
        ),
        "inner",
    )
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.col(
                    "relation.codigo_planta"
                ),
                F.col(
                    "relation.codigo_embalse"
                ),
            ),
            256,
        ).alias(
            "planta_embalse_key"
        ),

        F.col("relation.planta_key"),
        F.col("relation.embalse_key"),

        F.col("relation.codigo_planta"),
        F.col("relation.codigo_embalse"),

        F.col("relation.region"),

        F.col(
            "relation.nombre_planta_fuente"
        ),

        F.col(
            "relation.nombre_reservorio_fuente"
        ),

        F.col("relation.tipo_relacion"),
        F.col("relation.es_principal"),
        F.col("relation.permite_atribucion"),
        F.col("relation.fuente_relacion"),
        F.col("relation.estado_validacion"),
        F.col("relation.valido_desde"),
        F.col("relation.valido_hasta"),

        F.col(
            "count.cantidad_embalses_planta"
        ),

        (
            F.col(
                "count.cantidad_embalses_planta"
            )
            == F.lit(1)
        ).alias(
            "es_relacion_unica"
        ),

        F.col(
            "relation.requiere_revision_manual"
        ),

        F.current_timestamp().alias(
            "fecha_creacion"
        ),

        F.current_timestamp().alias(
            "fecha_actualizacion"
        ),

        F.lit(True).alias("activo"),
        F.lit(None).cast("timestamp").alias("fecha_retiro"),
    )
)

# COMMAND ----------

bridge_source_rows = (
    bridge_source_df.count()
)


bridge_distinct_keys = (
    bridge_source_df
    .select("planta_embalse_key")
    .distinct()
    .count()
)


bridge_distinct_pairs = (
    bridge_source_df
    .select(
        "codigo_planta",
        "codigo_embalse",
    )
    .distinct()
    .count()
)


invalid_bridge_ranges = (
    bridge_source_df
    .filter(
        F.col("valido_desde").isNotNull()
        & F.col("valido_hasta").isNotNull()
        & (
            F.col("valido_hasta")
            <
            F.col("valido_desde")
        )
    )
    .count()
)


invalid_unique_flags = (
    bridge_source_df
    .filter(
        (
            F.col("es_relacion_unica")
            &
            (
                F.col(
                    "cantidad_embalses_planta"
                )
                != 1
            )
        )
        |
        (
            (~F.col("es_relacion_unica"))
            &
            (
                F.col(
                    "cantidad_embalses_planta"
                )
                == 1
            )
        )
    )
    .count()
)


print("Relaciones fuente:", bridge_source_rows)
print("Claves distintas:", bridge_distinct_keys)
print("Pares distintos:", bridge_distinct_pairs)
print("Rangos inválidos:", invalid_bridge_ranges)
print(
    "Indicadores de cardinalidad inválidos:",
    invalid_unique_flags,
)


if not (
    bridge_source_rows
    == bridge_distinct_keys
    == bridge_distinct_pairs
):
    raise ValueError(
        "La fuente del bridge contiene "
        "relaciones duplicadas."
    )


if invalid_bridge_ranges > 0:
    raise ValueError(
        "Existen relaciones con rangos "
        "de vigencia inválidos."
    )


if invalid_unique_flags > 0:
    raise ValueError(
        "La cardinalidad calculada del bridge "
        "es inconsistente."
    )


print(
    "Fuente de bridge_planta_embalse aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Merge del Bridge

# COMMAND ----------

bridge_target = DeltaTable.forName(
    spark,
    BRIDGE_PLANTA_EMBALSE_TABLE,
)


(
    bridge_target.alias("target")
    .merge(
        bridge_source_df.alias("source"),
        """
        target.planta_embalse_key =
            source.planta_embalse_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.planta_key
                <=> source.planta_key
            )
            OR NOT (
                target.embalse_key
                <=> source.embalse_key
            )
            OR NOT (
                target.region
                <=> source.region
            )
            OR NOT (
                target.nombre_planta_fuente
                <=> source.nombre_planta_fuente
            )
            OR NOT (
                target.nombre_reservorio_fuente
                <=> source.nombre_reservorio_fuente
            )
            OR NOT (
                target.tipo_relacion
                <=> source.tipo_relacion
            )
            OR NOT (
                target.es_principal
                <=> source.es_principal
            )
            OR NOT (
                target.permite_atribucion
                <=> source.permite_atribucion
            )
            OR NOT (
                target.fuente_relacion
                <=> source.fuente_relacion
            )
            OR NOT (
                target.estado_validacion
                <=> source.estado_validacion
            )
            OR NOT (
                target.valido_desde
                <=> source.valido_desde
            )
            OR NOT (
                target.valido_hasta
                <=> source.valido_hasta
            )
            OR NOT (
                target.cantidad_embalses_planta
                <=> source.cantidad_embalses_planta
            )
            OR NOT (
                target.es_relacion_unica
                <=> source.es_relacion_unica
            )
            OR NOT (
                target.requiere_revision_manual
                <=> source.requiere_revision_manual
            )
            OR NOT (target.activo <=> true)
        """,
        set={
            "planta_key":
                "source.planta_key",

            "embalse_key":
                "source.embalse_key",

            "codigo_planta":
                "source.codigo_planta",

            "codigo_embalse":
                "source.codigo_embalse",

            "region":
                "source.region",

            "nombre_planta_fuente":
                "source.nombre_planta_fuente",

            "nombre_reservorio_fuente":
                "source.nombre_reservorio_fuente",

            "tipo_relacion":
                "source.tipo_relacion",

            "es_principal":
                "source.es_principal",

            "permite_atribucion":
                "source.permite_atribucion",

            "fuente_relacion":
                "source.fuente_relacion",

            "estado_validacion":
                "source.estado_validacion",

            "valido_desde":
                "source.valido_desde",

            "valido_hasta":
                "source.valido_hasta",

            "cantidad_embalses_planta":
                "source.cantidad_embalses_planta",

            "es_relacion_unica":
                "source.es_relacion_unica",

            "requiere_revision_manual":
                "source.requiere_revision_manual",

            "activo": "source.activo",
            "fecha_retiro": "source.fecha_retiro",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceUpdate(
        condition="target.activo = true",
        set={
            "activo": "false",
            "fecha_retiro": "current_timestamp()",
            "fecha_actualizacion": "current_timestamp()",
        },
    )
    .execute()
)


print(
    "MERGE de bridge_planta_embalse completado."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Las relaciones retiradas se conservan como inactivas por trazabilidad.

# COMMAND ----------

print("Las relaciones Gold retiradas permanecen con activo=false.")

# COMMAND ----------

bridge_validation_df = spark.table(
    BRIDGE_PLANTA_EMBALSE_TABLE
).filter(F.col("activo"))


total_bridge_rows = (
    bridge_validation_df.count()
)


distinct_bridge_keys = (
    bridge_validation_df
    .select("planta_embalse_key")
    .distinct()
    .count()
)


distinct_bridge_pairs = (
    bridge_validation_df
    .select(
        "codigo_planta",
        "codigo_embalse",
    )
    .distinct()
    .count()
)


bridge_plants = (
    bridge_validation_df
    .select("codigo_planta")
    .distinct()
    .count()
)


bridge_reservoirs = (
    bridge_validation_df
    .select("codigo_embalse")
    .distinct()
    .count()
)


unique_relationship_rows = (
    bridge_validation_df
    .filter("es_relacion_unica = true")
    .count()
)


multiple_relationship_rows = (
    bridge_validation_df
    .filter("es_relacion_unica = false")
    .count()
)


attributable_rows = (
    bridge_validation_df
    .filter("permite_atribucion = true")
    .count()
)


manual_review_bridge_rows = (
    bridge_validation_df
    .filter(
        "requiere_revision_manual = true"
    )
    .count()
)


null_bridge_dimensions = (
    bridge_validation_df
    .filter(
        F.col("planta_key").isNull()
        | F.col("embalse_key").isNull()
    )
    .count()
)


print("Relaciones en bridge:", total_bridge_rows)
print("Claves distintas:", distinct_bridge_keys)
print("Pares distintos:", distinct_bridge_pairs)
print("Plantas relacionadas:", bridge_plants)
print("Embalses relacionados:", bridge_reservoirs)
print(
    "Relaciones de plantas con un solo embalse:",
    unique_relationship_rows,
)
print(
    "Relaciones de plantas multirreservorio:",
    multiple_relationship_rows,
)
print(
    "Relaciones con atribución permitida:",
    attributable_rows,
)
print(
    "Relaciones pendientes de revisión:",
    manual_review_bridge_rows,
)
print(
    "Relaciones sin dimensión:",
    null_bridge_dimensions,
)


if not (
    total_bridge_rows
    == distinct_bridge_keys
    == distinct_bridge_pairs
):
    raise ValueError(
        "El bridge contiene relaciones duplicadas."
    )


if null_bridge_dimensions > 0:
    raise ValueError(
        "El bridge contiene relaciones sin "
        "dimensiones válidas."
    )


if manual_review_bridge_rows > 0:
    raise ValueError(
        "El bridge contiene relaciones pendientes "
        "de revisión manual."
    )


display(
    bridge_validation_df
    .orderBy(
        "codigo_planta",
        "codigo_embalse",
    )
)


print("bridge_planta_embalse aprobado.")

# COMMAND ----------

# MAGIC %md
# MAGIC Preparar Generación Silver

# COMMAND ----------

silver_generation_table = SILVER_TABLES[
    "generacion_real"
]


generation_base_df = (
    spark.table(silver_generation_table)
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

        F.col("valor")
        .cast("decimal(24,6)")
        .alias("valor"),

        F.coalesce(
            F.col("planta_encontrada"),
            F.lit(False),
        ).alias("planta_encontrada"),

        F.coalesce(
            F.col("agente_encontrado"),
            F.lit(False),
        ).alias("agente_encontrado"),

        F.col("silver_updated_at"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
    .filter(
        (F.col("codigo_variable") == "GREAL")
        & (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)


print(
    "Registros GREAL Silver:",
    f"{generation_base_df.count():,}",
)

# COMMAND ----------

invalid_generation_df = (
    generation_base_df
    .filter(
        F.col("fecha_hora").isNull()
        | F.col("codigo_planta").isNull()
        | (F.col("codigo_planta") == "")
        | F.col("codigo_agente").isNull()
        | (F.col("codigo_agente") == "")
        | F.col("version").isNull()
        | (F.col("version") == "")
        | F.col("valor").isNull()
    )
)


invalid_generation_rows = (
    invalid_generation_df.count()
)


negative_generation_rows = (
    generation_base_df
    .filter(F.col("valor") < 0)
    .count()
)


print(
    "Registros inválidos:",
    f"{invalid_generation_rows:,}",
)

print(
    "Valores negativos:",
    f"{negative_generation_rows:,}",
)


if invalid_generation_rows > 0:
    display(
        invalid_generation_df.limit(100)
    )

    raise ValueError(
        "Existen registros GREAL con llaves "
        "o valores obligatorios inválidos."
    )


if negative_generation_rows > 0:
    raise ValueError(
        "Existen valores negativos de generación. "
        f"Cantidad: {negative_generation_rows:,}"
    )

# COMMAND ----------

generation_prioritized_df = (
    generation_base_df
    .withColumn(
        "prioridad_version",
        tx_priority_expression("version", TX_POLICY),
    )
)

unknown_generation_versions_df = (
    generation_prioritized_df
    .filter(
        F.col("prioridad_version") == 0
    )
    .select("version")
    .distinct()
)


unknown_generation_versions = (
    unknown_generation_versions_df.count()
)


print(
    "Versiones sin orden de liquidación:",
    unknown_generation_versions,
)


if unknown_generation_versions > 0:
    display(
        unknown_generation_versions_df
    )

    raise ValueError(
        "Existen versiones de liquidación que "
        "no tienen un orden definido."
    )


print(
    "Versiones de liquidación validadas."
)

# COMMAND ----------

generation_business_key = [
    "fecha_hora",
    "codigo_planta",
    "codigo_agente",
    "codigo_variable",
    "codigo_duracion",
    "unidad_medida",
]


generation_version_window = (
    Window
    .partitionBy(
        *generation_business_key
    )
    .orderBy(
        F.col(
            "prioridad_version"
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

        F.col(
            "version"
        ).desc(),
    )
)


generation_selected_df = (
    generation_prioritized_df
    .withColumn(
        "row_number",
        F.row_number().over(
            generation_version_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


generation_silver_rows = (
    generation_base_df.count()
)


generation_selected_rows = (
    generation_selected_df.count()
)


print(
    "Filas Silver con todas las liquidaciones:",
    f"{generation_silver_rows:,}",
)

print(
    "Mediciones consolidadas:",
    f"{generation_selected_rows:,}",
)

print(
    "Versiones anteriores no seleccionadas:",
    f"{generation_silver_rows - generation_selected_rows:,}",
)

display(
    generation_selected_df
    .groupBy(
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias(
            "mediciones_seleccionadas"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),
    )
    .orderBy(
        F.desc("prioridad_version")
    )
)

# COMMAND ----------

generation_enriched_df = (
    generation_selected_df.alias("source")

    .join(
        spark.table(
            DIM_FECHA_TABLE
        )
        .select(
            "fecha_key",
            "fecha",
        )
        .alias("date_dim"),

        F.to_date(
            F.col("source.fecha_hora")
        )
        ==
        F.col("date_dim.fecha"),

        "left",
    )

    .join(
        spark.table(
            DIM_PERIODO_TABLE
        )
        .select(
            "periodo_key",
            "hora_inicio",
        )
        .alias("period_dim"),

        F.hour(
            F.col("source.fecha_hora")
        )
        ==
        F.col("period_dim.hora_inicio"),

        "left",
    )

    .join(
        spark.table(
            DIM_PLANTA_TABLE
        )
        .select(
            "planta_key",
            "codigo_planta",
        )
        .alias("plant_dim"),

        F.col("source.codigo_planta")
        ==
        F.col("plant_dim.codigo_planta"),

        "left",
    )

    .join(
        spark.table(
            DIM_AGENTE_TABLE
        )
        .select(
            "agente_key",
            "codigo_agente",
            "fecha_inicio",
            "fecha_fin",
        )
        .alias("agent_dim"),

        (
            F.col("source.codigo_agente")
            ==
            F.col("agent_dim.codigo_agente")
        )
        &
        (
            F.to_date(
                F.col("source.fecha_hora")
            )
            .between(
                F.col("agent_dim.fecha_inicio"),
                F.col("agent_dim.fecha_fin"),
            )
        ),

        "left",
    )

    .select(
        F.col("source.*"),
        F.col("date_dim.fecha_key"),
        F.col("period_dim.periodo_key"),
        F.col("plant_dim.planta_key"),
        F.col("agent_dim.agente_key"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC validar cobertura dimensional

# COMMAND ----------

generation_missing_dimensions_df = (
    generation_enriched_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
        | F.col("planta_key").isNull()
        | F.col("agente_key").isNull()
    )
)


generation_missing_dimensions = (
    generation_missing_dimensions_df.count()
)


display(
    generation_enriched_df
    .agg(
        F.count("*").alias(
            "total_mediciones"
        ),

        F.sum(
            F.when(
                F.col("fecha_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_fecha"),

        F.sum(
            F.when(
                F.col("periodo_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_periodo"),

        F.sum(
            F.when(
                F.col("planta_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_planta"),

        F.sum(
            F.when(
                F.col("agente_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_agente"),
    )
)


if generation_missing_dimensions > 0:
    display(
        generation_missing_dimensions_df
        .select(
            "fecha_hora",
            "codigo_planta",
            "codigo_agente",
            "version",
            "fecha_key",
            "periodo_key",
            "planta_key",
            "agente_key",
        )
        .limit(100)
    )

    raise ValueError(
        "Existen mediciones de generación "
        "sin cobertura dimensional."
    )


print(
    "Cobertura dimensional de generación aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Construir fuente del hecho

# COMMAND ----------

fact_generation_source_df = (
    generation_enriched_df
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.date_format(
                    "fecha_hora",
                    "yyyy-MM-dd HH:mm:ss",
                ),
                F.col("codigo_planta"),
                F.col("codigo_agente"),
                F.col("codigo_variable"),
                F.col("codigo_duracion"),
                F.col("unidad_medida"),
            ),
            256,
        ).alias(
            "generacion_key"
        ),

        F.col("fecha_key")
        .cast("int"),

        F.col("periodo_key")
        .cast("tinyint"),

        F.col("planta_key")
        .cast("bigint"),

        F.col("agente_key")
        .cast("bigint"),

        F.col("fecha_hora"),

        F.col("valor")
        .cast("decimal(24,6)")
        .alias(
            "generacion_real_kwh"
        ),

        F.col("version").alias(
            "version_seleccionada"
        ),

        F.col("prioridad_version")
        .cast("int"),

        F.col("planta_encontrada").alias(
            "planta_provenia_de_maestro"
        ),

        F.col("agente_encontrado").alias(
            "agente_encontrado_silver"
        ),

        F.current_timestamp().alias(
            "fecha_creacion"
        ),

        F.current_timestamp().alias(
            "fecha_actualizacion"
        ),
    )
)


fact_generation_rows = (
    fact_generation_source_df.count()
)


fact_generation_keys = (
    fact_generation_source_df
    .select("generacion_key")
    .distinct()
    .count()
)


print(
    "Filas fuente del hecho:",
    f"{fact_generation_rows:,}",
)

print(
    "Claves distintas:",
    f"{fact_generation_keys:,}",
)


if fact_generation_rows != fact_generation_keys:
    raise ValueError(
        "La fuente de fact_generacion_real "
        "contiene claves duplicadas."
    )

# COMMAND ----------

fact_generation_target = DeltaTable.forName(
    spark,
    FACT_GENERACION_TABLE,
)


(
    fact_generation_target.alias("target")
    .merge(
        fact_generation_source_df.alias("source"),
        """
        target.generacion_key =
            source.generacion_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.fecha_key
                <=> source.fecha_key
            )
            OR NOT (
                target.periodo_key
                <=> source.periodo_key
            )
            OR NOT (
                target.planta_key
                <=> source.planta_key
            )
            OR NOT (
                target.agente_key
                <=> source.agente_key
            )
            OR NOT (
                target.generacion_real_kwh
                <=> source.generacion_real_kwh
            )
            OR NOT (
                target.version_seleccionada
                <=> source.version_seleccionada
            )
            OR NOT (
                target.prioridad_version
                <=> source.prioridad_version
            )
            OR NOT (
                target.planta_provenia_de_maestro
                <=> source.planta_provenia_de_maestro
            )
            OR NOT (
                target.agente_encontrado_silver
                <=> source.agente_encontrado_silver
            )
        """,
        set={
            "fecha_key":
                "source.fecha_key",

            "periodo_key":
                "source.periodo_key",

            "planta_key":
                "source.planta_key",

            "agente_key":
                "source.agente_key",

            "fecha_hora":
                "source.fecha_hora",

            "generacion_real_kwh":
                "source.generacion_real_kwh",

            "version_seleccionada":
                "source.version_seleccionada",

            "prioridad_version":
                "source.prioridad_version",

            "planta_provenia_de_maestro":
                "source.planta_provenia_de_maestro",

            "agente_encontrado_silver":
                "source.agente_encontrado_silver",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print(
    "MERGE de fact_generacion_real completado."
)

# COMMAND ----------

fact_generation_validation_df = spark.table(
    FACT_GENERACION_TABLE
)


generation_fact_rows = (
    fact_generation_validation_df.count()
)


generation_fact_keys = (
    fact_generation_validation_df
    .select("generacion_key")
    .distinct()
    .count()
)


generation_null_dimensions = (
    fact_generation_validation_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
        | F.col("planta_key").isNull()
        | F.col("agente_key").isNull()
    )
    .count()
)


generation_negative_values = (
    fact_generation_validation_df
    .filter(
        F.col("generacion_real_kwh") < 0
    )
    .count()
)


print(
    "Filas fact_generacion_real:",
    f"{generation_fact_rows:,}",
)

print(
    "Claves distintas:",
    f"{generation_fact_keys:,}",
)

print(
    "Dimensiones nulas:",
    generation_null_dimensions,
)

print(
    "Valores negativos:",
    generation_negative_values,
)


if generation_fact_rows != generation_fact_keys:
    raise ValueError(
        "fact_generacion_real contiene "
        "claves duplicadas."
    )


if generation_null_dimensions > 0:
    raise ValueError(
        "fact_generacion_real contiene "
        "dimensiones nulas."
    )


if generation_negative_values > 0:
    raise ValueError(
        "fact_generacion_real contiene "
        "valores negativos."
    )


display(
    fact_generation_validation_df
    .groupBy(
        "version_seleccionada",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias(
            "mediciones"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.sum(
            "generacion_real_kwh"
        ).alias(
            "generacion_total_kwh"
        ),
    )
    .orderBy(
        F.desc("prioridad_version")
    )
)


display(
    fact_generation_validation_df
    .agg(
        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.countDistinct(
            "planta_key"
        ).alias(
            "plantas_distintas"
        ),

        F.countDistinct(
            "agente_key"
        ).alias(
            "agentes_distintos"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "planta_provenia_de_maestro"
                ),
                1,
            ).otherwise(0)
        ).alias(
            "mediciones_de_plantas_inferidas"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "agente_encontrado_silver"
                ),
                1,
            ).otherwise(0)
        ).alias(
            "mediciones_sin_agente_silver"
        ),
    )
)


print(
    "fact_generacion_real aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Preparar Disponibilidad Silver

# COMMAND ----------

silver_availability_table = SILVER_TABLES[
    "disponibilidad_plantas"
]


availability_base_df = (
    spark.table(silver_availability_table)
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

        F.col("valor")
        .cast("decimal(24,6)")
        .alias("valor"),

        F.coalesce(
            F.col("planta_encontrada"),
            F.lit(False),
        ).alias("planta_encontrada"),

        F.col("silver_updated_at"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
    .filter(
        (F.col("codigo_variable") == "DISPREAL")
        & (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)


print(
    "Registros DISPREAL Silver:",
    f"{availability_base_df.count():,}",
)

# COMMAND ----------

invalid_availability_df = (
    availability_base_df
    .filter(
        F.col("fecha_hora").isNull()
        | F.col("codigo_planta").isNull()
        | (F.col("codigo_planta") == "")
        | F.col("version").isNull()
        | (F.col("version") == "")
        | F.col("valor").isNull()
    )
)


invalid_availability_rows = (
    invalid_availability_df.count()
)


negative_availability_rows = (
    availability_base_df
    .filter(F.col("valor") < 0)
    .count()
)


print(
    "Registros inválidos:",
    f"{invalid_availability_rows:,}",
)

print(
    "Valores negativos:",
    f"{negative_availability_rows:,}",
)


if invalid_availability_rows > 0:
    display(
        invalid_availability_df.limit(100)
    )

    raise ValueError(
        "Existen registros DISPREAL con llaves "
        "o valores obligatorios inválidos."
    )


if negative_availability_rows > 0:
    raise ValueError(
        "Existen valores negativos de disponibilidad. "
        f"Cantidad: {negative_availability_rows:,}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Asignar orden de liquidación

# COMMAND ----------

availability_prioritized_df = (
    availability_base_df
    .withColumn(
        "prioridad_version",
        tx_priority_expression("version", TX_POLICY),
    )
)


unknown_availability_versions_df = (
    availability_prioritized_df
    .filter(
        F.col("prioridad_version") == 0
    )
    .select("version")
    .distinct()
)


unknown_availability_versions = (
    unknown_availability_versions_df.count()
)


print(
    "Versiones sin orden de liquidación:",
    unknown_availability_versions,
)


if unknown_availability_versions > 0:
    display(
        unknown_availability_versions_df
    )

    raise ValueError(
        "Existen versiones de disponibilidad "
        "sin orden de liquidación definido."
    )


display(
    availability_prioritized_df
    .groupBy(
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias("registros"),
        F.min("fecha_hora").alias("fecha_minima"),
        F.max("fecha_hora").alias("fecha_maxima"),
    )
    .orderBy("prioridad_version")
)

# COMMAND ----------

availability_business_key = [
    "fecha_hora",
    "codigo_planta",
    "codigo_variable",
    "codigo_duracion",
    "unidad_medida",
]


availability_version_window = (
    Window
    .partitionBy(
        *availability_business_key
    )
    .orderBy(
        F.col(
            "prioridad_version"
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

        F.col(
            "version"
        ).desc(),
    )
)


availability_selected_df = (
    availability_prioritized_df
    .withColumn(
        "row_number",
        F.row_number().over(
            availability_version_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


availability_silver_rows = (
    availability_base_df.count()
)


availability_selected_rows = (
    availability_selected_df.count()
)


print(
    "Filas Silver con todas las liquidaciones:",
    f"{availability_silver_rows:,}",
)

print(
    "Mediciones consolidadas:",
    f"{availability_selected_rows:,}",
)

print(
    "Versiones anteriores no seleccionadas:",
    f"{availability_silver_rows - availability_selected_rows:,}",
)


display(
    availability_selected_df
    .groupBy(
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias(
            "mediciones_seleccionadas"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),
    )
    .orderBy(
        F.desc("prioridad_version")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Enriquecer con dimensiones

# COMMAND ----------

availability_enriched_df = (
    availability_selected_df.alias("source")

    .join(
        spark.table(
            DIM_FECHA_TABLE
        )
        .select(
            "fecha_key",
            "fecha",
        )
        .alias("date_dim"),

        F.to_date(
            F.col("source.fecha_hora")
        )
        ==
        F.col("date_dim.fecha"),

        "left",
    )

    .join(
        spark.table(
            DIM_PERIODO_TABLE
        )
        .select(
            "periodo_key",
            "hora_inicio",
        )
        .alias("period_dim"),

        F.hour(
            F.col("source.fecha_hora")
        )
        ==
        F.col("period_dim.hora_inicio"),

        "left",
    )

    .join(
        spark.table(
            DIM_PLANTA_TABLE
        )
        .select(
            "planta_key",
            "codigo_planta",
        )
        .alias("plant_dim"),

        F.col("source.codigo_planta")
        ==
        F.col("plant_dim.codigo_planta"),

        "left",
    )

    .select(
        F.col("source.*"),
        F.col("date_dim.fecha_key"),
        F.col("period_dim.periodo_key"),
        F.col("plant_dim.planta_key"),
    )
)

# COMMAND ----------

availability_missing_dimensions_df = (
    availability_enriched_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
        | F.col("planta_key").isNull()
    )
)


availability_missing_dimensions = (
    availability_missing_dimensions_df.count()
)


display(
    availability_enriched_df
    .agg(
        F.count("*").alias(
            "total_mediciones"
        ),

        F.sum(
            F.when(
                F.col("fecha_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_fecha"),

        F.sum(
            F.when(
                F.col("periodo_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_periodo"),

        F.sum(
            F.when(
                F.col("planta_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_planta"),
    )
)


if availability_missing_dimensions > 0:
    display(
        availability_missing_dimensions_df
        .select(
            "fecha_hora",
            "codigo_planta",
            "version",
            "fecha_key",
            "periodo_key",
            "planta_key",
        )
        .limit(100)
    )

    raise ValueError(
        "Existen mediciones de disponibilidad "
        "sin cobertura dimensional."
    )


print(
    "Cobertura dimensional de disponibilidad aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Construir fuente del hecho

# COMMAND ----------

fact_availability_source_df = (
    availability_enriched_df
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.date_format(
                    "fecha_hora",
                    "yyyy-MM-dd HH:mm:ss",
                ),
                F.col("codigo_planta"),
                F.col("codigo_variable"),
                F.col("codigo_duracion"),
                F.col("unidad_medida"),
            ),
            256,
        ).alias(
            "disponibilidad_key"
        ),

        F.col("fecha_key")
        .cast("int"),

        F.col("periodo_key")
        .cast("tinyint"),

        F.col("planta_key")
        .cast("bigint"),

        F.col("fecha_hora"),

        F.col("valor")
        .cast("decimal(24,6)")
        .alias(
            "disponibilidad_real_kwh"
        ),

        F.col("version").alias(
            "version_seleccionada"
        ),

        F.col("prioridad_version")
        .cast("int"),

        F.col("planta_encontrada").alias(
            "planta_provenia_de_maestro"
        ),

        F.current_timestamp().alias(
            "fecha_creacion"
        ),

        F.current_timestamp().alias(
            "fecha_actualizacion"
        ),
    )
)


fact_availability_rows = (
    fact_availability_source_df.count()
)


fact_availability_keys = (
    fact_availability_source_df
    .select("disponibilidad_key")
    .distinct()
    .count()
)


print(
    "Filas fuente del hecho:",
    f"{fact_availability_rows:,}",
)

print(
    "Claves distintas:",
    f"{fact_availability_keys:,}",
)


if (
    fact_availability_rows
    != fact_availability_keys
):
    raise ValueError(
        "La fuente de fact_disponibilidad_planta "
        "contiene claves duplicadas."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Merge de disponibilidad

# COMMAND ----------

fact_availability_target = DeltaTable.forName(
    spark,
    FACT_DISPONIBILIDAD_TABLE,
)


(
    fact_availability_target.alias("target")
    .merge(
        fact_availability_source_df.alias("source"),
        """
        target.disponibilidad_key =
            source.disponibilidad_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.fecha_key
                <=> source.fecha_key
            )
            OR NOT (
                target.periodo_key
                <=> source.periodo_key
            )
            OR NOT (
                target.planta_key
                <=> source.planta_key
            )
            OR NOT (
                target.disponibilidad_real_kwh
                <=> source.disponibilidad_real_kwh
            )
            OR NOT (
                target.version_seleccionada
                <=> source.version_seleccionada
            )
            OR NOT (
                target.prioridad_version
                <=> source.prioridad_version
            )
            OR NOT (
                target.planta_provenia_de_maestro
                <=> source.planta_provenia_de_maestro
            )
        """,
        set={
            "fecha_key":
                "source.fecha_key",

            "periodo_key":
                "source.periodo_key",

            "planta_key":
                "source.planta_key",

            "fecha_hora":
                "source.fecha_hora",

            "disponibilidad_real_kwh":
                "source.disponibilidad_real_kwh",

            "version_seleccionada":
                "source.version_seleccionada",

            "prioridad_version":
                "source.prioridad_version",

            "planta_provenia_de_maestro":
                "source.planta_provenia_de_maestro",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print(
    "MERGE de fact_disponibilidad_planta completado."
)

# COMMAND ----------

fact_availability_validation_df = spark.table(
    FACT_DISPONIBILIDAD_TABLE
)


availability_fact_rows = (
    fact_availability_validation_df.count()
)


availability_fact_keys = (
    fact_availability_validation_df
    .select("disponibilidad_key")
    .distinct()
    .count()
)


availability_null_dimensions = (
    fact_availability_validation_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
        | F.col("planta_key").isNull()
    )
    .count()
)


availability_negative_values = (
    fact_availability_validation_df
    .filter(
        F.col("disponibilidad_real_kwh") < 0
    )
    .count()
)


print(
    "Filas fact_disponibilidad_planta:",
    f"{availability_fact_rows:,}",
)

print(
    "Claves distintas:",
    f"{availability_fact_keys:,}",
)

print(
    "Dimensiones nulas:",
    availability_null_dimensions,
)

print(
    "Valores negativos:",
    availability_negative_values,
)


if (
    availability_fact_rows
    != availability_fact_keys
):
    raise ValueError(
        "fact_disponibilidad_planta contiene "
        "claves duplicadas."
    )


if availability_null_dimensions > 0:
    raise ValueError(
        "fact_disponibilidad_planta contiene "
        "dimensiones nulas."
    )


if availability_negative_values > 0:
    raise ValueError(
        "fact_disponibilidad_planta contiene "
        "valores negativos."
    )


display(
    fact_availability_validation_df
    .groupBy(
        "version_seleccionada",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias(
            "mediciones"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.sum(
            "disponibilidad_real_kwh"
        ).alias(
            "disponibilidad_total_kwh"
        ),
    )
    .orderBy(
        F.desc("prioridad_version")
    )
)


display(
    fact_availability_validation_df
    .agg(
        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.countDistinct(
            "planta_key"
        ).alias(
            "plantas_distintas"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "planta_provenia_de_maestro"
                ),
                1,
            ).otherwise(0)
        ).alias(
            "mediciones_de_plantas_inferidas"
        ),
    )
)


print(
    "fact_disponibilidad_planta aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Preparar Demanda Silver

# COMMAND ----------

silver_demand_table = SILVER_TABLES[
    "demanda_real"
]


silver_demand_raw_df = spark.table(
    silver_demand_table
)


demand_columns = set(
    silver_demand_raw_df.columns
)


print(
    "Columnas de silver.demanda_real:"
)

print(
    sorted(demand_columns)
)

demand_value_candidates = [
    "demanda_real_kwh",
    "demanda_kwh",
    "valor_demanda",
    "valor",
]


demand_value_column = next(
    (
        column_name
        for column_name
        in demand_value_candidates
        if column_name in demand_columns
    ),
    None,
)


if demand_value_column is None:
    raise ValueError(
        "No se encontró la columna de medida "
        "de demanda. Columnas disponibles: "
        f"{sorted(demand_columns)}"
    )


print(
    "Columna de medida detectada:",
    demand_value_column,
)

demand_base_df = (
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

        F.coalesce(
            (
                F.col("es_demanda_cero")
                if "es_demanda_cero"
                in demand_columns
                else F.lit(None)
            ),
            F.col(demand_value_column) == 0,
        ).cast("boolean").alias(
            "es_demanda_cero"
        ),

        F.coalesce(
            (
                F.col("agente_encontrado")
                if "agente_encontrado"
                in demand_columns
                else F.lit(None)
            ),
            F.lit(False),
        ).cast("boolean").alias(
            "agente_encontrado"
        ),

        F.col("silver_updated_at"),
        F.col("ingestion_timestamp"),
        F.col("load_date"),
    )
    .filter(
        (F.col("codigo_duracion") == "PT1H")
        & (F.col("unidad_medida") == "KWH")
    )
)


print(
    "Registros de demanda Silver:",
    f"{demand_base_df.count():,}",
)


display(
    demand_base_df.limit(10)
)





# COMMAND ----------

invalid_demand_df = (
    demand_base_df
    .filter(
        F.col("fecha_hora").isNull()
        | F.col("codigo_agente").isNull()
        | (F.col("codigo_agente") == "")
        | F.col("tipo_mercado").isNull()
        | (F.col("tipo_mercado") == "")
        | F.col("codigo_variable").isNull()
        | (F.col("codigo_variable") == "")
        | F.col("version").isNull()
        | (F.col("version") == "")
        | F.col("valor").isNull()
    )
)


invalid_demand_rows = (
    invalid_demand_df.count()
)


negative_demand_rows = (
    demand_base_df
    .filter(F.col("valor") < 0)
    .count()
)


print(
    "Registros inválidos:",
    f"{invalid_demand_rows:,}",
)

print(
    "Valores negativos:",
    f"{negative_demand_rows:,}",
)


if invalid_demand_rows > 0:
    display(
        invalid_demand_df.limit(100)
    )

    raise ValueError(
        "Existen registros de demanda con "
        "llaves o valores obligatorios inválidos."
    )


if negative_demand_rows > 0:
    raise ValueError(
        "Existen valores negativos de demanda. "
        f"Cantidad: {negative_demand_rows:,}"
    )

# COMMAND ----------

demand_prioritized_df = (
    demand_base_df
    .withColumn(
        "prioridad_version",
        tx_priority_expression("version", TX_POLICY),
    )
)


unknown_demand_versions_df = (
    demand_prioritized_df
    .filter(
        F.col("prioridad_version") == 0
    )
    .select("version")
    .distinct()
)


unknown_demand_versions = (
    unknown_demand_versions_df.count()
)


print(
    "Versiones sin orden de liquidación:",
    unknown_demand_versions,
)


if unknown_demand_versions > 0:
    display(
        unknown_demand_versions_df
    )

    raise ValueError(
        "Existen versiones de demanda sin "
        "orden de liquidación definido."
    )


display(
    demand_prioritized_df
    .groupBy(
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias("registros"),
        F.min("fecha_hora").alias("fecha_minima"),
        F.max("fecha_hora").alias("fecha_maxima"),
    )
    .orderBy("prioridad_version")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Seleccionar la liquidación mas avanzada

# COMMAND ----------

demand_business_key = [
    "fecha_hora",
    "codigo_agente",
    "tipo_mercado",
    "codigo_variable",
    "codigo_duracion",
    "unidad_medida",
]


demand_version_window = (
    Window
    .partitionBy(
        *demand_business_key
    )
    .orderBy(
        F.col(
            "prioridad_version"
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

        F.col(
            "version"
        ).desc(),
    )
)


demand_selected_df = (
    demand_prioritized_df
    .withColumn(
        "row_number",
        F.row_number().over(
            demand_version_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


demand_silver_rows = (
    demand_base_df.count()
)


demand_selected_rows = (
    demand_selected_df.count()
)


print(
    "Filas Silver con todas las liquidaciones:",
    f"{demand_silver_rows:,}",
)

print(
    "Mediciones consolidadas:",
    f"{demand_selected_rows:,}",
)

print(
    "Versiones anteriores no seleccionadas:",
    f"{demand_silver_rows - demand_selected_rows:,}",
)


display(
    demand_selected_df
    .groupBy(
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias(
            "mediciones_seleccionadas"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),
    )
    .orderBy(
        F.desc("prioridad_version")
    )
)

# COMMAND ----------

demand_enriched_df = (
    demand_selected_df.alias("source")

    .join(
        spark.table(
            DIM_FECHA_TABLE
        )
        .select(
            "fecha_key",
            "fecha",
        )
        .alias("date_dim"),

        F.to_date(
            F.col("source.fecha_hora")
        )
        ==
        F.col("date_dim.fecha"),

        "left",
    )

    .join(
        spark.table(
            DIM_PERIODO_TABLE
        )
        .select(
            "periodo_key",
            "hora_inicio",
        )
        .alias("period_dim"),

        F.hour(
            F.col("source.fecha_hora")
        )
        ==
        F.col("period_dim.hora_inicio"),

        "left",
    )

    .join(
        spark.table(
            DIM_AGENTE_TABLE
        )
        .select(
            "agente_key",
            "codigo_agente",
            "fecha_inicio",
            "fecha_fin",
        )
        .alias("agent_dim"),

        (
            F.col("source.codigo_agente")
            ==
            F.col("agent_dim.codigo_agente")
        )
        &
        (
            F.to_date(
                F.col("source.fecha_hora")
            )
            .between(
                F.col("agent_dim.fecha_inicio"),
                F.col("agent_dim.fecha_fin"),
            )
        ),

        "left",
    )

    .select(
        F.col("source.*"),
        F.col("date_dim.fecha_key"),
        F.col("period_dim.periodo_key"),
        F.col("agent_dim.agente_key"),
    )
)

# COMMAND ----------

demand_missing_dimensions_df = (
    demand_enriched_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
        | F.col("agente_key").isNull()
    )
)


demand_missing_dimensions = (
    demand_missing_dimensions_df.count()
)


display(
    demand_enriched_df
    .agg(
        F.count("*").alias(
            "total_mediciones"
        ),

        F.sum(
            F.when(
                F.col("fecha_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_fecha"),

        F.sum(
            F.when(
                F.col("periodo_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_periodo"),

        F.sum(
            F.when(
                F.col("agente_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_agente"),
    )
)


if demand_missing_dimensions > 0:
    display(
        demand_missing_dimensions_df
        .select(
            "fecha_hora",
            "codigo_agente",
            "tipo_mercado",
            "version",
            "fecha_key",
            "periodo_key",
            "agente_key",
        )
        .limit(100)
    )

    raise ValueError(
        "Existen mediciones de demanda "
        "sin cobertura dimensional."
    )


print(
    "Cobertura dimensional de demanda aprobada."
)

# COMMAND ----------

fact_demand_source_df = (
    demand_enriched_df
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.date_format(
                    "fecha_hora",
                    "yyyy-MM-dd HH:mm:ss",
                ),
                F.col("codigo_agente"),
                F.col("tipo_mercado"),
                F.col("codigo_variable"),
                F.col("codigo_duracion"),
                F.col("unidad_medida"),
            ),
            256,
        ).alias(
            "demanda_key"
        ),

        F.col("fecha_key")
        .cast("int"),

        F.col("periodo_key")
        .cast("tinyint"),

        F.col("agente_key")
        .cast("bigint"),

        F.col("fecha_hora"),

        F.col("tipo_mercado"),

        F.col("valor")
        .cast("decimal(24,6)")
        .alias(
            "demanda_real_kwh"
        ),

        F.col("es_demanda_cero")
        .cast("boolean"),

        F.col("version").alias(
            "version_seleccionada"
        ),

        F.col("prioridad_version")
        .cast("int"),

        F.col("agente_encontrado").alias(
            "agente_encontrado_silver"
        ),

        F.current_timestamp().alias(
            "fecha_creacion"
        ),

        F.current_timestamp().alias(
            "fecha_actualizacion"
        ),
    )
)


fact_demand_rows = (
    fact_demand_source_df.count()
)


fact_demand_keys = (
    fact_demand_source_df
    .select("demanda_key")
    .distinct()
    .count()
)


print(
    "Filas fuente del hecho:",
    f"{fact_demand_rows:,}",
)

print(
    "Claves distintas:",
    f"{fact_demand_keys:,}",
)


if fact_demand_rows != fact_demand_keys:
    raise ValueError(
        "La fuente de fact_demanda_real "
        "contiene claves duplicadas."
    )

# COMMAND ----------

fact_demand_target = DeltaTable.forName(
    spark,
    FACT_DEMANDA_TABLE,
)


(
    fact_demand_target.alias("target")
    .merge(
        fact_demand_source_df.alias("source"),
        """
        target.demanda_key =
            source.demanda_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.fecha_key
                <=> source.fecha_key
            )
            OR NOT (
                target.periodo_key
                <=> source.periodo_key
            )
            OR NOT (
                target.agente_key
                <=> source.agente_key
            )
            OR NOT (
                target.tipo_mercado
                <=> source.tipo_mercado
            )
            OR NOT (
                target.demanda_real_kwh
                <=> source.demanda_real_kwh
            )
            OR NOT (
                target.es_demanda_cero
                <=> source.es_demanda_cero
            )
            OR NOT (
                target.version_seleccionada
                <=> source.version_seleccionada
            )
            OR NOT (
                target.prioridad_version
                <=> source.prioridad_version
            )
            OR NOT (
                target.agente_encontrado_silver
                <=> source.agente_encontrado_silver
            )
        """,
        set={
            "fecha_key":
                "source.fecha_key",

            "periodo_key":
                "source.periodo_key",

            "agente_key":
                "source.agente_key",

            "fecha_hora":
                "source.fecha_hora",

            "tipo_mercado":
                "source.tipo_mercado",

            "demanda_real_kwh":
                "source.demanda_real_kwh",

            "es_demanda_cero":
                "source.es_demanda_cero",

            "version_seleccionada":
                "source.version_seleccionada",

            "prioridad_version":
                "source.prioridad_version",

            "agente_encontrado_silver":
                "source.agente_encontrado_silver",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print(
    "MERGE de fact_demanda_real completado."
)

# COMMAND ----------

fact_demand_validation_df = spark.table(
    FACT_DEMANDA_TABLE
)


demand_fact_rows = (
    fact_demand_validation_df.count()
)


demand_fact_keys = (
    fact_demand_validation_df
    .select("demanda_key")
    .distinct()
    .count()
)


demand_null_dimensions = (
    fact_demand_validation_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
        | F.col("agente_key").isNull()
    )
    .count()
)


demand_negative_values = (
    fact_demand_validation_df
    .filter(
        F.col("demanda_real_kwh") < 0
    )
    .count()
)


print(
    "Filas fact_demanda_real:",
    f"{demand_fact_rows:,}",
)

print(
    "Claves distintas:",
    f"{demand_fact_keys:,}",
)

print(
    "Dimensiones nulas:",
    demand_null_dimensions,
)

print(
    "Valores negativos:",
    demand_negative_values,
)


if demand_fact_rows != demand_fact_keys:
    raise ValueError(
        "fact_demanda_real contiene "
        "claves duplicadas."
    )


if demand_null_dimensions > 0:
    raise ValueError(
        "fact_demanda_real contiene "
        "dimensiones nulas."
    )


if demand_negative_values > 0:
    raise ValueError(
        "fact_demanda_real contiene "
        "valores negativos."
    )


display(
    fact_demand_validation_df
    .groupBy(
        "tipo_mercado",
        "version_seleccionada",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias(
            "mediciones"
        ),

        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.sum(
            "demanda_real_kwh"
        ).alias(
            "demanda_total_kwh"
        ),
    )
    .orderBy(
        "tipo_mercado",
        F.desc("prioridad_version"),
    )
)


display(
    fact_demand_validation_df
    .agg(
        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.countDistinct(
            "agente_key"
        ).alias(
            "agentes_distintos"
        ),

        F.countDistinct(
            "tipo_mercado"
        ).alias(
            "mercados_distintos"
        ),

        F.sum(
            F.when(
                F.col("es_demanda_cero"),
                1,
            ).otherwise(0)
        ).alias(
            "mediciones_cero"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "agente_encontrado_silver"
                ),
                1,
            ).otherwise(0)
        ).alias(
            "mediciones_sin_agente_silver"
        ),
    )
)


print(
    "fact_demanda_real aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Inspeccionar y preparar precio de bolsa

# COMMAND ----------

silver_price_table = SILVER_TABLES[
    "precio_bolsa"
]


silver_price_raw_df = spark.table(
    silver_price_table
)


price_columns = set(
    silver_price_raw_df.columns
)


print(
    "Columnas de silver.precio_bolsa:"
)

print(
    sorted(price_columns)
)

# COMMAND ----------

price_value_candidates = [
    "precio_bolsa",
    "precio_kwh",
    "valor_precio",
    "precio",
    "valor",
]


price_value_column = next(
    (
        column_name
        for column_name
        in price_value_candidates
        if column_name in price_columns
    ),
    None,
)


if price_value_column is None:
    raise ValueError(
        "No se encontró la columna de valor "
        "en silver.precio_bolsa. "
        f"Columnas disponibles: {sorted(price_columns)}"
    )


print(
    "Columna de precio detectada:",
    price_value_column,
)

# COMMAND ----------

price_base_df = (
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
            if "silver_updated_at" in price_columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp" in price_columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date" in price_columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        F.col("codigo_variable").isin(
            "PB_INT",
            "PB_NAL",
            "PB_TIE",
        )
        & (F.col("codigo_duracion") == "PT1H")
    )
)


print(
    "Registros de precio Silver:",
    f"{price_base_df.count():,}",
)


display(
    price_base_df.limit(10)
)

# COMMAND ----------

price_base_df = (
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
            if "silver_updated_at" in price_columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp" in price_columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date" in price_columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        F.col("codigo_variable").isin(
            "PB_INT",
            "PB_NAL",
            "PB_TIE",
        )
        & (F.col("codigo_duracion") == "PT1H")
    )
)


print(
    "Registros de precio Silver:",
    f"{price_base_df.count():,}",
)


display(
    price_base_df.limit(10)
)

# COMMAND ----------

invalid_price_df = (
    price_base_df
    .filter(
        F.col("fecha_hora").isNull()
        | F.col("codigo_variable").isNull()
        | (F.col("codigo_variable") == "")
        | F.col("version").isNull()
        | (F.col("version") == "")
        | F.col("valor").isNull()
    )
)


invalid_price_rows = (
    invalid_price_df.count()
)


negative_price_rows = (
    price_base_df
    .filter(
        F.col("valor") < 0
    )
    .count()
)


unexpected_price_variables_df = (
    price_base_df
    .filter(
        ~F.col("codigo_variable").isin(
            "PB_INT",
            "PB_NAL",
            "PB_TIE",
        )
    )
    .select("codigo_variable")
    .distinct()
)


unexpected_price_variables = (
    unexpected_price_variables_df.count()
)


print(
    "Registros inválidos:",
    invalid_price_rows,
)

print(
    "Valores negativos:",
    negative_price_rows,
)

print(
    "Variables inesperadas:",
    unexpected_price_variables,
)


if invalid_price_rows > 0:
    display(
        invalid_price_df.limit(100)
    )

    raise ValueError(
        "Existen registros de precio de bolsa "
        "con campos obligatorios inválidos."
    )


if negative_price_rows > 0:
    raise ValueError(
        "Existen precios de bolsa negativos."
    )


if unexpected_price_variables > 0:
    display(
        unexpected_price_variables_df
    )

    raise ValueError(
        "Existen variables de precio no reconocidas."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Asignar orden de liquidación

# COMMAND ----------

price_prioritized_df = (
    price_base_df
    .withColumn(
        "prioridad_version",
        tx_priority_expression("version", TX_POLICY),
    )
)


unknown_price_versions_df = (
    price_prioritized_df
    .filter(
        F.col("prioridad_version") == 0
    )
    .select("version")
    .distinct()
)


unknown_price_versions = (
    unknown_price_versions_df.count()
)


print(
    "Versiones sin orden de liquidación:",
    unknown_price_versions,
)


if unknown_price_versions > 0:
    display(
        unknown_price_versions_df
    )

    raise ValueError(
        "Existen versiones de precio de bolsa "
        "sin orden de liquidación definido."
    )


display(
    price_prioritized_df
    .groupBy(
        "codigo_variable",
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias("registros"),
        F.min("fecha_hora").alias("fecha_minima"),
        F.max("fecha_hora").alias("fecha_maxima"),
    )
    .orderBy(
        "codigo_variable",
        "prioridad_version",
    )
)

# COMMAND ----------

price_business_key = [
    "fecha_hora",
    "codigo_variable",
    "codigo_duracion",
    "unidad_medida",
]


price_version_window = (
    Window
    .partitionBy(
        *price_business_key
    )
    .orderBy(
        F.col(
            "prioridad_version"
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

        F.col(
            "version"
        ).desc(),
    )
)


price_selected_df = (
    price_prioritized_df
    .withColumn(
        "row_number",
        F.row_number().over(
            price_version_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


price_silver_rows = (
    price_base_df.count()
)


price_selected_rows = (
    price_selected_df.count()
)


print(
    "Filas Silver con todas las liquidaciones:",
    f"{price_silver_rows:,}",
)

print(
    "Mediciones variable-hora consolidadas:",
    f"{price_selected_rows:,}",
)

print(
    "Versiones anteriores no seleccionadas:",
    f"{price_silver_rows - price_selected_rows:,}",
)


display(
    price_selected_df
    .groupBy(
        "codigo_variable",
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias(
            "mediciones_seleccionadas"
        ),
        F.min("fecha_hora").alias(
            "fecha_minima"
        ),
        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),
    )
    .orderBy(
        "codigo_variable",
        F.desc("prioridad_version"),
    )
)

# COMMAND ----------

duplicate_selected_prices_df = (
    price_selected_df
    .groupBy(
        "fecha_hora",
        "codigo_variable",
    )
    .count()
    .filter(
        F.col("count") > 1
    )
)


duplicate_selected_prices = (
    duplicate_selected_prices_df.count()
)


print(
    "Duplicados después de consolidar:",
    duplicate_selected_prices,
)


if duplicate_selected_prices > 0:
    display(
        duplicate_selected_prices_df.limit(100)
    )

    raise ValueError(
        "La selección de versiones produjo "
        "más de un precio por hora y variable."
    )

# COMMAND ----------

price_wide_df = (
    price_selected_df
    .groupBy("fecha_hora")
    .agg(
        F.max(
            F.when(
                F.col("codigo_variable") == "PB_INT",
                F.col("valor"),
            )
        )
        .cast("decimal(24,6)")
        .alias(
            "precio_bolsa_internacional_cop_kwh"
        ),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_NAL",
                F.col("valor"),
            )
        )
        .cast("decimal(24,6)")
        .alias(
            "precio_bolsa_nacional_cop_kwh"
        ),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_TIE",
                F.col("valor"),
            )
        )
        .cast("decimal(24,6)")
        .alias(
            "precio_bolsa_tie_cop_kwh"
        ),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_INT",
                F.col("version"),
            )
        ).alias("version_pb_int"),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_NAL",
                F.col("version"),
            )
        ).alias("version_pb_nal"),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_TIE",
                F.col("version"),
            )
        ).alias("version_pb_tie"),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_INT",
                F.col("prioridad_version"),
            )
        )
        .cast("int")
        .alias("prioridad_pb_int"),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_NAL",
                F.col("prioridad_version"),
            )
        )
        .cast("int")
        .alias("prioridad_pb_nal"),

        F.max(
            F.when(
                F.col("codigo_variable") == "PB_TIE",
                F.col("prioridad_version"),
            )
        )
        .cast("int")
        .alias("prioridad_pb_tie"),
    )
)


print(
    "Horas consolidadas:",
    f"{price_wide_df.count():,}",
)

# COMMAND ----------

incomplete_price_hours_df = (
    price_wide_df
    .filter(
        F.col(
            "precio_bolsa_internacional_cop_kwh"
        ).isNull()
        | F.col(
            "precio_bolsa_nacional_cop_kwh"
        ).isNull()
        | F.col(
            "precio_bolsa_tie_cop_kwh"
        ).isNull()
        | F.col("version_pb_int").isNull()
        | F.col("version_pb_nal").isNull()
        | F.col("version_pb_tie").isNull()
        | F.col("prioridad_pb_int").isNull()
        | F.col("prioridad_pb_nal").isNull()
        | F.col("prioridad_pb_tie").isNull()
    )
)


incomplete_price_hours = (
    incomplete_price_hours_df.count()
)


print(
    "Horas con variables incompletas:",
    incomplete_price_hours,
)


if incomplete_price_hours > 0:
    display(
        incomplete_price_hours_df
        .orderBy("fecha_hora")
        .limit(100)
    )

    raise ValueError(
        "Existen horas sin las tres variables "
        "de precio completas."
    )


print(
    "Completitud de precios aprobada."
)

# COMMAND ----------

price_enriched_df = (
    price_wide_df.alias("source")

    .join(
        spark.table(
            DIM_FECHA_TABLE
        )
        .select(
            "fecha_key",
            "fecha",
        )
        .alias("date_dim"),

        F.to_date(
            F.col("source.fecha_hora")
        )
        ==
        F.col("date_dim.fecha"),

        "left",
    )

    .join(
        spark.table(
            DIM_PERIODO_TABLE
        )
        .select(
            "periodo_key",
            "hora_inicio",
        )
        .alias("period_dim"),

        F.hour(
            F.col("source.fecha_hora")
        )
        ==
        F.col("period_dim.hora_inicio"),

        "left",
    )

    .select(
        F.col("source.*"),
        F.col("date_dim.fecha_key"),
        F.col("period_dim.periodo_key"),
    )
)

# COMMAND ----------

price_missing_dimensions_df = (
    price_enriched_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
    )
)


price_missing_dimensions = (
    price_missing_dimensions_df.count()
)


display(
    price_enriched_df
    .agg(
        F.count("*").alias(
            "total_horas"
        ),

        F.sum(
            F.when(
                F.col("fecha_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_fecha"),

        F.sum(
            F.when(
                F.col("periodo_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_periodo"),
    )
)


if price_missing_dimensions > 0:
    display(
        price_missing_dimensions_df
        .limit(100)
    )

    raise ValueError(
        "Existen precios de bolsa "
        "sin fecha o periodo dimensional."
    )


print(
    "Cobertura dimensional de precios aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Construir fuente del hecho

# COMMAND ----------

fact_price_source_df = (
    price_enriched_df
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.date_format(
                    "fecha_hora",
                    "yyyy-MM-dd HH:mm:ss",
                ),
                F.lit("PRECIO_BOLSA"),
            ),
            256,
        ).alias(
            "precio_bolsa_key"
        ),

        F.col("fecha_key")
        .cast("int"),

        F.col("periodo_key")
        .cast("tinyint"),

        F.col("fecha_hora"),

        F.col(
            "precio_bolsa_internacional_cop_kwh"
        ),

        F.col(
            "precio_bolsa_nacional_cop_kwh"
        ),

        F.col(
            "precio_bolsa_tie_cop_kwh"
        ),

        F.col("version_pb_int"),
        F.col("prioridad_pb_int")
        .cast("int"),

        F.col("version_pb_nal"),
        F.col("prioridad_pb_nal")
        .cast("int"),

        F.col("version_pb_tie"),
        F.col("prioridad_pb_tie")
        .cast("int"),

        F.current_timestamp().alias(
            "fecha_creacion"
        ),

        F.current_timestamp().alias(
            "fecha_actualizacion"
        ),
    )
)


fact_price_rows = (
    fact_price_source_df.count()
)


fact_price_keys = (
    fact_price_source_df
    .select("precio_bolsa_key")
    .distinct()
    .count()
)


print(
    "Filas fuente del hecho:",
    f"{fact_price_rows:,}",
)

print(
    "Claves distintas:",
    f"{fact_price_keys:,}",
)


if fact_price_rows != fact_price_keys:
    raise ValueError(
        "La fuente de fact_precio_bolsa "
        "contiene claves duplicadas."
    )

# COMMAND ----------

fact_price_target = DeltaTable.forName(
    spark,
    FACT_PRECIO_BOLSA_TABLE,
)


(
    fact_price_target.alias("target")
    .merge(
        fact_price_source_df.alias("source"),
        """
        target.precio_bolsa_key =
            source.precio_bolsa_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.fecha_key
                <=> source.fecha_key
            )
            OR NOT (
                target.periodo_key
                <=> source.periodo_key
            )
            OR NOT (
                target.fecha_hora
                <=> source.fecha_hora
            )
            OR NOT (
                target.precio_bolsa_internacional_cop_kwh
                <=>
                source.precio_bolsa_internacional_cop_kwh
            )
            OR NOT (
                target.precio_bolsa_nacional_cop_kwh
                <=>
                source.precio_bolsa_nacional_cop_kwh
            )
            OR NOT (
                target.precio_bolsa_tie_cop_kwh
                <=>
                source.precio_bolsa_tie_cop_kwh
            )
            OR NOT (
                target.version_pb_int
                <=> source.version_pb_int
            )
            OR NOT (
                target.prioridad_pb_int
                <=> source.prioridad_pb_int
            )
            OR NOT (
                target.version_pb_nal
                <=> source.version_pb_nal
            )
            OR NOT (
                target.prioridad_pb_nal
                <=> source.prioridad_pb_nal
            )
            OR NOT (
                target.version_pb_tie
                <=> source.version_pb_tie
            )
            OR NOT (
                target.prioridad_pb_tie
                <=> source.prioridad_pb_tie
            )
        """,
        set={
            "fecha_key":
                "source.fecha_key",

            "periodo_key":
                "source.periodo_key",

            "fecha_hora":
                "source.fecha_hora",

            "precio_bolsa_internacional_cop_kwh":
                "source.precio_bolsa_internacional_cop_kwh",

            "precio_bolsa_nacional_cop_kwh":
                "source.precio_bolsa_nacional_cop_kwh",

            "precio_bolsa_tie_cop_kwh":
                "source.precio_bolsa_tie_cop_kwh",

            "version_pb_int":
                "source.version_pb_int",

            "prioridad_pb_int":
                "source.prioridad_pb_int",

            "version_pb_nal":
                "source.version_pb_nal",

            "prioridad_pb_nal":
                "source.prioridad_pb_nal",

            "version_pb_tie":
                "source.version_pb_tie",

            "prioridad_pb_tie":
                "source.prioridad_pb_tie",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print(
    "MERGE de fact_precio_bolsa completado."
)

# COMMAND ----------

fact_price_validation_df = spark.table(
    FACT_PRECIO_BOLSA_TABLE
)


price_fact_rows = (
    fact_price_validation_df.count()
)


price_fact_keys = (
    fact_price_validation_df
    .select("precio_bolsa_key")
    .distinct()
    .count()
)


price_null_dimensions = (
    fact_price_validation_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("periodo_key").isNull()
    )
    .count()
)


price_incomplete_measures = (
    fact_price_validation_df
    .filter(
        F.col(
            "precio_bolsa_internacional_cop_kwh"
        ).isNull()
        | F.col(
            "precio_bolsa_nacional_cop_kwh"
        ).isNull()
        | F.col(
            "precio_bolsa_tie_cop_kwh"
        ).isNull()
    )
    .count()
)


price_negative_values = (
    fact_price_validation_df
    .filter(
        (
            F.col(
                "precio_bolsa_internacional_cop_kwh"
            ) < 0
        )
        | (
            F.col(
                "precio_bolsa_nacional_cop_kwh"
            ) < 0
        )
        | (
            F.col(
                "precio_bolsa_tie_cop_kwh"
            ) < 0
        )
    )
    .count()
)


print(
    "Filas fact_precio_bolsa:",
    f"{price_fact_rows:,}",
)

print(
    "Claves distintas:",
    f"{price_fact_keys:,}",
)

print(
    "Dimensiones nulas:",
    price_null_dimensions,
)

print(
    "Medidas incompletas:",
    price_incomplete_measures,
)

print(
    "Valores negativos:",
    price_negative_values,
)


if price_fact_rows != price_fact_keys:
    raise ValueError(
        "fact_precio_bolsa contiene "
        "claves duplicadas."
    )


if price_null_dimensions > 0:
    raise ValueError(
        "fact_precio_bolsa contiene "
        "dimensiones nulas."
    )


if price_incomplete_measures > 0:
    raise ValueError(
        "fact_precio_bolsa contiene "
        "medidas incompletas."
    )


if price_negative_values > 0:
    raise ValueError(
        "fact_precio_bolsa contiene "
        "precios negativos."
    )


display(
    fact_price_validation_df
    .agg(
        F.min("fecha_hora").alias(
            "fecha_minima"
        ),

        F.max("fecha_hora").alias(
            "fecha_maxima"
        ),

        F.min(
            "precio_bolsa_nacional_cop_kwh"
        ).alias(
            "precio_nacional_minimo"
        ),

        F.max(
            "precio_bolsa_nacional_cop_kwh"
        ).alias(
            "precio_nacional_maximo"
        ),

        F.avg(
            "precio_bolsa_nacional_cop_kwh"
        ).alias(
            "precio_nacional_promedio"
        ),
    )
)


display(
    fact_price_validation_df
    .groupBy(
        "version_pb_int",
        "version_pb_nal",
        "version_pb_tie",
    )
    .count()
    .orderBy(
        F.desc("count")
    )
)


print(
    "fact_precio_bolsa aprobada."
)

# COMMAND ----------

# MAGIC %md
# MAGIC Preparar niveles de embalses Silver

# COMMAND ----------

silver_reservoir_levels_table = SILVER_TABLES[
    "niveles_embalses"
]


silver_reservoir_levels_raw_df = spark.table(
    silver_reservoir_levels_table
)


reservoir_level_columns = set(
    silver_reservoir_levels_raw_df.columns
)


print(
    "Columnas de silver.niveles_embalses:"
)

print(
    sorted(reservoir_level_columns)
)

reservoir_value_candidates = [
    "energia_embalsada_kwh",
    "energia_embalsada",
    "nivel_embalse_kwh",
    "valor_nem",
    "valor",
]


reservoir_value_column = next(
    (
        column_name
        for column_name
        in reservoir_value_candidates
        if column_name in reservoir_level_columns
    ),
    None,
)


if reservoir_value_column is None:
    raise ValueError(
        "No se encontró la columna de energía "
        "embalsada en Silver. Columnas disponibles: "
        f"{sorted(reservoir_level_columns)}"
    )


print(
    "Columna de medida detectada:",
    reservoir_value_column,
)

reservoir_energy_base_df = (
    silver_reservoir_levels_raw_df
    .select(
        F.to_date(
            "fecha_inicio"
        ).alias("fecha_medicion"),

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

        F.coalesce(
            (
                F.col("es_valor_cero")
                if "es_valor_cero"
                in reservoir_level_columns
                else F.lit(None)
            ),
            F.col(reservoir_value_column) == 0,
        )
        .cast("boolean")
        .alias("es_valor_cero"),

        F.coalesce(
            (
                F.col("planta_encontrada")
                if "planta_encontrada"
                in reservoir_level_columns
                else F.lit(None)
            ),
            F.lit(False),
        )
        .cast("boolean")
        .alias("planta_encontrada"),

        (
            F.col("silver_updated_at")
            if "silver_updated_at"
            in reservoir_level_columns
            else F.lit(None).cast("timestamp")
        ).alias("silver_updated_at"),

        (
            F.col("ingestion_timestamp")
            if "ingestion_timestamp"
            in reservoir_level_columns
            else F.lit(None).cast("timestamp")
        ).alias("ingestion_timestamp"),

        (
            F.col("load_date")
            if "load_date"
            in reservoir_level_columns
            else F.lit(None).cast("date")
        ).alias("load_date"),
    )
    .filter(
        (F.col("codigo_variable") == "NEM")
        & (F.col("codigo_duracion") == "P1D")
        & (F.col("unidad_medida") == "KWH")
    )
)


print(
    "Registros NEM Silver:",
    f"{reservoir_energy_base_df.count():,}",
)


display(
    reservoir_energy_base_df.limit(10)
)

# COMMAND ----------

invalid_reservoir_energy_df = (
    reservoir_energy_base_df
    .filter(
        F.col("fecha_medicion").isNull()
        | F.col("codigo_planta").isNull()
        | (F.col("codigo_planta") == "")
        | F.col("version").isNull()
        | (F.col("version") == "")
        | F.col("valor").isNull()
    )
)


invalid_reservoir_energy_rows = (
    invalid_reservoir_energy_df.count()
)


negative_reservoir_energy_rows = (
    reservoir_energy_base_df
    .filter(
        F.col("valor") < 0
    )
    .count()
)


print(
    "Registros inválidos:",
    invalid_reservoir_energy_rows,
)

print(
    "Valores negativos:",
    negative_reservoir_energy_rows,
)


if invalid_reservoir_energy_rows > 0:
    display(
        invalid_reservoir_energy_df.limit(100)
    )

    raise ValueError(
        "Existen registros NEM con campos "
        "obligatorios inválidos."
    )


if negative_reservoir_energy_rows > 0:
    raise ValueError(
        "Existen valores negativos de "
        "energía embalsada."
    )

# COMMAND ----------

reservoir_energy_prioritized_df = (
    reservoir_energy_base_df
    .withColumn(
        "prioridad_version",
        tx_priority_expression("version", TX_POLICY),
    )
)


unknown_reservoir_versions_df = (
    reservoir_energy_prioritized_df
    .filter(
        F.col("prioridad_version") == 0
    )
    .select("version")
    .distinct()
)


unknown_reservoir_versions = (
    unknown_reservoir_versions_df.count()
)


print(
    "Versiones sin orden de liquidación:",
    unknown_reservoir_versions,
)


if unknown_reservoir_versions > 0:
    display(
        unknown_reservoir_versions_df
    )

    raise ValueError(
        "Existen versiones NEM sin orden "
        "de liquidación definido."
    )


display(
    reservoir_energy_prioritized_df
    .groupBy(
        "version",
        "prioridad_version",
    )
    .agg(
        F.count("*").alias("registros"),
        F.min("fecha_medicion").alias("fecha_minima"),
        F.max("fecha_medicion").alias("fecha_maxima"),
    )
    .orderBy("prioridad_version")
)

# COMMAND ----------

reservoir_energy_business_key = [
    "fecha_medicion",
    "codigo_planta",
    "codigo_variable",
    "codigo_duracion",
    "unidad_medida",
]


reservoir_energy_window = (
    Window
    .partitionBy(
        *reservoir_energy_business_key
    )
    .orderBy(
        F.col(
            "prioridad_version"
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

        F.col(
            "version"
        ).desc(),
    )
)


reservoir_energy_selected_df = (
    reservoir_energy_prioritized_df
    .withColumn(
        "row_number",
        F.row_number().over(
            reservoir_energy_window
        ),
    )
    .filter(
        F.col("row_number") == 1
    )
    .drop("row_number")
)


reservoir_energy_silver_rows = (
    reservoir_energy_base_df.count()
)


reservoir_energy_selected_rows = (
    reservoir_energy_selected_df.count()
)


print(
    "Filas Silver con todas las liquidaciones:",
    f"{reservoir_energy_silver_rows:,}",
)

print(
    "Mediciones consolidadas:",
    f"{reservoir_energy_selected_rows:,}",
)

print(
    "Versiones anteriores no seleccionadas:",
    f"{reservoir_energy_silver_rows - reservoir_energy_selected_rows:,}",
)

# COMMAND ----------

reservoir_energy_enriched_df = (
    reservoir_energy_selected_df.alias("source")

    .join(
        spark.table(
            DIM_FECHA_TABLE
        )
        .select(
            "fecha_key",
            "fecha",
        )
        .alias("date_dim"),

        F.col("source.fecha_medicion")
        ==
        F.col("date_dim.fecha"),

        "left",
    )

    .join(
        spark.table(
            DIM_PLANTA_TABLE
        )
        .select(
            "planta_key",
            "codigo_planta",
        )
        .alias("plant_dim"),

        F.col("source.codigo_planta")
        ==
        F.col("plant_dim.codigo_planta"),

        "left",
    )

    .select(
        F.col("source.*"),
        F.col("date_dim.fecha_key"),
        F.col("plant_dim.planta_key"),
    )
)

# COMMAND ----------

reservoir_energy_missing_dimensions_df = (
    reservoir_energy_enriched_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("planta_key").isNull()
    )
)


reservoir_energy_missing_dimensions = (
    reservoir_energy_missing_dimensions_df.count()
)


display(
    reservoir_energy_enriched_df
    .agg(
        F.count("*").alias(
            "total_mediciones"
        ),

        F.sum(
            F.when(
                F.col("fecha_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_fecha"),

        F.sum(
            F.when(
                F.col("planta_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("sin_planta"),
    )
)


if reservoir_energy_missing_dimensions > 0:
    display(
        reservoir_energy_missing_dimensions_df
        .select(
            "fecha_medicion",
            "codigo_planta",
            "version",
            "fecha_key",
            "planta_key",
        )
        .limit(100)
    )

    raise ValueError(
        "Existen mediciones NEM sin "
        "cobertura dimensional."
    )


print(
    "Cobertura dimensional de NEM aprobada."
)

# COMMAND ----------

fact_reservoir_energy_source_df = (
    reservoir_energy_enriched_df
    .select(
        F.sha2(
            F.concat_ws(
                "||",
                F.date_format(
                    "fecha_medicion",
                    "yyyy-MM-dd",
                ),
                F.col("codigo_planta"),
                F.col("codigo_variable"),
                F.col("codigo_duracion"),
                F.col("unidad_medida"),
            ),
            256,
        ).alias(
            "energia_embalsada_key"
        ),

        F.col("fecha_key")
        .cast("int"),

        F.col("planta_key")
        .cast("bigint"),

        F.col("fecha_medicion"),

        F.col("valor")
        .cast("decimal(24,6)")
        .alias(
            "energia_embalsada_kwh"
        ),

        F.col("es_valor_cero")
        .cast("boolean"),

        F.col("version").alias(
            "version_seleccionada"
        ),

        F.col("prioridad_version")
        .cast("int"),

        F.col("planta_encontrada").alias(
            "planta_provenia_de_maestro"
        ),

        F.current_timestamp().alias(
            "fecha_creacion"
        ),

        F.current_timestamp().alias(
            "fecha_actualizacion"
        ),
    )
)


fact_reservoir_energy_rows = (
    fact_reservoir_energy_source_df.count()
)


fact_reservoir_energy_keys = (
    fact_reservoir_energy_source_df
    .select("energia_embalsada_key")
    .distinct()
    .count()
)


print(
    "Filas fuente del hecho:",
    f"{fact_reservoir_energy_rows:,}",
)

print(
    "Claves distintas:",
    f"{fact_reservoir_energy_keys:,}",
)


if (
    fact_reservoir_energy_rows
    != fact_reservoir_energy_keys
):
    raise ValueError(
        "La fuente de energía embalsada "
        "contiene claves duplicadas."
    )

# COMMAND ----------

FACT_ENERGIA_EMBALSADA_TABLE = GOLD_TABLES["fact_energia_embalsada_planta"]


fact_reservoir_energy_target = (
    DeltaTable.forName(
        spark,
        FACT_ENERGIA_EMBALSADA_TABLE,
    )
)


(
    fact_reservoir_energy_target.alias("target")
    .merge(
        fact_reservoir_energy_source_df.alias("source"),
        """
        target.energia_embalsada_key =
            source.energia_embalsada_key
        """
    )
    .whenMatchedUpdate(
        condition="""
            NOT (
                target.fecha_key
                <=> source.fecha_key
            )
            OR NOT (
                target.planta_key
                <=> source.planta_key
            )
            OR NOT (
                target.fecha_medicion
                <=> source.fecha_medicion
            )
            OR NOT (
                target.energia_embalsada_kwh
                <=> source.energia_embalsada_kwh
            )
            OR NOT (
                target.es_valor_cero
                <=> source.es_valor_cero
            )
            OR NOT (
                target.version_seleccionada
                <=> source.version_seleccionada
            )
            OR NOT (
                target.prioridad_version
                <=> source.prioridad_version
            )
            OR NOT (
                target.planta_provenia_de_maestro
                <=> source.planta_provenia_de_maestro
            )
        """,
        set={
            "fecha_key":
                "source.fecha_key",

            "planta_key":
                "source.planta_key",

            "fecha_medicion":
                "source.fecha_medicion",

            "energia_embalsada_kwh":
                "source.energia_embalsada_kwh",

            "es_valor_cero":
                "source.es_valor_cero",

            "version_seleccionada":
                "source.version_seleccionada",

            "prioridad_version":
                "source.prioridad_version",

            "planta_provenia_de_maestro":
                "source.planta_provenia_de_maestro",

            "fecha_actualizacion":
                "source.fecha_actualizacion",
        },
    )
    .whenNotMatchedInsertAll()
    .execute()
)


print(
    "MERGE de fact_energia_embalsada_planta "
    "completado."
)

# COMMAND ----------

fact_reservoir_energy_validation_df = (
    spark.table(
        FACT_ENERGIA_EMBALSADA_TABLE
    )
)


reservoir_energy_fact_rows = (
    fact_reservoir_energy_validation_df.count()
)


reservoir_energy_fact_keys = (
    fact_reservoir_energy_validation_df
    .select("energia_embalsada_key")
    .distinct()
    .count()
)


reservoir_energy_null_dimensions = (
    fact_reservoir_energy_validation_df
    .filter(
        F.col("fecha_key").isNull()
        | F.col("planta_key").isNull()
    )
    .count()
)


reservoir_energy_negative_values = (
    fact_reservoir_energy_validation_df
    .filter(
        F.col("energia_embalsada_kwh") < 0
    )
    .count()
)


print(
    "Filas fact_energia_embalsada_planta:",
    f"{reservoir_energy_fact_rows:,}",
)

print(
    "Claves distintas:",
    f"{reservoir_energy_fact_keys:,}",
)

print(
    "Dimensiones nulas:",
    reservoir_energy_null_dimensions,
)

print(
    "Valores negativos:",
    reservoir_energy_negative_values,
)


if (
    reservoir_energy_fact_rows
    != reservoir_energy_fact_keys
):
    raise ValueError(
        "fact_energia_embalsada_planta contiene "
        "claves duplicadas."
    )


if reservoir_energy_null_dimensions > 0:
    raise ValueError(
        "fact_energia_embalsada_planta contiene "
        "dimensiones nulas."
    )


if reservoir_energy_negative_values > 0:
    raise ValueError(
        "fact_energia_embalsada_planta contiene "
        "valores negativos."
    )


display(
    fact_reservoir_energy_validation_df
    .agg(
        F.min("fecha_medicion").alias(
            "fecha_minima"
        ),

        F.max("fecha_medicion").alias(
            "fecha_maxima"
        ),

        F.countDistinct(
            "planta_key"
        ).alias(
            "plantas_distintas"
        ),

        F.sum(
            F.when(
                F.col("es_valor_cero"),
                1,
            ).otherwise(0)
        ).alias(
            "mediciones_cero"
        ),

        F.sum(
            F.when(
                ~F.col(
                    "planta_provenia_de_maestro"
                ),
                1,
            ).otherwise(0)
        ).alias(
            "mediciones_de_plantas_inferidas"
        ),
    )
)


display(
    fact_reservoir_energy_validation_df
    .groupBy(
        "version_seleccionada",
        "prioridad_version",
    )
    .count()
    .orderBy(
        F.desc("prioridad_version")
    )
)


print(
    "fact_energia_embalsada_planta aprobada."
)
