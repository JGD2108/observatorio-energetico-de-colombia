# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Load Bronze Tables
# MAGIC
# MAGIC This notebook loads the Bronze tables for the project from the Incoming JSON files.

# COMMAND ----------

from functools import reduce
from pathlib import Path
import sys

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from config.project_config import (
    BRONZE_TABLES,
    LANDING_FILES,
)

# COMMAND ----------

def file_name_from_path(file_path: str) -> str:
    return Path(file_path).name


def landing_file_exists(file_path: str) -> bool:
    try:
        dbutils.fs.ls(file_path)
        return True
    except Exception:
        return False

# COMMAND ----------

def append_new_records_to_bronze(
    df_source: DataFrame,
    target_table: str,
    payload_columns: list[str],
    source_path: str,
) -> dict:
    """
    Elimina duplicados del archivo Landing y agrega a Bronze
    únicamente registros que todavía no existen.

    La comparación utiliza las columnas del payload y no los
    metadatos técnicos de ingesta.
    """

    source_file_name = file_name_from_path(
        source_path
    )

    df_incoming = (
        df_source
        .withColumn(
            "source_file_name",
            F.lit(source_file_name),
        )
        .withColumn(
            "source_file_path",
            F.lit(source_path),
        )
        .withColumn(
            "ingestion_timestamp",
            F.current_timestamp(),
        )
        .withColumn(
            "load_date",
            F.current_date(),
        )
        .dropDuplicates(payload_columns)
    )

    received_rows = df_source.count()
    unique_rows = df_incoming.count()

    if not spark.catalog.tableExists(
        target_table
    ):
        (
            df_incoming.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )

        return {
            "target_table": target_table,
            "received_rows": received_rows,
            "unique_rows": unique_rows,
            "new_rows": unique_rows,
            "table_created": True,
        }

    df_existing = (
        spark.table(target_table)
        .select(*payload_columns)
        .dropDuplicates()
    )

    join_condition = reduce(
        lambda left, right: left & right,
        [
            F.col(
                f"incoming.{column}"
            ).eqNullSafe(
                F.col(
                    f"existing.{column}"
                )
            )
            for column in payload_columns
        ],
    )

    df_new = (
        df_incoming.alias("incoming")
        .join(
            df_existing.alias("existing"),
            join_condition,
            "left_anti",
        )
        .select("incoming.*")
    )

    new_rows = df_new.count()

    if new_rows > 0:
        (
            df_new.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

    return {
        "target_table": target_table,
        "received_rows": received_rows,
        "unique_rows": unique_rows,
        "new_rows": new_rows,
        "table_created": False,
    }

# COMMAND ----------

def print_load_summary(
    source_name: str,
    summary: dict,
) -> None:
    print("=" * 70)
    print("Fuente:", source_name)
    print(
        "Tabla:",
        summary["target_table"],
    )
    print(
        "Registros recibidos:",
        f'{summary["received_rows"]:,}',
    )
    print(
        "Registros únicos:",
        f'{summary["unique_rows"]:,}',
    )
    print(
        "Registros nuevos:",
        f'{summary["new_rows"]:,}',
    )
    print(
        "Tabla creada:",
        summary["table_created"],
    )

# COMMAND ----------

SOURCE_NAME = "agentes"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


df_source = (
    spark.read
    .json(source_path)
)


df_transformed = (
    df_source
    .select(
        F.col("Fecha")
        .cast("string")
        .alias("fecha"),

        F.col("CodigoDuracion")
        .cast("string")
        .alias("codigo_duracion"),

        F.col("CodigoSICAgente")
        .cast("string")
        .alias("codigo_sic_agente"),

        F.col("NombreAgente")
        .cast("string")
        .alias("nombre_agente"),

        F.col("ActividadAgente")
        .cast("string")
        .alias("actividad_agente"),
    )
)


payload_columns = [
    "fecha",
    "codigo_duracion",
    "codigo_sic_agente",
    "nombre_agente",
    "actividad_agente",
]


summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

SOURCE_NAME = "demanda_real"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


df_source = spark.read.json(
    source_path
)


df_transformed = (
    df_source
    .select(
        F.col("CodigoVariable")
        .cast("string")
        .alias("codigo_variable"),

        F.col("FechaHora")
        .cast("string")
        .alias("fecha_hora"),

        F.col("CodigoSICAgente")
        .cast("string")
        .alias("codigo_sic_agente"),

        F.col("TipoMercado")
        .cast("string")
        .alias("tipo_mercado"),

        F.col("Version")
        .cast("string")
        .alias("version"),

        F.col("Valor")
        .cast("string")
        .alias("valor"),

        F.col("UnidadMedida")
        .cast("string")
        .alias("unidad_medida"),

        F.col("CodigoDuracion")
        .cast("string")
        .alias("codigo_duracion"),
    )
)


payload_columns = [
    "codigo_variable",
    "fecha_hora",
    "codigo_sic_agente",
    "tipo_mercado",
    "version",
    "valor",
    "unidad_medida",
    "codigo_duracion",
]


summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

SOURCE_NAME = "disponibilidad_plantas"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


df_source = spark.read.json(
    source_path
)


df_transformed = (
    df_source
    .select(
        F.col("CodigoDuracion")
        .cast("string")
        .alias("codigo_duracion"),

        F.col("CodigoPlanta")
        .cast("string")
        .alias("codigo_planta"),

        F.col("CodigoVariable")
        .cast("string")
        .alias("codigo_variable"),

        F.col("FechaHora")
        .cast("string")
        .alias("fecha_hora"),

        F.col("UnidadMedida")
        .cast("string")
        .alias("unidad_medida"),

        F.col("Valor")
        .cast("string")
        .alias("valor"),

        F.col("Version")
        .cast("string")
        .alias("version"),
    )
)


payload_columns = [
    "codigo_duracion",
    "codigo_planta",
    "codigo_variable",
    "fecha_hora",
    "unidad_medida",
    "valor",
    "version",
]


summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

SOURCE_NAME = "plantas"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


df_source = spark.read.json(
    source_path
)


df_transformed = (
    df_source
    .select(
        F.col("Fecha")
        .cast("string")
        .alias("fecha"),

        F.col("CodigoDuracion")
        .cast("string")
        .alias("codigo_duracion"),

        F.col("CodigoPlanta")
        .cast("string")
        .alias("codigo_planta"),

        F.col("NombrePlanta")
        .cast("string")
        .alias("nombre_planta"),

        F.col("CodigoSICAgente")
        .cast("string")
        .alias("codigo_sic_agente"),

        F.col("CapEfectivaNeta")
        .cast("string")
        .alias("cap_efectiva_neta"),

        F.col("FPO")
        .cast("string")
        .alias("fpo"),

        F.col("CodigoSubAreaOperativa")
        .cast("string")
        .alias("codigo_sub_area_operativa"),

        F.col("CodigoAreaOperativa")
        .cast("string")
        .alias("codigo_area_operativa"),

        F.col("TipoDespachoRecurso")
        .cast("string")
        .alias("tipo_despacho_recurso"),

        F.col("TipoClasificacion")
        .cast("string")
        .alias("tipo_clasificacion"),

        F.col("TipoGeneracion")
        .cast("string")
        .alias("tipo_generacion"),
    )
)


payload_columns = [
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
]


summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

SOURCE_NAME = "generacion_real"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


df_source = spark.read.json(
    source_path
)


df_transformed = (
    df_source
    .select(
        F.col("CodigoDuracion")
        .cast("string")
        .alias("codigo_duracion"),

        F.col("CodigoPlanta")
        .cast("string")
        .alias("codigo_planta"),

        F.col("CodigoSICAgente")
        .cast("string")
        .alias("codigo_sic_agente"),

        F.col("CodigoVariable")
        .cast("string")
        .alias("codigo_variable"),

        F.col("FechaHora")
        .cast("string")
        .alias("fecha_hora"),

        F.col("UnidadMedida")
        .cast("string")
        .alias("unidad_medida"),

        F.col("Valor")
        .cast("string")
        .alias("valor"),

        F.col("Version")
        .cast("string")
        .alias("version"),
    )
)


payload_columns = [
    "codigo_duracion",
    "codigo_planta",
    "codigo_sic_agente",
    "codigo_variable",
    "fecha_hora",
    "unidad_medida",
    "valor",
    "version",
]


summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

SOURCE_NAME = "niveles_embalses"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


df_source = spark.read.json(
    source_path
)


df_transformed = (
    df_source
    .select(
        F.col("CodigoDuracion")
        .cast("string")
        .alias("codigo_duracion"),

        F.col("CodigoPlanta")
        .cast("string")
        .alias("codigo_planta"),

        F.col("CodigoVariable")
        .cast("string")
        .alias("codigo_variable"),

        F.col("FechaInicio")
        .cast("string")
        .alias("fecha_inicio"),

        F.col("UnidadMedida")
        .cast("string")
        .alias("unidad_medida"),

        F.col("Valor")
        .cast("string")
        .alias("valor"),

        F.col("Version")
        .cast("string")
        .alias("version"),
    )
)


payload_columns = [
    "codigo_duracion",
    "codigo_planta",
    "codigo_variable",
    "fecha_inicio",
    "unidad_medida",
    "valor",
    "version",
]


summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

SOURCE_NAME = "precio_bolsa"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


df_source = spark.read.json(
    source_path
)


df_transformed = (
    df_source
    .select(
        F.col("CodigoVariable")
        .cast("string")
        .alias("codigo_variable"),

        F.col("FechaHora")
        .cast("string")
        .alias("fecha_hora"),

        F.col("CodigoDuracion")
        .cast("string")
        .alias("codigo_duracion"),

        F.col("UnidadMedida")
        .cast("string")
        .alias("unidad_medida"),

        F.col("Version")
        .cast("string")
        .alias("version"),

        F.col("Valor")
        .cast("string")
        .alias("valor"),
    )
)


payload_columns = [
    "codigo_variable",
    "fecha_hora",
    "codigo_duracion",
    "unidad_medida",
    "version",
    "valor",
]


summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

SOURCE_NAME = "embalses"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]


if landing_file_exists(source_path):
    df_source = spark.read.json(
        source_path
    )

    df_transformed = (
        df_source
        .select(
            F.col("CodigoEmbalse")
            .cast("string")
            .alias("codigo_embalse"),

            F.col("NombreEmbalse")
            .cast("string")
            .alias("nombre_embalse"),

            F.col("Latitud")
            .cast("double")
            .alias("latitud"),

            F.col("Longitud")
            .cast("double")
            .alias("longitud"),

            F.col("TipoCoordenada")
            .cast("string")
            .alias("tipo_coordenada"),

            F.col("CoordinateSource")
            .cast("string")
            .alias("coordinate_source"),

            F.col("GeocodingStatus")
            .cast("string")
            .alias("geocoding_status"),

            F.col("GeocodingQuery")
            .cast("string")
            .alias("geocoding_query"),
        )
    )

    payload_columns = [
        "codigo_embalse",
        "nombre_embalse",
        "latitud",
        "longitud",
        "tipo_coordenada",
        "coordinate_source",
        "geocoding_status",
        "geocoding_query",
    ]

    summary = append_new_records_to_bronze(
        df_source=df_transformed,
        target_table=target_table,
        payload_columns=payload_columns,
        source_path=source_path,
    )

    print_load_summary(
        SOURCE_NAME,
        summary,
    )

else:
    print(
        "Fuente maestra omitida: no existe",
        source_path,
    )

# COMMAND ----------

SOURCE_NAME = "plantas_reservorios"

source_path = LANDING_FILES[SOURCE_NAME]
target_table = BRONZE_TABLES[SOURCE_NAME]

if landing_file_exists(source_path):
    df_source = spark.read.json(source_path)

else:
    raise FileNotFoundError(
        "No existe la fuente maestra planta-embalse. "
        "Se detiene Bronze para impedir que se reutilice el DataFrame "
        f"de otra fuente: {source_path}"
    )


df_transformed = (
    df_source
    .select(
        F.col("region")
        .cast("string")
        .alias("region"),

        F.col("plant_name")
        .cast("string")
        .alias("nombre_planta"),

        F.col("reservoir_name")
        .cast("string")
        .alias("nombre_reservorio"),

        F.col("relationship_type")
        .cast("string")
        .alias("tipo_relacion"),

        F.col("is_primary")
        .cast("boolean")
        .alias("es_principal"),

        F.col("attribution_allowed")
        .cast("boolean")
        .alias("permite_atribucion"),

        F.col("source_name")
        .cast("string")
        .alias("fuente_relacion"),

        F.col("validation_status")
        .cast("string")
        .alias("estado_validacion"),

        F.col("valid_from")
        .cast("date")
        .alias("valido_desde"),

        F.col("valid_to")
        .cast("date")
        .alias("valido_hasta"),
    )
)


payload_columns = [
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
]

summary = append_new_records_to_bronze(
    df_source=df_transformed,
    target_table=target_table,
    payload_columns=payload_columns,
    source_path=source_path,
)


print_load_summary(
    SOURCE_NAME,
    summary,
)

# COMMAND ----------

bronze_validation = []

for source_name, table_name in BRONZE_TABLES.items():
    if spark.catalog.tableExists(table_name):
        row_count = spark.table(
            table_name
        ).count()

        bronze_validation.append(
            {
                "source_name": source_name,
                "table_name": table_name,
                "row_count": row_count,
                "status": "OK",
            }
        )
    else:
        bronze_validation.append(
            {
                "source_name": source_name,
                "table_name": table_name,
                "row_count": None,
                "status": "MISSING",
            }
        )


df_bronze_validation = spark.createDataFrame(
    bronze_validation
)


display(
    df_bronze_validation.orderBy(
        "source_name"
    )
)
