# Databricks notebook source
# MAGIC %md
# MAGIC #Disponibilidad planta

# COMMAND ----------

# MAGIC %pip install pydataxm

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import sys

import pandas as pd
from pydataxm.pydatasimem import ReadSIMEM


NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backfill.runtime import chunk_days, read_simem_chunked, resolve_window  # noqa: E402


from config.project_config import (
    TIMEZONE,
    LOOKBACK_DAYS,
    DEFAULT_HISTORICAL_START_DATE,
    BRONZE_TABLES,
    LANDING_FILES,
)


SOURCE_NAME = "disponibilidad_plantas"
DATASET_ID = "9E77E5"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
landing_file = LANDING_FILES[SOURCE_NAME]


# Umbral operativo temporal.
# Permite que la fuente tenga hasta 20 días de rezago.
MAXIMUM_ACCEPTED_LAG_DAYS = 20

# COMMAND ----------

fecha_fin = datetime.now(
    ZoneInfo(TIMEZONE)
).date()


if not spark.catalog.tableExists(bronze_table):
    bronze_max_date = None
else:
    bronze_max_date = spark.sql(
        f"""
        SELECT MAX(CAST(fecha_hora AS DATE)) AS max_date
        FROM {bronze_table}
        """
    ).first()["max_date"]


if bronze_max_date is None:
    fecha_inicio = DEFAULT_HISTORICAL_START_DATE
    execution_mode = "BACKFILL"
else:
    fecha_inicio = max(
        DEFAULT_HISTORICAL_START_DATE,
        bronze_max_date - timedelta(
            days=LOOKBACK_DAYS
        ),
    )
    execution_mode = "INCREMENTAL"

fecha_inicio, fecha_fin, execution_mode = resolve_window(
    dbutils, fecha_inicio, fecha_fin, execution_mode
)


fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")


print("Modo de ejecución:", execution_mode)
print("Fuente:", SOURCE_NAME)
print("Dataset SIMEM:", DATASET_ID)
print("Tabla Bronze:", bronze_table)
print("Archivo Landing:", landing_file)
print("Fecha máxima actual en Bronze:", bronze_max_date)
print(
    "Rango solicitado a SIMEM:",
    fecha_inicio_str,
    "a",
    fecha_fin_str,
)

# COMMAND ----------

df_disponibilidad_planta = read_simem_chunked(
    ReadSIMEM, DATASET_ID, fecha_inicio, fecha_fin, chunk_days(dbutils)
)


if (
    df_disponibilidad_planta is None
    or df_disponibilidad_planta.empty
):
    raise ValueError(
        "SIMEM no devolvió disponibilidad de plantas "
        f"entre {fecha_inicio_str} y {fecha_fin_str}"
    )


required_columns = {
    "CodigoVariable",
    "FechaHora",
    "CodigoDuracion",
    "UnidadMedida",
    "CodigoPlanta",
    "Version",
    "Valor",
}


missing_columns = (
    required_columns
    - set(df_disponibilidad_planta.columns)
)


if missing_columns:
    raise ValueError(
        "Faltan columnas esperadas: "
        f"{sorted(missing_columns)}"
    )


parsed_dates = pd.to_datetime(
    df_disponibilidad_planta["FechaHora"],
    errors="coerce",
)


invalid_date_rows = int(
    parsed_dates.isna().sum()
)


if invalid_date_rows > 0:
    raise ValueError(
        f"Hay {invalid_date_rows:,} valores inválidos "
        "en FechaHora"
    )


source_min_date = parsed_dates.min().date()
source_max_date = parsed_dates.max().date()

source_lag_days = (
    fecha_fin - source_max_date
).days


natural_key = [
    "CodigoVariable",
    "FechaHora",
    "CodigoPlanta",
    "Version",
]


duplicate_rows = int(
    df_disponibilidad_planta
    .duplicated(natural_key)
    .sum()
)


if duplicate_rows > 0:
    raise ValueError(
        f"SIMEM devolvió {duplicate_rows:,} duplicados "
        f"para la llave natural {natural_key}"
    )


if (
    bronze_max_date is not None
    and source_max_date < bronze_max_date
):
    raise ValueError(
        "La extracción termina antes que Bronze. "
        f"SIMEM={source_max_date}, "
        f"Bronze={bronze_max_date}. "
        "No se publicará Landing."
    )


if source_lag_days > MAXIMUM_ACCEPTED_LAG_DAYS:
    raise ValueError(
        f"La fuente presenta {source_lag_days} días "
        "de rezago, por encima del umbral de "
        f"{MAXIMUM_ACCEPTED_LAG_DAYS}. "
        "No se publicará Landing."
    )


if bronze_max_date is None:
    new_rows = len(
        df_disponibilidad_planta
    )
else:
    new_rows = int(
        (
            parsed_dates.dt.date
            > bronze_max_date
        ).sum()
    )


validation_summary = pd.DataFrame(
    [
        {
            "execution_mode": execution_mode,
            "requested_start_date": fecha_inicio,
            "requested_end_date": fecha_fin,
            "bronze_max_date_before": bronze_max_date,
            "source_min_date": source_min_date,
            "source_max_date": source_max_date,
            "source_lag_days": source_lag_days,
            "downloaded_rows": len(
                df_disponibilidad_planta
            ),
            "new_rows_after_bronze_max": new_rows,
            "duplicate_rows": duplicate_rows,
        }
    ]
)


display(validation_summary)

# COMMAND ----------

timestamp = datetime.now(
    ZoneInfo(TIMEZONE)
).strftime("%Y%m%dT%H%M%S")


temporary_path = landing_file.replace(
    ".json.gz",
    f".{timestamp}.tmp.json.gz",
)


df_disponibilidad_planta.to_json(
    temporary_path,
    orient="records",
    lines=True,
    mode="w",
    compression="gzip",
)


if (
    not os.path.exists(temporary_path)
    or os.path.getsize(temporary_path) == 0
):
    raise IOError(
        "El archivo temporal no se creó "
        f"correctamente: {temporary_path}"
    )


os.replace(
    temporary_path,
    landing_file,
)


print("Ingesta finalizada correctamente")
print("Fuente:", SOURCE_NAME)
print("Dataset SIMEM:", DATASET_ID)
print("Modo:", execution_mode)
print("Archivo Landing:", landing_file)
print(
    "Registros escritos:",
    f"{len(df_disponibilidad_planta):,}",
)
print(
    "Cobertura recibida:",
    source_min_date,
    "a",
    source_max_date,
)
print(
    "Rezago observado:",
    source_lag_days,
    "días",
)

# COMMAND ----------

df_validation = spark.read.json(
    landing_file
)

written_rows = len(
    df_disponibilidad_planta
)

read_rows = df_validation.count()


print("Registros escritos:", written_rows)
print("Registros leídos:", read_rows)
print(
    "Coinciden:",
    written_rows == read_rows,
)

display(
    df_validation.limit(10)
)

# COMMAND ----------

df_validation.selectExpr(
    "MIN(FechaHora) AS fecha_minima",
    "MAX(FechaHora) AS fecha_maxima",
).show(truncate=False)


df_validation.groupBy(
    "Version"
).count().orderBy(
    "Version"
).show(truncate=False)
