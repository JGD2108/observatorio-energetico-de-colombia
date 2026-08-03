# Databricks notebook source
# MAGIC %md
# MAGIC # Volumen util de embalses diario

# COMMAND ----------

# pydataxm se instala como dependencia versionada del job.

# COMMAND ----------

# MAGIC %md
# MAGIC ### API SIMEM Volumen de embalses ingestion y guardar json en raw files landing

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from uuid import uuid4
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


from config.project_config import (
    TIMEZONE,
    LOOKBACK_DAYS,
    DEFAULT_HISTORICAL_START_DATE,
    BRONZE_TABLES,
    LANDING_FILES,
)


SOURCE_NAME = "niveles_embalses"
DATASET_ID = "BD26DC"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
landing_file = LANDING_FILES[SOURCE_NAME]

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
        SELECT MAX(CAST(fecha_inicio AS DATE)) AS max_date
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

df_niveles = ReadSIMEM(
    DATASET_ID,
    fecha_inicio_str,
    fecha_fin_str,
).main(filter=False)


if df_niveles is None or df_niveles.empty:
    raise ValueError(
        "SIMEM no devolvió niveles de embalses "
        f"entre {fecha_inicio_str} y {fecha_fin_str}"
    )


required_columns = {
    "CodigoDuracion",
    "CodigoPlanta",
    "CodigoVariable",
    "FechaInicio",
    "UnidadMedida",
    "Valor",
    "Version",
}


missing_columns = (
    required_columns
    - set(df_niveles.columns)
)


if missing_columns:
    raise ValueError(
        "Faltan columnas esperadas: "
        f"{sorted(missing_columns)}"
    )


parsed_dates = pd.to_datetime(
    df_niveles["FechaInicio"],
    errors="coerce",
)


invalid_date_rows = int(
    parsed_dates.isna().sum()
)


if invalid_date_rows > 0:
    raise ValueError(
        f"Hay {invalid_date_rows:,} valores inválidos "
        "en FechaInicio"
    )


source_min_date = parsed_dates.min().date()
source_max_date = parsed_dates.max().date()

source_lag_days = (
    fecha_fin - source_max_date
).days


natural_key = [
    "CodigoVariable",
    "FechaInicio",
    "CodigoPlanta",
    "Version",
]


duplicate_rows = int(
    df_niveles
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
        f"Bronze={bronze_max_date}."
    )


if source_lag_days > MAXIMUM_ACCEPTED_LAG_DAYS:
    raise ValueError(
        f"La fuente presenta {source_lag_days} días "
        "de rezago, por encima del umbral de "
        f"{MAXIMUM_ACCEPTED_LAG_DAYS}."
    )


print("Registros descargados:", f"{len(df_niveles):,}")
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

unique_id = uuid4().hex

temporary_path = landing_file.replace(
    ".json",
    f".{unique_id}.tmp.json",
)


df_niveles.to_json(
    temporary_path,
    orient="records",
    lines=True,
    mode="w",
    force_ascii=False,
)


if not os.path.exists(temporary_path):
    raise IOError(
        "El archivo temporal no fue creado: "
        f"{temporary_path}"
    )


temporary_line_count = 0

with open(
    temporary_path,
    "r",
    encoding="utf-8",
) as temporary_file:

    for _ in temporary_file:
        temporary_line_count += 1


expected_rows = len(df_niveles)


print("Filas esperadas:", expected_rows)
print(
    "Líneas del archivo temporal:",
    temporary_line_count,
)


if temporary_line_count != expected_rows:
    raise IOError(
        "La escritura temporal quedó incompleta. "
        f"Esperadas={expected_rows:,}, "
        f"escritas={temporary_line_count:,}. "
        "Landing no será reemplazado."
    )


os.replace(
    temporary_path,
    landing_file,
)


final_line_count = 0

with open(
    landing_file,
    "r",
    encoding="utf-8",
) as final_file:

    for _ in final_file:
        final_line_count += 1


if final_line_count != expected_rows:
    raise IOError(
        "El archivo Landing quedó incompleto. "
        f"Esperadas={expected_rows:,}, "
        f"publicadas={final_line_count:,}."
    )


print("Ingesta finalizada correctamente")
print("Fuente:", SOURCE_NAME)
print("Dataset SIMEM:", DATASET_ID)
print("Modo:", execution_mode)
print("Archivo Landing:", landing_file)
print(
    "Registros publicados:",
    f"{final_line_count:,}",
)

# COMMAND ----------

df_validation = spark.read.json(
    landing_file
)


written_rows = len(df_niveles)
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
    "MIN(FechaInicio) AS fecha_minima",
    "MAX(FechaInicio) AS fecha_maxima",
).show(truncate=False)


df_validation.groupBy(
    "CodigoVariable"
).count().orderBy(
    "CodigoVariable"
).show(truncate=False)


df_validation.groupBy(
    "Version"
).count().orderBy(
    "Version"
).show(truncate=False)


df_validation.groupBy(
    "UnidadMedida"
).count().show(
    truncate=False
)
