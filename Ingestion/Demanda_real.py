# Databricks notebook source
# MAGIC %md
# MAGIC ##Demanda real

# COMMAND ----------

# pydataxm se instala como dependencia versionada del job.

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import gzip
import sys

from pydataxm.pydatasimem import ReadSIMEM

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backfill.runtime import chunk_days, read_simem_chunked, resolve_window, retry_settings  # noqa: E402

from config.project_config import (
    TIMEZONE,
    LOOKBACK_DAYS,
    DEFAULT_HISTORICAL_START_DATE,
    BRONZE_TABLES,
    LANDING_FILES,
)

SOURCE_NAME = "demanda_real"
DATASET_ID = "14FABB"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
landing_file = LANDING_FILES[SOURCE_NAME]

fecha_fin = datetime.now(
    ZoneInfo(TIMEZONE)
).date()

bronze_table_exists = spark.catalog.tableExists(
    bronze_table
)

if bronze_table_exists:
    bronze_is_empty = len(
        spark.table(bronze_table).head(1)
    ) == 0
else:
    bronze_is_empty = True

if bronze_is_empty:
    fecha_inicio = DEFAULT_HISTORICAL_START_DATE
    execution_mode = "BACKFILL"
else:
    fecha_inicio = fecha_fin - timedelta(
        days=LOOKBACK_DAYS
    )
    execution_mode = "INCREMENTAL"

fecha_inicio, fecha_fin, execution_mode = resolve_window(
    dbutils, fecha_inicio, fecha_fin, execution_mode
)

fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")

print("Modo de ejecución:", execution_mode)
print("Tabla Bronze:", bronze_table)
print(
    f"Rango solicitado a SIMEM: "
    f"{fecha_inicio_str} a {fecha_fin_str}"
)

simem_max_retries, simem_retry_base_seconds = retry_settings(dbutils)
df_demanda = read_simem_chunked(
    ReadSIMEM, DATASET_ID, fecha_inicio, fecha_fin, chunk_days(dbutils),
    simem_max_retries, simem_retry_base_seconds,
)

if df_demanda is None or df_demanda.empty:
    raise ValueError(
        "SIMEM no devolvió demanda para el rango solicitado"
    )

print(f"Registros descargados: {len(df_demanda):,}")

df_demanda.to_json(
    landing_file,
    orient="records",
    lines=True,
    mode="w",
)

# COMMAND ----------

import os

print("Archivo existe:", os.path.exists(landing_file))
print(
    "Tamaño en bytes:",
    os.path.getsize(landing_file),
)

# COMMAND ----------

df_validation = spark.read.json(landing_file)

written_rows = len(df_demanda)
read_rows = df_validation.count()

print("Registros escritos:", written_rows)
print("Registros leídos:", read_rows)
print("Coinciden:", written_rows == read_rows)

display(df_validation.limit(10))
