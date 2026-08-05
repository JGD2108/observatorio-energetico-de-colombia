# Databricks notebook source
"""Memory-safe SIMEM ingestion for plant availability."""

from datetime import datetime, timedelta
import gzip
import os
import sys
from zoneinfo import ZoneInfo

import pandas as pd
from pydataxm.pydatasimem import ReadSIMEM

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from backfill.runtime import chunk_days, iter_simem_frames, resolve_window, retry_settings  # noqa: E402
from config.project_config import (  # noqa: E402
    BRONZE_TABLES,
    DEFAULT_HISTORICAL_START_DATE,
    LANDING_FILES,
    LOOKBACK_DAYS,
    TIMEZONE,
)

SOURCE_NAME = "disponibilidad_plantas"
DATASET_ID = "9E77E5"
MAXIMUM_ACCEPTED_LAG_DAYS = 20
MAX_SAFE_CHUNK_DAYS = 31

bronze_table = BRONZE_TABLES[SOURCE_NAME]
landing_file = LANDING_FILES[SOURCE_NAME]
today = datetime.now(ZoneInfo(TIMEZONE)).date()

if spark.catalog.tableExists(bronze_table):
    bronze_max_date = spark.sql(
        f"SELECT MAX(CAST(fecha_hora AS DATE)) AS max_date FROM {bronze_table}"
    ).first()["max_date"]
else:
    bronze_max_date = None

if bronze_max_date is None:
    default_start = DEFAULT_HISTORICAL_START_DATE
    default_mode = "BACKFILL"
else:
    default_start = max(
        DEFAULT_HISTORICAL_START_DATE,
        bronze_max_date - timedelta(days=LOOKBACK_DAYS),
    )
    default_mode = "INCREMENTAL"

start, end, execution_mode = resolve_window(dbutils, default_start, today, default_mode)
configured_chunk_days = chunk_days(dbutils)
effective_chunk_days = min(configured_chunk_days, MAX_SAFE_CHUNK_DAYS)
max_retries, retry_base_seconds = retry_settings(dbutils)

required_columns = {
    "CodigoVariable",
    "FechaHora",
    "CodigoDuracion",
    "UnidadMedida",
    "CodigoPlanta",
    "Version",
    "Valor",
}
natural_key = ["CodigoVariable", "FechaHora", "CodigoPlanta", "Version"]

timestamp = datetime.now(ZoneInfo(TIMEZONE)).strftime("%Y%m%dT%H%M%S")
temporary_path = landing_file.replace(".json.gz", f".{timestamp}.tmp.json.gz")
downloaded_rows = 0
new_rows = 0
source_min_date = None
source_max_date = None

print("Modo:", execution_mode)
print("Rango solicitado:", start, "a", end)
print("Chunk configurado/general:", configured_chunk_days)
print("Chunk seguro para disponibilidad:", effective_chunk_days)

# Each frame is validated and written before the next request. The annual
# pandas DataFrame is never materialized in driver memory.
with gzip.open(temporary_path, "wt", encoding="utf-8") as landing_handle:
    for frame in iter_simem_frames(
        ReadSIMEM,
        DATASET_ID,
        start,
        end,
        effective_chunk_days,
        max_retries,
        retry_base_seconds,
    ):
        missing = required_columns - set(frame.columns)
        if missing:
            raise ValueError(f"Faltan columnas esperadas: {sorted(missing)}")

        parsed_dates = pd.to_datetime(frame["FechaHora"], errors="coerce")
        invalid_dates = int(parsed_dates.isna().sum())
        if invalid_dates:
            raise ValueError(f"Hay {invalid_dates:,} valores invalidos en FechaHora")

        duplicate_rows = int(frame.duplicated(natural_key).sum())
        if duplicate_rows:
            raise ValueError(
                f"SIMEM devolvio {duplicate_rows:,} duplicados para {natural_key}"
            )

        chunk_min = parsed_dates.min().date()
        chunk_max = parsed_dates.max().date()
        source_min_date = min(source_min_date, chunk_min) if source_min_date else chunk_min
        source_max_date = max(source_max_date, chunk_max) if source_max_date else chunk_max
        downloaded_rows += len(frame)
        if bronze_max_date is None:
            new_rows += len(frame)
        else:
            new_rows += int((parsed_dates.dt.date > bronze_max_date).sum())

        frame.to_json(landing_handle, orient="records", lines=True)

if downloaded_rows == 0:
    raise ValueError(f"SIMEM no devolvio datos entre {start} y {end}")

source_lag_days = (end - source_max_date).days
if bronze_max_date is not None and source_max_date < bronze_max_date:
    raise ValueError(
        f"La extraccion termina antes que Bronze: SIMEM={source_max_date}, "
        f"Bronze={bronze_max_date}"
    )
if source_lag_days > MAXIMUM_ACCEPTED_LAG_DAYS:
    raise ValueError(
        f"La fuente presenta {source_lag_days} dias de rezago; "
        f"el maximo es {MAXIMUM_ACCEPTED_LAG_DAYS}"
    )
if not os.path.exists(temporary_path) or os.path.getsize(temporary_path) == 0:
    raise IOError(f"El archivo temporal no se creo correctamente: {temporary_path}")

os.replace(temporary_path, landing_file)

validation = spark.read.json(landing_file)
read_rows = validation.count()
if read_rows != downloaded_rows:
    raise ValueError(
        f"Landing incompleto: escritos={downloaded_rows:,}, leidos={read_rows:,}"
    )

print("Ingesta finalizada correctamente")
print("Archivo Landing:", landing_file)
print("Registros escritos:", f"{downloaded_rows:,}")
print("Registros nuevos frente a Bronze:", f"{new_rows:,}")
print("Cobertura recibida:", source_min_date, "a", source_max_date)
print("Rezago observado:", source_lag_days, "dias")
display(validation.limit(10))
