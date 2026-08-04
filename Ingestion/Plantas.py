# Databricks notebook source
# MAGIC %md
# MAGIC Plantas

# COMMAND ----------

# pydataxm se instala como dependencia versionada del job.

# COMMAND ----------

from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from pydataxm.pydatasimem import ReadSIMEM
import sys

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


SOURCE_NAME = "plantas"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
landing_file = LANDING_FILES[SOURCE_NAME]
DATASET_ID = "0bfc9d"

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

df_plantas = read_simem_chunked(
    ReadSIMEM, DATASET_ID, fecha_inicio, fecha_fin, chunk_days(dbutils)
)

if df_plantas is None or df_plantas.empty:
    raise ValueError(
        "SIMEM no devolvió plantas para el rango solicitado"
    )

print(f"Registros descargados: {len(df_plantas):,}")

df_plantas.to_json(
    landing_file,
    orient="records",
    lines=True,
    mode="w",
)

# COMMAND ----------

print("Ingesta finalizada correctamente")
print("Fuente:", SOURCE_NAME)
print("Dataset SIMEM:", DATASET_ID)
print("Modo:", execution_mode)
print("Archivo Landing:", landing_file)
print("Registros escritos:", f"{len(df_plantas):,}")
