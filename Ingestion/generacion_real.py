# Databricks notebook source
# MAGIC %md
# MAGIC # Generación real por planta

# COMMAND ----------

# MAGIC %pip install pydataxm

# COMMAND ----------

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import gzip
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

SOURCE_NAME = "generacion_real"

DATASET_ID = "055A4D"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
landing_file = LANDING_FILES[SOURCE_NAME]

# Fecha actual en Colombia
fecha_fin = datetime.now(
    ZoneInfo(TIMEZONE)
).date()

# Revisar si Bronze está vacía sin contar toda la tabla
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

print(f"Modo de ejecución: {execution_mode}")
print(f"Tabla Bronze: {bronze_table}")
print(
    f"Rango solicitado a SIMEM: "
    f"{fecha_inicio_str} a {fecha_fin_str}"
)

df_generacion = read_simem_chunked(
    ReadSIMEM, DATASET_ID, fecha_inicio, fecha_fin, chunk_days(dbutils)
)

if df_generacion is None or df_generacion.empty:
    raise ValueError(
        f"SIMEM no devolvió registros entre "
        f"{fecha_inicio_str} y {fecha_fin_str}"
    )

print(f"Registros descargados: {len(df_generacion):,}")

display(df_generacion)

# COMMAND ----------

# Guardar como un solo archivo JSON usando pandas, sobrescribiendo si ya existe
# Usar método 'to_json' con compresión para acelerar escritura y reducir tamaño
df_generacion.to_json(
    landing_file,
    orient="records",
    lines=True,
    compression="gzip",
)
# Para 10,000 filas, guardar en un solo archivo JSON comprimido es eficiente y adecuado; no es necesario dividir en varios archivos.
