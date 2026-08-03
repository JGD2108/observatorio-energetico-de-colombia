# Databricks notebook source
# MAGIC %md
# MAGIC # Precio Bolsa Energia

# COMMAND ----------

# pydataxm se instala como dependencia versionada del job.

# COMMAND ----------

from pydataxm.pydatasimem import ReadSIMEM

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

from config.project_config import (
    TIMEZONE,
    LOOKBACK_DAYS,
    DEFAULT_HISTORICAL_START_DATE,
    BRONZE_TABLES,
    LANDING_FILES,
)


SOURCE_NAME = "precio_bolsa"
DATASET_ID = "EC6945"

bronze_table = BRONZE_TABLES[SOURCE_NAME]
landing_file = LANDING_FILES[SOURCE_NAME]

MAXIMUM_ACCEPTED_LAG_DAYS = 20

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


fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")

print(f"Modo de ejecución: {execution_mode}")
print(f"Tabla Bronze: {bronze_table}")
print(
    f"Rango solicitado a SIMEM: "
    f"{fecha_inicio_str} a {fecha_fin_str}"
)

df_precio_bolsa = ReadSIMEM(
    DATASET_ID,
    fecha_inicio_str,
    fecha_fin_str,
).main(filter=False)

if (
    df_precio_bolsa is None
    or df_precio_bolsa.empty
):
    raise ValueError(
        "SIMEM no devolvió datos de precio de bolsa "
        f"entre {fecha_inicio_str} y {fecha_fin_str}"
    )

# COMMAND ----------

from uuid import uuid4
import os


unique_id = uuid4().hex

temporary_path = landing_file.replace(
    ".json",
    f".{unique_id}.tmp.json",
)


print(
    "Escribiendo archivo temporal:",
    temporary_path,
)


df_precio_bolsa.to_json(
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


temporary_size = os.path.getsize(
    temporary_path
)


if temporary_size == 0:
    raise IOError(
        "El archivo temporal quedó vacío: "
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


expected_rows = len(
    df_precio_bolsa
)


print(
    "Filas esperadas:",
    expected_rows,
)

print(
    "Líneas del archivo temporal:",
    temporary_line_count,
)


if temporary_line_count != expected_rows:
    raise IOError(
        "La escritura temporal está incompleta. "
        f"Esperadas={expected_rows:,}, "
        f"escritas={temporary_line_count:,}. "
        "El archivo Landing anterior no será reemplazado."
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
        "El archivo Landing quedó incompleto después "
        "de la publicación. "
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

written_rows = len(
    df_precio_bolsa
)

read_rows = df_validation.count()


print("Registros escritos:", written_rows)
print("Registros leídos:", read_rows)
print(
    "Coinciden:",
    written_rows == read_rows,
)

physical_lines = 0

with open(
    landing_file,
    "r",
    encoding="utf-8",
) as file:

    for _ in file:
        physical_lines += 1


print(
    "Líneas físicas:",
    physical_lines,
)
