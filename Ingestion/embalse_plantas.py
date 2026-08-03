# Databricks notebook source
# MAGIC %md
# MAGIC # Conexión embalses plantas

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql import functions as F

data = [
    Row(region="Antioquia", plant_name="Miel I", reservoir_name="Amani"),
    Row(region="Antioquia", plant_name="Guatrón", reservoir_name="Miraflores"),
    Row(region="Antioquia", plant_name="Guatapé", reservoir_name="Peñol"),
    Row(region="Antioquia", plant_name="Playas", reservoir_name="Playas"),
    Row(region="Antioquia", plant_name="Porce II", reservoir_name="Porce II"),
    Row(region="Antioquia", plant_name="Porce III", reservoir_name="Porce III"),
    Row(region="Antioquia", plant_name="San Carlos", reservoir_name="Punchiná"),
    Row(region="Antioquia", plant_name="La Tasajera", reservoir_name="Riogrande2"),
    Row(region="Antioquia", plant_name="Jaguas", reservoir_name="San Lorenzo"),
    Row(region="Antioquia", plant_name="Guatrón", reservoir_name="Troneras"),
    Row(region="Caribe", plant_name="Urra", reservoir_name="Urra1"),
    Row(region="Centro", plant_name="Pagua", reservoir_name="Agregado Bogotá"),
    Row(region="Centro", plant_name="Betania", reservoir_name="Betania"),
    Row(region="Centro", plant_name="El Quimbo", reservoir_name="El Quimbo"),
    Row(region="Centro", plant_name="Pagua", reservoir_name="Muna"),
    Row(region="Centro", plant_name="Prado", reservoir_name="Prado"),
    Row(region="Centro", plant_name="Sogamosos", reservoir_name="Toporoco"),
    Row(region="Oriente", plant_name="Pagua", reservoir_name="Chuza"),
    Row(region="Oriente", plant_name="Chivor", reservoir_name="Esmeralda"),
    Row(region="Oriente", plant_name="Guavio", reservoir_name="Guavio"),
    Row(region="Valle", plant_name="Albán", reservoir_name="Altoanchicaya"),
    Row(region="Valle", plant_name="Calima", reservoir_name="Calima 1"),
    Row(region="Valle", plant_name="Salvajina", reservoir_name="Salvajina"),
]

df_plant_reservoir_mapping = spark.createDataFrame(data)

display(df_plant_reservoir_mapping)

# COMMAND ----------

from datetime import datetime
from zoneinfo import ZoneInfo
from uuid import uuid4
import os
import sys

import pandas as pd
from pyspark.sql import Row


NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from config.project_config import (
    TIMEZONE,
    LANDING_FILES,
)


SOURCE_NAME = "plantas_reservorios"
landing_file = LANDING_FILES[SOURCE_NAME]


RELATIONSHIP_SOURCE = "manual_curated"
VALIDATION_STATUS = "validated"

# COMMAND ----------

data = [
    Row(
        region="Antioquia",
        plant_name="Miel I",
        reservoir_name="Amani",
    ),
    Row(
        region="Antioquia",
        plant_name="Guatrón",
        reservoir_name="Miraflores",
    ),
    Row(
        region="Antioquia",
        plant_name="Guatapé",
        reservoir_name="Peñol",
    ),
    Row(
        region="Antioquia",
        plant_name="Playas",
        reservoir_name="Playas",
    ),
    Row(
        region="Antioquia",
        plant_name="Porce II",
        reservoir_name="Porce II",
    ),
    Row(
        region="Antioquia",
        plant_name="Porce III",
        reservoir_name="Porce III",
    ),
    Row(
        region="Antioquia",
        plant_name="San Carlos",
        reservoir_name="Punchiná",
    ),
    Row(
        region="Antioquia",
        plant_name="La Tasajera",
        reservoir_name="Riogrande2",
    ),
    Row(
        region="Antioquia",
        plant_name="Jaguas",
        reservoir_name="San Lorenzo",
    ),
    Row(
        region="Antioquia",
        plant_name="Guatrón",
        reservoir_name="Troneras",
    ),
    Row(
        region="Caribe",
        plant_name="Urra",
        reservoir_name="Urra1",
    ),
    Row(
        region="Centro",
        plant_name="Pagua",
        reservoir_name="Agregado Bogotá",
    ),
    Row(
        region="Centro",
        plant_name="Betania",
        reservoir_name="Betania",
    ),
    Row(
        region="Centro",
        plant_name="El Quimbo",
        reservoir_name="El Quimbo",
    ),
    Row(
        region="Centro",
        plant_name="Pagua",
        reservoir_name="Muna",
    ),
    Row(
        region="Centro",
        plant_name="Prado",
        reservoir_name="Prado",
    ),
    Row(
        region="Centro",
        plant_name="Sogamoso",
        reservoir_name="Topocoro",
    ),
    Row(
        region="Oriente",
        plant_name="Pagua",
        reservoir_name="Chuza",
    ),
    Row(
        region="Oriente",
        plant_name="Chivor",
        reservoir_name="Esmeralda",
    ),
    Row(
        region="Oriente",
        plant_name="Guavio",
        reservoir_name="Guavio",
    ),
    Row(
        region="Valle",
        plant_name="Albán",
        reservoir_name="Altoanchicaya",
    ),
    Row(
        region="Valle",
        plant_name="Calima",
        reservoir_name="Calima 1",
    ),
    Row(
        region="Valle",
        plant_name="Salvajina",
        reservoir_name="Salvajina",
    ),
]


df_plant_reservoir_mapping = spark.createDataFrame(
    data
)

display(df_plant_reservoir_mapping)

# COMMAND ----------

df_mapping = (
    df_plant_reservoir_mapping
    .toPandas()
)


for column in [
    "region",
    "plant_name",
    "reservoir_name",
]:
    df_mapping[column] = (
        df_mapping[column]
        .astype(str)
        .str.strip()
    )


df_mapping["relationship_type"] = (
    "hydraulic_association"
)

df_mapping["is_primary"] = False

df_mapping["attribution_allowed"] = False

df_mapping["source_name"] = (
    RELATIONSHIP_SOURCE
)

df_mapping["validation_status"] = (
    VALIDATION_STATUS
)

df_mapping["valid_from"] = None

df_mapping["valid_to"] = None

# COMMAND ----------

required_columns = {
    "region",
    "plant_name",
    "reservoir_name",
}


missing_columns = (
    required_columns
    - set(df_mapping.columns)
)


if missing_columns:
    raise ValueError(
        "Faltan columnas requeridas: "
        f"{sorted(missing_columns)}"
    )

# COMMAND ----------

natural_key = [
    "plant_name",
    "reservoir_name",
]


duplicate_rows = int(
    df_mapping
    .duplicated(
        subset=natural_key
    )
    .sum()
)


if duplicate_rows > 0:
    raise ValueError(
        "Existen relaciones duplicadas para la llave "
        f"{natural_key}. "
        f"Duplicados: {duplicate_rows:,}"
    )

# COMMAND ----------

display(
    df_mapping[
        df_mapping["plant_name"].str.contains(
            "Sogam",
            case=False,
            na=False,
        )
        |
        df_mapping["reservoir_name"].str.contains(
            "Topo",
            case=False,
            na=False,
        )
    ]
)

# COMMAND ----------

unique_id = uuid4().hex

temporary_path = landing_file.replace(
    ".json",
    f".{unique_id}.tmp.json",
)


df_mapping.to_json(
    temporary_path,
    orient="records",
    lines=True,
    mode="w",
    force_ascii=False,
)


if not os.path.exists(temporary_path):
    raise IOError(
        "No se creó el archivo temporal: "
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


expected_rows = len(df_mapping)


print("Filas esperadas:", expected_rows)
print(
    "Líneas escritas:",
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


print("Catálogo publicado correctamente")
print("Fuente:", SOURCE_NAME)
print("Archivo Landing:", landing_file)
print(
    "Relaciones publicadas:",
    final_line_count,
)
