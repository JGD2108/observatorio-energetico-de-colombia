# Databricks notebook source
# MAGIC %md
# MAGIC # Embalses

# COMMAND ----------

# MAGIC %md
# MAGIC ### API SIMEM embalses ingestion y guardar json en raw files landing

# COMMAND ----------

# pydataxm se instala como dependencia versionada del job.

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
    LANDING_FILES,
)


SOURCE_NAME = "embalses"
DATASET_ID = "A0CF2A"

landing_file = LANDING_FILES[SOURCE_NAME]

REFERENCE_WINDOW_DAYS = 60
MAXIMUM_ACCEPTED_LAG_DAYS = 45

# COMMAND ----------

fecha_fin = datetime.now(
    ZoneInfo(TIMEZONE)
).date()

fecha_inicio = (
    fecha_fin
    - timedelta(days=REFERENCE_WINDOW_DAYS)
)


fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%d")
fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")


print("Fuente:", SOURCE_NAME)
print("Dataset SIMEM:", DATASET_ID)
print("Archivo Landing:", landing_file)
print(
    "Ventana de referencia:",
    fecha_inicio_str,
    "a",
    fecha_fin_str,
)


df_embalses = ReadSIMEM(
    DATASET_ID,
    fecha_inicio_str,
    fecha_fin_str,
).main(filter=False)


if df_embalses is None or df_embalses.empty:
    raise ValueError(
        "SIMEM no devolvió información de embalses "
        f"entre {fecha_inicio_str} y {fecha_fin_str}"
    )


print(
    "Registros descargados:",
    f"{len(df_embalses):,}",
)

# COMMAND ----------

required_columns = {
    "CodigoEmbalse",
    "NombreEmbalse",
}


missing_columns = (
    required_columns
    - set(df_embalses.columns)
)


if missing_columns:
    raise ValueError(
        "Faltan columnas esperadas en la fuente: "
        f"{sorted(missing_columns)}"
    )

# COMMAND ----------

invalid_catalog_rows = df_embalses[
    df_embalses["CodigoEmbalse"].isna()
    | df_embalses["NombreEmbalse"].isna()
    | (
        df_embalses["CodigoEmbalse"]
        .astype(str)
        .str.strip()
        == ""
    )
    | (
        df_embalses["NombreEmbalse"]
        .astype(str)
        .str.strip()
        == ""
    )
]


if not invalid_catalog_rows.empty:
    raise ValueError(
        "La fuente contiene registros sin código "
        "o nombre de embalse. "
        f"Registros inválidos: "
        f"{len(invalid_catalog_rows):,}"
    )

# COMMAND ----------

df_embalses_clean = df_embalses.copy()


df_embalses_clean["CodigoEmbalse"] = (
    df_embalses_clean["CodigoEmbalse"]
    .astype(str)
    .str.strip()
)


df_embalses_clean["NombreEmbalse"] = (
    df_embalses_clean["NombreEmbalse"]
    .astype(str)
    .str.strip()
)


df_embalses_unicos = (
    df_embalses_clean
    .drop_duplicates(
        subset=[
            "CodigoEmbalse",
            "NombreEmbalse",
        ]
    )
    .sort_values(
        by="CodigoEmbalse"
    )
    .reset_index(drop=True)
)


print(
    "Embalses únicos:",
    f"{len(df_embalses_unicos):,}",
)


display(df_embalses_unicos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener latitud y longitud de cada uno de los embalses

# COMMAND ----------

# MAGIC %pip install geopy

# COMMAND ----------

import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep

geolocator = Nominatim(user_agent="simem_reservoir_geocoder")

def geocode_reservoir(row):
    codigo = row["CodigoEmbalse"]
    nombre = row["NombreEmbalse"]

    queries = [
        f"Embalse {nombre}, Colombia",
        f"Represa {nombre}, Colombia",
        f"Central Hidroeléctrica {nombre}, Colombia",
        f"{nombre}, Colombia",
        f"{codigo}, Colombia"
    ]

    for query in queries:
        try:
            location = geolocator.geocode(query, timeout=10)
            sleep(1)  # Reduce sleep to 0.5s to improve speed, but avoid rate limit issues

            if location:
                return pd.Series({
                    "LatitudGeopy": location.latitude,
                    "LongitudGeopy": location.longitude,
                    "GeocodingQuery": query,
                    "GeocodingStatus": "found"
                })
        except Exception as e:
            sleep(1)
            continue

    return pd.Series({
        "LatitudGeopy": None,
        "LongitudGeopy": None,
        "GeocodingQuery": " | ".join(queries),
        "GeocodingStatus": "not_found"
    })

# Use a cache to avoid repeated geocoding for the same embalse
geocode_cache = {}

def geocode_reservoir_with_cache(row):
    key = (row["CodigoEmbalse"], row["NombreEmbalse"])
    if key in geocode_cache:
        return geocode_cache[key]
    result = geocode_reservoir(row)
    geocode_cache[key] = result
    return result

df_coords_geopy = df_embalses_unicos.apply(geocode_reservoir_with_cache, axis=1)

df_embalses_geo = pd.concat(
    [df_embalses_unicos.reset_index(drop=True), df_coords_geopy],
    axis=1
)

fallback_coords = {
    "AGREGADO":  {"lat": 4.70, "lon": -73.90, "tipo": "representative"},
    "MIRATRON":  {"lat": 6.76, "lon": -75.28, "tipo": "representative"},
    "ALTOANCH":  {"lat": 3.55, "lon": -76.90, "tipo": "approximate"},
    "LAFE":      {"lat": 6.13, "lon": -75.50, "tipo": "approximate"},
    "EMBABOGO":  {"lat": 4.85, "lon": -73.88, "tipo": "representative"},
    "ESMERALD":  {"lat": 4.90, "lon": -73.35, "tipo": "approximate"},
    "ITUANGO":   {"lat": 7.13, "lon": -75.69, "tipo": "approximate"},
    "PORCE2":    {"lat": 6.79, "lon": -75.07, "tipo": "approximate"},
    "PORCE3":    {"lat": 6.85, "lon": -75.03, "tipo": "approximate"},
    "SANLOREN":  {"lat": 6.25, "lon": -74.88, "tipo": "approximate"},
    "SOGAMOSO":  {"lat": 6.83, "lon": -73.36, "tipo": "approximate"}
}

def complete_coordinates(row):
    codigo = row["CodigoEmbalse"]

    if pd.notna(row["LatitudGeopy"]) and pd.notna(row["LongitudGeopy"]):
        return pd.Series({
            "Latitud": row["LatitudGeopy"],
            "Longitud": row["LongitudGeopy"],
            "TipoCoordenada": "geopy",
            "CoordinateSource": "Nominatim"
        })

    fallback = fallback_coords.get(codigo)

    if fallback:
        return pd.Series({
            "Latitud": fallback["lat"],
            "Longitud": fallback["lon"],
            "TipoCoordenada": fallback["tipo"],
            "CoordinateSource": "fallback_manual"
        })

    return pd.Series({
        "Latitud": None,
        "Longitud": None,
        "TipoCoordenada": "missing",
        "CoordinateSource": "missing"
    })

df_completed_coords = df_embalses_geo.apply(complete_coordinates, axis=1)

df_dim_embalses = pd.concat(
    [df_embalses_geo, df_completed_coords],
    axis=1
)

df_dim_embalses = df_dim_embalses[
    [
        "CodigoEmbalse",
        "NombreEmbalse",
        "Latitud",
        "Longitud",
        "TipoCoordenada",
        "CoordinateSource",
        "GeocodingStatus",
        "GeocodingQuery"
    ]
]

display(df_dim_embalses)

display(
    df_dim_embalses[
        df_dim_embalses["Latitud"].isna() |
        df_dim_embalses["Longitud"].isna()
    ]
)

# COMMAND ----------

# Completar Riogrande II, que quedó sin coordenadas
df_dim_embalses.loc[
    df_dim_embalses["CodigoEmbalse"] == "RIOGRAN2",
    [
        "Latitud",
        "Longitud",
        "TipoCoordenada",
        "CoordinateSource",
    ]
] = [
    6.516009,
    -75.456121,
    "manual_validated",
    "manual_override",
]

# Dejar solo columnas útiles para la dimensión
df_dim_embalses_final = df_dim_embalses[
    [
        "CodigoEmbalse",
        "NombreEmbalse",
        "Latitud",
        "Longitud",
        "TipoCoordenada",
        "CoordinateSource",
        "GeocodingStatus",
        "GeocodingQuery",
    ]
].copy()

display(df_dim_embalses_final)

# COMMAND ----------

duplicate_codes = int(
    df_dim_embalses_final[
        "CodigoEmbalse"
    ].duplicated().sum()
)


if duplicate_codes > 0:
    raise ValueError(
        "El catálogo final contiene códigos de embalse "
        f"duplicados: {duplicate_codes:,}"
    )

# COMMAND ----------

invalid_coordinates = df_dim_embalses_final[
    (
        df_dim_embalses_final["Latitud"].notna()
        & ~df_dim_embalses_final["Latitud"]
        .between(-90, 90)
    )
    |
    (
        df_dim_embalses_final["Longitud"].notna()
        & ~df_dim_embalses_final["Longitud"]
        .between(-180, 180)
    )
]


if not invalid_coordinates.empty:
    raise ValueError(
        "Se encontraron coordenadas fuera de rango. "
        f"Registros: {len(invalid_coordinates):,}"
    )

# COMMAND ----------

missing_coordinates = int(
    (
        df_dim_embalses_final["Latitud"].isna()
        | df_dim_embalses_final["Longitud"].isna()
    ).sum()
)


print(
    "Embalses sin coordenadas:",
    missing_coordinates,
)

# COMMAND ----------

unique_id = uuid4().hex

temporary_path = landing_file.replace(
    ".json",
    f".{unique_id}.tmp.json",
)


df_dim_embalses_final.to_json(
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


expected_rows = len(
    df_dim_embalses_final
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
        "El archivo final quedó incompleto. "
        f"Esperadas={expected_rows:,}, "
        f"publicadas={final_line_count:,}."
    )


print("Catálogo publicado correctamente")
print("Fuente:", SOURCE_NAME)
print("Dataset SIMEM:", DATASET_ID)
print("Archivo Landing:", landing_file)
print(
    "Embalses publicados:",
    f"{final_line_count:,}",
)
print(
    "Embalses sin coordenadas:",
    missing_coordinates,
)

# COMMAND ----------

df_validation = spark.read.json(
    landing_file
)


written_rows = len(
    df_dim_embalses_final
)

read_rows = df_validation.count()


print("Registros escritos:", written_rows)
print("Registros leídos:", read_rows)
print(
    "Coinciden:",
    written_rows == read_rows,
)


display(
    df_validation.orderBy(
        "CodigoEmbalse"
    )
)

# COMMAND ----------

df_validation.groupBy(
    "CoordinateSource"
).count().orderBy(
    "CoordinateSource"
).show(truncate=False)

# COMMAND ----------

df_validation.filter(
    "Latitud IS NULL OR Longitud IS NULL"
).show(truncate=False)
