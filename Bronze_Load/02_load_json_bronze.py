# Databricks notebook source
# NO ACTIVO: el job usa 02_bronze_daily.py.
# MAGIC %md
# MAGIC # 02 - Load Bronze Tables
# MAGIC
# MAGIC This notebook loads the Bronze tables for the project from the Incoming JSON files.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, current_date, input_file_name, monotonically_increasing_id, lit

# COMMAND ----------

"""
Funcion para leer Archivos JSON
"""
def read_json(file_path):
    df = spark.read.json(file_path)
    return df

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F


source_path = (
    "/Volumes/observatorio_dev/landing/raw_files/"
    "agentes.json"
)

target_table = "observatorio_dev.bronze.agentes"

df_source = spark.read.json(source_path)

df_transformed = (
    df_source
    .select(
        F.col("Fecha").cast("string").alias("fecha"),
        F.col("CodigoDuracion").cast("string")
            .alias("codigo_duracion"),
        F.col("CodigoSICAgente").cast("string")
            .alias("codigo_sic_agente"),
        F.col("NombreAgente").cast("string")
            .alias("nombre_agente"),
        F.col("ActividadAgente").cast("string")
            .alias("actividad_agente")
    )
    .withColumn(
        "source_file_name",
        F.lit("agentes.json")
    )
    .withColumn(
        "source_file_path",
        F.lit(source_path)
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "load_date",
        F.current_date()
    )
)

payload_columns = [
    "fecha",
    "codigo_duracion",
    "codigo_sic_agente",
    "nombre_agente",
    "actividad_agente"
]

df_incoming = df_transformed.dropDuplicates(payload_columns)

df_existing = (
    spark.table(target_table)
    .select(*payload_columns)
    .dropDuplicates()
)

join_condition = reduce(
    lambda left, right: left & right,
    [
        F.col(f"incoming.{column}").eqNullSafe(
            F.col(f"existing.{column}")
        )
        for column in payload_columns
    ]
)

df_new = (
    df_incoming.alias("incoming")
    .join(
        df_existing.alias("existing"),
        join_condition,
        "left_anti"
    )
    .select("incoming.*")
)

print("Registros recibidos:", df_source.count())
print("Registros únicos del archivo:", df_incoming.count())
print("Registros nuevos para Bronze:", df_new.count())

(
    df_new.write
    .mode("append")
    .saveAsTable(target_table)
)

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F


source_path = (
    "/Volumes/observatorio_dev/landing/raw_files/"
    "demanda_real.json"
)

target_table = "observatorio_dev.bronze.demanda_real"

df_source = spark.read.json(source_path)

df_transformed = (
    df_source
    .select(
        F.col("CodigoVariable").cast("string")
            .alias("codigo_variable"),
        F.col("FechaHora").cast("string")
            .alias("fecha_hora"),
        F.col("CodigoSICAgente").cast("string")
            .alias("codigo_sic_agente"),
        F.col("TipoMercado").cast("string")
            .alias("tipo_mercado"),
        F.col("Version").cast("string")
            .alias("version"),
        F.col("Valor").cast("string")
            .alias("valor"),
        F.col("UnidadMedida").cast("string")
            .alias("unidad_medida"),
        F.col("CodigoDuracion").cast("string")
            .alias("codigo_duracion")
    )
    .withColumn(
        "source_file_name",
        F.lit("demanda_real.json")
    )
    .withColumn(
        "source_file_path",
        F.lit(source_path)
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "load_date",
        F.current_date()
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
    "codigo_duracion"
]

df_incoming = df_transformed.dropDuplicates(payload_columns)

df_existing = (
    spark.table(target_table)
    .select(*payload_columns)
    .dropDuplicates()
)

join_condition = reduce(
    lambda left, right: left & right,
    [
        F.col(f"incoming.{column}").eqNullSafe(
            F.col(f"existing.{column}")
        )
        for column in payload_columns
    ]
)

df_new = (
    df_incoming.alias("incoming")
    .join(
        df_existing.alias("existing"),
        join_condition,
        "left_anti"
    )
    .select("incoming.*")
)

print("Registros recibidos:", df_source.count())
print("Registros únicos del archivo:", df_incoming.count())
print("Registros nuevos para Bronze:", df_new.count())

(
    df_new.write
    .mode("append")
    .saveAsTable(target_table)
)

# COMMAND ----------

df = read_json("dbfs:/Volumes/observatorio_dev/landing/raw_files/disponibilidad_plantas.json")
df_transformed = df.select(
    col("CodigoDuracion").alias("codigo_duracion"),
    col("CodigoPlanta").alias("codigo_planta"),
    col("CodigoVariable").alias("codigo_variable"),
    col("FechaHora").alias("fecha_hora"),
    col("UnidadMedida").alias("unidad_medida"),
    col("Valor").alias("valor"),
    col("Version").alias("version")
) \
.withColumn("source_file_name", lit("disponibilidad_plantas.json")) \
.withColumn("source_file_path", lit("dbfs:/Volumes/observatorio_dev/landing/raw_files/disponibilidad_plantas.json")) \
.withColumn("ingestion_timestamp", current_timestamp()) \
.withColumn("load_date", current_date())

# Leer tabla existente
df_existing = spark.table("observatorio_dev.bronze.disponibilidad_plantas")

# Identificar registros nuevos
df_incremental = df_transformed.join(
    df_existing,
    on=[
        "codigo_duracion",
        "codigo_planta",
        "codigo_variable",
        "fecha_hora",
        "unidad_medida",
        "valor",
        "version"
    ],
    how="left_anti"
)

# Escribir solo registros nuevos
df_incremental.write.mode("append").option("overwriteSchema", "true").saveAsTable("observatorio_dev.bronze.disponibilidad_plantas")


# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F


source_path = (
    "/Volumes/observatorio_dev/landing/raw_files/"
    "plantas.json"
)

target_table = "observatorio_dev.bronze.plantas"

df_source = spark.read.json(source_path)

df_transformed = (
    df_source
    .select(
        F.col("Fecha").cast("string").alias("fecha"),
        F.col("CodigoDuracion").cast("string")
            .alias("codigo_duracion"),
        F.col("CodigoPlanta").cast("string")
            .alias("codigo_planta"),
        F.col("NombrePlanta").cast("string")
            .alias("nombre_planta"),
        F.col("CodigoSICAgente").cast("string")
            .alias("codigo_sic_agente"),
        F.col("CapEfectivaNeta").cast("string")
            .alias("cap_efectiva_neta"),
        F.col("FPO").cast("string").alias("fpo"),
        F.col("CodigoSubAreaOperativa").cast("string")
            .alias("codigo_sub_area_operativa"),
        F.col("CodigoAreaOperativa").cast("string")
            .alias("codigo_area_operativa"),
        F.col("TipoDespachoRecurso").cast("string")
            .alias("tipo_despacho_recurso"),
        F.col("TipoClasificacion").cast("string")
            .alias("tipo_clasificacion"),
        F.col("TipoGeneracion").cast("string")
            .alias("tipo_generacion")
    )
    .withColumn(
        "source_file_name",
        F.lit("plantas.json")
    )
    .withColumn(
        "source_file_path",
        F.lit(source_path)
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "load_date",
        F.current_date()
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
    "tipo_generacion"
]

df_incoming = df_transformed.dropDuplicates(payload_columns)

df_existing = (
    spark.table(target_table)
    .select(*payload_columns)
    .dropDuplicates()
)

join_condition = reduce(
    lambda left, right: left & right,
    [
        F.col(f"incoming.{column}").eqNullSafe(
            F.col(f"existing.{column}")
        )
        for column in payload_columns
    ]
)

df_new = (
    df_incoming.alias("incoming")
    .join(
        df_existing.alias("existing"),
        join_condition,
        "left_anti"
    )
    .select("incoming.*")
)

print("Registros recibidos:", df_source.count())
print("Registros únicos del archivo:", df_incoming.count())
print("Registros nuevos para Bronze:", df_new.count())

(
    df_new.write
    .mode("append")
    .saveAsTable(target_table)
)

# COMMAND ----------

df = spark.read.json("dbfs:/Volumes/observatorio_dev/landing/raw_files/embalses_unicos.json")
df_transformed = df.select(
    col("CodigoEmbalse").alias("codigo_embalse"),
    col("Latitud").cast("string").alias("latitud"),
    col("Longitud").cast("string").alias("longitud"),
    col("NombreEmbalse").alias("nombre_embalse")
) \
.withColumn("source_file_name", lit("embalses_unicos.json")) \
.withColumn("source_file_path", lit("dbfs:/Volumes/observatorio_dev/landing/raw_files/embalses_unicos.json")) \
.withColumn("ingestion_timestamp", current_timestamp()) \
.withColumn("load_date", current_date())

# Leer tabla existente
df_existing = spark.table("observatorio_dev.bronze.embalses")

# Identificar registros nuevos
df_incremental = df_transformed.join(
    df_existing,
    on=[
        "codigo_embalse",
        "latitud",
        "longitud",
        "nombre_embalse"
    ],
    how="left_anti"
)

# Escribir solo registros nuevos
df_incremental.write.mode("append").option("overwriteSchema", "true").saveAsTable("observatorio_dev.bronze.embalses")

# COMMAND ----------

df_test = spark.read.json("dbfs:/Volumes/observatorio_dev/landing/raw_files/generacion_real.json")
print("Incoming DataFrame schema:")
df_test.printSchema()
print(f"\nValor column type: {df_test.schema['Valor'].dataType}")

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F


source_path = (
    "/Volumes/observatorio_dev/"
    "landing/raw_files/generacion_real.json"
)

bronze_table = "observatorio_dev.bronze.generacion_real"

# Leer el JSON histórico
raw_df = spark.read.json(source_path)

df_transformed = (
    raw_df
    .select(
        F.col("CodigoDuracion").alias("codigo_duracion"),
        F.col("CodigoPlanta").alias("codigo_planta"),
        F.col("CodigoSICAgente").alias("codigo_sic_agente"),
        F.col("CodigoVariable").alias("codigo_variable"),
        F.col("FechaHora").alias("fecha_hora"),
        F.col("UnidadMedida").alias("unidad_medida"),
        F.col("Valor").cast("string").alias("valor"),
        F.col("Version").alias("version")
    )
    .withColumn(
        "source_file_name",
        F.lit("generacion_real.json")
    )
    .withColumn(
        "source_file_path",
        F.lit(source_path)
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "load_date",
        F.current_date()
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
    "version"
]

# Eliminar duplicados que pudieran venir en el mismo JSON
incoming_df = (
    df_transformed
    .dropDuplicates(payload_columns)
    .alias("incoming")
)

# Leer registros existentes en Bronze
existing_df = (
    spark.table(bronze_table)
    .select(payload_columns)
    .dropDuplicates()
    .alias("existing")
)

# Comparar únicamente el contenido del registro
join_condition = reduce(
    lambda condition_a, condition_b: condition_a & condition_b,
    [
        F.col(f"incoming.{column_name}").eqNullSafe(
            F.col(f"existing.{column_name}")
        )
        for column_name in payload_columns
    ]
)

df_incremental = incoming_df.join(
    existing_df,
    join_condition,
    "left_anti"
)

# Insertar los registros que no existen
(
    df_incremental
    .write
    .mode("append")
    .saveAsTable(bronze_table)
)

print("Carga de generación real a Bronze finalizada.")

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F


source_path = (
    "/Volumes/observatorio_dev/landing/raw_files/"
    "niveles_embalses_plantas.json"
)

target_table = "observatorio_dev.bronze.niveles_embalses"

df_source = spark.read.json(source_path)

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
            .alias("version")
    )
    .withColumn(
        "source_file_name",
        F.lit("niveles_embalses_plantas.json")
    )
    .withColumn(
        "source_file_path",
        F.lit(source_path)
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "load_date",
        F.current_date()
    )
)

payload_columns = [
    "codigo_duracion",
    "codigo_planta",
    "codigo_variable",
    "fecha_inicio",
    "unidad_medida",
    "valor",
    "version"
]

df_incoming = df_transformed.dropDuplicates(payload_columns)

df_existing = (
    spark.table(target_table)
    .select(*payload_columns)
    .dropDuplicates()
)

join_condition = reduce(
    lambda left, right: left & right,
    [
        F.col(f"incoming.{column}").eqNullSafe(
            F.col(f"existing.{column}")
        )
        for column in payload_columns
    ]
)

df_new = (
    df_incoming.alias("incoming")
    .join(
        df_existing.alias("existing"),
        join_condition,
        "left_anti"
    )
    .select("incoming.*")
)

print("Registros recibidos:", df_source.count())
print("Registros únicos del archivo:", df_incoming.count())
print("Registros nuevos para Bronze:", df_new.count())

(
    df_new.write
    .mode("append")
    .saveAsTable(target_table)
)

# COMMAND ----------

df = read_json("dbfs:/Volumes/observatorio_dev/landing/raw_files/plantas.json")
display(df.dtypes)
df_transformed = df.select(
    col("Fecha").alias("fecha"),
    col("CodigoDuracion").alias("codigo_duracion"),
    col("CodigoPlanta").alias("codigo_planta"),
    col("NombrePlanta").alias("nombre_planta"),
    col("CodigoSICAgente").alias("codigo_sic_agente"),
    col("CapEfectivaNeta").cast("string").alias("cap_efectiva_neta"),
    col("FPO").alias("fpo"),
    col("CodigoSubAreaOperativa").alias("codigo_sub_area_operativa"),
    col("CodigoAreaOperativa").alias("codigo_area_operativa"),
    col("TipoDespachoRecurso").alias("tipo_despacho_recurso"),
    col("TipoClasificacion").alias("tipo_clasificacion"),
    col("TipoGeneracion").alias("tipo_generacion")
).withColumn("source_file_name", lit("plantas.json")) \
.withColumn("source_file_path", lit("dbfs:/Volumes/observatorio_dev/landing/raw_files/plantas.json")) \
.withColumn("ingestion_timestamp", current_timestamp()) \
.withColumn("load_date", current_date())

# Leer tabla existente
df_existing = spark.table("observatorio_dev.bronze.plantas")

# Identificar registros nuevos
df_incremental = df_transformed.join(
    df_existing,
    on=[
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
        "tipo_generacion"
    ],
    how="left_anti"
)

# Insertar registros nuevos
df_incremental.write.mode("append").option("overwriteSchema", "true").saveAsTable("observatorio_dev.bronze.plantas")

# COMMAND ----------

from functools import reduce
from pyspark.sql import functions as F


source_path = (
    "/Volumes/observatorio_dev/landing/raw_files/"
    "precio_bolsa.json"
)

bronze_table = "observatorio_dev.bronze.precio_bolsa"

raw_df = spark.read.json(source_path)

df_transformed = (
    raw_df
    .select(
        F.col("CodigoVariable").alias("codigo_variable"),
        F.col("FechaHora").alias("fecha_hora"),
        F.col("CodigoDuracion").alias("codigo_duracion"),
        F.col("UnidadMedida").alias("unidad_medida"),
        F.col("Version").alias("version"),
        F.col("Valor").cast("string").alias("valor")
    )
    .withColumn(
        "source_file_name",
        F.lit("precio_bolsa.json")
    )
    .withColumn(
        "source_file_path",
        F.lit(source_path)
    )
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
    .withColumn(
        "load_date",
        F.current_date()
    )
)

payload_columns = [
    "codigo_variable",
    "fecha_hora",
    "codigo_duracion",
    "unidad_medida",
    "version",
    "valor"
]

incoming_df = (
    df_transformed
    .dropDuplicates(payload_columns)
    .alias("incoming")
)

existing_df = (
    spark.table(bronze_table)
    .select(payload_columns)
    .dropDuplicates()
    .alias("existing")
)

join_condition = reduce(
    lambda condition_a, condition_b: condition_a & condition_b,
    [
        F.col(f"incoming.{column_name}").eqNullSafe(
            F.col(f"existing.{column_name}")
        )
        for column_name in payload_columns
    ]
)

df_incremental = incoming_df.join(
    existing_df,
    join_condition,
    "left_anti"
)

(
    df_incremental
    .write
    .mode("append")
    .saveAsTable(bronze_table)
)

print("Carga incremental de precio a Bronze finalizada.")
