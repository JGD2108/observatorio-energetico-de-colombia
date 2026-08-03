# Databricks notebook source
# MAGIC %md
# MAGIC # EDA completo — Bronze del Observatorio Energético
# MAGIC
# MAGIC **Catálogo:** `observatorio_dev`
# MAGIC **Esquema:** `bronze`
# MAGIC **Fuentes:** agentes, demanda, disponibilidad, embalses, generación, niveles de embalses, plantas, relaciones planta–reservorio y precio de bolsa.
# MAGIC **Evidencia revisada:** ejecución conservada en el notebook, con corte de análisis al 23 de julio de 2026.
# MAGIC
# MAGIC > Este notebook diagnostica la capa Bronze sin modificarla. Las conclusiones siguientes se basan exclusivamente en las salidas ejecutadas.
# MAGIC
# MAGIC ## TL;DR
# MAGIC
# MAGIC - **Estado general: apto condicionado.** Las nueve tablas reúnen **24.590.150 registros** y presentan una base técnica sólida: no se detectaron valores no convertibles, dominios inesperados, duplicados exactos, duplicados de llave dentro de la misma versión ni conflictos de valor dentro de una misma versión.
# MAGIC - **El principal riesgo es la dimensión de plantas.** No tienen correspondencia en `plantas` **161 de 612 códigos de generación (26,31 %)** y **168 de 620 códigos de disponibilidad (27,10 %)**. Un `INNER JOIN` eliminaría silenciosamente una parte material de los recursos.
# MAGIC - **Bronze no puede sumarse directamente.** Las versiones `TX` son revisiones legítimas, no duplicados de ingestión. Después de aplicar la prioridad configurada `TXF > TXR > TX3 > TX2 > TX1`, los hechos se reducen a su grano canónico: 437.856 filas de demanda, 2.609.784 de disponibilidad, 2.553.576 de generación, 4.200 de niveles y 14.400 de precio.
# MAGIC - **La cobertura horaria es fuerte**, pero las fuentes no tienen el mismo corte. Generación termina el 13 de julio, demanda y disponibilidad el 17, y precio y niveles el 19. Para comparar el sistema, el último corte común válido es **2026-07-13**.
# MAGIC - **La geografía de embalses está incompleta:** 13 de 32 embalses (40,63 %) no tienen latitud ni longitud. Las 19 coordenadas presentes son válidas y están dentro de la caja geográfica aproximada de Colombia.
# MAGIC - **Decisión:** Silver puede construirse o reforzarse con selección canónica, dimensión de plantas conformada, uniones temporales y controles de frescura. Gold y el dashboard no deben consumir Bronze de forma directa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Contexto, alcance y supuestos
# MAGIC
# MAGIC ### Objetivo
# MAGIC
# MAGIC Evaluar si los datos crudos son suficientemente completos, válidos, trazables y coherentes para alimentar Silver, Gold y el dashboard del MVP.
# MAGIC
# MAGIC ### Qué se revisa
# MAGIC
# MAGIC 1. Inventario, esquema y trazabilidad de carga.
# MAGIC 2. Completitud, cardinalidad y conversiones de tipo.
# MAGIC 3. Duplicados exactos, duplicados de llave y revisiones entre versiones.
# MAGIC 4. Cobertura temporal, continuidad horaria/diaria y frescura.
# MAGIC 5. Dominios, unidades, duraciones, mercados y versiones.
# MAGIC 6. Rangos, ceros, negativos y valores atípicos.
# MAGIC 7. Integridad entre plantas, agentes, embalses y hechos.
# MAGIC 8. Coherencia entre generación, disponibilidad, capacidad, demanda y precios.
# MAGIC 9. Candidatos de hallazgos priorizados para redactar las conclusiones.
# MAGIC
# MAGIC ### Supuestos importantes
# MAGIC
# MAGIC - Bronze conserva el dato recibido y, por diseño, puede contener reingestas y distintas versiones.
# MAGIC - `TX1`, `TX2`, `TX3`, `TXR` y `TXF` se analizan por separado. Para comparaciones canónicas se usa una prioridad **configurable**: `TXF > TXR > TX3 > TX2 > TX1`.
# MAGIC - Los campos `valor`, `cap_efectiva_neta`, `latitud` y `longitud` se validan con `try_cast`; el notebook no cambia su tipo en Bronze.
# MAGIC - En datos `PT1H`, 24 periodos por entidad-día representan cobertura completa. Los días extremos del rango pueden estar parcialmente cargados y deben interpretarse con cautela.
# MAGIC - `plantas` y `agentes` se consideran snapshots, no series obligatoriamente publicadas todos los días.
# MAGIC - La comparación `kWh` de una hora frente a `kW` de capacidad es numéricamente válida para ese intervalo de una hora.

# COMMAND ----------

from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType
from functools import reduce
from datetime import date

CATALOG = "observatorio_dev"
SCHEMA = "bronze"

# Mantener True para el EDA completo. Cambiar a False solo para una prueba rápida.
EJECUTAR_PERFIL_PESADO = True
MAX_FILAS_MUESTRA = 100
TOLERANCIA_OPERATIVA = 1.05

TABLAS = [
    "agentes",
    "demanda_real",
    "disponibilidad_plantas",
    "embalses",
    "generacion_real",
    "niveles_embalses",
    "plantas",
    "plantas_reservorios",
    "precio_bolsa",
]

AUDITORIA = {
    "id", "source_file_name", "source_file_path",
    "ingestion_timestamp", "load_date"
}

VERSION_PRIORITY = {
    "TX1": 10,
    "TX2": 20,
    "TX3": 30,
    "TXR": 40,
    "TXF": 50,
}

DOMINIOS_ESPERADOS = {
    "agentes": {
        "codigo_duracion": {"P1D"},
    },
    "demanda_real": {
        "codigo_duracion": {"PT1H"},
        "codigo_variable": {"DdaReal"},
        "unidad_medida": {"kWh"},
        "version": set(VERSION_PRIORITY),
        "tipo_mercado": {"Regulado", "No Regulado"},
    },
    "disponibilidad_plantas": {
        "codigo_duracion": {"PT1H"},
        "codigo_variable": {"DispReal"},
        "unidad_medida": {"kWh"},
        "version": set(VERSION_PRIORITY),
    },
    "generacion_real": {
        "codigo_duracion": {"PT1H"},
        "codigo_variable": {"GReal"},
        "unidad_medida": {"kWh"},
        "version": set(VERSION_PRIORITY),
    },
    "niveles_embalses": {
        "codigo_duracion": {"P1D"},
        "codigo_variable": {"NEM"},
        "unidad_medida": {"kWh"},
        "version": set(VERSION_PRIORITY),
    },
    "plantas": {
        "codigo_duracion": {"P1D"},
    },
    "precio_bolsa": {
        "codigo_duracion": {"PT1H"},
        "codigo_variable": {"PB_Nal", "PB_Int", "PB_Tie"},
        "unidad_medida": {"COP/kWh"},
        "version": set(VERSION_PRIORITY),
    },
}

CONFIG = {
    "agentes": {
        "fecha": "fecha",
        "frecuencia": "snapshot_diario",
        "entidad": ["codigo_sic_agente", "actividad_agente"],
        "llave_version": ["fecha", "codigo_sic_agente", "actividad_agente"],
        "llave_canonica": ["fecha", "codigo_sic_agente", "actividad_agente"],
        "numericas": [],
        "categoricas": ["codigo_duracion", "actividad_agente"],
    },
    "demanda_real": {
        "fecha": "fecha_hora",
        "frecuencia": "horaria",
        "entidad": ["codigo_sic_agente", "tipo_mercado", "codigo_variable"],
        "llave_version": [
            "fecha_hora", "codigo_sic_agente", "tipo_mercado",
            "codigo_variable", "codigo_duracion", "version"
        ],
        "llave_canonica": [
            "fecha_hora", "codigo_sic_agente", "tipo_mercado",
            "codigo_variable", "codigo_duracion"
        ],
        "numericas": ["valor"],
        "categoricas": [
            "codigo_duracion", "codigo_variable", "tipo_mercado",
            "unidad_medida", "version"
        ],
    },
    "disponibilidad_plantas": {
        "fecha": "fecha_hora",
        "frecuencia": "horaria",
        "entidad": ["codigo_planta", "codigo_variable"],
        "llave_version": [
            "fecha_hora", "codigo_planta", "codigo_variable",
            "codigo_duracion", "version"
        ],
        "llave_canonica": [
            "fecha_hora", "codigo_planta", "codigo_variable",
            "codigo_duracion"
        ],
        "numericas": ["valor"],
        "categoricas": [
            "codigo_duracion", "codigo_variable", "unidad_medida", "version"
        ],
    },
    "embalses": {
        "fecha": None,
        "frecuencia": "maestro",
        "entidad": ["codigo_embalse"],
        "llave_version": ["codigo_embalse"],
        "llave_canonica": ["codigo_embalse"],
        "numericas": ["latitud", "longitud"],
        "categoricas": [],
    },
    "generacion_real": {
        "fecha": "fecha_hora",
        "frecuencia": "horaria",
        "entidad": ["codigo_planta", "codigo_variable"],
        "llave_version": [
            "fecha_hora", "codigo_planta", "codigo_sic_agente",
            "codigo_variable", "codigo_duracion", "version"
        ],
        "llave_canonica": [
            "fecha_hora", "codigo_planta", "codigo_sic_agente",
            "codigo_variable", "codigo_duracion"
        ],
        "numericas": ["valor"],
        "categoricas": [
            "codigo_duracion", "codigo_variable", "unidad_medida", "version"
        ],
    },
    "niveles_embalses": {
        "fecha": "fecha_inicio",
        "frecuencia": "diaria",
        "entidad": ["codigo_planta", "codigo_variable"],
        "llave_version": [
            "fecha_inicio", "codigo_planta", "codigo_variable",
            "codigo_duracion", "version"
        ],
        "llave_canonica": [
            "fecha_inicio", "codigo_planta", "codigo_variable",
            "codigo_duracion"
        ],
        "numericas": ["valor"],
        "categoricas": [
            "codigo_duracion", "codigo_variable", "unidad_medida", "version"
        ],
    },
    "plantas": {
        "fecha": "fecha",
        "frecuencia": "snapshot_diario",
        "entidad": ["codigo_planta"],
        "llave_version": ["fecha", "codigo_planta"],
        "llave_canonica": ["fecha", "codigo_planta"],
        "numericas": ["cap_efectiva_neta"],
        "fechas_adicionales": ["fpo"],
        "categoricas": [
            "codigo_duracion", "tipo_despacho_recurso",
            "tipo_clasificacion", "tipo_generacion",
            "codigo_sub_area_operativa", "codigo_area_operativa"
        ],
    },
    "plantas_reservorios": {
        "fecha": None,
        "frecuencia": "maestro",
        "entidad": ["nombre_planta", "nombre_reservorio"],
        "llave_version": ["nombre_planta", "nombre_reservorio"],
        "llave_canonica": ["nombre_planta", "nombre_reservorio"],
        "numericas": [],
        "categoricas": ["region"],
    },
    "precio_bolsa": {
        "fecha": "fecha_hora",
        "frecuencia": "horaria",
        "entidad": ["codigo_variable"],
        "llave_version": [
            "fecha_hora", "codigo_variable", "codigo_duracion", "version"
        ],
        "llave_canonica": [
            "fecha_hora", "codigo_variable", "codigo_duracion"
        ],
        "numericas": ["valor"],
        "categoricas": [
            "codigo_duracion", "codigo_variable", "unidad_medida", "version"
        ],
    },
}

def nombre_tabla(tabla):
    return f"{CATALOG}.{SCHEMA}.{tabla}"

def fecha_evento(tabla):
    return F.to_timestamp(F.col(CONFIG[tabla]["fecha"]))

def normalizar_texto(columna):
    texto = F.upper(F.trim(columna))
    texto = F.regexp_replace(texto, r"\s+", " ")
    return F.translate(
        texto,
        "ÁÉÍÓÚÜÑÀÈÌÒÙÂÊÎÔÛÄËÏÖÜ",
        "AEIOUUNAEIOUAEIOUAEIOU"
    )

def prioridad_version():
    elementos = []
    for version, prioridad in VERSION_PRIORITY.items():
        elementos.extend([F.lit(version), F.lit(prioridad)])
    mapa = F.create_map(*elementos)
    return F.coalesce(
        F.element_at(mapa, F.upper(F.trim(F.col("version")))),
        F.lit(0)
    )

def seleccionar_version_canonica(tabla):
    df = spark.table(nombre_tabla(tabla))
    llaves = CONFIG[tabla]["llave_canonica"]
    if "version" not in df.columns:
        return df
    orden = [
        prioridad_version().desc(),
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("load_date").desc_nulls_last(),
    ]
    if "id" in df.columns:
        orden.append(F.col("id").desc_nulls_last())
    ventana = Window.partitionBy(*llaves).orderBy(*orden)
    return (
        df.withColumn("_rn_canonica", F.row_number().over(ventana))
          .filter(F.col("_rn_canonica") == 1)
          .drop("_rn_canonica")
    )

print(f"Configuración cargada para {len(TABLAS)} tablas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validación de entradas y diccionario de datos

# COMMAND ----------

tablas_disponibles = {
    fila.tableName
    for fila in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
}

faltantes = sorted(set(TABLAS) - tablas_disponibles)
extras = sorted(tablas_disponibles - set(TABLAS))

assert not faltantes, f"Faltan tablas requeridas: {faltantes}"

print(f"Tablas requeridas encontradas: {len(TABLAS)}")
print(f"Tablas adicionales no incluidas: {extras or 'ninguna'}")

errores_config = []
for tabla in TABLAS:
    columnas = set(spark.table(nombre_tabla(tabla)).columns)
    requeridas = set(CONFIG[tabla]["llave_version"])
    requeridas.update(CONFIG[tabla].get("numericas", []))
    requeridas.update(CONFIG[tabla].get("categoricas", []))
    if CONFIG[tabla]["fecha"]:
        requeridas.add(CONFIG[tabla]["fecha"])
    ausentes = sorted(requeridas - columnas)
    if ausentes:
        errores_config.append((tabla, ausentes))

assert not errores_config, f"Columnas configuradas ausentes: {errores_config}"
print("La configuración coincide con los esquemas actuales.")

# COMMAND ----------

diccionario_filas = []
for tabla in TABLAS:
    for campo in spark.table(nombre_tabla(tabla)).schema.fields:
        diccionario_filas.append({
            "tabla": tabla,
            "columna": campo.name,
            "tipo_bronze": campo.dataType.simpleString(),
            "nullable": campo.nullable,
            "rol": (
                "auditoría" if campo.name in AUDITORIA
                else "llave con versión" if campo.name in CONFIG[tabla]["llave_version"]
                else "medida a validar" if campo.name in CONFIG[tabla].get("numericas", [])
                else "atributo"
            )
        })

diccionario_df = spark.createDataFrame(diccionario_filas)
display(diccionario_df.orderBy("tabla", "columna"))

# COMMAND ----------

for tabla in TABLAS:
    print(f"Muestra — {tabla}")
    display(spark.table(nombre_tabla(tabla)).limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inventario, volumen, cobertura y trazabilidad

# COMMAND ----------

inventario_filas = []
conteos_tabla = {}

for tabla in TABLAS:
    df = spark.table(nombre_tabla(tabla))
    total = df.count()
    conteos_tabla[tabla] = total

    detalle = spark.sql(f"DESCRIBE DETAIL {nombre_tabla(tabla)}").first().asDict()
    fila = {
        "tabla": tabla,
        "registros": total,
        "columnas": len(df.columns),
        "archivos_delta": detalle.get("numFiles"),
        "tamano_bytes": detalle.get("sizeInBytes"),
        "ultima_modificacion_delta": detalle.get("lastModified"),
        "primera_carga": None,
        "ultima_carga": None,
        "primera_ingesta": None,
        "ultima_ingesta": None,
        "primera_fecha_evento": None,
        "ultima_fecha_evento": None,
    }

    auditoria = df.agg(
        F.min("load_date").alias("primera_carga"),
        F.max("load_date").alias("ultima_carga"),
        F.min("ingestion_timestamp").alias("primera_ingesta"),
        F.max("ingestion_timestamp").alias("ultima_ingesta"),
    ).first().asDict()
    fila.update(auditoria)

    if CONFIG[tabla]["fecha"]:
        rango = (
            df.select(fecha_evento(tabla).alias("_fecha_evento"))
              .agg(
                  F.min("_fecha_evento").alias("primera_fecha_evento"),
                  F.max("_fecha_evento").alias("ultima_fecha_evento"),
              )
              .first()
              .asDict()
        )
        fila.update(rango)

    inventario_filas.append(fila)

inventario_df = spark.createDataFrame(inventario_filas)
display(inventario_df.orderBy(F.desc("registros")))

# COMMAND ----------

trazabilidad_particiones = []
for tabla in TABLAS:
    resumen = (
        spark.table(nombre_tabla(tabla))
        .groupBy("load_date", "source_file_name", "source_file_path")
        .agg(
            F.count("*").alias("registros"),
            F.min("ingestion_timestamp").alias("primera_ingesta"),
            F.max("ingestion_timestamp").alias("ultima_ingesta"),
        )
        .withColumn("tabla", F.lit(tabla))
    )
    trazabilidad_particiones.append(resumen)

trazabilidad_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    trazabilidad_particiones,
)
display(
    trazabilidad_df
    .orderBy(F.desc("load_date"), "tabla")
    .limit(200)
)

# COMMAND ----------

rezago_filas = []
for tabla in TABLAS:
    if not CONFIG[tabla]["fecha"]:
        continue
    df = (
        spark.table(nombre_tabla(tabla))
        .withColumn("_evento", fecha_evento(tabla))
        .withColumn(
            "_rezago_dias",
            F.datediff(F.to_date("ingestion_timestamp"), F.to_date("_evento"))
        )
    )
    r = df.agg(
        F.count("*").alias("registros"),
        F.sum(F.when(F.col("_evento").isNull(), 1).otherwise(0)).alias("fecha_invalida"),
        F.sum(F.when(F.col("_rezago_dias") < 0, 1).otherwise(0)).alias("eventos_futuros"),
        F.min("_rezago_dias").alias("rezago_min_dias"),
        F.expr("percentile_approx(_rezago_dias, 0.5, 10000)").alias("rezago_mediana_dias"),
        F.expr("percentile_approx(_rezago_dias, 0.95, 10000)").alias("rezago_p95_dias"),
        F.max("_rezago_dias").alias("rezago_max_dias"),
    ).first().asDict()
    r["tabla"] = tabla
    rezago_filas.append(r)

rezago_df = spark.createDataFrame(rezago_filas)
display(rezago_df.orderBy(F.desc("rezago_p95_dias")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Completitud, vacíos y cardinalidad

# COMMAND ----------

completitud_filas = []

for tabla in TABLAS:
    df = spark.table(nombre_tabla(tabla))
    total = conteos_tabla[tabla]
    expresiones = []

    for campo in df.schema.fields:
        columna = campo.name
        expresiones.append(
            F.sum(F.when(F.col(columna).isNull(), 1).otherwise(0))
             .alias(f"{columna}__nulos")
        )
        if isinstance(campo.dataType, StringType):
            expresiones.append(
                F.sum(
                    F.when(
                        F.col(columna).isNotNull()
                        & (F.trim(F.col(columna)) == ""),
                        1
                    ).otherwise(0)
                ).alias(f"{columna}__vacios")
            )
        expresiones.append(
            F.approx_count_distinct(F.col(columna)).alias(f"{columna}__distintos")
        )

    resultado = df.agg(*expresiones).first().asDict()

    for campo in df.schema.fields:
        columna = campo.name
        nulos = resultado.get(f"{columna}__nulos", 0) or 0
        vacios = resultado.get(f"{columna}__vacios", 0) or 0
        completitud_filas.append({
            "tabla": tabla,
            "columna": columna,
            "tipo_bronze": campo.dataType.simpleString(),
            "registros": total,
            "nulos": nulos,
            "vacios": vacios,
            "faltantes_total": nulos + vacios,
            "pct_faltante": round((nulos + vacios) / total * 100, 6) if total else None,
            "distintos_aprox": resultado.get(f"{columna}__distintos"),
        })

completitud_df = spark.createDataFrame(completitud_filas)
display(
    completitud_df
    .orderBy(F.desc("pct_faltante"), "tabla", "columna")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validez de tipos y dominios básicos

# COMMAND ----------

validez_tipo_filas = []

for tabla in TABLAS:
    df = spark.table(nombre_tabla(tabla))
    total = conteos_tabla[tabla]

    for columna in CONFIG[tabla].get("numericas", []):
        fuente_no_vacia = (
            F.col(columna).isNotNull()
            & (F.trim(F.col(columna).cast("string")) != "")
        )
        invalida = fuente_no_vacia & F.expr(f"try_cast(`{columna}` as double)").isNull()
        n = df.agg(
            F.sum(F.when(invalida, 1).otherwise(0)).alias("invalidos")
        ).first()["invalidos"] or 0
        validez_tipo_filas.append({
            "tabla": tabla,
            "columna": columna,
            "tipo_objetivo": "double",
            "registros_invalidos": n,
            "pct_invalido": round(n / total * 100, 6) if total else None,
        })

    columnas_fecha = []
    if CONFIG[tabla]["fecha"]:
        columnas_fecha.append(CONFIG[tabla]["fecha"])
    columnas_fecha.extend(CONFIG[tabla].get("fechas_adicionales", []))

    for columna in columnas_fecha:
        fuente_no_vacia = (
            F.col(columna).isNotNull()
            & (F.trim(F.col(columna).cast("string")) != "")
        )
        invalida = fuente_no_vacia & F.to_timestamp(F.col(columna)).isNull()
        n = df.agg(
            F.sum(F.when(invalida, 1).otherwise(0)).alias("invalidos")
        ).first()["invalidos"] or 0
        validez_tipo_filas.append({
            "tabla": tabla,
            "columna": columna,
            "tipo_objetivo": "timestamp/date",
            "registros_invalidos": n,
            "pct_invalido": round(n / total * 100, 6) if total else None,
        })

validez_tipo_df = spark.createDataFrame(validez_tipo_filas)
display(validez_tipo_df.orderBy(F.desc("pct_invalido"), "tabla", "columna"))

# COMMAND ----------

coordenadas = (
    spark.table(nombre_tabla("embalses"))
    .withColumn("latitud_num", F.expr("try_cast(latitud as double)"))
    .withColumn("longitud_num", F.expr("try_cast(longitud as double)"))
)

resumen_coordenadas = coordenadas.agg(
    F.count("*").alias("embalses"),
    F.sum(F.when(F.col("latitud_num").isNull(), 1).otherwise(0)).alias("latitud_no_valida"),
    F.sum(F.when(F.col("longitud_num").isNull(), 1).otherwise(0)).alias("longitud_no_valida"),
    F.sum(
        F.when(
            ~F.col("latitud_num").between(-90, 90)
            | ~F.col("longitud_num").between(-180, 180),
            1
        ).otherwise(0)
    ).alias("fuera_rango_geografico"),
    F.sum(
        F.when(
            ~F.col("latitud_num").between(-5, 14)
            | ~F.col("longitud_num").between(-82, -66),
            1
        ).otherwise(0)
    ).alias("fuera_caja_colombia_aprox"),
)
display(resumen_coordenadas)

display(
    coordenadas.filter(
        F.col("latitud_num").isNull()
        | F.col("longitud_num").isNull()
        | ~F.col("latitud_num").between(-5, 14)
        | ~F.col("longitud_num").between(-82, -66)
    ).select(
        "codigo_embalse", "nombre_embalse",
        "latitud", "longitud", "source_file_name"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Duplicados y conflictos de llave
# MAGIC
# MAGIC Se calculan tres niveles:
# MAGIC
# MAGIC - **Duplicado exacto de negocio:** todos los campos no técnicos son iguales.
# MAGIC - **Duplicado de llave con versión:** la misma llave y versión aparece más de una vez; puede ser reingesta o conflicto.
# MAGIC - **Revisión multiversión:** la misma llave canónica existe en varias versiones; no es necesariamente un error y debe resolverse explícitamente en Silver.

# COMMAND ----------

duplicados_filas = []

if EJECUTAR_PERFIL_PESADO:
    for tabla in TABLAS:
        df = spark.table(nombre_tabla(tabla))
        columnas_negocio = [
            c for c in df.columns
            if c not in AUDITORIA
        ]
        # El id surrogate no define igualdad del dato fuente.
        columnas_payload = [c for c in columnas_negocio if c != "id"]

        perfiles = [
            ("payload_exacto", columnas_payload),
            ("llave_con_version", CONFIG[tabla]["llave_version"]),
        ]
        if "id" in df.columns:
            perfiles.append(("id_tecnico", ["id"]))

        for nombre_perfil, llaves in perfiles:
            grupos = df.groupBy(*llaves).count().filter(F.col("count") > 1)
            stats = grupos.agg(
                F.count("*").alias("grupos_duplicados"),
                F.coalesce(F.sum("count"), F.lit(0)).alias("filas_en_grupos"),
                F.coalesce(F.sum(F.col("count") - 1), F.lit(0)).alias("filas_excedentes"),
                F.coalesce(F.max("count"), F.lit(1)).alias("max_repeticiones"),
            ).first().asDict()
            stats.update({
                "tabla": tabla,
                "perfil": nombre_perfil,
                "registros_tabla": conteos_tabla[tabla],
                "pct_excedente": round(
                    (stats["filas_excedentes"] or 0) / conteos_tabla[tabla] * 100,
                    6
                ) if conteos_tabla[tabla] else None,
            })
            duplicados_filas.append(stats)

        if "version" in df.columns:
            revisiones = (
                df.select(*CONFIG[tabla]["llave_canonica"], "version")
                .dropDuplicates()
                .groupBy(*CONFIG[tabla]["llave_canonica"])
                .agg(F.countDistinct("version").alias("n_versiones"))
                .filter(F.col("n_versiones") > 1)
            )
            r = revisiones.agg(
                F.count("*").alias("grupos_duplicados"),
                F.coalesce(F.sum("n_versiones"), F.lit(0)).alias("filas_en_grupos"),
                F.coalesce(F.sum(F.col("n_versiones") - 1), F.lit(0)).alias("filas_excedentes"),
                F.coalesce(F.max("n_versiones"), F.lit(1)).alias("max_repeticiones"),
            ).first().asDict()
            r.update({
                "tabla": tabla,
                "perfil": "llaves_con_multiples_versiones",
                "registros_tabla": conteos_tabla[tabla],
                "pct_excedente": None,
            })
            duplicados_filas.append(r)

        if "valor" in df.columns:
            conflictos = (
                df.groupBy(*CONFIG[tabla]["llave_version"])
                .agg(F.countDistinct("valor").alias("valores_distintos"))
                .filter(F.col("valores_distintos") > 1)
            )
            r = conflictos.agg(
                F.count("*").alias("grupos_duplicados"),
                F.coalesce(F.sum("valores_distintos"), F.lit(0)).alias("filas_en_grupos"),
                F.coalesce(F.sum(F.col("valores_distintos") - 1), F.lit(0)).alias("filas_excedentes"),
                F.coalesce(F.max("valores_distintos"), F.lit(1)).alias("max_repeticiones"),
            ).first().asDict()
            r.update({
                "tabla": tabla,
                "perfil": "conflicto_valor_misma_version",
                "registros_tabla": conteos_tabla[tabla],
                "pct_excedente": None,
            })
            duplicados_filas.append(r)

    duplicados_df = spark.createDataFrame(duplicados_filas)
    display(duplicados_df.orderBy(F.desc("filas_excedentes"), "tabla", "perfil"))
else:
    duplicados_df = spark.createDataFrame(
        [], "tabla string, perfil string, registros_tabla long, grupos_duplicados long, "
        "filas_en_grupos long, filas_excedentes long, max_repeticiones long, pct_excedente double"
    )
    print("Perfil pesado omitido. Active EJECUTAR_PERFIL_PESADO para evaluar duplicados.")

# COMMAND ----------

if EJECUTAR_PERFIL_PESADO:
    for tabla in TABLAS:
        df = spark.table(nombre_tabla(tabla))
        grupos = (
            df.groupBy(*CONFIG[tabla]["llave_version"])
            .agg(
                F.count("*").alias("repeticiones"),
                F.countDistinct(
                    *([F.col("valor")] if "valor" in df.columns else [F.struct(*df.columns)])
                ).alias("variantes")
            )
            .filter(F.col("repeticiones") > 1)
            .orderBy(F.desc("repeticiones"))
            .limit(20)
        )
        if grupos.take(1):
            print(f"Principales llaves duplicadas — {tabla}")
            display(grupos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dominios categóricos, unidades, duraciones y versiones

# COMMAND ----------

dominios = []
for tabla in TABLAS:
    df = spark.table(nombre_tabla(tabla))
    total = conteos_tabla[tabla]
    for columna in CONFIG[tabla].get("categoricas", []):
        perfil = (
            df.groupBy(columna)
              .agg(F.count("*").alias("registros"))
              .withColumn("tabla", F.lit(tabla))
              .withColumn("columna", F.lit(columna))
              .withColumn(
                  "pct_tabla",
                  F.round(F.col("registros") / F.lit(total) * 100, 6)
              )
              .select(
                  "tabla", "columna",
                  F.col(columna).cast("string").alias("valor"),
                  "registros", "pct_tabla"
              )
        )
        dominios.append(perfil)

dominios_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    dominios,
)
display(
    dominios_df
    .orderBy("tabla", "columna", F.desc("registros"))
    .limit(500)
)

# COMMAND ----------

dominios_inesperados = []
for tabla, reglas in DOMINIOS_ESPERADOS.items():
    df = spark.table(nombre_tabla(tabla))
    for columna, permitidos in reglas.items():
        inesperados = (
            df.select(F.col(columna).cast("string").alias("valor"))
            .filter(
                F.col("valor").isNotNull()
                & ~F.col("valor").isin(sorted(permitidos))
            )
            .groupBy("valor")
            .agg(F.count("*").alias("registros"))
            .withColumn("tabla", F.lit(tabla))
            .withColumn("columna", F.lit(columna))
            .withColumn("valores_esperados", F.lit(", ".join(sorted(permitidos))))
        )
        dominios_inesperados.append(inesperados)

dominios_inesperados_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    dominios_inesperados,
)
display(
    dominios_inesperados_df
    .select("tabla", "columna", "valor", "registros", "valores_esperados")
    .orderBy("tabla", "columna", F.desc("registros"))
)

# COMMAND ----------

for tabla in [
    "demanda_real", "disponibilidad_plantas",
    "generacion_real", "niveles_embalses", "precio_bolsa"
]:
    print(f"Versiones observadas — {tabla}")
    display(
        spark.table(nombre_tabla(tabla))
        .groupBy("version", "codigo_variable", "codigo_duracion", "unidad_medida")
        .agg(
            F.count("*").alias("registros"),
            F.min(CONFIG[tabla]["fecha"]).alias("primera_fecha"),
            F.max(CONFIG[tabla]["fecha"]).alias("ultima_fecha"),
        )
        .orderBy("version", "codigo_variable")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Perfil numérico, ceros, negativos y outliers robustos

# COMMAND ----------

perfil_numerico_filas = []

for tabla in TABLAS:
    df_base = spark.table(nombre_tabla(tabla))
    for columna in CONFIG[tabla].get("numericas", []):
        df = df_base.withColumn("_x", F.expr(f"try_cast(`{columna}` as double)"))
        r = df.agg(
            F.count("*").alias("registros"),
            F.count("_x").alias("validos"),
            F.sum(F.when(F.col("_x") == 0, 1).otherwise(0)).alias("ceros"),
            F.sum(F.when(F.col("_x") < 0, 1).otherwise(0)).alias("negativos"),
            F.min("_x").alias("minimo"),
            F.expr("percentile_approx(_x, 0.01, 10000)").alias("p01"),
            F.expr("percentile_approx(_x, 0.25, 10000)").alias("q1"),
            F.expr("percentile_approx(_x, 0.5, 10000)").alias("mediana"),
            F.expr("percentile_approx(_x, 0.75, 10000)").alias("q3"),
            F.expr("percentile_approx(_x, 0.95, 10000)").alias("p95"),
            F.expr("percentile_approx(_x, 0.99, 10000)").alias("p99"),
            F.max("_x").alias("maximo"),
            F.avg("_x").alias("promedio"),
            F.stddev("_x").alias("desv_estandar"),
        ).first().asDict()

        q1, q3 = r.get("q1"), r.get("q3")
        limite_inferior = q1 - 1.5 * (q3 - q1) if q1 is not None and q3 is not None else None
        limite_superior = q3 + 1.5 * (q3 - q1) if q1 is not None and q3 is not None else None
        if limite_inferior is not None:
            outliers = df.filter(
                (F.col("_x") < F.lit(limite_inferior))
                | (F.col("_x") > F.lit(limite_superior))
            ).count()
        else:
            outliers = None

        r.update({
            "tabla": tabla,
            "columna": columna,
            "pct_ceros": round((r["ceros"] or 0) / r["registros"] * 100, 6) if r["registros"] else None,
            "pct_negativos": round((r["negativos"] or 0) / r["registros"] * 100, 6) if r["registros"] else None,
            "limite_iqr_inferior": limite_inferior,
            "limite_iqr_superior": limite_superior,
            "outliers_iqr": outliers,
            "pct_outliers_iqr": round(outliers / r["registros"] * 100, 6) if r["registros"] and outliers is not None else None,
        })
        perfil_numerico_filas.append(r)

perfil_numerico_df = spark.createDataFrame(perfil_numerico_filas)
display(perfil_numerico_df.orderBy("tabla", "columna"))

# COMMAND ----------

perfiles_medida = []
for tabla in [
    "demanda_real", "disponibilidad_plantas",
    "generacion_real", "niveles_embalses", "precio_bolsa"
]:
    agrupadores = [
        c for c in ["codigo_variable", "unidad_medida", "version", "tipo_mercado"]
        if c in spark.table(nombre_tabla(tabla)).columns
    ]
    perfil = (
        spark.table(nombre_tabla(tabla))
        .withColumn("_valor_num", F.expr("try_cast(valor as double)"))
        .groupBy(*agrupadores)
        .agg(
            F.count("*").alias("registros"),
            F.sum(F.when(F.col("_valor_num") == 0, 1).otherwise(0)).alias("ceros"),
            F.sum(F.when(F.col("_valor_num") < 0, 1).otherwise(0)).alias("negativos"),
            F.min("_valor_num").alias("minimo"),
            F.expr("percentile_approx(_valor_num, 0.5, 10000)").alias("mediana"),
            F.expr("percentile_approx(_valor_num, 0.99, 10000)").alias("p99"),
            F.max("_valor_num").alias("maximo"),
        )
        .withColumn("tabla", F.lit(tabla))
    )
    perfiles_medida.append(perfil)

perfil_medida_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    perfiles_medida,
)
display(
    perfil_medida_df
    .select(
        "tabla", "codigo_variable", "tipo_mercado",
        "unidad_medida", "version", "registros",
        "ceros", "negativos", "minimo", "mediana", "p99", "maximo"
    )
    .orderBy("tabla", "codigo_variable", "tipo_mercado", "version")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Volumen diario, cobertura temporal y continuidad

# COMMAND ----------

volumenes_diarios = []
for tabla in TABLAS:
    if not CONFIG[tabla]["fecha"]:
        continue
    df = (
        spark.table(nombre_tabla(tabla))
        .withColumn("fecha_evento", F.to_date(fecha_evento(tabla)))
    )
    entidad = CONFIG[tabla]["entidad"]
    resumen = (
        df.groupBy("fecha_evento")
        .agg(
            F.count("*").alias("registros"),
            F.approx_count_distinct(F.struct(*entidad)).alias("entidades"),
            F.approx_count_distinct("source_file_path").alias("archivos_fuente"),
        )
        .withColumn("tabla", F.lit(tabla))
    )
    volumenes_diarios.append(resumen)

volumen_diario_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    volumenes_diarios,
)
display(
    volumen_diario_df
    .orderBy(F.desc("fecha_evento"), "tabla")
    .limit(300)
)

# COMMAND ----------

continuidad_filas = []
faltantes_fecha_frames = []

for tabla in [
    "demanda_real", "disponibilidad_plantas",
    "generacion_real", "niveles_embalses", "precio_bolsa"
]:
    fechas = (
        spark.table(nombre_tabla(tabla))
        .select(F.to_date(fecha_evento(tabla)).alias("fecha"))
        .filter(F.col("fecha").isNotNull())
        .distinct()
    )
    limites = fechas.agg(F.min("fecha").alias("min"), F.max("fecha").alias("max")).first()
    if limites["min"] is None:
        continue
    calendario = spark.range(1).select(
        F.explode(
            F.sequence(F.lit(limites["min"]), F.lit(limites["max"]))
        ).alias("fecha")
    )
    faltantes = (
        calendario.join(fechas, "fecha", "left_anti")
        .withColumn("tabla", F.lit(tabla))
    )
    faltantes_fecha_frames.append(faltantes)
    continuidad_filas.append({
        "tabla": tabla,
        "control": "dias_globales_faltantes",
        "grupos_observados": fechas.count(),
        "grupos_completos": None,
        "grupos_incompletos": faltantes.count(),
        "grupos_sobrecompletos": None,
        "fecha_min": limites["min"],
        "fecha_max": limites["max"],
    })

faltantes_fecha_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    faltantes_fecha_frames,
)
display(faltantes_fecha_df.orderBy("tabla", "fecha"))

# COMMAND ----------

periodos_incompletos_frames = []

for tabla in [
    "demanda_real", "disponibilidad_plantas",
    "generacion_real", "precio_bolsa"
]:
    df = (
        seleccionar_version_canonica(tabla)
        .withColumn("_ts", fecha_evento(tabla))
        .withColumn("fecha", F.to_date("_ts"))
        .withColumn("hora", F.date_trunc("hour", F.col("_ts")))
    )
    grupos = CONFIG[tabla]["entidad"]
    por_entidad_dia = (
        df.groupBy("fecha", *grupos)
        .agg(F.countDistinct("hora").alias("periodos_horas"))
    )
    resumen = por_entidad_dia.agg(
        F.count("*").alias("observados"),
        F.sum(F.when(F.col("periodos_horas") == 24, 1).otherwise(0)).alias("completos"),
        F.sum(F.when(F.col("periodos_horas") < 24, 1).otherwise(0)).alias("incompletos"),
        F.sum(F.when(F.col("periodos_horas") > 24, 1).otherwise(0)).alias("sobrecompletos"),
        F.min("fecha").alias("fecha_min"),
        F.max("fecha").alias("fecha_max"),
    ).first().asDict()

    continuidad_filas.append({
        "tabla": tabla,
        "control": "entidad_dia_24_horas",
        "grupos_observados": resumen["observados"],
        "grupos_completos": resumen["completos"],
        "grupos_incompletos": resumen["incompletos"],
        "grupos_sobrecompletos": resumen["sobrecompletos"],
        "fecha_min": resumen["fecha_min"],
        "fecha_max": resumen["fecha_max"],
    })

    detalle = (
        por_entidad_dia
        .filter(F.col("periodos_horas") != 24)
        .withColumn("tabla", F.lit(tabla))
    )
    periodos_incompletos_frames.append(detalle)

continuidad_df = spark.createDataFrame(continuidad_filas)
display(continuidad_df.orderBy("tabla", "control"))

periodos_incompletos_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    periodos_incompletos_frames,
)
display(
    periodos_incompletos_df
    .orderBy("tabla", F.desc("fecha"), "periodos_horas")
    .limit(200)
)

# COMMAND ----------

# Visualización acotada: volumen diario por fuente.
try:
    import matplotlib.pyplot as plt

    volumen_pdf = (
        volumen_diario_df
        .select("fecha_evento", "tabla", "registros")
        .orderBy("fecha_evento")
        .toPandas()
    )
    pivote = volumen_pdf.pivot(
        index="fecha_evento", columns="tabla", values="registros"
    )
    ax = pivote.plot(figsize=(14, 7), linewidth=1.8)
    ax.set_title("Registros diarios por fuente Bronze")
    ax.set_xlabel("Fecha del dato")
    ax.set_ylabel("Registros (escala logarítmica)")
    ax.set_yscale("log")
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.show()
except Exception as exc:
    print(f"No fue posible renderizar el gráfico: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Revisiones entre versiones

# COMMAND ----------

revision_resumen_filas = []
revision_detalles = {}

for tabla in [
    "demanda_real", "disponibilidad_plantas",
    "generacion_real", "niveles_embalses", "precio_bolsa"
]:
    df = (
        spark.table(nombre_tabla(tabla))
        .select(
            *CONFIG[tabla]["llave_canonica"],
            "version",
            F.expr("try_cast(valor as double)").alias("valor_num")
        )
        .dropDuplicates()
    )
    por_llave = (
        df.groupBy(*CONFIG[tabla]["llave_canonica"])
        .agg(
            F.countDistinct("version").alias("n_versiones"),
            F.collect_set("version").alias("versiones"),
            F.min("valor_num").alias("valor_min"),
            F.max("valor_num").alias("valor_max"),
        )
        .withColumn("rango_revision", F.col("valor_max") - F.col("valor_min"))
        .withColumn(
            "pct_rango_sobre_max",
            F.when(
                F.abs(F.col("valor_max")) > 0,
                F.abs(F.col("rango_revision")) / F.abs(F.col("valor_max")) * 100
            )
        )
    )
    revisadas = por_llave.filter(F.col("n_versiones") > 1)
    r = revisadas.agg(
        F.count("*").alias("llaves_revisadas"),
        F.avg("n_versiones").alias("versiones_promedio"),
        F.max("n_versiones").alias("versiones_maximas"),
        F.avg("rango_revision").alias("rango_promedio"),
        F.expr("percentile_approx(rango_revision, 0.95, 10000)").alias("rango_p95"),
        F.max("rango_revision").alias("rango_maximo"),
        F.expr("percentile_approx(pct_rango_sobre_max, 0.5, 10000)").alias("pct_cambio_mediana"),
        F.expr("percentile_approx(pct_rango_sobre_max, 0.95, 10000)").alias("pct_cambio_p95"),
    ).first().asDict()
    r["tabla"] = tabla
    revision_resumen_filas.append(r)
    revision_detalles[tabla] = revisadas

revision_resumen_df = spark.createDataFrame(revision_resumen_filas)
display(revision_resumen_df.orderBy(F.desc("llaves_revisadas")))

# COMMAND ----------

for tabla, detalle in revision_detalles.items():
    print(f"Mayores variaciones entre versiones — {tabla}")
    display(
        detalle
        .orderBy(F.desc("rango_revision"))
        .limit(30)
    )

# COMMAND ----------

seleccion_version_frames = []
for tabla in [
    "demanda_real", "disponibilidad_plantas",
    "generacion_real", "niveles_embalses", "precio_bolsa"
]:
    elegido = (
        seleccionar_version_canonica(tabla)
        .groupBy("version")
        .agg(F.count("*").alias("llaves_seleccionadas"))
        .withColumn("tabla", F.lit(tabla))
    )
    seleccion_version_frames.append(elegido)

seleccion_version_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    seleccion_version_frames,
)
display(seleccion_version_df.orderBy("tabla", F.desc("llaves_seleccionadas")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Calidad de identificadores y estabilidad de nombres

# COMMAND ----------

identificadores = [
    ("agentes", "codigo_sic_agente"),
    ("demanda_real", "codigo_sic_agente"),
    ("generacion_real", "codigo_sic_agente"),
    ("plantas", "codigo_sic_agente"),
    ("plantas", "codigo_planta"),
    ("generacion_real", "codigo_planta"),
    ("disponibilidad_plantas", "codigo_planta"),
    ("niveles_embalses", "codigo_planta"),
    ("embalses", "codigo_embalse"),
]

calidad_id_filas = []
for tabla, columna in identificadores:
    df = spark.table(nombre_tabla(tabla))
    r = df.agg(
        F.count("*").alias("registros"),
        F.sum(F.when(F.col(columna).isNull(), 1).otherwise(0)).alias("nulos"),
        F.sum(
            F.when(F.trim(F.col(columna)) == "", 1).otherwise(0)
        ).alias("vacios"),
        F.sum(
            F.when(F.col(columna) != F.trim(F.col(columna)), 1).otherwise(0)
        ).alias("con_espacios_externos"),
        F.sum(
            F.when(F.col(columna) != F.upper(F.col(columna)), 1).otherwise(0)
        ).alias("no_mayuscula"),
        F.min(F.length(F.trim(F.col(columna)))).alias("longitud_min"),
        F.max(F.length(F.trim(F.col(columna)))).alias("longitud_max"),
        F.approx_count_distinct(columna).alias("distintos"),
    ).first().asDict()
    r.update({"tabla": tabla, "columna": columna})
    calidad_id_filas.append(r)

calidad_id_df = spark.createDataFrame(calidad_id_filas)
display(calidad_id_df.orderBy("columna", "tabla"))

# COMMAND ----------

estabilidad_frames = []
for tabla, codigo, nombre in [
    ("agentes", "codigo_sic_agente", "nombre_agente"),
    ("plantas", "codigo_planta", "nombre_planta"),
    ("embalses", "codigo_embalse", "nombre_embalse"),
]:
    df = spark.table(nombre_tabla(tabla))
    cambios = (
        df.groupBy(F.upper(F.trim(F.col(codigo))).alias("codigo_normalizado"))
        .agg(
            F.countDistinct(normalizar_texto(F.col(nombre))).alias("nombres_distintos"),
            F.collect_set(nombre).alias("nombres_observados"),
        )
        .filter(F.col("nombres_distintos") > 1)
        .withColumn("tabla", F.lit(tabla))
        .withColumn("campo_codigo", F.lit(codigo))
    )
    estabilidad_frames.append(cambios)

estabilidad_nombres_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    estabilidad_frames,
)
display(
    estabilidad_nombres_df
    .orderBy(F.desc("nombres_distintos"), "tabla")
    .limit(200)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Integridad referencial y cobertura entre fuentes

# COMMAND ----------

plantas_codigos = (
    spark.table(nombre_tabla("plantas"))
    .select(F.upper(F.trim("codigo_planta")).alias("codigo"))
    .filter(F.col("codigo").isNotNull() & (F.col("codigo") != ""))
    .distinct()
)
agentes_codigos = (
    spark.table(nombre_tabla("agentes"))
    .select(F.upper(F.trim("codigo_sic_agente")).alias("codigo"))
    .filter(F.col("codigo").isNotNull() & (F.col("codigo") != ""))
    .distinct()
)

integridad_filas = []
orfanos_frames = []

def evaluar_fk(tabla_hija, columna_hija, padre_df, relacion):
    hijos = (
        spark.table(nombre_tabla(tabla_hija))
        .select(F.upper(F.trim(F.col(columna_hija))).alias("codigo"))
        .filter(F.col("codigo").isNotNull() & (F.col("codigo") != ""))
        .distinct()
    )
    orfanos = hijos.join(padre_df, "codigo", "left_anti")
    total = hijos.count()
    n_orfanos = orfanos.count()
    integridad_filas.append({
        "relacion": relacion,
        "tabla_hija": tabla_hija,
        "columna_hija": columna_hija,
        "codigos_hijos": total,
        "codigos_huerfanos": n_orfanos,
        "cobertura_pct": round((total - n_orfanos) / total * 100, 4) if total else None,
    })
    orfanos_frames.append(
        orfanos
        .withColumn("relacion", F.lit(relacion))
        .withColumn("tabla_hija", F.lit(tabla_hija))
    )

for tabla in ["generacion_real", "disponibilidad_plantas", "niveles_embalses"]:
    evaluar_fk(
        tabla, "codigo_planta", plantas_codigos,
        f"{tabla}.codigo_planta -> plantas.codigo_planta"
    )

for tabla in ["demanda_real", "generacion_real", "plantas"]:
    evaluar_fk(
        tabla, "codigo_sic_agente", agentes_codigos,
        f"{tabla}.codigo_sic_agente -> agentes.codigo_sic_agente"
    )

integridad_df = spark.createDataFrame(integridad_filas)
display(integridad_df.orderBy("cobertura_pct"))

orfanos_df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    orfanos_frames,
)
display(orfanos_df.orderBy("relacion", "codigo").limit(300))

# COMMAND ----------

plantas_nombres = (
    spark.table(nombre_tabla("plantas"))
    .select(normalizar_texto(F.col("nombre_planta")).alias("nombre"))
    .filter(F.col("nombre").isNotNull() & (F.col("nombre") != ""))
    .distinct()
)
embalses_nombres = (
    spark.table(nombre_tabla("embalses"))
    .select(normalizar_texto(F.col("nombre_embalse")).alias("nombre"))
    .filter(F.col("nombre").isNotNull() & (F.col("nombre") != ""))
    .distinct()
)
relaciones = spark.table(nombre_tabla("plantas_reservorios"))

plantas_relacion = (
    relaciones
    .select(normalizar_texto(F.col("nombre_planta")).alias("nombre"))
    .distinct()
)
reservorios_relacion = (
    relaciones
    .select(normalizar_texto(F.col("nombre_reservorio")).alias("nombre"))
    .distinct()
)

plantas_sin_match = plantas_relacion.join(plantas_nombres, "nombre", "left_anti")
reservorios_sin_match = reservorios_relacion.join(embalses_nombres, "nombre", "left_anti")

integridad_nombres_df = spark.createDataFrame([
    {
        "relacion": "plantas_reservorios.nombre_planta -> plantas.nombre_planta",
        "valores_relacion": plantas_relacion.count(),
        "sin_match_exacto_normalizado": plantas_sin_match.count(),
    },
    {
        "relacion": "plantas_reservorios.nombre_reservorio -> embalses.nombre_embalse",
        "valores_relacion": reservorios_relacion.count(),
        "sin_match_exacto_normalizado": reservorios_sin_match.count(),
    },
])
display(integridad_nombres_df)

print("Plantas sin correspondencia exacta normalizada")
display(plantas_sin_match)
print("Reservorios sin correspondencia exacta normalizada")
display(reservorios_sin_match)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Coherencia operativa entre generación, disponibilidad y capacidad

# COMMAND ----------

generacion_hora = (
    seleccionar_version_canonica("generacion_real")
    .withColumn("fecha_hora_ts", fecha_evento("generacion_real"))
    .withColumn("codigo_planta_norm", F.upper(F.trim("codigo_planta")))
    .withColumn("generacion_kwh", F.expr("try_cast(valor as double)"))
    .groupBy("fecha_hora_ts", "codigo_planta_norm")
    .agg(F.sum("generacion_kwh").alias("generacion_kwh"))
)

disponibilidad_hora = (
    seleccionar_version_canonica("disponibilidad_plantas")
    .withColumn("fecha_hora_ts", fecha_evento("disponibilidad_plantas"))
    .withColumn("codigo_planta_norm", F.upper(F.trim("codigo_planta")))
    .withColumn("disponibilidad_kwh", F.expr("try_cast(valor as double)"))
    .groupBy("fecha_hora_ts", "codigo_planta_norm")
    .agg(F.sum("disponibilidad_kwh").alias("disponibilidad_kwh"))
)

comparacion_operativa = (
    generacion_hora.join(
        disponibilidad_hora,
        ["fecha_hora_ts", "codigo_planta_norm"],
        "inner"
    )
    .withColumn(
        "ratio_generacion_disponibilidad",
        F.when(
            F.col("disponibilidad_kwh") > 0,
            F.col("generacion_kwh") / F.col("disponibilidad_kwh")
        )
    )
)

resumen_operativo = comparacion_operativa.agg(
    F.count("*").alias("horas_comparables"),
    F.sum(
        F.when(
            F.col("generacion_kwh") > F.col("disponibilidad_kwh") * TOLERANCIA_OPERATIVA,
            1
        ).otherwise(0)
    ).alias("horas_generacion_supera_disponibilidad_5pct"),
    F.sum(
        F.when(
            (F.col("disponibilidad_kwh") == 0)
            & (F.col("generacion_kwh") > 0),
            1
        ).otherwise(0)
    ).alias("horas_disponibilidad_cero_con_generacion"),
    F.expr(
        "percentile_approx(ratio_generacion_disponibilidad, 0.5, 10000)"
    ).alias("ratio_mediana"),
    F.expr(
        "percentile_approx(ratio_generacion_disponibilidad, 0.95, 10000)"
    ).alias("ratio_p95"),
    F.max("ratio_generacion_disponibilidad").alias("ratio_maximo"),
)
display(resumen_operativo)

# COMMAND ----------

anomalias_por_planta = (
    comparacion_operativa
    .groupBy("codigo_planta_norm")
    .agg(
        F.count("*").alias("horas_comparables"),
        F.sum(
            F.when(
                F.col("generacion_kwh") > F.col("disponibilidad_kwh") * TOLERANCIA_OPERATIVA,
                1
            ).otherwise(0)
        ).alias("horas_supera_5pct"),
        F.sum(
            F.when(
                (F.col("disponibilidad_kwh") == 0)
                & (F.col("generacion_kwh") > 0),
                1
            ).otherwise(0)
        ).alias("horas_disp_cero_con_gen"),
        F.max("ratio_generacion_disponibilidad").alias("ratio_maximo"),
    )
    .withColumn(
        "pct_horas_supera_5pct",
        F.round(F.col("horas_supera_5pct") / F.col("horas_comparables") * 100, 4)
    )
    .orderBy(F.desc("horas_supera_5pct"))
)
display(anomalias_por_planta.limit(100))

# COMMAND ----------

plantas_base = (
    spark.table(nombre_tabla("plantas"))
    .withColumn("codigo_planta_norm", F.upper(F.trim("codigo_planta")))
    .withColumn("fecha_planta", F.to_date("fecha"))
    .withColumn("capacidad_kw", F.expr("try_cast(cap_efectiva_neta as double)"))
)
ventana_planta = Window.partitionBy("codigo_planta_norm").orderBy(
    F.col("fecha_planta").desc_nulls_last(),
    F.col("ingestion_timestamp").desc_nulls_last(),
    F.col("id").desc_nulls_last(),
)
plantas_actuales = (
    plantas_base
    .withColumn("_rn", F.row_number().over(ventana_planta))
    .filter(F.col("_rn") == 1)
    .select(
        "codigo_planta_norm", "nombre_planta", "codigo_sic_agente",
        "tipo_generacion", "capacidad_kw"
    )
)

generacion_capacidad = (
    generacion_hora.join(plantas_actuales, "codigo_planta_norm", "inner")
    .withColumn(
        "factor_horario",
        F.when(F.col("capacidad_kw") > 0, F.col("generacion_kwh") / F.col("capacidad_kw"))
    )
)

resumen_capacidad = generacion_capacidad.agg(
    F.count("*").alias("horas_con_maestro"),
    F.sum(F.when(F.col("capacidad_kw").isNull(), 1).otherwise(0)).alias("horas_sin_capacidad"),
    F.sum(
        F.when(F.col("generacion_kwh") > F.col("capacidad_kw") * TOLERANCIA_OPERATIVA, 1)
         .otherwise(0)
    ).alias("horas_generacion_supera_capacidad_5pct"),
    F.expr("percentile_approx(factor_horario, 0.5, 10000)").alias("factor_mediana"),
    F.expr("percentile_approx(factor_horario, 0.95, 10000)").alias("factor_p95"),
    F.max("factor_horario").alias("factor_maximo"),
)
display(resumen_capacidad)

display(
    generacion_capacidad
    .filter(F.col("factor_horario") > TOLERANCIA_OPERATIVA)
    .orderBy(F.desc("factor_horario"))
    .select(
        "fecha_hora_ts", "codigo_planta_norm", "nombre_planta",
        "tipo_generacion", "generacion_kwh", "capacidad_kw", "factor_horario"
    )
    .limit(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Comportamiento del sistema: generación, demanda, disponibilidad y precio

# COMMAND ----------

demanda_hora = (
    seleccionar_version_canonica("demanda_real")
    .withColumn("fecha_hora_ts", fecha_evento("demanda_real"))
    .withColumn("demanda_kwh", F.expr("try_cast(valor as double)"))
    .groupBy("fecha_hora_ts")
    .agg(F.sum("demanda_kwh").alias("demanda_kwh"))
)

generacion_sistema_hora = (
    generacion_hora.groupBy("fecha_hora_ts")
    .agg(F.sum("generacion_kwh").alias("generacion_kwh"))
)

disponibilidad_sistema_hora = (
    disponibilidad_hora.groupBy("fecha_hora_ts")
    .agg(F.sum("disponibilidad_kwh").alias("disponibilidad_kwh"))
)

operacion_hora = (
    generacion_sistema_hora
    .join(demanda_hora, "fecha_hora_ts", "full")
    .join(disponibilidad_sistema_hora, "fecha_hora_ts", "full")
)

operacion_diaria = (
    operacion_hora
    .withColumn("fecha", F.to_date("fecha_hora_ts"))
    .groupBy("fecha")
    .agg(
        (F.sum("generacion_kwh") / F.lit(1_000_000)).alias("generacion_gwh"),
        (F.sum("demanda_kwh") / F.lit(1_000_000)).alias("demanda_gwh"),
        (F.sum("disponibilidad_kwh") / F.lit(1_000_000)).alias("disponibilidad_gwh"),
        F.countDistinct(
            F.when(F.col("generacion_kwh").isNotNull(), F.hour("fecha_hora_ts"))
        ).alias("horas_generacion"),
        F.countDistinct(
            F.when(F.col("demanda_kwh").isNotNull(), F.hour("fecha_hora_ts"))
        ).alias("horas_demanda"),
        F.countDistinct(
            F.when(F.col("disponibilidad_kwh").isNotNull(), F.hour("fecha_hora_ts"))
        ).alias("horas_disponibilidad"),
    )
    .withColumn(
        "margen_generacion_demanda_gwh",
        F.col("generacion_gwh") - F.col("demanda_gwh")
    )
    .withColumn(
        "utilizacion_disponibilidad_pct",
        F.when(
            F.col("disponibilidad_gwh") > 0,
            F.col("generacion_gwh") / F.col("disponibilidad_gwh") * 100
        )
    )
)

display(operacion_diaria.orderBy(F.desc("fecha")).limit(250))

# COMMAND ----------

mercado_diario = (
    seleccionar_version_canonica("demanda_real")
    .withColumn("fecha", F.to_date(fecha_evento("demanda_real")))
    .withColumn("demanda_gwh", F.expr("try_cast(valor as double)") / F.lit(1_000_000))
    .groupBy("fecha", "tipo_mercado")
    .agg(F.sum("demanda_gwh").alias("demanda_gwh"))
)
display(mercado_diario.orderBy(F.desc("fecha"), "tipo_mercado").limit(400))

agentes_demanda = (
    seleccionar_version_canonica("demanda_real")
    .withColumn("demanda_gwh", F.expr("try_cast(valor as double)") / F.lit(1_000_000))
    .groupBy("codigo_sic_agente", "tipo_mercado")
    .agg(
        F.sum("demanda_gwh").alias("demanda_gwh"),
        F.min(CONFIG["demanda_real"]["fecha"]).alias("primera_fecha"),
        F.max(CONFIG["demanda_real"]["fecha"]).alias("ultima_fecha"),
    )
    .orderBy(F.desc("demanda_gwh"))
)
display(agentes_demanda.limit(100))

# COMMAND ----------

precio_diario = (
    seleccionar_version_canonica("precio_bolsa")
    .withColumn("fecha", F.to_date(fecha_evento("precio_bolsa")))
    .withColumn("precio", F.expr("try_cast(valor as double)"))
    .groupBy("fecha", "codigo_variable")
    .agg(
        F.avg("precio").alias("precio_promedio_cop_kwh"),
        F.min("precio").alias("precio_minimo_cop_kwh"),
        F.max("precio").alias("precio_maximo_cop_kwh"),
        F.countDistinct(F.hour(fecha_evento("precio_bolsa"))).alias("horas"),
    )
)
display(precio_diario.orderBy(F.desc("fecha"), "codigo_variable").limit(400))

# COMMAND ----------

try:
    import matplotlib.pyplot as plt

    operacion_pdf = operacion_diaria.orderBy("fecha").toPandas()
    ax = operacion_pdf.plot(
        x="fecha",
        y=["generacion_gwh", "demanda_gwh", "disponibilidad_gwh"],
        figsize=(14, 6),
        linewidth=2,
        color=["#1473E6", "#F2B705", "#00979D"],
    )
    ax.set_title("Operación diaria del sistema")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("GWh")
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.show()

    precio_nal_pdf = (
        precio_diario
        .filter(F.col("codigo_variable") == "PB_Nal")
        .orderBy("fecha")
        .toPandas()
    )
    ax = precio_nal_pdf.plot(
        x="fecha",
        y="precio_promedio_cop_kwh",
        figsize=(14, 4),
        linewidth=2,
        color="#0B2E5E",
        legend=False,
    )
    ax.set_title("Precio de bolsa nacional promedio diario")
    ax.set_xlabel("Fecha")
    ax.set_ylabel("COP/kWh")
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.show()
except Exception as exc:
    print(f"No fue posible renderizar los gráficos operativos: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Niveles de embalses: continuidad y variaciones

# COMMAND ----------

niveles_canonicos = (
    seleccionar_version_canonica("niveles_embalses")
    .withColumn("fecha", F.to_date(fecha_evento("niveles_embalses")))
    .withColumn("codigo_planta_norm", F.upper(F.trim("codigo_planta")))
    .withColumn("nivel_kwh", F.expr("try_cast(valor as double)"))
)

ventana_nivel = Window.partitionBy("codigo_planta_norm").orderBy("fecha")
niveles_variacion = (
    niveles_canonicos
    .withColumn("nivel_anterior_kwh", F.lag("nivel_kwh").over(ventana_nivel))
    .withColumn("dias_desde_anterior", F.datediff("fecha", F.lag("fecha").over(ventana_nivel)))
    .withColumn("variacion_kwh", F.col("nivel_kwh") - F.col("nivel_anterior_kwh"))
    .withColumn(
        "variacion_pct",
        F.when(
            F.abs(F.col("nivel_anterior_kwh")) > 0,
            F.col("variacion_kwh") / F.abs(F.col("nivel_anterior_kwh")) * 100
        )
    )
)

display(
    niveles_variacion
    .filter(F.col("nivel_anterior_kwh").isNotNull())
    .orderBy(F.desc(F.abs(F.col("variacion_pct"))))
    .select(
        "fecha", "codigo_planta_norm", "version",
        "nivel_anterior_kwh", "nivel_kwh",
        "dias_desde_anterior", "variacion_kwh", "variacion_pct"
    )
    .limit(100)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 16. Rankings descriptivos

# COMMAND ----------

top_generacion = (
    seleccionar_version_canonica("generacion_real")
    .withColumn("generacion_gwh", F.expr("try_cast(valor as double)") / F.lit(1_000_000))
    .groupBy("codigo_planta", "codigo_sic_agente")
    .agg(
        F.sum("generacion_gwh").alias("generacion_gwh"),
        F.countDistinct(F.to_date(fecha_evento("generacion_real"))).alias("dias_observados"),
    )
    .orderBy(F.desc("generacion_gwh"))
)
display(top_generacion.limit(100))

top_disponibilidad = (
    seleccionar_version_canonica("disponibilidad_plantas")
    .withColumn("disponibilidad_gwh", F.expr("try_cast(valor as double)") / F.lit(1_000_000))
    .groupBy("codigo_planta")
    .agg(
        F.sum("disponibilidad_gwh").alias("disponibilidad_gwh"),
        F.countDistinct(F.to_date(fecha_evento("disponibilidad_plantas"))).alias("dias_observados"),
    )
    .orderBy(F.desc("disponibilidad_gwh"))
)
display(top_disponibilidad.limit(100))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 17. Scorecard automático de candidatos a hallazgo

# COMMAND ----------

hallazgos = []

def agregar_hallazgo(tabla, dimension, hallazgo, evidencia, severidad):
    hallazgos.append({
        "tabla": tabla,
        "dimension": dimension,
        "hallazgo_candidato": hallazgo,
        "evidencia": evidencia,
        "severidad_preliminar": severidad,
    })

for r in validez_tipo_df.collect():
    if r["registros_invalidos"] and r["registros_invalidos"] > 0:
        sev = "ALTA" if (r["pct_invalido"] or 0) >= 1 else "MEDIA"
        agregar_hallazgo(
            r["tabla"], "VALIDEZ",
            f"{r['columna']} contiene valores no convertibles a {r['tipo_objetivo']}",
            f"{r['registros_invalidos']:,} registros ({r['pct_invalido']:.4f} %)",
            sev,
        )

for r in duplicados_df.collect():
    if r["filas_excedentes"] and r["filas_excedentes"] > 0:
        if r["perfil"] in {"conflicto_valor_misma_version", "id_tecnico"}:
            sev = "CRÍTICA"
        elif r["perfil"] == "llave_con_version":
            sev = "ALTA"
        else:
            sev = "MEDIA"
        agregar_hallazgo(
            r["tabla"], "UNICIDAD",
            f"Se detectó {r['perfil'].replace('_', ' ')}",
            f"{r['grupos_duplicados']:,} grupos; {r['filas_excedentes']:,} filas excedentes; máximo {r['max_repeticiones']} repeticiones",
            sev,
        )

for r in dominios_inesperados_df.collect():
    agregar_hallazgo(
        r["tabla"], "VALIDEZ",
        f"Valor no esperado en {r['columna']}: {r['valor']}",
        f"{r['registros']:,} registros; dominio esperado: {r['valores_esperados']}",
        "ALTA",
    )

for r in completitud_df.filter(F.col("pct_faltante") > 0).collect():
    if r["columna"] in AUDITORIA:
        continue
    pct = r["pct_faltante"] or 0
    sev = "ALTA" if pct >= 10 else "MEDIA" if pct >= 1 else "BAJA"
    agregar_hallazgo(
        r["tabla"], "COMPLETITUD",
        f"{r['columna']} presenta nulos o vacíos",
        f"{r['faltantes_total']:,} registros ({pct:.4f} %)",
        sev,
    )

for r in continuidad_df.collect():
    if r["grupos_incompletos"] and r["grupos_incompletos"] > 0:
        agregar_hallazgo(
            r["tabla"], "CONTINUIDAD",
            r["control"].replace("_", " "),
            f"{r['grupos_incompletos']:,} grupos o días incompletos entre {r['fecha_min']} y {r['fecha_max']}",
            "MEDIA",
        )

for r in integridad_df.collect():
    if r["codigos_huerfanos"] and r["codigos_huerfanos"] > 0:
        cobertura = r["cobertura_pct"] or 0
        sev = "ALTA" if cobertura < 95 else "MEDIA"
        agregar_hallazgo(
            r["tabla_hija"], "INTEGRIDAD",
            f"Claves sin correspondencia: {r['relacion']}",
            f"{r['codigos_huerfanos']:,} de {r['codigos_hijos']:,} códigos; cobertura {cobertura:.2f} %",
            sev,
        )

for r in perfil_numerico_df.collect():
    if r["negativos"] and r["negativos"] > 0:
        agregar_hallazgo(
            r["tabla"], "VALIDEZ",
            f"{r['columna']} contiene valores negativos",
            f"{r['negativos']:,} registros ({r['pct_negativos']:.4f} %)",
            "MEDIA",
        )

orden_severidad = F.create_map(
    F.lit("CRÍTICA"), F.lit(1),
    F.lit("ALTA"), F.lit(2),
    F.lit("MEDIA"), F.lit(3),
    F.lit("BAJA"), F.lit(4),
)

if hallazgos:
    hallazgos_df = (
        spark.createDataFrame(hallazgos)
        .withColumn("_orden", F.element_at(orden_severidad, F.col("severidad_preliminar")))
        .orderBy("_orden", "tabla", "dimension")
        .drop("_orden")
    )
    display(hallazgos_df)
else:
    hallazgos_df = spark.createDataFrame(
        [], "tabla string, dimension string, hallazgo_candidato string, "
        "evidencia string, severidad_preliminar string"
    )
    print("No se generaron candidatos automáticos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 18. Controles de razonabilidad revisados
# MAGIC
# MAGIC Los controles se reconciliaron con las salidas ejecutadas:
# MAGIC
# MAGIC - **Inventario:** 9 tablas y 24.590.150 registros; no faltan tablas requeridas.
# MAGIC - **Esquema y validez:** todos los campos numéricos y temporales evaluados convierten correctamente; no se observaron dominios, unidades ni duraciones fuera de lo esperado.
# MAGIC - **Unicidad:** no existen duplicados de payload, de llave con versión, de `id` técnico ni conflictos de valor dentro de una misma versión. La multiplicidad observada corresponde a revisiones `TX`.
# MAGIC - **Continuidad:** todos los grupos entidad-día observados en fuentes horarias contienen 24 periodos y no hay grupos sobrecompletos. La única fecha global faltante es `2026-07-14` en demanda.
# MAGIC - **Comparabilidad temporal:** la comparación completa entre generación, demanda y disponibilidad se limita a 194 días, desde `2026-01-01` hasta `2026-07-13`.
# MAGIC - **Integridad:** los agentes tienen cobertura referencial de 100 %, pero la cobertura del maestro de plantas es solo 73,69 % para generación y 72,90 % para disponibilidad.
# MAGIC - **Coordenadas:** 13 pares faltantes; las 19 coordenadas disponibles pasan los controles de rango.
# MAGIC - **Outliers:** los límites IQR se interpretan como señal exploratoria y no como regla de rechazo, porque las medidas mezclan plantas y agentes de escalas muy distintas y contienen muchos ceros operativos.
# MAGIC - **Alertas automáticas reinterpretadas:** las longitudes negativas son correctas en Colombia y las múltiples versiones `TX` no son duplicados defectuosos.
# MAGIC - **Capacidad:** el análisis horario utilizó la capacidad más reciente de cada planta para todo el historial. Por ello, los excesos frente a capacidad son candidatos de investigación y deben reconfirmarse con una unión temporal por fecha.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 19. Conclusiones generales
# MAGIC
# MAGIC ### Estado general de Bronze
# MAGIC
# MAGIC La capa Bronze es **técnicamente confiable para conservar el dato crudo y alimentar transformaciones controladas**, pero es **apta con condiciones** para análisis. La mayor parte de los problemas no está en el formato ni en la ingestión: está en la resolución de versiones, la conformación de dimensiones, la sincronización temporal y algunas reglas de negocio.
# MAGIC
# MAGIC | Dimensión | Evidencia observada | Evaluación |
# MAGIC |---|---|---|
# MAGIC | Estructura y tipos | 0 conversiones inválidas en fechas y medidas evaluadas | Fuerte |
# MAGIC | Dominios | 0 valores inesperados en duraciones, variables, unidades, mercados y versiones | Fuerte |
# MAGIC | Unicidad técnica | 0 duplicados exactos, de llave con versión, de `id` y 0 conflictos de valor en la misma versión | Fuerte |
# MAGIC | Continuidad horaria | 24 periodos en todos los grupos entidad-día observados; 0 grupos incompletos o sobrecompletos | Fuerte |
# MAGIC | Continuidad global | 1 día faltante en demanda: 2026-07-14 | Riesgo medio |
# MAGIC | Integridad planta | 161 códigos huérfanos en generación y 168 en disponibilidad | Riesgo alto |
# MAGIC | Integridad agente | 100 % de cobertura en demanda, generación y plantas | Fuerte |
# MAGIC | Geografía de embalses | 13 de 32 sin coordenadas | Riesgo alto para mapas |
# MAGIC | Frescura comparativa | último corte común de los tres hechos operativos: 2026-07-13 | Riesgo alto para lectura “actual” |
# MAGIC | Coherencia operativa | anomalías minoritarias, pero concentradas en plantas específicas | Requiere investigación |
# MAGIC
# MAGIC ### Principales fortalezas
# MAGIC
# MAGIC 1. **La ingestión no introdujo duplicación técnica.** Esto permite distinguir claramente la historia de revisiones de un error de reingesta.
# MAGIC 2. **Los identificadores están limpios:** no tienen nulos, vacíos, espacios externos ni problemas de mayúsculas; los códigos de plantas y agentes conservan longitud de cuatro caracteres.
# MAGIC 3. **Los dominios son estables.** Las fuentes usan las variables, unidades, duraciones y mercados esperados.
# MAGIC 4. **La continuidad interna es alta.** Cuando una entidad aparece en un día horario, conserva sus 24 observaciones.
# MAGIC 5. **La relación con agentes es completa.** Los códigos de agente de demanda, generación y plantas encuentran correspondencia en el maestro.
# MAGIC
# MAGIC ### Principales riesgos
# MAGIC
# MAGIC #### 1. Las versiones `TX` deben resolverse antes de cualquier agregación
# MAGIC
# MAGIC La multiplicidad entre versiones es masiva pero esperada. El dato canónico representa entre 21,80 % y 30,75 % del volumen crudo de los cinco hechos versionados. Sumar Bronze sin resolver versiones sobrestimaría los resultados aproximadamente entre 3,25 y 4,59 veces.
# MAGIC
# MAGIC Además, las revisiones no son siempre cosméticas:
# MAGIC
# MAGIC - Demanda: cambio p95 de 0,3200 % y rango máximo de 1.047.744,88 kWh.
# MAGIC - Precio: cambio mediano de 1,0375 %, p95 de 8,5109 % y rango máximo de 774,851 COP/kWh.
# MAGIC - Generación y disponibilidad: p95 igual a cero, pero con casos aislados de hasta 212.593,99 kWh y 376.000 kWh.
# MAGIC - Niveles de embalses: las versiones observadas mantienen el mismo valor para cada llave.
# MAGIC
# MAGIC Por tanto, Silver debe conservar trazabilidad de la versión recibida y publicar una sola fila vigente por llave canónica.
# MAGIC
# MAGIC #### 2. El maestro de plantas no conforma por sí solo todos los recursos operativos
# MAGIC
# MAGIC La cobertura por código es 73,69 % en generación y 72,90 % en disponibilidad. Esto no parece un problema de formato porque los códigos son válidos y normalizados; apunta a una diferencia de alcance entre el maestro y los hechos, recursos retirados/nuevos, interconexiones o códigos que requieren otra tabla de referencia.
# MAGIC
# MAGIC Hasta resolverlo, ningún hecho debe unirse a `plantas` mediante `INNER JOIN`. Se necesita una dimensión conformada que preserve todos los códigos y marque explícitamente los no clasificados.
# MAGIC
# MAGIC #### 3. Las fuentes tienen cortes diferentes
# MAGIC
# MAGIC Al 23 de julio de 2026:
# MAGIC
# MAGIC | Fuente | Última fecha del evento | Rezago frente al 23-jul |
# MAGIC |---|---:|---:|
# MAGIC | Agentes | 2026-07-22 | 1 día |
# MAGIC | Plantas | 2026-07-22 | 1 día |
# MAGIC | Precio de bolsa | 2026-07-19 | 4 días |
# MAGIC | Niveles de embalses | 2026-07-19 | 4 días |
# MAGIC | Demanda real | 2026-07-17 | 6 días |
# MAGIC | Disponibilidad | 2026-07-17 | 6 días |
# MAGIC | Generación real | 2026-07-13 | 10 días |
# MAGIC
# MAGIC Esto no prueba por sí solo una falla de carga, porque SIMEM publica fuentes con latencias distintas. Sí implica que cualquier visual combinado debe mostrar su fecha de corte y trabajar con la mínima fecha máxima común.
# MAGIC
# MAGIC #### 4. Existen señales operativas concentradas, no un problema generalizado
# MAGIC
# MAGIC En 2.550.480 combinaciones planta-hora comparables:
# MAGIC
# MAGIC - 13.176 horas (0,5166 %) presentan generación superior a disponibilidad por más de 5 %.
# MAGIC - 2.873 horas (0,1126 %) tienen disponibilidad cero con generación positiva.
# MAGIC - La mediana del cociente generación/disponibilidad es 1,0000 y el p95 es 1,000097; las excepciones se concentran en pocas plantas.
# MAGIC
# MAGIC Frente a la capacidad más reciente del maestro, 33.094 de 2.000.424 horas (1,6543 %) superan la capacidad por más de 5 %. `TYP1` domina los valores extremos visibles, con un factor máximo de 2,3775. Este resultado debe reconfirmarse usando la capacidad vigente en la fecha del evento; todavía no debe clasificarse como error confirmado.
# MAGIC
# MAGIC ### Lectura descriptiva del sistema
# MAGIC
# MAGIC En los 194 días comparables entre `2026-01-01` y `2026-07-13`:
# MAGIC
# MAGIC - Generación media: **240,74 GWh/día**.
# MAGIC - Demanda media: **234,90 GWh/día**.
# MAGIC - Disponibilidad media: **404,47 GWh/día**.
# MAGIC - Diferencia media generación–demanda: **5,83 GWh/día**, equivalente al 2,48 % de la demanda media.
# MAGIC - Utilización media de disponibilidad: **59,72 %**, con rango diario entre 48,00 % y 69,88 %.
# MAGIC
# MAGIC La generación agregada supera a la demanda agregada en los 194 días. No debe interpretarse automáticamente como “excedente”: antes hay que confirmar que ambas fuentes tengan el mismo alcance e incluir pérdidas, exportaciones, autoconsumo y demás componentes del balance.
# MAGIC
# MAGIC ### Implicaciones para Silver y Gold
# MAGIC
# MAGIC - **Silver:** resolver versiones, tipar columnas, conservar auditoría, implementar dimensiones conformadas y aplicar uniones temporales.
# MAGIC - **Gold:** consumir solo filas canónicas, usar fecha de corte común y mantener miembros `DESCONOCIDO/NO CLASIFICADO` para no perder hechos.
# MAGIC - **Dashboard:** mostrar fecha de actualización por fuente; los KPI combinados deben quedar vacíos fuera de la ventana común, no completar con cero.
# MAGIC - **Modelado histórico:** tratar `agentes` y `plantas` como snapshots susceptibles de cambios de nombre, atributos y capacidad.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 20. Conclusiones por fuente
# MAGIC
# MAGIC ### Agentes
# MAGIC
# MAGIC - Contiene **73.570 registros**, con cobertura de eventos desde `2026-01-01` hasta `2026-07-22`.
# MAGIC - No presenta nulos, dominios inesperados, duplicados ni problemas de formato en `codigo_sic_agente`.
# MAGIC - Las cuatro actividades observadas son Comercializador (42,999 % de las filas), Generador (42,357 %), Distribuidor (10,506 %) y Transportador (4,139 %).
# MAGIC - Cinco códigos muestran más de un nombre normalizado: `NESC`, `BTGC`, `SOEG`, `SOEC` y `TEMG`. En algunos casos hay cambio societario; en `TEMG` parece una variación de puntuación.
# MAGIC - **Conclusión:** fuente apta como dimensión histórica, pero no conviene sobrescribir el nombre. Se requiere SCD tipo 2 o, como mínimo, vigencia por fecha y tabla de alias.
# MAGIC
# MAGIC ### Demanda real
# MAGIC
# MAGIC - Contiene **1.423.752 filas crudas** y **437.856 llaves canónicas**.
# MAGIC - Cubre `2026-01-01` a `2026-07-17`; falta globalmente **2026-07-14**. Fuera de ese día, todos los grupos observados tienen 24 horas.
# MAGIC - Solo aparecen `TX2`, `TX3`, `TXR` y `TXF`; la ausencia de `TX1` es propia de esta extracción y no genera valores fuera del dominio permitido.
# MAGIC - No hay negativos ni conversiones inválidas; 23.004 filas crudas son cero (1,6157 %).
# MAGIC - Las revisiones son generalmente pequeñas, pero existen cambios extremos en `BEIC` y `CSIC`; el rango máximo alcanza 1.047.744,88 kWh.
# MAGIC - En los 197 días con demanda, el mercado Regulado representa **69,05 %** y el No Regulado **30,95 %** de la energía agregada.
# MAGIC - **Conclusión:** apta después de seleccionar versión canónica. Debe investigarse o documentarse el faltante del 14 de julio y monitorear las revisiones grandes por agente.
# MAGIC
# MAGIC ### Disponibilidad de plantas
# MAGIC
# MAGIC - Contiene **11.028.576 filas crudas** y **2.609.784 llaves canónicas**.
# MAGIC - Cubre sin días globales faltantes desde `2026-01-01` hasta `2026-07-17`; todos los grupos entidad-día observados tienen 24 horas.
# MAGIC - Los valores convierten correctamente y no hay negativos. Los ceros representan 42,7899 % del histórico crudo; por el tipo de variable, son estados operativos posibles y no deben rechazarse sin contexto.
# MAGIC - Hay **168 códigos huérfanos de 620 (27,10 %)** frente al maestro de plantas.
# MAGIC - Las revisiones suelen conservar el valor (p95 de cambio igual a cero), aunque existen casos puntuales de hasta 376.000 kWh.
# MAGIC - **Conclusión:** temporalmente sólida, pero condicionada por la integridad con plantas. Gold debe preservar recursos no clasificados y no perderlos en la unión.
# MAGIC
# MAGIC ### Embalses
# MAGIC
# MAGIC - El maestro contiene **32 embalses**, sin duplicados ni problemas en los códigos.
# MAGIC - Faltan latitud y longitud en **13 embalses (40,625 %)**: `SALVAJIN`, `PUNCHINA`, `MIEL1`, `SNRAFAEL`, `ELQUIMBO`, `CALIMA1`, `QUBRADON`, `MUNA`, `PENOL`, `MIRAFLOR`, `URRA1`, `NEUSA` y `TOMINE`.
# MAGIC - Las 19 coordenadas presentes convierten correctamente, cumplen rangos geográficos y están dentro de la caja aproximada de Colombia.
# MAGIC - Las longitudes negativas son correctas para Colombia y no constituyen una anomalía.
# MAGIC - **Conclusión:** apta para identificación, no apta todavía para cobertura geográfica completa. La geocodificación debe resolverse antes de publicar mapas o cálculos espaciales.
# MAGIC
# MAGIC ### Generación real
# MAGIC
# MAGIC - Contiene **11.716.152 filas crudas** y **2.553.576 llaves canónicas**.
# MAGIC - Cubre de forma continua `2026-01-01` a `2026-07-13`; todos los grupos observados tienen 24 horas.
# MAGIC - No hay negativos ni conversiones inválidas. Los ceros representan 46,9147 % del histórico crudo y son compatibles con horas sin generación.
# MAGIC - La cobertura de agentes es 100 %, pero **161 de 612 códigos de planta (26,31 %)** no aparecen en el maestro.
# MAGIC - El 99,8788 % de las llaves canónicas planta-hora encuentra disponibilidad comparable.
# MAGIC - Las anomalías generación/disponibilidad se concentran principalmente en `PSUA`, `ESMR`, `HMIN`, `CUC1`, `TYP1`, `MOY1` y `CLL1`. `PSUA` registra 1.435 horas con disponibilidad cero y generación positiva.
# MAGIC - Las plantas con mayor generación acumulada en el periodo son `PES1` (4.971,11 GWh), `SNCR` (3.294,63 GWh), `SOG1` (3.064,97 GWh) y `GVIO` (2.847,95 GWh).
# MAGIC - **Conclusión:** fuente fuerte en continuidad y formato, pero condicionada por su fecha de corte, los códigos sin maestro y anomalías focalizadas que requieren trazabilidad por planta y versión.
# MAGIC
# MAGIC ### Niveles de embalses
# MAGIC
# MAGIC - Contiene **19.026 filas crudas** y **4.200 llaves canónicas**, equivalentes a 21 códigos por 200 días.
# MAGIC - Cubre sin días faltantes desde `2026-01-01` hasta `2026-07-19`; los 21 códigos tienen correspondencia en plantas.
# MAGIC - Las cinco versiones disponibles repiten el mismo valor por llave: el rango de revisión es cero.
# MAGIC - Hay cinco ceros en las filas crudas, correspondientes a la repetición multiversión de un evento canónico; el caso visible es `ALBG` el `2026-02-01`.
# MAGIC - Las mayores variaciones porcentuales diarias se concentran en bases pequeñas de `ALBG` y `SNCR`; por ello deben evaluarse junto con la variación absoluta y no solo con el porcentaje.
# MAGIC - **Conclusión:** fuente completa y estable. Conviene validar el cero canónico y usar umbrales por embalse o por escala, no un único umbral porcentual global.
# MAGIC
# MAGIC ### Plantas
# MAGIC
# MAGIC - Contiene **268.107 snapshots** desde `2026-01-01` hasta `2026-07-22`.
# MAGIC - No presenta nulos ni valores no convertibles. Existen 406 filas con capacidad efectiva neta igual a cero (0,1514 %), que deben excluirse del denominador del factor de capacidad y clasificarse según su estado operativo.
# MAGIC - En el histórico de snapshots, Solar representa 81,724 % de las filas, No Despachado Centralmente 93,785 % y Autogenerador de Pequeña Escala 69,641 %. Estos porcentajes describen filas históricas, no necesariamente el inventario vigente en una fecha específica.
# MAGIC - Cinco códigos cambian de nombre normalizado: `ARG1`, `2S6Q`, `5FPL`, `5GFB` y `5QTJ`. Algunos cambios parecen reasignaciones reales de código, no simples variaciones ortográficas.
# MAGIC - **Conclusión:** debe modelarse como dimensión histórica por vigencia. Para cálculos de capacidad se requiere una unión `as-of` por planta y fecha, no el último snapshot aplicado a todo el pasado.
# MAGIC
# MAGIC ### Relaciones planta–reservorio
# MAGIC
# MAGIC - Contiene **23 relaciones**, sin nulos ni duplicados.
# MAGIC - Una de 20 plantas normalizadas no hace `match` exacto: `SOGAMOSOS`.
# MAGIC - Cinco de 23 nombres de reservorio no hacen `match` exacto: `TOPOROCO`, `PORCE III`, `URRA1`, `PORCE II` y `CALIMA 1`.
# MAGIC - **Conclusión:** la relación es pequeña y controlable, pero necesita un crosswalk explícito. No conviene resolver estas diferencias con coincidencia difusa durante cada carga.
# MAGIC
# MAGIC ### Precio de bolsa
# MAGIC
# MAGIC - Contiene **60.912 filas crudas** y **14.400 llaves canónicas**, exactamente 3 variables × 24 horas × 200 días.
# MAGIC - Cubre de forma continua `2026-01-01` a `2026-07-19`; todos los días y variables tienen 24 horas.
# MAGIC - No hay nulos, ceros, negativos, conversiones inválidas ni dominios inesperados. El rango crudo observado es 88,19 a 1.581,038 COP/kWh.
# MAGIC - Es la fuente con revisiones relativas más materiales: cambio mediano de 1,0375 %, p95 de 8,5109 % y rango máximo de 774,851 COP/kWh.
# MAGIC - En los 133 días comparables visibles en la salida diaria, `PB_Int` y `PB_Tie` tienen exactamente el mismo promedio, mínimo y máximo diario. Debe confirmarse si esta igualdad responde a la definición de SIMEM antes de tratarlas como variables redundantes.
# MAGIC - **Conclusión:** fuente de alta calidad estructural; la selección de versión es indispensable para evitar publicar precios provisionales como definitivos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 21. Recomendaciones y pruebas automatizables
# MAGIC
# MAGIC ### Plan de acción priorizado
# MAGIC
# MAGIC | Prioridad | Acción | Motivo y resultado esperado |
# MAGIC |---|---|---|
# MAGIC | P0 | Publicar una sola fila canónica por llave con prioridad `TXF > TXR > TX3 > TX2 > TX1` y desempate por ingestión | Evita multiplicar medidas y conserva el valor más definitivo disponible |
# MAGIC | P0 | Construir una dimensión conformada de plantas a partir del maestro y la unión de códigos observados en los hechos | Impide perder 161 códigos de generación y 168 de disponibilidad |
# MAGIC | P0 | Sustituir uniones internas por `LEFT JOIN` y miembro `DESCONOCIDO/NO CLASIFICADO` mientras se resuelven los códigos | Preserva el grano y el volumen de los hechos |
# MAGIC | P0 | Unir capacidad y atributos de planta por vigencia de fecha (`as-of`) | Revalida correctamente los 33.094 excesos frente a capacidad |
# MAGIC | P0 | Calcular y exponer una fecha de corte común por producto analítico | Evita comparar generación ausente con demanda o disponibilidad más recientes |
# MAGIC | P1 | Investigar la ausencia de demanda del `2026-07-14` y el corte de generación en `2026-07-13` | Distingue latencia esperada, hueco de extracción o publicación incompleta |
# MAGIC | P1 | Completar las 13 coordenadas de embalses con fuente y fecha de enriquecimiento | Habilita mapas con cobertura total y trazabilidad |
# MAGIC | P1 | Crear crosswalk controlado para planta–reservorio y nombres históricos | Resuelve 1 planta y 5 reservorios sin coincidencia exacta |
# MAGIC | P1 | Crear una tabla de anomalías operativas por planta, hora, versión y regla | Permite revisar los casos sin contaminar el hecho principal |
# MAGIC | P2 | Modelar agentes y plantas como SCD tipo 2 | Conserva cambios societarios, reasignaciones, capacidad y clasificación |
# MAGIC | P2 | Confirmar con la documentación de SIMEM la igualdad `PB_Int = PB_Tie` y el significado de los ceros NEM | Evita deduplicar o corregir valores legítimos |
# MAGIC
# MAGIC ### Pruebas automatizables recomendadas
# MAGIC
# MAGIC | Control | Regla propuesta | Aplicación |
# MAGIC |---|---|---|
# MAGIC | Llaves obligatorias | 0 nulos o vacíos en fechas, códigos, variable, duración y versión | Todas las tablas según su grano |
# MAGIC | Conversión | 100 % de éxito en `try_cast` de fecha, valor, capacidad y coordenadas no nulas | Bronze → Silver |
# MAGIC | Unicidad | 0 filas excedentes en la llave **incluyendo versión** | Hechos versionados |
# MAGIC | Conflicto | 1 valor como máximo por llave y versión | Hechos versionados |
# MAGIC | Dominio | Solo variables, unidades, duraciones, mercados y versiones permitidas | Todas las fuentes |
# MAGIC | Canonización | Exactamente 1 fila por llave canónica después del ranking de versión | Silver |
# MAGIC | Cobertura horaria | 24 horas por entidad-día; excepciones en tabla controlada | Demanda, generación, disponibilidad y precio |
# MAGIC | Continuidad diaria | 0 fechas globales faltantes dentro del rango publicado | Todos los hechos; investigar el 14-jul en demanda |
# MAGIC | Frescura | Umbral específico por fuente, acordado con la latencia oficial | Jobs y dashboard |
# MAGIC | Integridad agente | Cobertura de 100 % o miembro desconocido explícito | Demanda, generación y plantas |
# MAGIC | Integridad planta | No permitir pérdida de filas; medir cobertura y reducir huérfanos desde la línea base | Generación, disponibilidad y niveles |
# MAGIC | Coordenadas | 100 % de pares completos y dentro de Colombia; fuente de geocodificación registrada | Embalses |
# MAGIC | Capacidad | Capacidad positiva para calcular factor; unión vigente por fecha | Generación |
# MAGIC | Anomalía operativa | Monitorear generación > disponibilidad × 1,05 y generación > capacidad × 1,05 | Gold de calidad |
# MAGIC | Revisión TX | Monitorear p95 y máximo de cambio por fuente y periodo | Todos los hechos versionados |
# MAGIC
# MAGIC ### Severidad final de los hallazgos
# MAGIC
# MAGIC | Hallazgo | Severidad | Confianza | Impacto principal |
# MAGIC |---|---|---|---|
# MAGIC | Códigos de planta sin maestro | Alta | Alta | Pérdida de hechos y KPI sesgados en joins |
# MAGIC | Coordenadas faltantes en 13 embalses | Alta para geografía | Alta | Mapas y análisis espaciales incompletos |
# MAGIC | Cortes temporales desalineados | Alta para dashboard operativo | Alta | Comparaciones parciales o engañosas |
# MAGIC | Día faltante de demanda 2026-07-14 | Media | Alta | Hueco diario y series discontinuas |
# MAGIC | Revisiones TX materiales | Alta como requisito de transformación, no como defecto | Alta | Doble conteo o publicación de valores provisionales |
# MAGIC | Generación superior a disponibilidad | Media | Alta en detección, media en causa | Alertas focalizadas por planta |
# MAGIC | Generación superior a capacidad | Media provisional | Media | Requiere unión temporal antes de confirmar |
# MAGIC | Cambios de nombre por código | Media | Alta | Historia dimensional y etiquetas inconsistentes |
# MAGIC | Nombres planta–reservorio sin match | Media | Alta | Relaciones incompletas sin crosswalk |
# MAGIC
# MAGIC ### Reglas que deben documentarse, no bloquear la carga
# MAGIC
# MAGIC - Longitudes negativas de Colombia.
# MAGIC - Varias versiones `TX` de una misma llave canónica.
# MAGIC - Ceros en generación y disponibilidad cuando representan estados operativos válidos.
# MAGIC - Outliers IQR en medidas heterogéneas; primero deben segmentarse por planta, agente, tecnología y versión.
# MAGIC - Diferencias entre generación y demanda hasta confirmar el alcance exacto del balance energético.
# MAGIC
# MAGIC ### Preguntas abiertas para cerrar con la fuente
# MAGIC
# MAGIC 1. ¿Los códigos de planta huérfanos corresponden a recursos retirados, generación distribuida, interconexiones u otra taxonomía de SIMEM?
# MAGIC 2. ¿Cuál es la latencia oficial esperada para generación, disponibilidad, demanda, precio y NEM?
# MAGIC 3. ¿La capacidad efectiva neta debe aplicarse por fecha del snapshot y está expresada en kW para todos los recursos?
# MAGIC 4. ¿La igualdad observada entre `PB_Int` y `PB_Tie` es una regla de negocio o una redundancia de la fuente?
# MAGIC 5. ¿El cero de `ALBG` el `2026-02-01` representa un nivel válido o un valor centinela?
# MAGIC
# MAGIC ### Dictamen
# MAGIC
# MAGIC **Bronze queda aprobada como capa de conservación y trazabilidad. Silver queda aprobada de forma condicionada a las acciones P0. Gold y el dashboard deben consumir exclusivamente datos canónicos, temporalmente alineados y con dimensiones conformadas.**
