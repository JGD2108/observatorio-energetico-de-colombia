# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics — Vistas para Dashboard
# MAGIC
# MAGIC Este notebook crea la capa semántica del Observatorio Energético en:
# MAGIC
# MAGIC `<catalogo>.gold_analytics`
# MAGIC
# MAGIC Debe ejecutarse después de que `quality_check` apruebe Gold.
# MAGIC
# MAGIC Las vistas se construyen únicamente sobre las tablas Gold validadas.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración y validación de tablas

# COMMAND ----------

from pyspark.sql import functions as F
import sys

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from config.project_config import ANALYTICS_SCHEMA, CATALOG, SCHEMAS  # noqa: E402
GOLD_SCHEMA = f"{CATALOG}.{SCHEMAS['gold']}"
spark.sql(f"USE CATALOG `{CATALOG}`")

spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS_SCHEMA}"
)

required_tables = [
    f"{GOLD_SCHEMA}.dim_fecha",
    f"{GOLD_SCHEMA}.dim_periodo",
    f"{GOLD_SCHEMA}.dim_agente",
    f"{GOLD_SCHEMA}.dim_planta",
    f"{GOLD_SCHEMA}.dim_embalse",
    f"{GOLD_SCHEMA}.bridge_planta_embalse",
    f"{GOLD_SCHEMA}.fact_generacion_real",
    f"{GOLD_SCHEMA}.fact_disponibilidad_planta",
    f"{GOLD_SCHEMA}.fact_demanda_real",
    f"{GOLD_SCHEMA}.fact_precio_bolsa",
    f"{GOLD_SCHEMA}.fact_energia_embalsada_planta",
]

missing_tables = [
    table_name
    for table_name in required_tables
    if not spark.catalog.tableExists(table_name)
]

print("Esquema analítico:", ANALYTICS_SCHEMA)
print("Tablas faltantes:", missing_tables)

if missing_tables:
    raise ValueError(
        f"Faltan tablas Gold requeridas: {missing_tables}"
    )

print("Todas las tablas Gold requeridas están disponibles.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Vista horaria del sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_sistema_horario AS
# MAGIC
# MAGIC WITH horas AS (
# MAGIC     SELECT fecha_hora
# MAGIC     FROM gold.fact_generacion_real
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT fecha_hora
# MAGIC     FROM gold.fact_disponibilidad_planta
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT fecha_hora
# MAGIC     FROM gold.fact_demanda_real
# MAGIC
# MAGIC     UNION
# MAGIC
# MAGIC     SELECT fecha_hora
# MAGIC     FROM gold.fact_precio_bolsa
# MAGIC ),
# MAGIC
# MAGIC generacion AS (
# MAGIC     SELECT
# MAGIC         fecha_hora,
# MAGIC         MAX(fecha_key) AS fecha_key,
# MAGIC         MAX(periodo_key) AS periodo_key,
# MAGIC         SUM(generacion_real_kwh) AS generacion_real_kwh
# MAGIC     FROM gold.fact_generacion_real
# MAGIC     GROUP BY fecha_hora
# MAGIC ),
# MAGIC
# MAGIC disponibilidad AS (
# MAGIC     SELECT
# MAGIC         fecha_hora,
# MAGIC         MAX(fecha_key) AS fecha_key,
# MAGIC         MAX(periodo_key) AS periodo_key,
# MAGIC         SUM(disponibilidad_real_kwh) AS disponibilidad_real_kwh
# MAGIC     FROM gold.fact_disponibilidad_planta
# MAGIC     GROUP BY fecha_hora
# MAGIC ),
# MAGIC
# MAGIC demanda AS (
# MAGIC     SELECT
# MAGIC         fecha_hora,
# MAGIC         MAX(fecha_key) AS fecha_key,
# MAGIC         MAX(periodo_key) AS periodo_key,
# MAGIC         SUM(demanda_real_kwh) AS demanda_total_kwh,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN UPPER(TRIM(tipo_mercado)) = 'REGULADO'
# MAGIC                 THEN demanda_real_kwh
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS demanda_regulada_kwh,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN UPPER(TRIM(tipo_mercado)) IN (
# MAGIC                     'NO REGULADO',
# MAGIC                     'NO_REGULADO'
# MAGIC                 )
# MAGIC                 THEN demanda_real_kwh
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS demanda_no_regulada_kwh
# MAGIC
# MAGIC     FROM gold.fact_demanda_real
# MAGIC     GROUP BY fecha_hora
# MAGIC ),
# MAGIC
# MAGIC precio AS (
# MAGIC     SELECT
# MAGIC         fecha_hora,
# MAGIC         MAX(fecha_key) AS fecha_key,
# MAGIC         MAX(periodo_key) AS periodo_key,
# MAGIC         MAX(precio_bolsa_nacional_cop_kwh)
# MAGIC             AS precio_bolsa_nacional_cop_kwh,
# MAGIC         MAX(precio_bolsa_internacional_cop_kwh)
# MAGIC             AS precio_bolsa_internacional_cop_kwh,
# MAGIC         MAX(precio_bolsa_tie_cop_kwh)
# MAGIC             AS precio_bolsa_tie_cop_kwh
# MAGIC     FROM gold.fact_precio_bolsa
# MAGIC     GROUP BY fecha_hora
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     COALESCE(
# MAGIC         g.fecha_key,
# MAGIC         d.fecha_key,
# MAGIC         disp.fecha_key,
# MAGIC         p.fecha_key,
# MAGIC         CAST(DATE_FORMAT(h.fecha_hora, 'yyyyMMdd') AS INT)
# MAGIC     ) AS fecha_key,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         g.periodo_key,
# MAGIC         d.periodo_key,
# MAGIC         disp.periodo_key,
# MAGIC         p.periodo_key,
# MAGIC         CAST(HOUR(h.fecha_hora) + 1 AS TINYINT)
# MAGIC     ) AS periodo_key,
# MAGIC
# MAGIC     h.fecha_hora,
# MAGIC     CAST(h.fecha_hora AS DATE) AS fecha,
# MAGIC     YEAR(h.fecha_hora) AS anio,
# MAGIC     MONTH(h.fecha_hora) AS mes_numero,
# MAGIC     HOUR(h.fecha_hora) + 1 AS periodo,
# MAGIC
# MAGIC     g.generacion_real_kwh,
# MAGIC     d.demanda_total_kwh,
# MAGIC     d.demanda_regulada_kwh,
# MAGIC     d.demanda_no_regulada_kwh,
# MAGIC     disp.disponibilidad_real_kwh,
# MAGIC
# MAGIC     p.precio_bolsa_nacional_cop_kwh,
# MAGIC     p.precio_bolsa_internacional_cop_kwh,
# MAGIC     p.precio_bolsa_tie_cop_kwh,
# MAGIC
# MAGIC     g.generacion_real_kwh
# MAGIC         - d.demanda_total_kwh
# MAGIC         AS balance_generacion_demanda_kwh,
# MAGIC
# MAGIC     disp.disponibilidad_real_kwh
# MAGIC         - g.generacion_real_kwh
# MAGIC         AS margen_disponibilidad_kwh,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN disp.disponibilidad_real_kwh > 0
# MAGIC         THEN
# MAGIC             100.0
# MAGIC             * g.generacion_real_kwh
# MAGIC             / disp.disponibilidad_real_kwh
# MAGIC     END AS utilizacion_disponibilidad_pct,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN d.demanda_total_kwh > 0
# MAGIC         THEN
# MAGIC             100.0
# MAGIC             * g.generacion_real_kwh
# MAGIC             / d.demanda_total_kwh
# MAGIC     END AS relacion_generacion_demanda_pct,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN d.demanda_total_kwh IS NOT NULL
# MAGIC          AND p.precio_bolsa_nacional_cop_kwh IS NOT NULL
# MAGIC         THEN
# MAGIC             d.demanda_total_kwh
# MAGIC             * p.precio_bolsa_nacional_cop_kwh
# MAGIC     END AS valor_referencia_bolsa_cop
# MAGIC
# MAGIC FROM horas h
# MAGIC LEFT JOIN generacion g
# MAGIC     ON h.fecha_hora = g.fecha_hora
# MAGIC LEFT JOIN demanda d
# MAGIC     ON h.fecha_hora = d.fecha_hora
# MAGIC LEFT JOIN disponibilidad disp
# MAGIC     ON h.fecha_hora = disp.fecha_hora
# MAGIC LEFT JOIN precio p
# MAGIC     ON h.fecha_hora = p.fecha_hora

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Resumen diario del sistema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_resumen_diario_sistema AS
# MAGIC
# MAGIC SELECT
# MAGIC     fecha_key,
# MAGIC     fecha,
# MAGIC     MAX(anio) AS anio,
# MAGIC     MAX(mes_numero) AS mes_numero,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(generacion_real_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS generacion_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(demanda_total_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS demanda_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(demanda_regulada_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS demanda_regulada_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(demanda_no_regulada_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS demanda_no_regulada_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(disponibilidad_real_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS disponibilidad_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(balance_generacion_demanda_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS balance_generacion_demanda_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(margen_disponibilidad_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS margen_disponibilidad_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0
# MAGIC         * SUM(generacion_real_kwh)
# MAGIC         / CASE
# MAGIC             WHEN SUM(disponibilidad_real_kwh) = 0
# MAGIC             THEN NULL
# MAGIC             ELSE SUM(disponibilidad_real_kwh)
# MAGIC           END,
# MAGIC         2
# MAGIC     ) AS utilizacion_disponibilidad_pct,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(precio_bolsa_nacional_cop_kwh),
# MAGIC         2
# MAGIC     ) AS precio_nacional_promedio_cop_kwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         MIN(precio_bolsa_nacional_cop_kwh),
# MAGIC         2
# MAGIC     ) AS precio_nacional_minimo_cop_kwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         MAX(precio_bolsa_nacional_cop_kwh),
# MAGIC         2
# MAGIC     ) AS precio_nacional_maximo_cop_kwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(precio_bolsa_internacional_cop_kwh),
# MAGIC         2
# MAGIC     ) AS precio_internacional_promedio_cop_kwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(precio_bolsa_tie_cop_kwh),
# MAGIC         2
# MAGIC     ) AS precio_tie_promedio_cop_kwh,
# MAGIC
# MAGIC     COUNT(DISTINCT fecha_hora) AS horas_con_datos
# MAGIC
# MAGIC FROM gold_analytics.vw_sistema_horario
# MAGIC
# MAGIC GROUP BY
# MAGIC     fecha_key,
# MAGIC     fecha

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estado de actualización de fuentes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_actualizacion_fuentes AS
# MAGIC
# MAGIC WITH cobertura AS (
# MAGIC     SELECT
# MAGIC         'Generación real' AS fuente,
# MAGIC         MIN(CAST(fecha_hora AS DATE)) AS fecha_minima,
# MAGIC         MAX(CAST(fecha_hora AS DATE)) AS fecha_maxima,
# MAGIC         COUNT(DISTINCT CAST(fecha_hora AS DATE))
# MAGIC             AS dias_disponibles,
# MAGIC         COUNT(*) AS registros
# MAGIC     FROM gold.fact_generacion_real
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'Demanda real',
# MAGIC         MIN(CAST(fecha_hora AS DATE)),
# MAGIC         MAX(CAST(fecha_hora AS DATE)),
# MAGIC         COUNT(DISTINCT CAST(fecha_hora AS DATE)),
# MAGIC         COUNT(*)
# MAGIC     FROM gold.fact_demanda_real
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'Disponibilidad de plantas',
# MAGIC         MIN(CAST(fecha_hora AS DATE)),
# MAGIC         MAX(CAST(fecha_hora AS DATE)),
# MAGIC         COUNT(DISTINCT CAST(fecha_hora AS DATE)),
# MAGIC         COUNT(*)
# MAGIC     FROM gold.fact_disponibilidad_planta
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'Precio de bolsa',
# MAGIC         MIN(CAST(fecha_hora AS DATE)),
# MAGIC         MAX(CAST(fecha_hora AS DATE)),
# MAGIC         COUNT(DISTINCT CAST(fecha_hora AS DATE)),
# MAGIC         COUNT(*)
# MAGIC     FROM gold.fact_precio_bolsa
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     SELECT
# MAGIC         'Energía embalsada',
# MAGIC         MIN(fecha_medicion),
# MAGIC         MAX(fecha_medicion),
# MAGIC         COUNT(DISTINCT fecha_medicion),
# MAGIC         COUNT(*)
# MAGIC     FROM gold.fact_energia_embalsada_planta
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     fuente,
# MAGIC     fecha_minima,
# MAGIC     fecha_maxima,
# MAGIC     dias_disponibles,
# MAGIC     registros,
# MAGIC     DATEDIFF(CURRENT_DATE(), fecha_maxima) AS dias_rezago,
# MAGIC     CURRENT_TIMESTAMP() AS fecha_consulta
# MAGIC FROM cobertura

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Demanda diaria por mercado

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_demanda_diaria_mercado AS
# MAGIC
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         demanda.fecha_key,
# MAGIC         CAST(demanda.fecha_hora AS DATE) AS fecha,
# MAGIC         UPPER(TRIM(demanda.tipo_mercado)) AS tipo_mercado,
# MAGIC         demanda.agente_key,
# MAGIC         demanda.fecha_hora,
# MAGIC         demanda.demanda_real_kwh
# MAGIC     FROM gold.fact_demanda_real demanda
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     base.fecha_key,
# MAGIC     base.fecha,
# MAGIC     calendario.anio,
# MAGIC     calendario.trimestre,
# MAGIC     calendario.mes_numero,
# MAGIC     calendario.mes_nombre,
# MAGIC     calendario.anio_mes,
# MAGIC     calendario.anio_mes_nombre,
# MAGIC     calendario.semana_anio,
# MAGIC     calendario.dia_semana_numero,
# MAGIC     calendario.dia_semana_nombre,
# MAGIC     calendario.es_fin_semana,
# MAGIC
# MAGIC     base.tipo_mercado,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(base.demanda_real_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS demanda_total_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(base.demanda_real_kwh) / 1000,
# MAGIC         3
# MAGIC     ) AS demanda_promedio_mw,
# MAGIC
# MAGIC     ROUND(
# MAGIC         MAX(base.demanda_real_kwh) / 1000,
# MAGIC         3
# MAGIC     ) AS demanda_pico_mw,
# MAGIC
# MAGIC     COUNT(DISTINCT base.agente_key)
# MAGIC         AS agentes_con_demanda,
# MAGIC
# MAGIC     COUNT(DISTINCT base.fecha_hora)
# MAGIC         AS horas_con_datos
# MAGIC
# MAGIC FROM base
# MAGIC
# MAGIC LEFT JOIN gold.dim_fecha calendario
# MAGIC     ON base.fecha_key = calendario.fecha_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     base.fecha_key,
# MAGIC     base.fecha,
# MAGIC     calendario.anio,
# MAGIC     calendario.trimestre,
# MAGIC     calendario.mes_numero,
# MAGIC     calendario.mes_nombre,
# MAGIC     calendario.anio_mes,
# MAGIC     calendario.anio_mes_nombre,
# MAGIC     calendario.semana_anio,
# MAGIC     calendario.dia_semana_numero,
# MAGIC     calendario.dia_semana_nombre,
# MAGIC     calendario.es_fin_semana,
# MAGIC     base.tipo_mercado

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Demanda diaria por agente

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_demanda_diaria_agente AS
# MAGIC
# MAGIC SELECT
# MAGIC     demanda.fecha_key,
# MAGIC     CAST(demanda.fecha_hora AS DATE) AS fecha,
# MAGIC
# MAGIC     calendario.anio,
# MAGIC     calendario.trimestre,
# MAGIC     calendario.mes_numero,
# MAGIC     calendario.mes_nombre,
# MAGIC     calendario.anio_mes,
# MAGIC     calendario.anio_mes_nombre,
# MAGIC
# MAGIC     demanda.agente_key,
# MAGIC     agente.codigo_agente,
# MAGIC     agente.nombre_agente,
# MAGIC     agente.actividad_agente,
# MAGIC     UPPER(TRIM(demanda.tipo_mercado)) AS tipo_mercado,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(demanda.demanda_real_kwh) / 1000000,
# MAGIC         3
# MAGIC     ) AS demanda_total_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         AVG(demanda.demanda_real_kwh) / 1000,
# MAGIC         3
# MAGIC     ) AS demanda_promedio_mw,
# MAGIC
# MAGIC     ROUND(
# MAGIC         MAX(demanda.demanda_real_kwh) / 1000,
# MAGIC         3
# MAGIC     ) AS demanda_pico_mw,
# MAGIC
# MAGIC     COUNT(DISTINCT demanda.fecha_hora)
# MAGIC         AS horas_con_datos
# MAGIC
# MAGIC FROM gold.fact_demanda_real demanda
# MAGIC
# MAGIC LEFT JOIN gold.dim_fecha calendario
# MAGIC     ON demanda.fecha_key = calendario.fecha_key
# MAGIC
# MAGIC LEFT JOIN gold.dim_agente agente
# MAGIC     ON demanda.agente_key = agente.agente_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     demanda.fecha_key,
# MAGIC     CAST(demanda.fecha_hora AS DATE),
# MAGIC     calendario.anio,
# MAGIC     calendario.trimestre,
# MAGIC     calendario.mes_numero,
# MAGIC     calendario.mes_nombre,
# MAGIC     calendario.anio_mes,
# MAGIC     calendario.anio_mes_nombre,
# MAGIC     demanda.agente_key,
# MAGIC     agente.codigo_agente,
# MAGIC     agente.nombre_agente,
# MAGIC     agente.actividad_agente,
# MAGIC     UPPER(TRIM(demanda.tipo_mercado))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Operación diaria por planta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_operacion_diaria_planta AS
# MAGIC
# MAGIC WITH generacion AS (
# MAGIC     SELECT
# MAGIC         fecha_key,
# MAGIC         CAST(fecha_hora AS DATE) AS fecha,
# MAGIC         planta_key,
# MAGIC         SUM(generacion_real_kwh) AS generacion_total_kwh,
# MAGIC         AVG(generacion_real_kwh) AS generacion_promedio_kwh,
# MAGIC         MAX(generacion_real_kwh) AS generacion_pico_kwh,
# MAGIC         COUNT(DISTINCT fecha_hora) AS horas_con_generacion
# MAGIC     FROM gold.fact_generacion_real
# MAGIC     GROUP BY
# MAGIC         fecha_key,
# MAGIC         CAST(fecha_hora AS DATE),
# MAGIC         planta_key
# MAGIC ),
# MAGIC
# MAGIC disponibilidad AS (
# MAGIC     SELECT
# MAGIC         fecha_key,
# MAGIC         CAST(fecha_hora AS DATE) AS fecha,
# MAGIC         planta_key,
# MAGIC         SUM(disponibilidad_real_kwh)
# MAGIC             AS disponibilidad_total_kwh,
# MAGIC         AVG(disponibilidad_real_kwh)
# MAGIC             AS disponibilidad_promedio_kwh,
# MAGIC         MAX(disponibilidad_real_kwh)
# MAGIC             AS disponibilidad_pico_kwh,
# MAGIC         COUNT(DISTINCT fecha_hora)
# MAGIC             AS horas_con_disponibilidad
# MAGIC     FROM gold.fact_disponibilidad_planta
# MAGIC     GROUP BY
# MAGIC         fecha_key,
# MAGIC         CAST(fecha_hora AS DATE),
# MAGIC         planta_key
# MAGIC ),
# MAGIC
# MAGIC llaves AS (
# MAGIC     SELECT fecha_key, fecha, planta_key FROM generacion
# MAGIC     UNION
# MAGIC     SELECT fecha_key, fecha, planta_key FROM disponibilidad
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     llaves.fecha_key,
# MAGIC     llaves.fecha,
# MAGIC
# MAGIC     calendario.anio,
# MAGIC     calendario.trimestre,
# MAGIC     calendario.mes_numero,
# MAGIC     calendario.mes_nombre,
# MAGIC     calendario.anio_mes,
# MAGIC     calendario.anio_mes_nombre,
# MAGIC
# MAGIC     llaves.planta_key,
# MAGIC     planta.codigo_planta,
# MAGIC     planta.nombre_planta,
# MAGIC     planta.codigo_sic_agente,
# MAGIC     planta.tipo_generacion,
# MAGIC     planta.cap_efectiva_neta,
# MAGIC     planta.es_registro_inferido,
# MAGIC     planta.esta_en_maestro_actual,
# MAGIC
# MAGIC     ROUND(
# MAGIC         generacion.generacion_total_kwh / 1000000,
# MAGIC         3
# MAGIC     ) AS generacion_total_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         disponibilidad.disponibilidad_total_kwh / 1000000,
# MAGIC         3
# MAGIC     ) AS disponibilidad_total_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         generacion.generacion_promedio_kwh / 1000,
# MAGIC         3
# MAGIC     ) AS generacion_promedio_mw,
# MAGIC
# MAGIC     ROUND(
# MAGIC         disponibilidad.disponibilidad_promedio_kwh / 1000,
# MAGIC         3
# MAGIC     ) AS disponibilidad_promedio_mw,
# MAGIC
# MAGIC     ROUND(
# MAGIC         generacion.generacion_pico_kwh / 1000,
# MAGIC         3
# MAGIC     ) AS generacion_pico_mw,
# MAGIC
# MAGIC     ROUND(
# MAGIC         disponibilidad.disponibilidad_pico_kwh / 1000,
# MAGIC         3
# MAGIC     ) AS disponibilidad_pico_mw,
# MAGIC
# MAGIC     generacion.horas_con_generacion,
# MAGIC     disponibilidad.horas_con_disponibilidad,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0
# MAGIC         * generacion.generacion_total_kwh
# MAGIC         / CASE
# MAGIC             WHEN disponibilidad.disponibilidad_total_kwh = 0
# MAGIC             THEN NULL
# MAGIC             ELSE disponibilidad.disponibilidad_total_kwh
# MAGIC           END,
# MAGIC         2
# MAGIC     ) AS utilizacion_disponibilidad_pct,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN generacion.generacion_total_kwh IS NULL
# MAGIC          AND disponibilidad.disponibilidad_total_kwh IS NOT NULL
# MAGIC         THEN 'SOLO DISPONIBILIDAD'
# MAGIC
# MAGIC         WHEN generacion.generacion_total_kwh IS NOT NULL
# MAGIC          AND disponibilidad.disponibilidad_total_kwh IS NULL
# MAGIC         THEN 'SOLO GENERACION'
# MAGIC
# MAGIC         WHEN generacion.generacion_total_kwh
# MAGIC              > disponibilidad.disponibilidad_total_kwh
# MAGIC         THEN 'GENERACION MAYOR A DISPONIBILIDAD'
# MAGIC
# MAGIC         ELSE 'CONSISTENTE'
# MAGIC     END AS estado_consistencia
# MAGIC
# MAGIC FROM llaves
# MAGIC
# MAGIC LEFT JOIN generacion
# MAGIC     ON llaves.fecha_key = generacion.fecha_key
# MAGIC    AND llaves.planta_key = generacion.planta_key
# MAGIC
# MAGIC LEFT JOIN disponibilidad
# MAGIC     ON llaves.fecha_key = disponibilidad.fecha_key
# MAGIC    AND llaves.planta_key = disponibilidad.planta_key
# MAGIC
# MAGIC LEFT JOIN gold.dim_fecha calendario
# MAGIC     ON llaves.fecha_key = calendario.fecha_key
# MAGIC
# MAGIC LEFT JOIN gold.dim_planta planta
# MAGIC     ON llaves.planta_key = planta.planta_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Generación diaria por tipo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_generacion_diaria_tipo AS
# MAGIC
# MAGIC SELECT
# MAGIC     fecha_key,
# MAGIC     fecha,
# MAGIC
# MAGIC     MAX(anio) AS anio,
# MAGIC     MAX(trimestre) AS trimestre,
# MAGIC     MAX(mes_numero) AS mes_numero,
# MAGIC     MAX(mes_nombre) AS mes_nombre,
# MAGIC     MAX(anio_mes) AS anio_mes,
# MAGIC     MAX(anio_mes_nombre) AS anio_mes_nombre,
# MAGIC
# MAGIC     COALESCE(
# MAGIC         NULLIF(TRIM(tipo_generacion), ''),
# MAGIC         'SIN CLASIFICAR'
# MAGIC     ) AS tipo_generacion,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(generacion_total_gwh),
# MAGIC         3
# MAGIC     ) AS generacion_total_gwh,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(disponibilidad_total_gwh),
# MAGIC         3
# MAGIC     ) AS disponibilidad_total_gwh,
# MAGIC
# MAGIC     COUNT(DISTINCT planta_key)
# MAGIC         AS plantas_con_datos,
# MAGIC
# MAGIC     ROUND(
# MAGIC         100.0
# MAGIC         * SUM(generacion_total_gwh)
# MAGIC         / CASE
# MAGIC             WHEN SUM(disponibilidad_total_gwh) = 0
# MAGIC             THEN NULL
# MAGIC             ELSE SUM(disponibilidad_total_gwh)
# MAGIC           END,
# MAGIC         2
# MAGIC     ) AS utilizacion_disponibilidad_pct
# MAGIC
# MAGIC FROM gold_analytics.vw_operacion_diaria_planta
# MAGIC
# MAGIC GROUP BY
# MAGIC     fecha_key,
# MAGIC     fecha,
# MAGIC     COALESCE(
# MAGIC         NULLIF(TRIM(tipo_generacion), ''),
# MAGIC         'SIN CLASIFICAR'
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Energía embalsada diaria por planta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_energia_embalsada_diaria AS
# MAGIC
# MAGIC WITH relaciones AS (
# MAGIC     SELECT
# MAGIC         planta_key,
# MAGIC         COUNT(DISTINCT embalse_key)
# MAGIC             AS cantidad_embalses,
# MAGIC         MAX(
# MAGIC             CASE
# MAGIC                 WHEN es_relacion_unica
# MAGIC                 THEN embalse_key
# MAGIC             END
# MAGIC         ) AS embalse_key_unico,
# MAGIC         CONCAT_WS(
# MAGIC             ', ',
# MAGIC             SORT_ARRAY(
# MAGIC                 COLLECT_SET(codigo_embalse)
# MAGIC             )
# MAGIC         ) AS codigos_embalses_relacionados
# MAGIC     FROM gold.bridge_planta_embalse
# MAGIC     GROUP BY planta_key
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     energia.fecha_key,
# MAGIC     energia.fecha_medicion AS fecha,
# MAGIC
# MAGIC     calendario.anio,
# MAGIC     calendario.trimestre,
# MAGIC     calendario.mes_numero,
# MAGIC     calendario.mes_nombre,
# MAGIC     calendario.anio_mes,
# MAGIC     calendario.anio_mes_nombre,
# MAGIC
# MAGIC     energia.planta_key,
# MAGIC     planta.codigo_planta,
# MAGIC     planta.nombre_planta,
# MAGIC     planta.codigo_sic_agente,
# MAGIC
# MAGIC     relaciones.cantidad_embalses,
# MAGIC     relaciones.embalse_key_unico AS embalse_key,
# MAGIC     embalse.codigo_embalse,
# MAGIC     embalse.nombre_embalse,
# MAGIC     embalse.latitud,
# MAGIC     embalse.longitud,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN relaciones.cantidad_embalses = 1
# MAGIC         THEN 'RELACION UNICA'
# MAGIC         WHEN relaciones.cantidad_embalses > 1
# MAGIC         THEN 'RELACION MULTIPLE'
# MAGIC         ELSE 'SIN RELACION'
# MAGIC     END AS estado_relacion,
# MAGIC
# MAGIC     relaciones.codigos_embalses_relacionados,
# MAGIC
# MAGIC     ROUND(
# MAGIC         energia.energia_embalsada_kwh / 1000000,
# MAGIC         3
# MAGIC     ) AS energia_embalsada_gwh,
# MAGIC
# MAGIC     energia.version_seleccionada,
# MAGIC     energia.prioridad_version,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN relaciones.cantidad_embalses = 1
# MAGIC         THEN TRUE
# MAGIC         ELSE FALSE
# MAGIC     END AS es_asignacion_directa
# MAGIC
# MAGIC FROM gold.fact_energia_embalsada_planta energia
# MAGIC
# MAGIC LEFT JOIN gold.dim_fecha calendario
# MAGIC     ON energia.fecha_key = calendario.fecha_key
# MAGIC
# MAGIC LEFT JOIN gold.dim_planta planta
# MAGIC     ON energia.planta_key = planta.planta_key
# MAGIC
# MAGIC LEFT JOIN relaciones
# MAGIC     ON energia.planta_key = relaciones.planta_key
# MAGIC
# MAGIC LEFT JOIN gold.dim_embalse embalse
# MAGIC     ON relaciones.embalse_key_unico = embalse.embalse_key

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumen diario de energía embalsada

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_resumen_energia_embalsada_diaria AS
# MAGIC
# MAGIC WITH resumen AS (
# MAGIC     SELECT
# MAGIC         fecha_key,
# MAGIC         fecha,
# MAGIC
# MAGIC         MAX(anio) AS anio,
# MAGIC         MAX(mes_numero) AS mes_numero,
# MAGIC         MAX(mes_nombre) AS mes_nombre,
# MAGIC         MAX(anio_mes) AS anio_mes,
# MAGIC
# MAGIC         SUM(energia_embalsada_gwh)
# MAGIC             AS energia_total_gwh,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN estado_relacion = 'RELACION UNICA'
# MAGIC                 THEN energia_embalsada_gwh
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS energia_asignacion_directa_gwh,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN estado_relacion = 'RELACION MULTIPLE'
# MAGIC                 THEN energia_embalsada_gwh
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS energia_relacion_multiple_gwh,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN estado_relacion = 'SIN RELACION'
# MAGIC                 THEN energia_embalsada_gwh
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS energia_sin_relacion_gwh,
# MAGIC
# MAGIC         COUNT(DISTINCT planta_key)
# MAGIC             AS plantas_con_medicion
# MAGIC
# MAGIC     FROM gold_analytics.vw_energia_embalsada_diaria
# MAGIC
# MAGIC     GROUP BY
# MAGIC         fecha_key,
# MAGIC         fecha
# MAGIC ),
# MAGIC
# MAGIC comparacion AS (
# MAGIC     SELECT
# MAGIC         resumen.*,
# MAGIC         LAG(energia_total_gwh) OVER (
# MAGIC             ORDER BY fecha
# MAGIC         ) AS energia_dia_anterior_gwh
# MAGIC     FROM resumen
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     comparacion.*,
# MAGIC
# MAGIC     energia_total_gwh
# MAGIC         - energia_dia_anterior_gwh
# MAGIC         AS variacion_diaria_gwh,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN energia_dia_anterior_gwh > 0
# MAGIC         THEN
# MAGIC             100.0
# MAGIC             * (
# MAGIC                 energia_total_gwh
# MAGIC                 - energia_dia_anterior_gwh
# MAGIC               )
# MAGIC             / energia_dia_anterior_gwh
# MAGIC     END AS variacion_diaria_pct,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN energia_total_gwh > 0
# MAGIC         THEN
# MAGIC             100.0
# MAGIC             * energia_asignacion_directa_gwh
# MAGIC             / energia_total_gwh
# MAGIC     END AS cobertura_asignacion_directa_pct,
# MAGIC
# MAGIC     MAX(fecha) OVER ()
# MAGIC         AS fecha_maxima_disponible
# MAGIC
# MAGIC FROM comparacion

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Dimensiones simplificadas para Power BI

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_dim_agente_powerbi AS
# MAGIC
# MAGIC SELECT
# MAGIC     agente_key,
# MAGIC     codigo_agente,
# MAGIC     nombre_agente,
# MAGIC     nombre_agente_normalizado,
# MAGIC     actividad_agente
# MAGIC FROM gold.dim_agente
# MAGIC WHERE es_actual = TRUE

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW
# MAGIC gold_analytics.vw_dim_planta_powerbi AS
# MAGIC
# MAGIC SELECT
# MAGIC     planta_key,
# MAGIC     codigo_planta,
# MAGIC     nombre_planta,
# MAGIC     codigo_sic_agente,
# MAGIC     tipo_generacion,
# MAGIC     cap_efectiva_neta,
# MAGIC     es_registro_inferido,
# MAGIC     esta_en_maestro_actual
# MAGIC FROM gold.dim_planta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Validación final de vistas

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    BooleanType,
    LongType,
)

ANALYTICS_VIEWS = [
    f"{ANALYTICS_SCHEMA}.vw_sistema_horario",
    f"{ANALYTICS_SCHEMA}.vw_resumen_diario_sistema",
    f"{ANALYTICS_SCHEMA}.vw_actualizacion_fuentes",
    f"{ANALYTICS_SCHEMA}.vw_demanda_diaria_mercado",
    f"{ANALYTICS_SCHEMA}.vw_demanda_diaria_agente",
    f"{ANALYTICS_SCHEMA}.vw_operacion_diaria_planta",
    f"{ANALYTICS_SCHEMA}.vw_generacion_diaria_tipo",
    f"{ANALYTICS_SCHEMA}.vw_energia_embalsada_diaria",
    f"{ANALYTICS_SCHEMA}.vw_resumen_energia_embalsada_diaria",
    f"{ANALYTICS_SCHEMA}.vw_dim_agente_powerbi",
    f"{ANALYTICS_SCHEMA}.vw_dim_planta_powerbi",
]

validation_results = []

for view_name in ANALYTICS_VIEWS:
    exists = spark.catalog.tableExists(
        view_name
    )

    if exists:
        try:
            row_count = int(
                spark.table(
                    view_name
                ).count()
            )

            error_message = ""

        except Exception as exc:
            row_count = None
            error_message = str(exc)[:500]

    else:
        row_count = None
        error_message = "La vista no existe."

    approved = bool(
        exists
        and row_count is not None
        and row_count > 0
    )

    validation_results.append(
        (
            str(view_name),
            bool(exists),
            row_count,
            str(error_message),
            approved,
        )
    )


validation_schema = StructType([
    StructField(
        "vista",
        StringType(),
        False,
    ),

    StructField(
        "existe",
        BooleanType(),
        False,
    ),

    StructField(
        "filas",
        LongType(),
        True,
    ),

    StructField(
        "error",
        StringType(),
        True,
    ),

    StructField(
        "aprobada",
        BooleanType(),
        False,
    ),
])


validation_df = spark.createDataFrame(
    validation_results,
    schema=validation_schema,
)


display(
    validation_df
    .orderBy("vista")
)


failed_views = (
    validation_df
    .filter(
        ~F.col("aprobada")
    )
    .count()
)


print(
    "Vistas evaluadas:",
    len(ANALYTICS_VIEWS),
)

print(
    "Vistas fallidas:",
    failed_views,
)


if failed_views > 0:
    display(
        validation_df
        .filter(
            ~F.col("aprobada")
        )
        .orderBy("vista")
    )

    raise ValueError(
        "La creación o validación de "
        "las vistas analíticas falló."
    )


print(
    "GOLD ANALYTICS APROBADO.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Inventario final

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA gold_analytics;
# MAGIC SHOW VIEWS IN gold_analytics
