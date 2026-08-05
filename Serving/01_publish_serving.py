# Databricks notebook source
"""Publish stable business and technical serving views for Phase 6."""

import sys

NOTEBOOK_PATH = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import (  # noqa: E402
    ANALYTICS_SCHEMA,
    AUDIT_TABLES,
    CATALOG,
    MAX_LAG_DAYS,
    SERVING_SCHEMA,
    SERVING_TECHNICAL_SCHEMA,
    SERVING_VIEWS,
)

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SERVING_SCHEMA}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SERVING_TECHNICAL_SCHEMA}")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['kpi_sistema_diario']} AS
WITH metricas AS (
SELECT
  base.*,
  ROUND(AVG(generacion_gwh) OVER (ORDER BY fecha ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 3) AS generacion_media_7d_gwh,
  ROUND(AVG(demanda_gwh) OVER (ORDER BY fecha ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 3) AS demanda_media_7d_gwh,
  ROUND(AVG(precio_nacional_promedio_cop_kwh) OVER (ORDER BY fecha ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS precio_media_7d_cop_kwh,
  ROUND(100.0 * horas_con_datos / 24.0, 2) AS completitud_horaria_pct,
  DATEDIFF(CURRENT_DATE(), fecha) AS dias_desde_dato,
  MAX(CASE
    WHEN generacion_gwh IS NOT NULL
      AND demanda_gwh IS NOT NULL
      AND disponibilidad_gwh IS NOT NULL
      AND precio_nacional_promedio_cop_kwh IS NOT NULL
      AND horas_con_datos = 24
    THEN fecha
  END) OVER () AS fecha_corte_comparable
FROM {ANALYTICS_SCHEMA}.vw_resumen_diario_sistema base
)
SELECT
  metricas.*,
  fecha <= fecha_corte_comparable AS es_periodo_comparable,
  CASE WHEN fecha <= fecha_corte_comparable THEN generacion_gwh END AS generacion_comparable_gwh,
  CASE WHEN fecha <= fecha_corte_comparable THEN demanda_gwh END AS demanda_comparable_gwh,
  CASE WHEN fecha <= fecha_corte_comparable THEN disponibilidad_gwh END AS disponibilidad_comparable_gwh,
  CASE WHEN fecha <= fecha_corte_comparable THEN precio_nacional_promedio_cop_kwh END AS precio_comparable_cop_kwh
FROM metricas
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['operacion_planta_diaria']} AS
SELECT * FROM {ANALYTICS_SCHEMA}.vw_operacion_diaria_planta
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['demanda_mercado_diaria']} AS
SELECT * FROM {ANALYTICS_SCHEMA}.vw_demanda_diaria_mercado
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['energia_embalsada_diaria']} AS
SELECT * FROM {ANALYTICS_SCHEMA}.vw_resumen_energia_embalsada_diaria
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['estado_fuentes']} AS
SELECT
  *,
  dias_rezago <= {MAX_LAG_DAYS} AS cumple_sla_frescura,
  CASE
    WHEN dias_rezago <= 7 THEN 'SALUDABLE'
    WHEN dias_rezago <= {MAX_LAG_DAYS} THEN 'EN OBSERVACION'
    ELSE 'INCUMPLIDO'
  END AS estado_operativo
FROM {ANALYTICS_SCHEMA}.vw_actualizacion_fuentes
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['generacion_tecnologia_diaria']} AS
SELECT * FROM {ANALYTICS_SCHEMA}.vw_generacion_diaria_tipo
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['pipeline_health']} AS
SELECT
  run_id, job_name, environment, catalog, trigger_type,
  started_at, finished_at, duration_seconds, status,
  tasks_total, tasks_succeeded, tasks_failed,
  CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END AS run_exitoso,
  AVG(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0.0 END) OVER (
    ORDER BY started_at ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
  ) * 100.0 AS tasa_exito_ultimas_10_pct,
  error_message
FROM {AUDIT_TABLES['pipeline_runs']}
WHERE started_at >= CURRENT_TIMESTAMP() - INTERVAL 90 DAYS
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['task_performance']} AS
SELECT
  task_key, layer, source_name,
  COUNT(*) AS ejecuciones_30d,
  ROUND(AVG(duration_seconds), 2) AS duracion_promedio_segundos,
  PERCENTILE_APPROX(duration_seconds, 0.95) AS duracion_p95_segundos,
  MAX(duration_seconds) AS duracion_maxima_segundos,
  ROUND(100.0 * AVG(CASE WHEN status = 'SUCCESS' THEN 1.0 ELSE 0.0 END), 2) AS tasa_exito_pct,
  MAX(finished_at) AS ultima_ejecucion
FROM {AUDIT_TABLES['task_runs']}
WHERE started_at >= CURRENT_TIMESTAMP() - INTERVAL 30 DAYS
GROUP BY task_key, layer, source_name
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {SERVING_VIEWS['quality_alerts']} AS
SELECT
  alert_id, run_id, rule_id, component, severity, status,
  error_count, message, created_at, resolved_at,
  TIMESTAMPDIFF(HOUR, created_at, COALESCE(resolved_at, CURRENT_TIMESTAMP())) AS edad_horas
FROM {AUDIT_TABLES['data_quality_alerts']}
""")

# COMMAND ----------

failed = []
for view_name in SERVING_VIEWS.values():
    if not spark.catalog.tableExists(view_name):
        failed.append(f"{view_name}: no existe")
        continue
    try:
        spark.table(view_name).limit(1).collect()
    except Exception as exc:
        failed.append(f"{view_name}: {str(exc)[:300]}")

if failed:
    raise ValueError(f"Fallo publicando vistas serving: {failed}")

print("Vistas de consumo publicadas:", len(SERVING_VIEWS))
print("Capa negocio:", SERVING_SCHEMA)
print("Capa tecnica:", SERVING_TECHNICAL_SCHEMA)
