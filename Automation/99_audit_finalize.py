# Databricks notebook source
"""Consolidate task states and layer metrics after every pipeline run."""

import sys

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import (  # noqa: E402
    AUDIT_TABLES,
    BRONZE_TABLES,
    CATALOG,
    GOLD_TABLES,
    LANDING_FILES,
    MONITORING_TABLES,
    QUARANTINE_TABLES,
    SILVER_TABLES,
)
from observability.audit import finish_pipeline_run  # noqa: E402

spark.sql(f"USE CATALOG `{CATALOG}`")

TASK_SPECS = {
    "setup_catalog": ("SETUP", "catalog"),
    "backfill_control": ("CONTROL", "backfill"),
    "ing_demanda_real": ("LANDING", "demanda_real"),
    "ing_disponibilidad": ("LANDING", "disponibilidad_plantas"),
    "ing_agentes": ("LANDING", "agentes"),
    "ing_generacion_real": ("LANDING", "generacion_real"),
    "ing_niveles_embalses": ("LANDING", "niveles_embalses"),
    "ing_plantas": ("LANDING", "plantas"),
    "ing_precio_bolsa": ("LANDING", "precio_bolsa"),
    "ing_embalses": ("LANDING", "embalses"),
    "ing_plantas_reservorios": ("LANDING", "plantas_reservorios"),
    "bronze_daily": ("BRONZE", "all_sources"),
    "slv_agentes": ("SILVER", "agentes"),
    "slv_plantas": ("SILVER", "plantas"),
    "slv_embalses": ("SILVER", "embalses"),
    "slv_precio_bolsa": ("SILVER", "precio_bolsa"),
    "slv_demanda_real": ("SILVER", "demanda_real"),
    "slv_disponibilidad": ("SILVER", "disponibilidad_plantas"),
    "slv_generacion": ("SILVER", "generacion_real"),
    "slv_niveles_embalses": ("SILVER", "niveles_embalses"),
    "slv_plantas_reservorios": ("SILVER", "plantas_reservorios"),
    "backfill_validate": ("CONTROL", "backfill_coverage"),
    "gold_daily": ("GOLD", "dimensional_model"),
    "governance_check": ("GOVERNANCE", "phase4_gate"),
    "quality_check": ("QUALITY", "gold_incremental"),
    "gold_analytics": ("ANALYTICS", "dashboard_views"),
    "serving_publish": ("SERVING", "consumer_views"),
    "operational_readiness": ("MONITORING", "production_gate"),
}

TABLE_SPECS = [
    *(("BRONZE", source, table) for source, table in BRONZE_TABLES.items()),
    *(("SILVER", source, table) for source, table in SILVER_TABLES.items()),
    *(("GOLD", source, table) for source, table in GOLD_TABLES.items()),
    *(("MONITORING", source, table) for source, table in MONITORING_TABLES.items()),
]

STATUS = finish_pipeline_run(
    spark,
    dbutils,
    AUDIT_TABLES["pipeline_runs"],
    AUDIT_TABLES["task_runs"],
    AUDIT_TABLES["layer_metrics"],
    TASK_SPECS,
    TABLE_SPECS,
    LANDING_FILES,
    QUARANTINE_TABLES["data_quality_exceptions"],
)
try:
    backfill_id = dbutils.widgets.get("backfill_id")
    escaped_status = str(STATUS).replace("'", "''")
    escaped_id = str(backfill_id).replace("'", "''")
    spark.sql(f"""
      UPDATE {CATALOG}.audit.backfill_runs
      SET status = '{escaped_status}', completed_at = current_timestamp()
      WHERE backfill_id = '{escaped_id}'
    """)
except Exception as exc:
    print("No se actualizo audit.backfill_runs:", exc)
print("Auditoria finalizada con estado:", STATUS)
