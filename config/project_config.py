"""Configuracion central del Observatorio Energetico de Colombia."""

from datetime import date
import os


def _runtime_parameter(name: str, default: str) -> str:
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return os.getenv(f"OBSERVATORIO_{name.upper()}", default)
        value = DBUtils(spark).widgets.get(name)
        if value:
            return value
    except Exception:
        pass
    return os.getenv(f"OBSERVATORIO_{name.upper()}", default)


ENVIRONMENT = _runtime_parameter("environment", "dev")
TIMEZONE = "America/Bogota"
CATALOG = _runtime_parameter("catalog", f"observatorio_{ENVIRONMENT}")

SCHEMAS = {
    "landing": "landing",
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "gold_analytics": "gold_analytics",
    "monitoring": "monitoring",
    "audit": "audit",
    "governance": "governance",
    "quarantine": "quarantine",
    "serving": "serving",
    "serving_technical": "serving_technical",
}

LANDING_VOLUME_NAME = "raw_files"
LANDING_VOLUME = f"/Volumes/{CATALOG}/{SCHEMAS['landing']}/{LANDING_VOLUME_NAME}"

LOOKBACK_DAYS = int(_runtime_parameter("lookback_days", "45"))
MAX_LAG_DAYS = int(_runtime_parameter("max_lag_days", "45"))
DEFAULT_HISTORICAL_START_DATE = date.fromisoformat(
    _runtime_parameter("historical_start_date", "2026-01-01")
)

PIPELINE_NAME = "observatorio_energetico_daily"
DAILY_EXECUTION_HOUR = 8

BRONZE_TABLES = {
    name: f"{CATALOG}.{SCHEMAS['bronze']}.{name}"
    for name in (
        "agentes", "plantas", "generacion_real", "demanda_real",
        "disponibilidad_plantas", "precio_bolsa", "niveles_embalses",
        "embalses", "plantas_reservorios",
    )
}

SILVER_TABLES = {
    name: f"{CATALOG}.{SCHEMAS['silver']}.{name}" for name in BRONZE_TABLES
}

GOLD_TABLES = {
    name: f"{CATALOG}.{SCHEMAS['gold']}.{name}"
    for name in (
        "dim_fecha", "dim_periodo", "dim_agente", "dim_planta",
        "dim_embalse", "fact_generacion_real", "fact_demanda_real",
        "fact_disponibilidad_planta", "fact_precio_bolsa",
        "fact_energia_embalsada_planta", "bridge_planta_embalse",
    )
}

ANALYTICS_SCHEMA = f"{CATALOG}.{SCHEMAS['gold_analytics']}"
MONITORING_SCHEMA = f"{CATALOG}.{SCHEMAS['monitoring']}"
AUDIT_SCHEMA = f"{CATALOG}.{SCHEMAS['audit']}"
SERVING_SCHEMA = f"{CATALOG}.{SCHEMAS['serving']}"
SERVING_TECHNICAL_SCHEMA = f"{CATALOG}.{SCHEMAS['serving_technical']}"

AUDIT_TABLES = {
    "pipeline_runs": f"{AUDIT_SCHEMA}.pipeline_runs",
    "task_runs": f"{AUDIT_SCHEMA}.task_runs",
    "layer_metrics": f"{AUDIT_SCHEMA}.layer_metrics",
    "data_quality_results": f"{AUDIT_SCHEMA}.data_quality_results",
    "data_quality_alerts": f"{AUDIT_SCHEMA}.data_quality_alerts",
}

MONITORING_TABLES = {
    "slo_results": f"{MONITORING_SCHEMA}.slo_results",
    "operational_alerts": f"{MONITORING_SCHEMA}.operational_alerts",
}

SERVING_VIEWS = {
    "kpi_sistema_diario": f"{SERVING_SCHEMA}.kpi_sistema_diario",
    "operacion_planta_diaria": f"{SERVING_SCHEMA}.operacion_planta_diaria",
    "demanda_mercado_diaria": f"{SERVING_SCHEMA}.demanda_mercado_diaria",
    "energia_embalsada_diaria": f"{SERVING_SCHEMA}.energia_embalsada_diaria",
    "estado_fuentes": f"{SERVING_SCHEMA}.estado_fuentes",
    "generacion_tecnologia_diaria": f"{SERVING_SCHEMA}.generacion_tecnologia_diaria",
    "pipeline_health": f"{SERVING_TECHNICAL_SCHEMA}.pipeline_health",
    "task_performance": f"{SERVING_TECHNICAL_SCHEMA}.task_performance",
    "quality_alerts": f"{SERVING_TECHNICAL_SCHEMA}.quality_alerts",
}

QUARANTINE_SCHEMA = f"{CATALOG}.{SCHEMAS['quarantine']}"
QUARANTINE_TABLES = {
    "data_quality_exceptions": f"{QUARANTINE_SCHEMA}.data_quality_exceptions",
}

GOVERNANCE_SCHEMA = f"{CATALOG}.{SCHEMAS['governance']}"
GOVERNANCE_TABLES = {
    "ref_version_tx": f"{GOVERNANCE_SCHEMA}.ref_version_tx",
    "ref_entity_alias": f"{GOVERNANCE_SCHEMA}.ref_entity_alias",
    "layer_reconciliation": f"{GOVERNANCE_SCHEMA}.layer_reconciliation",
}

LANDING_FILES = {
    "agentes": f"{LANDING_VOLUME}/agentes.json",
    "plantas": f"{LANDING_VOLUME}/plantas.json",
    "generacion_real": f"{LANDING_VOLUME}/generacion_real.json.gz",
    "demanda_real": f"{LANDING_VOLUME}/demanda_real.json",
    "disponibilidad_plantas": f"{LANDING_VOLUME}/disponibilidad_plantas.json.gz",
    "precio_bolsa": f"{LANDING_VOLUME}/precio_bolsa.json",
    "niveles_embalses": f"{LANDING_VOLUME}/niveles_embalses_plantas.json",
    "embalses": f"{LANDING_VOLUME}/embalses_unicos.json",
    "plantas_reservorios": f"{LANDING_VOLUME}/plantas-embalses.json",
}
