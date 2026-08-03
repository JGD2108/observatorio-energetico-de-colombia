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
