from datetime import date


# ============================================================
# ENVIRONMENT
# ============================================================

ENVIRONMENT = "dev"
TIMEZONE = "America/Bogota"


# ============================================================
# UNITY CATALOG
# ============================================================

CATALOG = "observatorio_dev"

SCHEMAS = {
    "landing": "landing",
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    "audit": "audit",
    "quarantine": "quarantine",
    "serving": "serving",
    "serving_technical": "serving_technical",
}


# ============================================================
# LANDING
# ============================================================

LANDING_VOLUME = (
    f"/Volumes/{CATALOG}/{SCHEMAS['landing']}/raw_files"
)


# ============================================================
# INCREMENTAL LOAD
# ============================================================

LOOKBACK_DAYS = 45

DEFAULT_HISTORICAL_START_DATE = date(2026, 1, 1)


# ============================================================
# PIPELINE
# ============================================================

PIPELINE_NAME = "observatorio_energetico_daily"

DAILY_EXECUTION_HOUR = 8


# ============================================================
# BRONZE TABLES
# ============================================================

BRONZE_TABLES = {
    "agentes": f"{CATALOG}.{SCHEMAS['bronze']}.agentes",
    "plantas": f"{CATALOG}.{SCHEMAS['bronze']}.plantas",
    "generacion_real": (
        f"{CATALOG}.{SCHEMAS['bronze']}.generacion_real"
    ),
    "demanda_real": (
        f"{CATALOG}.{SCHEMAS['bronze']}.demanda_real"
    ),
    "disponibilidad_plantas": (
        f"{CATALOG}.{SCHEMAS['bronze']}.disponibilidad_plantas"
    ),
    "precio_bolsa": (
        f"{CATALOG}.{SCHEMAS['bronze']}.precio_bolsa"
    ),
    "niveles_embalses": (
        f"{CATALOG}.{SCHEMAS['bronze']}.niveles_embalses"
    ),
    "embalses": (
        f"{CATALOG}.{SCHEMAS['bronze']}.embalses"
    ),
    "plantas_reservorios": (
        f"{CATALOG}.{SCHEMAS['bronze']}.plantas_reservorios"
    ),
}


# ============================================================
# SILVER TABLES
# ============================================================

SILVER_TABLES = {
    "agentes": f"{CATALOG}.{SCHEMAS['silver']}.agentes",
    "plantas": f"{CATALOG}.{SCHEMAS['silver']}.plantas",
    "generacion_real": (
        f"{CATALOG}.{SCHEMAS['silver']}.generacion_real"
    ),
    "demanda_real": (
        f"{CATALOG}.{SCHEMAS['silver']}.demanda_real"
    ),
    "disponibilidad_plantas": (
        f"{CATALOG}.{SCHEMAS['silver']}.disponibilidad_plantas"
    ),
    "precio_bolsa": (
        f"{CATALOG}.{SCHEMAS['silver']}.precio_bolsa"
    ),
    "niveles_embalses": (
        f"{CATALOG}.{SCHEMAS['silver']}.niveles_embalses"
    ),
    "embalses": (
        f"{CATALOG}.{SCHEMAS['silver']}.embalses"
    ),
    "plantas_reservorios": (
        f"{CATALOG}.{SCHEMAS['silver']}.plantas_reservorios"
    ),
}


# ============================================================
# GOLD TABLES
# ============================================================

GOLD_TABLES = {
    "dim_fecha": f"{CATALOG}.{SCHEMAS['gold']}.dim_fecha",
    "dim_periodo": f"{CATALOG}.{SCHEMAS['gold']}.dim_periodo",
    "dim_agente": f"{CATALOG}.{SCHEMAS['gold']}.dim_agente",
    "dim_planta": f"{CATALOG}.{SCHEMAS['gold']}.dim_planta",
    "dim_embalse": f"{CATALOG}.{SCHEMAS['gold']}.dim_embalse",
    "fact_generacion_real": (
        f"{CATALOG}.{SCHEMAS['gold']}.fact_generacion_real"
    ),
    "fact_demanda_real": (
        f"{CATALOG}.{SCHEMAS['gold']}.fact_demanda_real"
    ),
    "fact_disponibilidad_planta": (
        f"{CATALOG}.{SCHEMAS['gold']}.fact_disponibilidad_planta"
    ),
    "fact_precio_bolsa": (
        f"{CATALOG}.{SCHEMAS['gold']}.fact_precio_bolsa"
    ),
    "fact_energia_embalsada_planta": (
        f"{CATALOG}.{SCHEMAS['gold']}."
        "fact_energia_embalsada_planta"
    ),
    "bridge_planta_embalse": (
        f"{CATALOG}.{SCHEMAS['gold']}.bridge_planta_embalse"
    ),
}


# ============================================================
# LANDING FILES
# ============================================================

LANDING_FILES = {
    "agentes": f"{LANDING_VOLUME}/agentes.json",
    "plantas": f"{LANDING_VOLUME}/plantas.json",
    "generacion_real": (
        f"{LANDING_VOLUME}/generacion_real.json.gz"
    ),
    "demanda_real": f"{LANDING_VOLUME}/demanda_real.json",
    "disponibilidad_plantas": (
        f"{LANDING_VOLUME}/disponibilidad_plantas.json.gz"
    ),
    "precio_bolsa": f"{LANDING_VOLUME}/precio_bolsa.json",
    "niveles_embalses": (
        f"{LANDING_VOLUME}/niveles_embalses.json"
    ),
    "embalses": f"{LANDING_VOLUME}/embalses_unicos.json",
    "plantas_reservorios": (
        f"{LANDING_VOLUME}/plantas-embalses.json"
    ),
    "niveles_embalses": (
    f"{LANDING_VOLUME}/niveles_embalses_plantas.json"
),"embalses": f"{LANDING_VOLUME}/embalses_unicos.json",
}