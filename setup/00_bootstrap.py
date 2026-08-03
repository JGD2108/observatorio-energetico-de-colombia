# Databricks notebook source
"""Bootstrap idempotente. No elimina tablas ni datos."""

import re
import sys
from pathlib import Path

NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import (  # noqa: E402
    AUDIT_SCHEMA, AUDIT_TABLES, BRONZE_TABLES, CATALOG, GOLD_TABLES,
    LANDING_VOLUME_NAME, SCHEMAS,
    SILVER_TABLES,
)

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{CATALOG}`")
for schema_name in SCHEMAS.values():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema_name}`")
spark.sql(
    f"CREATE VOLUME IF NOT EXISTS "
    f"`{CATALOG}`.`{SCHEMAS['landing']}`.`{LANDING_VOLUME_NAME}`"
)

COMMON_BRONZE = """
source_file_name STRING, source_file_path STRING,
ingestion_timestamp TIMESTAMP, load_date DATE
"""
BRONZE_CONTRACTS = {
    "agentes": "fecha STRING, codigo_duracion STRING, codigo_sic_agente STRING, nombre_agente STRING, actividad_agente STRING",
    "plantas": "fecha STRING, codigo_duracion STRING, codigo_planta STRING, nombre_planta STRING, codigo_sic_agente STRING, cap_efectiva_neta STRING, fpo STRING, codigo_sub_area_operativa STRING, codigo_area_operativa STRING, tipo_despacho_recurso STRING, tipo_clasificacion STRING, tipo_generacion STRING",
    "generacion_real": "codigo_variable STRING, fecha_hora STRING, codigo_duracion STRING, unidad_medida STRING, codigo_sic_agente STRING, codigo_planta STRING, version STRING, valor STRING",
    "demanda_real": "codigo_variable STRING, fecha_hora STRING, codigo_sic_agente STRING, tipo_mercado STRING, version STRING, valor STRING, unidad_medida STRING, codigo_duracion STRING",
    "disponibilidad_plantas": "codigo_variable STRING, fecha_hora STRING, codigo_duracion STRING, unidad_medida STRING, codigo_planta STRING, version STRING, valor STRING",
    "precio_bolsa": "codigo_variable STRING, fecha_hora STRING, codigo_duracion STRING, unidad_medida STRING, version STRING, valor STRING",
    "niveles_embalses": "codigo_duracion STRING, codigo_planta STRING, codigo_variable STRING, fecha_inicio STRING, unidad_medida STRING, valor STRING, version STRING",
    "embalses": "codigo_embalse STRING, nombre_embalse STRING, latitud DOUBLE, longitud DOUBLE, tipo_coordenada STRING, coordinate_source STRING, geocoding_status STRING, geocoding_query STRING",
    "plantas_reservorios": "region STRING, nombre_planta STRING, nombre_reservorio STRING, tipo_relacion STRING, es_principal BOOLEAN, permite_atribucion BOOLEAN, fuente_relacion STRING, estado_validacion STRING, valido_desde DATE, valido_hasta DATE",
}

COMMON_SILVER = """
source_file_name STRING, source_file_path STRING, ingestion_timestamp TIMESTAMP,
load_date DATE, silver_created_at TIMESTAMP, silver_updated_at TIMESTAMP
"""
SILVER_CONTRACTS = {
    "agentes": "fecha DATE, codigo_duracion STRING, codigo_agente STRING, nombre_agente STRING, actividad_agente STRING",
    "plantas": "fecha DATE, codigo_duracion STRING, codigo_planta STRING, nombre_planta STRING, codigo_sic_agente STRING, cap_efectiva_neta DOUBLE, fpo DATE, codigo_sub_area_operativa STRING, codigo_area_operativa STRING, tipo_despacho_recurso STRING, tipo_clasificacion STRING, tipo_generacion STRING",
    "generacion_real": "codigo_variable STRING, fecha_hora TIMESTAMP, codigo_duracion STRING, unidad_medida STRING, codigo_agente STRING, codigo_planta STRING, version STRING, valor DOUBLE, planta_encontrada BOOLEAN, agente_encontrado BOOLEAN",
    "demanda_real": "codigo_variable STRING, fecha_hora TIMESTAMP, codigo_agente STRING, tipo_mercado STRING, version STRING, demanda_real_kwh DOUBLE, unidad_medida STRING, codigo_duracion STRING, es_demanda_cero BOOLEAN, agente_encontrado BOOLEAN",
    "disponibilidad_plantas": "codigo_variable STRING, fecha_hora TIMESTAMP, codigo_duracion STRING, unidad_medida STRING, codigo_planta STRING, version STRING, valor DOUBLE, planta_encontrada BOOLEAN",
    "precio_bolsa": "codigo_variable STRING, fecha_hora TIMESTAMP, codigo_duracion STRING, unidad_medida STRING, version STRING, valor DOUBLE, es_precio_cero BOOLEAN, es_precio_negativo BOOLEAN",
    "niveles_embalses": "codigo_variable STRING, fecha_inicio TIMESTAMP, codigo_duracion STRING, unidad_medida STRING, codigo_planta STRING, version STRING, valor DOUBLE, es_valor_cero BOOLEAN, es_valor_negativo BOOLEAN, planta_encontrada BOOLEAN",
    "embalses": "codigo_embalse STRING, nombre_embalse STRING, latitud DOUBLE, longitud DOUBLE, tipo_coordenada STRING, fuente_coordenada STRING, estado_geocodificacion STRING, consulta_geocodificacion STRING, coordenadas_validas BOOLEAN, requiere_revision_manual BOOLEAN",
    "plantas_reservorios": "region STRING, nombre_planta STRING, nombre_reservorio STRING, tipo_relacion STRING, es_principal BOOLEAN, permite_atribucion BOOLEAN, fuente_relacion STRING, estado_validacion STRING, valido_desde DATE, valido_hasta DATE, codigo_planta STRING, codigo_embalse STRING, planta_encontrada BOOLEAN, embalse_encontrado BOOLEAN, relacion_completa BOOLEAN, requiere_revision_manual BOOLEAN",
}


def _definitions(contract: str) -> list[str]:
    return [part.strip() for part in contract.split(",") if part.strip()]


def _create_or_extend(table: str, domain: str, common: str, quality: str) -> None:
    contract = f"{domain}, {common}"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({contract}) USING DELTA "
        f"TBLPROPERTIES ('delta.enableChangeDataFeed'='true','quality'='{quality}')"
    )
    existing = {column.lower() for column in spark.table(table).columns}
    missing = [d for d in _definitions(contract) if d.split()[0].lower() not in existing]
    if missing:
        spark.sql(f"ALTER TABLE {table} ADD COLUMNS ({', '.join(missing)})")


for name, contract in BRONZE_CONTRACTS.items():
    _create_or_extend(BRONZE_TABLES[name], contract, COMMON_BRONZE, "bronze")
for name, contract in SILVER_CONTRACTS.items():
    _create_or_extend(SILVER_TABLES[name], contract, COMMON_SILVER, "silver")


spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLES['pipeline_runs']} (
    run_id STRING NOT NULL,
    job_id STRING,
    job_name STRING,
    environment STRING,
    catalog STRING,
    trigger_type STRING,
    repair_count BIGINT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds BIGINT,
    status STRING NOT NULL,
    tasks_total BIGINT,
    tasks_succeeded BIGINT,
    tasks_failed BIGINT,
    error_message STRING,
    updated_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='audit', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLES['task_runs']} (
    run_id STRING NOT NULL,
    task_key STRING NOT NULL,
    task_run_id STRING,
    layer STRING,
    source_name STRING,
    status STRING NOT NULL,
    execution_count BIGINT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    duration_seconds BIGINT,
    error_code STRING,
    error_message STRING,
    updated_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='audit', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLES['layer_metrics']} (
    run_id STRING NOT NULL,
    layer STRING NOT NULL,
    source_name STRING NOT NULL,
    table_name STRING NOT NULL,
    rows_received BIGINT,
    rows_inserted BIGINT,
    rows_updated BIGINT,
    rows_rejected BIGINT,
    rows_unchanged BIGINT,
    rows_current BIGINT,
    min_event_time TIMESTAMP,
    max_event_time TIMESTAMP,
    lag_seconds BIGINT,
    status STRING NOT NULL,
    error_message STRING,
    collected_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='audit', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {AUDIT_SCHEMA}.vw_latest_pipeline_run AS
SELECT * EXCEPT (row_number)
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY started_at DESC, run_id DESC) AS row_number
    FROM {AUDIT_TABLES['pipeline_runs']}
)
WHERE row_number = 1
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {AUDIT_SCHEMA}.vw_source_freshness AS
SELECT
    source_name,
    layer,
    MAX(max_event_time) AS max_event_time,
    MAX_BY(lag_seconds, collected_at) AS latest_lag_seconds,
    MAX(collected_at) AS collected_at
FROM {AUDIT_TABLES['layer_metrics']}
WHERE status = 'SUCCESS'
GROUP BY source_name, layer
""")


def _gold_sql_cells() -> list[str]:
    text = (Path(PROJECT_ROOT) / "DDL's" / "DDL GOLD.py").read_text(encoding="utf-8")
    cells = []
    for cell in text.split("# COMMAND ----------"):
        lines = [line.removeprefix("# MAGIC").lstrip() for line in cell.splitlines() if line.startswith("# MAGIC")]
        if lines and lines[0] == "%sql":
            cells.append("\n".join(lines[1:]))
    return cells


for cell in _gold_sql_cells():
    for statement in cell.split(";"):
        sql = statement.strip()
        if not sql or re.match(r"(?is)^(DROP\s+TABLE|SHOW\s+TABLES)", sql):
            continue
        sql = re.sub(r"(?i)\bobservatorio_dev\b", CATALOG, sql)
        sql = re.sub(
            r"(?is)^CREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS)",
            "CREATE TABLE IF NOT EXISTS ", sql, count=1,
        )
        spark.sql(sql)

required = [
    *BRONZE_TABLES.values(),
    *SILVER_TABLES.values(),
    *GOLD_TABLES.values(),
    *AUDIT_TABLES.values(),
]
missing = [table for table in required if not spark.catalog.tableExists(table)]
if missing:
    raise RuntimeError(f"Bootstrap incompleto: {missing}")
print(f"Bootstrap completado en {CATALOG}: {len(required)} contratos")
