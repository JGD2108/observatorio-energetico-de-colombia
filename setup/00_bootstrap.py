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
    GOVERNANCE_TABLES, LANDING_VOLUME_NAME, MONITORING_TABLES,
    QUARANTINE_TABLES, SCHEMAS, SILVER_TABLES,
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
    "plantas_reservorios": "region STRING, nombre_planta STRING, nombre_reservorio STRING, tipo_relacion STRING, es_principal BOOLEAN, permite_atribucion BOOLEAN, fuente_relacion STRING, estado_validacion STRING, valido_desde DATE, valido_hasta DATE, codigo_planta STRING, codigo_embalse STRING, planta_encontrada BOOLEAN, embalse_encontrado BOOLEAN, relacion_completa BOOLEAN, requiere_revision_manual BOOLEAN, activo BOOLEAN, fecha_retiro TIMESTAMP",
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
spark.sql(
    f"UPDATE {SILVER_TABLES['plantas_reservorios']} SET activo = true "
    "WHERE activo IS NULL"
)


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
CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.backfill_runs (
    backfill_id STRING NOT NULL,
    job_run_id STRING,
    environment STRING,
    execution_mode STRING NOT NULL,
    start_date DATE,
    end_date DATE,
    chunk_days INT,
    status STRING NOT NULL,
    started_at TIMESTAMP NOT NULL,
    completed_at TIMESTAMP,
    message STRING
) USING DELTA
TBLPROPERTIES ('quality'='audit', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.backfill_coverage (
    backfill_id STRING NOT NULL,
    source_name STRING NOT NULL,
    requested_start DATE NOT NULL,
    requested_end DATE NOT NULL,
    actual_start DATE,
    actual_end DATE,
    row_count BIGINT,
    covered BOOLEAN NOT NULL,
    measured_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='audit', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLES['data_quality_results']} (
    run_id STRING NOT NULL,
    rule_id STRING NOT NULL,
    component STRING NOT NULL,
    rule_name STRING NOT NULL,
    quality_dimension STRING NOT NULL,
    severity STRING NOT NULL,
    is_blocking BOOLEAN NOT NULL,
    error_count BIGINT NOT NULL,
    error_rate DOUBLE,
    passed BOOLEAN NOT NULL,
    detail STRING,
    window_start_date DATE,
    window_end_date DATE,
    executed_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='audit', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {AUDIT_TABLES['data_quality_alerts']} (
    run_id STRING NOT NULL,
    alert_id STRING NOT NULL,
    rule_id STRING NOT NULL,
    component STRING NOT NULL,
    severity STRING NOT NULL,
    status STRING NOT NULL,
    error_count BIGINT NOT NULL,
    message STRING,
    created_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP
) USING DELTA
TBLPROPERTIES ('quality'='audit', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MONITORING_TABLES['slo_results']} (
    run_id STRING NOT NULL,
    slo_id STRING NOT NULL,
    metric_name STRING NOT NULL,
    metric_value DOUBLE,
    operator STRING NOT NULL,
    threshold DOUBLE NOT NULL,
    passed BOOLEAN NOT NULL,
    blocking BOOLEAN NOT NULL,
    detail STRING,
    measured_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='monitoring', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {MONITORING_TABLES['operational_alerts']} (
    alert_id STRING NOT NULL,
    run_id STRING NOT NULL,
    slo_id STRING NOT NULL,
    severity STRING NOT NULL,
    status STRING NOT NULL,
    metric_value DOUBLE,
    threshold DOUBLE,
    message STRING,
    created_at TIMESTAMP NOT NULL,
    resolved_at TIMESTAMP
) USING DELTA
TBLPROPERTIES ('quality'='monitoring', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLES['data_quality_exceptions']} (
    run_id STRING NOT NULL,
    exception_id STRING NOT NULL,
    rule_id STRING NOT NULL,
    component STRING NOT NULL,
    source_table STRING,
    record_key STRING,
    event_time TIMESTAMP,
    severity STRING NOT NULL,
    reason STRING NOT NULL,
    payload_json STRING,
    quarantined_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='quarantine', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOVERNANCE_TABLES['ref_version_tx']} (
    rule_code STRING NOT NULL,
    match_type STRING NOT NULL,
    version_pattern STRING NOT NULL,
    priority INT,
    multiplier INT,
    description STRING NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_active BOOLEAN NOT NULL,
    approved_by STRING,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='governance', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
MERGE INTO {GOVERNANCE_TABLES['ref_version_tx']} AS target
USING (
  SELECT * FROM VALUES
    ('TXF','EXACT','TXF',10000,NULL,'Versión final',DATE'2026-01-01',TRUE),
    ('TXR','EXACT','TXR',9000,NULL,'Versión revisada',DATE'2026-01-01',TRUE),
    ('TX_NUMERIC','REGEX','^TX[0-9]+$',NULL,100,'Versión numérica',DATE'2026-01-01',TRUE)
  AS source(rule_code,match_type,version_pattern,priority,multiplier,description,valid_from,is_active)
) AS source
ON target.rule_code = source.rule_code
WHEN NOT MATCHED THEN INSERT (
  rule_code, match_type, version_pattern, priority, multiplier, description,
  valid_from, valid_to, is_active, approved_by, created_at, updated_at
) VALUES (
  source.rule_code, source.match_type, source.version_pattern, source.priority,
  source.multiplier, source.description, source.valid_from, NULL, source.is_active,
  'FASE_4_BOOTSTRAP', current_timestamp(), current_timestamp()
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOVERNANCE_TABLES['ref_entity_alias']} (
    entity_type STRING NOT NULL,
    alias_normalized STRING NOT NULL,
    canonical_code STRING NOT NULL,
    source STRING NOT NULL,
    status STRING NOT NULL,
    valid_from DATE NOT NULL,
    valid_to DATE,
    approved_by STRING,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='governance', 'delta.enableChangeDataFeed'='true')
""")

spark.sql(f"""
MERGE INTO {GOVERNANCE_TABLES['ref_entity_alias']} AS target
USING (
  SELECT * FROM VALUES
    ('EMBALSE','CALIMA1','CALIMA1'),
    ('EMBALSE','PORCEII','PORCE2'),
    ('EMBALSE','PORCEIII','PORCE3'),
    ('EMBALSE','URRA1','URRA1')
  AS source(entity_type,alias_normalized,canonical_code)
) AS source
ON target.entity_type = source.entity_type
AND target.alias_normalized = source.alias_normalized
WHEN NOT MATCHED THEN INSERT (
  entity_type, alias_normalized, canonical_code, source, status, valid_from,
  valid_to, approved_by, created_at, updated_at
) VALUES (
  source.entity_type, source.alias_normalized, source.canonical_code,
  'FASE_4_MIGRATION', 'APPROVED', DATE'2026-01-01', NULL,
  'FASE_4_BOOTSTRAP', current_timestamp(), current_timestamp()
)
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {GOVERNANCE_TABLES['layer_reconciliation']} (
    run_id STRING NOT NULL,
    source_name STRING NOT NULL,
    bronze_rows BIGINT,
    silver_rows BIGINT,
    gold_rows BIGINT,
    bronze_silver_delta BIGINT,
    silver_gold_delta BIGINT,
    status STRING NOT NULL,
    detail STRING,
    reconciled_at TIMESTAMP NOT NULL
) USING DELTA
TBLPROPERTIES ('quality'='governance', 'delta.enableChangeDataFeed'='true')
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

spark.sql(f"""
CREATE OR REPLACE VIEW {AUDIT_SCHEMA}.vw_latest_data_quality AS
SELECT * EXCEPT (row_number)
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY component, rule_id ORDER BY executed_at DESC, run_id DESC
    ) AS row_number
    FROM {AUDIT_TABLES['data_quality_results']}
)
WHERE row_number = 1
""")

spark.sql(f"""
CREATE OR REPLACE VIEW {AUDIT_SCHEMA}.vw_open_data_quality_alerts AS
SELECT * FROM {AUDIT_TABLES['data_quality_alerts']}
WHERE status = 'OPEN'
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

bridge_columns = set(spark.table(GOLD_TABLES["bridge_planta_embalse"]).columns)
missing_bridge_columns = [
    definition for definition in ("activo BOOLEAN", "fecha_retiro TIMESTAMP")
    if definition.split()[0] not in bridge_columns
]
if missing_bridge_columns:
    spark.sql(
        f"ALTER TABLE {GOLD_TABLES['bridge_planta_embalse']} ADD COLUMNS "
        f"({', '.join(missing_bridge_columns)})"
    )
spark.sql(
    f"UPDATE {GOLD_TABLES['bridge_planta_embalse']} SET activo = true "
    "WHERE activo IS NULL"
)

required = [
    *BRONZE_TABLES.values(),
    *SILVER_TABLES.values(),
    *GOLD_TABLES.values(),
    *AUDIT_TABLES.values(),
    *MONITORING_TABLES.values(),
    *GOVERNANCE_TABLES.values(),
    *QUARANTINE_TABLES.values(),
]
missing = [table for table in required if not spark.catalog.tableExists(table)]
if missing:
    raise RuntimeError(f"Bootstrap incompleto: {missing}")
print(f"Bootstrap completado en {CATALOG}: {len(required)} contratos")
