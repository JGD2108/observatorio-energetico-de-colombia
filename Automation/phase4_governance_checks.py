# Databricks notebook source
"""Gate de gobierno y reconciliación Bronze-Silver-Gold para Fase 4."""

import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F


NOTEBOOK_PATH = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
PROJECT_ROOT = "/Workspace/" + NOTEBOOK_PATH.strip("/").rsplit("/", 2)[0]
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from config.project_config import (  # noqa: E402
    BRONZE_TABLES, CATALOG, GOLD_TABLES, GOVERNANCE_TABLES, SILVER_TABLES,
)
from governance.rules import load_tx_policy  # noqa: E402

spark.sql(f"USE CATALOG `{CATALOG}`")

try:
    dbutils.widgets.text("run_id", "", "Identificador de ejecución")
    RUN_ID = dbutils.widgets.get("run_id").strip()
except Exception:
    RUN_ID = ""
if not RUN_ID:
    raise ValueError("run_id es obligatorio para el gate de gobierno.")


# COMMAND ----------

tx_policy = load_tx_policy(spark, GOVERNANCE_TABLES["ref_version_tx"])
alias_duplicates = (
    spark.table(GOVERNANCE_TABLES["ref_entity_alias"])
    .filter(F.col("status") == "APPROVED")
    .groupBy("entity_type", "alias_normalized")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

agents = spark.table(GOLD_TABLES["dim_agente"])
agent_overlaps = (
    agents.alias("left")
    .join(
        agents.alias("right"),
        (F.col("left.codigo_agente") == F.col("right.codigo_agente"))
        & (F.col("left.fecha_inicio") < F.col("right.fecha_inicio"))
        & (
            F.coalesce(F.col("left.fecha_fin"), F.lit("9999-12-31").cast("date"))
            >= F.col("right.fecha_inicio")
        ),
        "inner",
    )
    .count()
)
invalid_current_agents = (
    agents.groupBy("codigo_agente")
    .agg(F.sum(F.when(F.col("es_actual"), 1).otherwise(0)).alias("current_versions"))
    .filter(F.col("current_versions") != 1)
    .count()
)

plants = spark.table(GOLD_TABLES["dim_planta"])
invalid_inferred = plants.filter(
    F.col("es_registro_inferido") & F.col("esta_en_maestro_actual")
).count()

bridge = spark.table(GOLD_TABLES["bridge_planta_embalse"])
invalid_bridge = bridge.filter(
    F.col("activo")
    & (
        F.col("planta_key").isNull()
        | F.col("embalse_key").isNull()
        | F.col("requiere_revision_manual")
        | (
            F.col("valido_desde").isNotNull()
            & F.col("valido_hasta").isNotNull()
            & (F.col("valido_hasta") < F.col("valido_desde"))
        )
    )
).count()

governance_errors = {
    "alias_duplicados": alias_duplicates,
    "solapamientos_scd2": agent_overlaps,
    "agentes_sin_version_actual_unica": invalid_current_agents,
    "inferidos_marcados_oficiales": invalid_inferred,
    "relaciones_activas_invalidas": invalid_bridge,
}
print("Política TX:", tx_policy)
print("Validaciones de gobierno:", governance_errors)
if any(governance_errors.values()):
    raise ValueError(f"Gate de gobierno no aprobado: {governance_errors}")


# COMMAND ----------

GOLD_BY_SOURCE = {
    "generacion_real": GOLD_TABLES["fact_generacion_real"],
    "demanda_real": GOLD_TABLES["fact_demanda_real"],
    "disponibilidad_plantas": GOLD_TABLES["fact_disponibilidad_planta"],
    "precio_bolsa": GOLD_TABLES["fact_precio_bolsa"],
    "niveles_embalses": GOLD_TABLES["fact_energia_embalsada_planta"],
}

reconciliation_frames = []
for source_name, bronze_table in BRONZE_TABLES.items():
    bronze_count = spark.table(bronze_table).agg(F.count("*").alias("bronze_rows"))
    silver_count = spark.table(SILVER_TABLES[source_name]).agg(
        F.count("*").alias("silver_rows")
    )
    if source_name in GOLD_BY_SOURCE:
        gold_count = spark.table(GOLD_BY_SOURCE[source_name]).agg(
            F.count("*").alias("gold_rows")
        )
    else:
        gold_count = spark.range(1).select(F.lit(None).cast("long").alias("gold_rows"))
    reconciliation_frames.append(
        bronze_count.crossJoin(silver_count).crossJoin(gold_count)
        .withColumn("run_id", F.lit(RUN_ID))
        .withColumn("source_name", F.lit(source_name))
    )

reconciliation_df = reconciliation_frames[0]
for frame in reconciliation_frames[1:]:
    reconciliation_df = reconciliation_df.unionByName(frame)

reconciliation_df = (
    reconciliation_df
    .withColumn("bronze_silver_delta", F.col("silver_rows") - F.col("bronze_rows"))
    .withColumn("silver_gold_delta", F.col("gold_rows") - F.col("silver_rows"))
    .withColumn(
        "status",
        F.when(F.col("silver_rows") > F.col("bronze_rows"), F.lit("WARNING"))
        .when(
            F.col("gold_rows").isNotNull() & (F.col("gold_rows") > F.col("silver_rows")),
            F.lit("WARNING"),
        )
        .otherwise(F.lit("SUCCESS")),
    )
    .withColumn(
        "detail",
        F.lit(
            "Conteos de capa; las diferencias esperadas provienen de deduplicación "
            "y selección de versión TX. La reconciliación semántica Silver-Gold "
            "permanece en el quality gate."
        ),
    )
    .withColumn("reconciled_at", F.current_timestamp())
    .select(
        "run_id", "source_name", "bronze_rows", "silver_rows", "gold_rows",
        "bronze_silver_delta", "silver_gold_delta", "status", "detail",
        "reconciled_at",
    )
)
reconciliation_df.createOrReplaceTempView("phase4_layer_reconciliation")
spark.sql(f"""
MERGE INTO {GOVERNANCE_TABLES['layer_reconciliation']} AS target
USING phase4_layer_reconciliation AS source
ON target.run_id = source.run_id AND target.source_name = source.source_name
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

summary = reconciliation_df.groupBy("status").count().collect()
print("Reconciliación Bronze-Silver-Gold:", [row.asDict() for row in summary])
print("GATE DE GOBIERNO FASE 4 APROBADO.")
