# Databricks notebook source
# NO ACTIVO: reemplazado por gold_incremental_quality_checks.py.
from datetime import date


checks = []


def add_check(name, actual, expected=0):
    checks.append({
        "check": name,
        "actual": int(actual),
        "expected": int(expected),
        "passed": int(actual) == int(expected)
    })


# Generación
generation = spark.sql("""
    SELECT
        COUNT(*) - COUNT(DISTINCT generacion_key) AS duplicates,
        SUM(CASE WHEN planta_key IS NULL THEN 1 ELSE 0 END)
            AS missing_plants,
        SUM(CASE WHEN agente_key IS NULL THEN 1 ELSE 0 END)
            AS missing_agents
    FROM observatorio_dev.gold.fact_generacion_real
""").first()

add_check(
    "generacion_duplicates",
    generation["duplicates"]
)
add_check(
    "generacion_missing_plants",
    generation["missing_plants"]
)
add_check(
    "generacion_missing_agents",
    generation["missing_agents"]
)


# Disponibilidad
availability = spark.sql("""
    SELECT
        COUNT(*) - COUNT(DISTINCT disponibilidad_key)
            AS duplicates,
        SUM(CASE WHEN planta_key IS NULL THEN 1 ELSE 0 END)
            AS missing_plants
    FROM observatorio_dev.gold.fact_disponibilidad_planta
""").first()

add_check(
    "disponibilidad_duplicates",
    availability["duplicates"]
)
add_check(
    "disponibilidad_missing_plants",
    availability["missing_plants"]
)


# Precio de bolsa
price = spark.sql("""
    SELECT
        COUNT(*) - COUNT(DISTINCT precio_bolsa_key)
            AS duplicates,
        SUM(
            CASE
                WHEN precio_bolsa_internacional_cop_kwh IS NULL
                  OR precio_bolsa_nacional_cop_kwh IS NULL
                  OR precio_bolsa_tie_cop_kwh IS NULL
                THEN 1 ELSE 0
            END
        ) AS missing_prices
    FROM observatorio_dev.gold.fact_precio_bolsa
""").first()

add_check(
    "precio_duplicates",
    price["duplicates"]
)
add_check(
    "precio_missing_values",
    price["missing_prices"]
)


# Energía embalsada
reservoir_energy = spark.sql("""
    SELECT
        COUNT(*) - COUNT(DISTINCT energia_embalsada_key)
            AS duplicates,
        SUM(CASE WHEN planta_key IS NULL THEN 1 ELSE 0 END)
            AS missing_plants
    FROM observatorio_dev.gold.fact_energia_embalsada_planta
""").first()

add_check(
    "energia_embalsada_duplicates",
    reservoir_energy["duplicates"]
)
add_check(
    "energia_embalsada_missing_plants",
    reservoir_energy["missing_plants"]
)


# Dimensión planta
plant_dimension = spark.sql("""
    SELECT COUNT(*) AS invalid_plants
    FROM (
        SELECT
            codigo_planta,
            SUM(CASE WHEN es_actual THEN 1 ELSE 0 END)
                AS current_versions
        FROM observatorio_dev.gold.dim_planta
        GROUP BY codigo_planta
        HAVING current_versions <> 1
    )
""").first()

add_check(
    "dim_planta_invalid_current_versions",
    plant_dimension["invalid_plants"]
)


# Dimensión agente
agent_dimension = spark.sql("""
    SELECT COUNT(*) AS invalid_agents
    FROM (
        SELECT
            codigo_agente,
            SUM(CASE WHEN es_actual THEN 1 ELSE 0 END)
                AS current_versions
        FROM observatorio_dev.gold.dim_agente
        GROUP BY codigo_agente
        HAVING current_versions <> 1
    )
""").first()

add_check(
    "dim_agente_invalid_current_versions",
    agent_dimension["invalid_agents"]
)


# Bridge planta-embalse
bridge = spark.sql("""
    SELECT
        COUNT(*) - COUNT(DISTINCT planta_embalse_key)
            AS duplicates,
        SUM(
            CASE
                WHEN planta_key IS NULL OR embalse_key IS NULL
                THEN 1 ELSE 0
            END
        ) AS missing_dimensions
    FROM observatorio_dev.gold.bridge_planta_embalse
""").first()

add_check(
    "bridge_duplicates",
    bridge["duplicates"]
)
add_check(
    "bridge_missing_dimensions",
    bridge["missing_dimensions"]
)


results_df = spark.createDataFrame(checks)

display(results_df.orderBy("check"))

failed_checks = [
    row.asDict()
    for row in results_df.filter("passed = false").collect()
]

if failed_checks:
    raise AssertionError(
        f"Fallaron validaciones de calidad: {failed_checks}"
    )

print("Todas las validaciones de calidad finalizaron correctamente.")
