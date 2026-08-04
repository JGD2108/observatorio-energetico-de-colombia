"""Load governed rules and translate them into Spark expressions."""

from __future__ import annotations

from typing import Any

from pyspark.sql import functions as F


def load_tx_policy(spark: Any, table: str) -> dict[str, int]:
    """Return the active TX policy after validating its governed contract."""
    rows = (
        spark.table(table)
        .filter(F.col("is_active"))
        .select("rule_code", "priority", "multiplier")
        .collect()
    )
    policy = {
        row["rule_code"]: int(
            row["priority"] if row["priority"] is not None else row["multiplier"]
        )
        for row in rows
    }
    required = {"TXF", "TXR", "TX_NUMERIC"}
    if len(rows) != len(required) or set(policy) != required:
        raise ValueError(
            f"Política TX incompleta o duplicada en {table}: "
            f"esperada={sorted(required)}, encontrada={sorted(policy)}"
        )
    return policy


def tx_priority_expression(version_column: str, policy: dict[str, int]):
    """Build the canonical priority expression from the governed policy."""
    version = F.upper(F.trim(F.col(version_column)))
    numeric_tx = F.regexp_extract(version, r"^TX([0-9]+)$", 1).cast("int")
    return (
        F.when(version == "TXF", F.lit(policy["TXF"]))
        .when(version == "TXR", F.lit(policy["TXR"]))
        .when(version.rlike(r"^TX[0-9]+$"), numeric_tx * policy["TX_NUMERIC"])
        .otherwise(F.lit(0))
    )
