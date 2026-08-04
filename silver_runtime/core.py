"""Reusable primitives that keep Silver notebooks to a small number of actions."""

from __future__ import annotations

from typing import Any, Iterable

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def require_table_contract(spark: Any, table: str, required: Iterable[str]) -> None:
    if not spark.catalog.tableExists(table):
        raise ValueError(f"No existe la tabla requerida: {table}")
    missing = set(required) - set(spark.table(table).columns)
    if missing:
        raise ValueError(f"{table} no contiene las columnas: {sorted(missing)}")


def incremental_source(spark: Any, source_table: str, target_table: str):
    watermark = (
        spark.table(target_table)
        .agg(F.max("ingestion_timestamp").alias("watermark"))
        .first()["watermark"]
    )
    source = spark.table(source_table)
    if watermark is not None:
        source = source.filter(F.col("ingestion_timestamp") >= F.lit(watermark))
    return source, watermark


def input_profile(dataframe, invalid_key, invalid_value=None, value_column=None):
    invalid_value = invalid_value if invalid_value is not None else F.lit(False)
    expressions = [
        F.count("*").alias("rows_received"),
        F.sum(F.when(invalid_key, 1).otherwise(0)).alias("invalid_keys"),
        F.sum(F.when(invalid_value, 1).otherwise(0)).alias("invalid_values"),
    ]
    if value_column:
        expressions.extend([
            F.sum(F.when(F.col(value_column) < 0, 1).otherwise(0)).alias("negative_values"),
            F.sum(F.when(F.col(value_column) == 0, 1).otherwise(0)).alias("zero_values"),
        ])
    return dataframe.agg(*expressions).first()


def invalid_key_condition(key: list[str], timestamp_columns: Iterable[str] = ()):
    """Build one predicate for null/blank business keys."""
    timestamps = set(timestamp_columns)
    condition = F.lit(False)
    for column in key:
        condition = condition | F.col(column).isNull()
        if column not in timestamps:
            condition = condition | (F.col(column) == "")
    return condition


def with_record_hash(dataframe, columns: list[str]):
    return dataframe.withColumn(
        "record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                *[
                    F.coalesce(F.col(column).cast("string"), F.lit(""))
                    for column in columns
                ],
            ),
            256,
        ),
    )


def deduplicate(dataframe, key: list[str]):
    ordering = [
        F.col("ingestion_timestamp").desc_nulls_last(),
        F.col("load_date").desc_nulls_last(),
    ]
    if "record_hash" in dataframe.columns:
        ordering.append(F.col("record_hash").desc_nulls_last())
    window = Window.partitionBy(*key).orderBy(*ordering)
    return (
        dataframe.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number", "record_hash")
    )


def merge_delta(
    spark: Any,
    table: str,
    source,
    key: list[str],
    mutable_columns: list[str],
    insert_columns: list[str],
) -> None:
    change_condition = " OR ".join(
        f"NOT (target.{column} <=> source.{column})" for column in mutable_columns
    )
    (
        DeltaTable.forName(spark, table).alias("target")
        .merge(
            source.alias("source"),
            " AND ".join(f"target.{column} = source.{column}" for column in key),
        )
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                **{column: f"source.{column}" for column in mutable_columns},
                "silver_updated_at": "source.silver_updated_at",
            },
        )
        .whenNotMatchedInsert(
            values={column: f"source.{column}" for column in insert_columns}
        )
        .execute()
    )


def delta_operation(spark: Any, table: str) -> dict:
    row = (
        spark.sql(f"DESCRIBE HISTORY {table}")
        .select("version", "timestamp", "operation", "operationMetrics")
        .limit(1)
        .first()
    )
    return row.asDict(recursive=True)


def final_profile(spark: Any, table: str, key: list[str], date_column: str):
    row = (
        spark.table(table)
        .agg(
            F.count("*").alias("total_rows"),
            F.countDistinct(F.struct(*[F.col(column) for column in key])).alias(
                "distinct_keys"
            ),
            F.min(date_column).alias("min_event_time"),
            F.max(date_column).alias("max_event_time"),
        )
        .first()
    )
    duplicates = int(row["total_rows"] or 0) - int(row["distinct_keys"] or 0)
    if duplicates:
        raise ValueError(f"{table} contiene {duplicates:,} llaves duplicadas")
    return row.asDict(recursive=True)
