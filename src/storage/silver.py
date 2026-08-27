"""Persistence backends for canonical AdStream Silver data."""

import os

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


SILVER_PATH = "data/silver/impressions"
SILVER_QUARANTINE_PATH = "data/silver/quarantine"

ICEBERG_SILVER_TABLE = "supabase.silver.impressions"
ICEBERG_QUARANTINE_TABLE = "supabase.silver.quarantine"
ICEBERG_NAMESPACE = "supabase.silver"


class DeltaSilverBackend:
    """Local Delta Silver persistence with idempotent keys."""

    def __init__(
        self,
        spark: SparkSession,
        silver_path: str = SILVER_PATH,
        quarantine_path: str = SILVER_QUARANTINE_PATH,
    ):
        self.spark = spark
        self.silver_path = silver_path
        self.quarantine_path = quarantine_path

    def _append_new(
        self,
        df: DataFrame,
        path: str,
        key: str,
        partition_by: str | None = None,
    ) -> int:
        if DeltaTable.isDeltaTable(self.spark, path):
            existing = (
                self.spark.read
                .format("delta")
                .load(path)
                .select(key)
            )
            df = df.join(existing, key, "left_anti")

        count = df.count()
        if count == 0:
            return 0

        writer = (
            df.write
            .format("delta")
            .mode("append")
        )

        if partition_by:
            writer = writer.partitionBy(partition_by)

        writer.save(path)
        return count

    def write(
        self,
        silver_df: DataFrame,
        quarantine_df: DataFrame,
    ) -> tuple[int, int]:
        silver_count = self._append_new(
            silver_df,
            self.silver_path,
            "event_id",
            "event_date",
        )

        quarantine_count = self._append_new(
            quarantine_df,
            self.quarantine_path,
            "quarantine_id",
            "ingestion_date",
        )

        return silver_count, quarantine_count

    def read_silver(self) -> DataFrame:
        return (
            self.spark.read
            .format("delta")
            .load(self.silver_path)
        )

    def read_quarantine(self) -> DataFrame:
        return (
            self.spark.read
            .format("delta")
            .load(self.quarantine_path)
        )


class IcebergSilverBackend:
    """Supabase Analytics / Iceberg Silver persistence."""

    def __init__(
        self,
        spark: SparkSession,
        silver_table: str = ICEBERG_SILVER_TABLE,
        quarantine_table: str = ICEBERG_QUARANTINE_TABLE,
    ):
        self.spark = spark
        self.silver_table = silver_table
        self.quarantine_table = quarantine_table

    def _ensure_table(
        self,
        df: DataFrame,
        table: str,
        partition_by: str | None = None,
    ) -> None:
        self.spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_NAMESPACE}"
        )

        columns_sql = ",\n    ".join(
            f"`{field.name}` {field.dataType.simpleString()}"
            for field in df.schema.fields
        )

        partition_clause = (
            f"\nPARTITIONED BY (`{partition_by}`)"
            if partition_by
            else ""
        )

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {columns_sql}
            )
            USING iceberg
            {partition_clause}
            """
        )

    def _append_new(
        self,
        df: DataFrame,
        table: str,
        key: str,
        view: str,
        partition_by: str | None = None,
    ) -> int:
        self._ensure_table(df, table, partition_by)

        existing = self.spark.table(table).select(key)
        new_df = df.join(existing, key, "left_anti")

        count = new_df.count()
        if count == 0:
            return 0

        new_df.createOrReplaceTempView(view)

        self.spark.sql(
            f"""
            INSERT INTO {table}
            SELECT *
            FROM {view}
            """
        )

        return count

    def write(
        self,
        silver_df: DataFrame,
        quarantine_df: DataFrame,
    ) -> tuple[int, int]:
        silver_count = self._append_new(
            silver_df,
            self.silver_table,
            "event_id",
            "adstream_silver_batch",
            "event_date",
        )

        quarantine_count = self._append_new(
            quarantine_df,
            self.quarantine_table,
            "quarantine_id",
            "adstream_quarantine_batch",
            "ingestion_date",
        )

        return silver_count, quarantine_count

    def read_silver(self) -> DataFrame:
        return self.spark.table(self.silver_table)

    def read_quarantine(self) -> DataFrame:
        return self.spark.table(self.quarantine_table)


def build_silver_backend(
    spark: SparkSession,
    backend_name: str,
):
    if backend_name == "delta":
        return DeltaSilverBackend(spark)

    if backend_name == "iceberg":
        return IcebergSilverBackend(spark)

    raise ValueError(
        f"Unknown Silver backend {backend_name!r}; "
        "expected one of: delta, iceberg"
    )
