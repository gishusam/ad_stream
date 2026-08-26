"""Persistence backends for AdStream Bronze impression data."""

from pyspark.sql import DataFrame, SparkSession

BRONZE_PATH = "data/bronze/impressions"
ICEBERG_TABLE = "supabase.bronze.impressions"
ICEBERG_NAMESPACE = "supabase.bronze"
ICEBERG_BATCH_VIEW = "adstream_bronze_batch"


class DeltaBronzeBackend:
    """Local Delta Lake Bronze persistence."""

    def __init__(
        self,
        spark: SparkSession,
        path: str = BRONZE_PATH,
    ):
        self.spark = spark
        self.path = path

    def write(self, df: DataFrame) -> None:
        (
            df.write
            .format("delta")
            .mode("append")
            .partitionBy("ingestion_date")
            .save(self.path)
        )

    def read(self) -> DataFrame:
        return (
            self.spark.read
            .format("delta")
            .load(self.path)
        )


class IcebergBronzeBackend:
    """Supabase Analytics / Iceberg Bronze persistence."""

    def __init__(
        self,
        spark: SparkSession,
        table: str = ICEBERG_TABLE,
    ):
        self.spark = spark
        self.table = table

    def _ensure_table(self, df: DataFrame) -> None:
        self.spark.sql(
            f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_NAMESPACE}"
        )

        columns_sql = ",\n    ".join(
            f"`{field.name}` {field.dataType.simpleString()}"
            for field in df.schema.fields
        )

        field_names = {
            field.name
            for field in df.schema.fields
        }

        partition_clause = (
            "\nPARTITIONED BY (`ingestion_date`)"
            if "ingestion_date" in field_names
            else ""
        )

        self.spark.sql(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                {columns_sql}
            )
            USING iceberg
            {partition_clause}
            """
        )

    def write(self, df: DataFrame) -> None:
        # Supabase's current Iceberg REST catalog does not support
        # Spark's staged CTAS flow, so table creation and insertion
        # remain deliberately separate operations.
        self._ensure_table(df)

        df.createOrReplaceTempView(ICEBERG_BATCH_VIEW)

        self.spark.sql(
            f"""
            INSERT INTO {self.table}
            SELECT *
            FROM {ICEBERG_BATCH_VIEW}
            """
        )

    def read(self) -> DataFrame:
        return self.spark.table(self.table)


def build_bronze_backend(
    spark: SparkSession,
    backend: str,
):
    """Create the configured Bronze persistence backend."""

    if backend == "delta":
        return DeltaBronzeBackend(spark)

    if backend == "iceberg":
        return IcebergBronzeBackend(spark)

    raise ValueError(
        f"Unknown Bronze backend {backend!r}; "
        "expected one of: delta, iceberg"
    )
