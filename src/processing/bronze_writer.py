from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, TimestampType
)
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from src.models.events import ImpressionEvent
from src.utils.logger import get_logger
import os

logger = get_logger("bronze_writer")

BRONZE_PATH = "data/bronze/impressions"


def get_spark() -> SparkSession:
    """
    Create or retrieve the SparkSession with Delta Lake support.

    Why a function instead of a global?
    SparkSession is expensive to create. We create it once and reuse it.
    The function pattern lets us mock it cleanly in tests.

    The Delta Lake config tells Spark:
    - Use Delta as the default table format
    - Enable Delta-specific SQL extensions
    """
    builder = (
        SparkSession.builder
        .appName("AdStream-BronzeIngestion")
        .master("local[*]")          # use all CPU cores on local machine
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")   # small for local dev
        .config("spark.ui.showConsoleProgress", "false")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# Explicit schema — never infer schema from data in production
# Inferred schemas change silently when data changes. Explicit schemas fail loudly.
IMPRESSION_SCHEMA = StructType([
    StructField("impression_id",  StringType(),    nullable=False),
    StructField("user_id",        StringType(),    nullable=False),
    StructField("advertiser_id",  StringType(),    nullable=False),
    StructField("campaign_id",    StringType(),    nullable=False),
    StructField("content_id",     StringType(),    nullable=False),
    StructField("bid_price",      DoubleType(),    nullable=False),
    StructField("currency",       StringType(),    nullable=False),
    StructField("country_code",   StringType(),    nullable=False),
    StructField("device_type",    StringType(),    nullable=False),
    StructField("ad_format",      StringType(),    nullable=False),
    StructField("timestamp",      TimestampType(), nullable=False),
    StructField("is_fraud",       BooleanType(),   nullable=False),
])


class BronzeWriter:
    """
    Writes validated impression events to Delta Lake Bronze layer.

    Design decisions:
    1. Explicit schema — no schema inference ever
    2. Partition by date — enables partition pruning on time queries
    3. Append mode — Bronze is immutable, we never update or delete
    4. Idempotent writes — if the same batch is written twice (replay),
       Delta's transaction log prevents duplicate files corrupting the table
    """

    def __init__(self, spark: SparkSession = None):
        self.spark = spark or get_spark()
        os.makedirs(BRONZE_PATH, exist_ok=True)
        logger.info("bronze_writer_ready", path=BRONZE_PATH)

    def write_batch(self, events: list[ImpressionEvent]) -> int:
        """
        Write a batch of ImpressionEvents to Delta Lake Bronze.

        Args:
            events: list of validated ImpressionEvent objects

        Returns:
            count of events written

        Steps:
        1. Convert Pydantic objects to plain dicts
        2. Create a Spark DataFrame with our explicit schema
        3. Add ingestion_date partition column
        4. Write to Delta in append mode, partitioned by date
        """
        if not events:
            logger.debug("empty_batch_skipped")
            return 0

        # Convert Pydantic models to plain dicts
        rows = [event.model_dump(mode="json") for event in events]

        # Create DataFrame with explicit schema
        df: DataFrame = self.spark.createDataFrame(rows, schema=IMPRESSION_SCHEMA)

        # Add partition column — extract date from timestamp
        # This is how Spark knows which folder to write each row into
        df = df.withColumn(
            "ingestion_date",
            F.to_date(F.col("timestamp"))
        )

        # Write to Delta Lake
        # partitionBy creates folder structure: ingestion_date=2026-04-27/
        (
            df.write
            .format("delta")
            .mode("append")
            .partitionBy("ingestion_date")
            .save(BRONZE_PATH)
        )

        logger.info(
            "batch_written_to_bronze",
            count=len(events),
            path=BRONZE_PATH,
            fraud_count=sum(1 for e in events if e.is_fraud),
        )

        return len(events)

    def read_bronze(self) -> DataFrame:
        """
        Read the entire Bronze table back as a Spark DataFrame.
        Used for verification and downstream Silver processing.
        """
        return self.spark.read.format("delta").load(BRONZE_PATH)

    def get_row_count(self) -> int:
        """Return total rows in Bronze table."""
        return self.read_bronze().count()