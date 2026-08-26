from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, BooleanType, TimestampType
)
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from src.models.events import ImpressionEvent
from src.processing.spark import get_storage_spark_config
from src.storage.bronze import build_bronze_backend
from src.utils.logger import get_logger
from datetime import datetime
import os

logger = get_logger("bronze_writer")

BRONZE_PATH = "data/bronze/impressions"


def resolve_bronze_backend_name(env=None) -> str:
    """Return the explicitly configured Bronze storage backend."""
    env = os.environ if env is None else env
    backend = env.get("ADSTREAM_STORAGE_BACKEND", "delta").strip().lower()

    if backend not in {"delta", "iceberg"}:
        raise ValueError(
            "Invalid ADSTREAM_STORAGE_BACKEND "
            f"{backend!r}; expected one of: delta, iceberg"
        )

    return backend


def get_spark(
    backend_name: str | None = None,
) -> SparkSession:
    """Create a resource-constrained SparkSession for the selected backend."""
    backend_name = (
        backend_name
        or resolve_bronze_backend_name()
    )

    storage_config = get_storage_spark_config(
        backend_name
    )

    builder = (
        SparkSession.builder
        .appName("AdStream-BronzeIngestion")
        .master("local[2]")  # two local Spark worker threads
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.showConsoleProgress", "false")
    )

    for key, value in storage_config.items():
        builder = builder.config(key, value)

    if backend_name == "delta":
        return configure_spark_with_delta_pip(
            builder
        ).getOrCreate()

    # Reproduce the verified Supabase Iceberg environment.
    region = os.environ["SUPABASE_S3_REGION"]
    os.environ["AWS_REGION"] = region
    os.environ["AWS_DEFAULT_REGION"] = region

    return builder.getOrCreate()


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
    StructField("paying_price",   DoubleType(),    nullable=True),
    StructField("pricing_basis",  StringType(),    nullable=True),
    StructField("clicked",        BooleanType(),   nullable=True),
    StructField("source_dataset", StringType(),    nullable=True),
    StructField("source_bid_id",  StringType(),    nullable=True),
    StructField("ad_exchange",    StringType(),    nullable=True),
    StructField("slot_id",        StringType(),    nullable=True),
    StructField("source_user_agent", StringType(), nullable=True),
])


class BronzeWriter:
    """Prepare validated impression events and persist them to Bronze."""

    def __init__(
        self,
        spark: SparkSession = None,
        backend_name: str | None = None,
    ):
        self.backend_name = (
            backend_name
            or resolve_bronze_backend_name()
        )

        self.spark = (
            spark
            or get_spark(self.backend_name)
        )

        self.backend = build_bronze_backend(
            self.spark,
            self.backend_name,
        )

        logger.info(
            "bronze_writer_ready",
            backend=self.backend_name,
        )

    def write_batch(
        self,
        events: list[ImpressionEvent],
    ) -> int:
        """Persist one validated impression batch to Bronze."""

        if not events:
            logger.debug("empty_batch_skipped")
            return 0

        rows = []

        for event in events:
            row = event.model_dump(mode="json")

            row["timestamp"] = datetime.fromisoformat(
                row["timestamp"].replace(
                    "Z",
                    "+00:00",
                )
            )

            rows.append(row)

        df: DataFrame = self.spark.createDataFrame(
            rows,
            schema=IMPRESSION_SCHEMA,
        )

        df = df.withColumn(
            "ingestion_date",
            F.to_date(F.col("timestamp")),
        )

        self.backend.write(df)

        logger.info(
            "batch_written_to_bronze",
            count=len(events),
            backend=self.backend_name,
            fraud_count=sum(
                1
                for event in events
                if event.is_fraud
            ),
        )

        return len(events)

    def read_bronze(self) -> DataFrame:
        """Read Bronze through the configured persistence backend."""
        return self.backend.read()

    def get_row_count(self) -> int:
        """Return total rows in the configured Bronze table."""
        return self.read_bronze().count()

