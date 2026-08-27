"""Canonical Silver -> Gold ingestion for AdStream."""

from src.processing.bronze_writer import (
    get_spark,
    resolve_bronze_backend_name,
)
from src.processing.gold_aggregator import GoldAggregator
from src.storage.gold import build_gold_backend
from src.storage.silver import build_silver_backend
from src.utils.logger import get_logger
from pyspark.errors import AnalysisException


logger = get_logger("gold_ingestion")


class GoldIngestionPipeline:
    """Build the three Gold v2 daily aggregate tables."""

    def __init__(
        self,
        spark=None,
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

        self.silver = build_silver_backend(
            self.spark,
            self.backend_name,
        )

        self.gold = build_gold_backend(
            self.spark,
            self.backend_name,
        )

        self.aggregator = GoldAggregator()

    def run(self) -> dict[str, int]:
        silver_df = self.silver.read_silver()

        try:
            quarantine_df = self.silver.read_quarantine()
        except AnalysisException as exc:
            if (
                self.backend_name == "delta"
                and "PATH_NOT_FOUND" in str(exc)
            ):
                quarantine_df = self.spark.createDataFrame(
                    [],
                    "quarantine_id string, event_date date",
                )
            else:
                raise

        silver_count = silver_df.count()
        quarantine_count = quarantine_df.count()

        advertiser_df = (
            self.aggregator.compute_advertiser_daily(
                silver_df
            )
        )

        creative_df = (
            self.aggregator.compute_creative_daily(
                silver_df
            )
        )

        quality_df = (
            self.aggregator.compute_traffic_quality_daily(
                silver_df,
                quarantine_df,
            )
        )

        advertiser_count = advertiser_df.count()
        creative_count = creative_df.count()
        quality_count = quality_df.count()

        self.gold.write(
            advertiser_df,
            creative_df,
            quality_df,
        )

        result = {
            "silver": silver_count,
            "quarantine": quarantine_count,
            "advertiser_daily": advertiser_count,
            "creative_daily": creative_count,
            "traffic_quality_daily": quality_count,
        }

        logger.info(
            "gold_ingestion_complete",
            backend=self.backend_name,
            **result,
        )

        return result


if __name__ == "__main__":
    GoldIngestionPipeline().run()
