from src.processing.bronze_writer import get_spark
from src.processing.silver_transformer import (
    SILVER_LEGITIMATE_PATH,
    SILVER_FRAUD_PATH,
)
from src.processing.gold_aggregator import (
    GoldAggregator,
    GOLD_REVENUE_PATH,
    GOLD_CONTENT_PATH,
    GOLD_FRAUD_PATH,
)
from src.utils.logger import get_logger
import os

logger = get_logger("gold_ingestion")


class GoldIngestionPipeline:
    """
    Orchestrates Silver → Gold aggregation.

    Reads from Silver Delta Lake tables, computes three
    Gold aggregations, writes results to Gold Delta Lake.

    This runs after every Silver update. In Week 4,
    Airflow will chain Silver → Gold automatically.
    Silver finishes → Gold starts. No manual steps.
    """

    def __init__(self):
        self.spark       = get_spark()
        self.aggregator  = GoldAggregator()

        for path in [
            GOLD_REVENUE_PATH,
            GOLD_CONTENT_PATH,
            GOLD_FRAUD_PATH,
        ]:
            os.makedirs(path, exist_ok=True)

        logger.info("gold_pipeline_ready")

    def read_silver(self):
        """Read Silver legitimate and fraud tables."""
        legitimate_df = (
            self.spark.read
            .format("delta")
            .load(SILVER_LEGITIMATE_PATH)
        )
        fraud_df = (
            self.spark.read
            .format("delta")
            .load(SILVER_FRAUD_PATH)
        )
        logger.info(
            "silver_read",
            legitimate=legitimate_df.count(),
            fraud=fraud_df.count(),
        )
        return legitimate_df, fraud_df

    def write_gold(self, df, path: str, table_name: str):
        """
        Write a Gold table to Delta Lake.

        Gold uses overwrite mode — same as Silver.
        Gold is always recomputed from Silver.
        If Silver is reprocessed, Gold is reprocessed too.
        The chain is: Bronze → Silver → Gold.
        Fix Bronze, rerun Silver, rerun Gold. Clean.
        """
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .save(path)
        )
        logger.info(
            "gold_table_written",
            table=table_name,
            path=path,
            rows=df.count(),
        )

    def run(self):
        """Full Silver → Gold pipeline run."""
        logger.info("gold_ingestion_started")

        # Read Silver
        legitimate_df, fraud_df = self.read_silver()

        # Compute Gold tables
        revenue_df = self.aggregator.compute_revenue_by_advertiser(
            legitimate_df
        )
        content_df = self.aggregator.compute_content_performance(
            legitimate_df
        )
        fraud_summary_df = self.aggregator.compute_fraud_summary(
            fraud_df
        )

        # Write Gold tables
        self.write_gold(revenue_df,      GOLD_REVENUE_PATH, "revenue_by_advertiser")
        self.write_gold(content_df,      GOLD_CONTENT_PATH, "content_performance")
        self.write_gold(fraud_summary_df, GOLD_FRAUD_PATH,  "fraud_summary")

        logger.info(
            "gold_ingestion_complete",
            revenue_rows=revenue_df.count(),
            content_rows=content_df.count(),
            fraud_rows=fraud_summary_df.count(),
        )


if __name__ == "__main__":
    pipeline = GoldIngestionPipeline()
    pipeline.run()