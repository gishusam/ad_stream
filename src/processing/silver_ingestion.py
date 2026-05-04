from src.processing.bronze_writer import get_spark, BRONZE_PATH
from src.processing.silver_transformer import (
    SilverTransformer,
    SILVER_LEGITIMATE_PATH,
    SILVER_FRAUD_PATH,
    SILVER_QUARANTINE_PATH,
)
from src.processing.user_profiles import generate_user_profiles
from src.utils.logger import get_logger
import os

logger = get_logger("silver_ingestion")


class SilverIngestionPipeline:
    """
    Orchestrates Bronze → Silver transformation.

    Reads from Bronze Delta Lake, applies all four Silver
    transformations, writes three output tables.

    This runs as a scheduled batch job — in Week 4 Airflow
    will trigger this every 15 minutes automatically.
    For now we run it manually.
    """

    def __init__(self):
        self.spark       = get_spark()
        self.transformer = SilverTransformer()
        self.user_profiles = generate_user_profiles(self.spark)

        # Create output directories
        for path in [
            SILVER_LEGITIMATE_PATH,
            SILVER_FRAUD_PATH,
            SILVER_QUARANTINE_PATH,
        ]:
            os.makedirs(path, exist_ok=True)

        logger.info("silver_pipeline_ready")

    def read_bronze(self):
        """Read entire Bronze table."""
        df = self.spark.read.format("delta").load(BRONZE_PATH)
        logger.info("bronze_read", rows=df.count())
        return df

    def write_silver(
        self,
        legitimate_df,
        fraud_df,
        quarantine_df,
    ):
        """
        Write all three Silver tables to Delta Lake.

        Why overwrite mode for Silver?
        Silver is recomputed from Bronze every run.
        If we fix a bug in the transformer, we rerun Silver
        from scratch and overwrite the previous version.
        Bronze is append-only. Silver is recomputable.
        This is the key architectural difference.
        """
        logger.info("writing_silver_tables")

        (
            legitimate_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ingestion_date")
            .save(SILVER_LEGITIMATE_PATH)
        )
        logger.info(
            "silver_legitimate_written",
            path=SILVER_LEGITIMATE_PATH,
        )

        (
            fraud_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("ingestion_date")
            .save(SILVER_FRAUD_PATH)
        )
        logger.info(
            "silver_fraud_written",
            path=SILVER_FRAUD_PATH,
        )

        if quarantine_df.count() > 0:
            (
                quarantine_df.write
                .format("delta")
                .mode("overwrite")
                .save(SILVER_QUARANTINE_PATH)
            )
            logger.info(
                "silver_quarantine_written",
                path=SILVER_QUARANTINE_PATH,
            )

    def run(self):
        """Full Bronze → Silver pipeline run."""
        logger.info("silver_ingestion_started")

        # Read
        bronze_df = self.read_bronze()

        # Transform
        legitimate_df, fraud_df, quarantine_df = (
            self.transformer.transform(bronze_df, self.user_profiles)
        )

        # Write
        self.write_silver(legitimate_df, fraud_df, quarantine_df)

        logger.info(
            "silver_ingestion_complete",
            legitimate=legitimate_df.count(),
            fraud=fraud_df.count(),
            quarantined=quarantine_df.count(),
        )


if __name__ == "__main__":
    pipeline = SilverIngestionPipeline()
    pipeline.run()