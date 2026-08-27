"""Bronze → Silver ingestion for AdStream."""

from src.processing.bronze_writer import (
    BronzeWriter,
    get_spark,
    resolve_bronze_backend_name,
)
from src.processing.silver_transformer import SilverTransformer
from src.storage.silver import build_silver_backend
from src.utils.logger import get_logger


logger = get_logger("silver_ingestion")


class SilverIngestionPipeline:
    """Transform configured Bronze storage into canonical Silver."""

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

        self.bronze = BronzeWriter(
            spark=self.spark,
            backend_name=self.backend_name,
        )

        self.silver = build_silver_backend(
            self.spark,
            self.backend_name,
        )

        self.transformer = SilverTransformer()

        logger.info(
            "silver_pipeline_ready",
            backend=self.backend_name,
        )

    def run(self) -> dict[str, int]:
        bronze_df = self.bronze.read_bronze()
        bronze_count = bronze_df.count()

        silver_df, quarantine_df = (
            self.transformer.transform(bronze_df)
        )

        usable_count = silver_df.count()
        quarantine_count = quarantine_df.count()

        if bronze_count != usable_count + quarantine_count:
            raise RuntimeError(
                "Silver reconciliation failed: "
                f"bronze={bronze_count}, "
                f"silver={usable_count}, "
                f"quarantine={quarantine_count}"
            )

        written_silver, written_quarantine = (
            self.silver.write(
                silver_df,
                quarantine_df,
            )
        )

        result = {
            "bronze": bronze_count,
            "silver": usable_count,
            "quarantine": quarantine_count,
            "written_silver": written_silver,
            "written_quarantine": written_quarantine,
        }

        logger.info(
            "silver_ingestion_complete",
            backend=self.backend_name,
            **result,
        )

        return result


if __name__ == "__main__":
    SilverIngestionPipeline().run()
