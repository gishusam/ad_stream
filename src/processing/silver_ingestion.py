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


def _count_persisted_batch(
    expected_df,
    persisted_df,
    key: str,
) -> int:
    """Count expected batch keys confirmed present in persisted storage."""

    expected_keys = (
        expected_df
        .select(key)
        .dropDuplicates([key])
    )

    persisted_keys = (
        persisted_df
        .select(key)
        .dropDuplicates([key])
    )

    return (
        expected_keys
        .join(
            persisted_keys,
            key,
            "inner",
        )
        .count()
    )


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

        if (
            bronze_count
            != usable_count + quarantine_count
        ):
            raise RuntimeError(
                "Silver reconciliation failed: "
                f"bronze={bronze_count}, "
                f"silver={usable_count}, "
                f"quarantine={quarantine_count}"
            )

        (
            inserted_silver,
            inserted_quarantine,
        ) = self.silver.write(
            silver_df,
            quarantine_df,
        )

        persisted_silver = (
            _count_persisted_batch(
                silver_df,
                self.silver.read_silver(),
                "event_id",
            )
        )

        persisted_quarantine = (
            _count_persisted_batch(
                quarantine_df,
                self.silver.read_quarantine(),
                "quarantine_id",
            )
        )

        result = {
            "bronze": bronze_count,
            "silver": usable_count,
            "quarantine": quarantine_count,

            # Physical inserts performed during this run.
            "inserted_silver": inserted_silver,
            "inserted_quarantine": (
                inserted_quarantine
            ),

            # Current batch rows confirmed present after write.
            "written_silver": persisted_silver,
            "written_quarantine": (
                persisted_quarantine
            ),
        }

        logger.info(
            "silver_ingestion_complete",
            backend=self.backend_name,
            **result,
        )

        return result


if __name__ == "__main__":
    SilverIngestionPipeline().run()
