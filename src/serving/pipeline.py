"""Production entry point for refreshing AdStream serving tables."""

import os

from src.processing.bronze_writer import get_spark
from src.storage.gold import build_gold_backend
from src.serving.postgres_store import PostgresServingStore
from src.serving.refresh_job import ServingRefreshJob


class ServingPipeline:
    def __init__(
        self,
        database_url: str | None = None,
        schema: str = "serving",
    ):
        self.database_url = (
            database_url
            or os.getenv("SUPABASE_POSTGRES_URL")
        )

        if not self.database_url:
            raise RuntimeError(
                "SUPABASE_POSTGRES_URL is required "
                "for the serving pipeline"
            )

        self.schema = schema

    def run(self) -> dict[str, int]:
        spark = get_spark("iceberg")

        gold_backend = build_gold_backend(
            spark,
            "iceberg",
        )

        serving_store = PostgresServingStore(
            self.database_url,
            schema=self.schema,
        )

        return ServingRefreshJob(
            gold_backend=gold_backend,
            serving_store=serving_store,
        ).run()
