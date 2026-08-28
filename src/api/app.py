import os

from fastapi import FastAPI

from src.serving.postgres_store import PostgresServingStore


def create_app(serving_store=None) -> FastAPI:
    app = FastAPI(
        title="AdStream Query API",
        version="1.0.0",
    )

    store = serving_store

    if store is None:
        database_url = os.getenv("SUPABASE_POSTGRES_URL")

        if not database_url:
            raise RuntimeError(
                "SUPABASE_POSTGRES_URL is required "
                "for the query API"
            )

        store = PostgresServingStore(database_url)

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.get("/api/v1/advertisers/daily")
    def advertiser_daily(
        event_date: str | None = None,
        advertiser_id: str | None = None,
    ):
        return store.list_advertiser_daily(
            event_date=event_date,
            advertiser_id=advertiser_id,
        )

    @app.get("/api/v1/creatives/daily")
    def creative_daily(
        event_date: str | None = None,
        advertiser_id: str | None = None,
        creative_id: str | None = None,
    ):
        return store.list_creative_daily(
            event_date=event_date,
            advertiser_id=advertiser_id,
            creative_id=creative_id,
        )

    @app.get("/api/v1/traffic-quality/daily")
    def traffic_quality_daily(
        event_date: str | None = None,
    ):
        return store.list_traffic_quality_daily(
            event_date=event_date,
        )

    return app
