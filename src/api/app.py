import logging
import os
import time
import uuid

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.observability.postgres_store import (
    PostgresPipelineMetricsStore,
)
from src.serving.postgres_store import PostgresServingStore


logger = logging.getLogger("adstream.api")


DASHBOARD_ORIGINS = [
    "http://localhost:8088",
    "http://127.0.0.1:8088",
]


def create_app(
    serving_store=None,
    metrics_store=None,
) -> FastAPI:
    app = FastAPI(
        title="AdStream Query API",
        version="1.0.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=DASHBOARD_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    database_url = os.getenv(
        "SUPABASE_POSTGRES_URL"
    )

    store = serving_store

    if store is None:
        if not database_url:
            raise RuntimeError(
                "SUPABASE_POSTGRES_URL is required "
                "for the query API"
            )

        store = PostgresServingStore(
            database_url
        )

    pipeline_metrics = metrics_store

    if (
        pipeline_metrics is None
        and database_url
    ):
        pipeline_metrics = (
            PostgresPipelineMetricsStore(
                database_url
            )
        )

    @app.middleware("http")
    async def request_observability(
        request,
        call_next,
    ):
        request_id = (
            request.headers.get(
                "X-Request-ID"
            )
            or str(uuid.uuid4())
        )

        started = time.perf_counter()

        response = await call_next(
            request
        )

        duration_ms = (
            time.perf_counter()
            - started
        ) * 1000

        response.headers[
            "X-Request-ID"
        ] = request_id

        response.headers[
            "X-Process-Time-Ms"
        ] = f"{duration_ms:.3f}"

        logger.info(
            "http_request "
            "request_id=%s "
            "method=%s "
            "path=%s "
            "status=%s "
            "duration_ms=%.3f",
            request_id,
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )

        return response

    @app.get("/health")
    def health():
        return {
            "status": "ok"
        }

    @app.get("/ready")
    def ready():
        try:
            store.ping()

        except Exception:
            logger.exception(
                "serving_database_readiness_failed"
            )

            return JSONResponse(
                status_code=503,
                content={
                    "status": "not_ready",
                    "serving_database": (
                        "unavailable"
                    ),
                },
            )

        return {
            "status": "ready",
            "serving_database": "ok",
        }

    @app.get(
        "/api/v1/advertisers/daily"
    )
    def advertiser_daily(
        event_date: str | None = None,
        advertiser_id: str | None = None,
    ):
        return store.list_advertiser_daily(
            event_date=event_date,
            advertiser_id=advertiser_id,
        )

    @app.get(
        "/api/v1/creatives/daily"
    )
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

    @app.get(
        "/api/v1/traffic-quality/daily"
    )
    def traffic_quality_daily(
        event_date: str | None = None,
    ):
        return (
            store.list_traffic_quality_daily(
                event_date=event_date,
            )
        )

    @app.get(
        "/api/v1/pipeline-health"
    )
    def pipeline_health():
        try:
            store.ping()
            database_status = "ready"

        except Exception:
            database_status = (
                "unavailable"
            )

        system = {
            "api": "healthy",
            "serving_database": (
                database_status
            ),
        }

        if pipeline_metrics is None:
            return {
                "system": system,
                "latest_run": None,
                "stages": [],
                "recent_runs": [],
            }

        recent_runs = (
            pipeline_metrics.list_recent_runs(
                limit=10
            )
        )

        if not recent_runs:
            return {
                "system": system,
                "latest_run": None,
                "stages": [],
                "recent_runs": [],
            }

        latest_run = recent_runs[0]

        stages = (
            pipeline_metrics.list_stage_runs(
                run_id=latest_run[
                    "run_id"
                ]
            )
        )

        return {
            "system": system,
            "latest_run": latest_run,
            "stages": stages,
            "recent_runs": recent_runs,
        }

    return app
