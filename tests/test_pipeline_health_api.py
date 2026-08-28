from fastapi.testclient import TestClient

from src.api.app import create_app


class FakeServingStore:
    def ping(self):
        return True

    def list_advertiser_daily(self, **kwargs):
        return []

    def list_creative_daily(self, **kwargs):
        return []

    def list_traffic_quality_daily(self, **kwargs):
        return []


class FakeMetricsStore:
    def list_recent_runs(self, limit=10):
        return [
            {
                "run_id": "run-123",
                "status": "success",
                "duration_ms": 120.5,
                "recorded_at": "2026-08-28T09:00:00+00:00",
            },
            {
                "run_id": "run-122",
                "status": "failed",
                "duration_ms": 98.2,
                "recorded_at": "2026-08-28T08:00:00+00:00",
            },
        ]

    def list_stage_runs(self, run_id):
        assert run_id == "run-123"

        return [
            {
                "run_id": "run-123",
                "stage": "silver",
                "status": "success",
                "duration_ms": 40.0,
                "result": {"written": 1000},
                "error_type": None,
                "recorded_at": "2026-08-28T09:00:00+00:00",
            },
            {
                "run_id": "run-123",
                "stage": "gold",
                "status": "success",
                "duration_ms": 30.0,
                "result": {"advertiser_daily": 1},
                "error_type": None,
                "recorded_at": "2026-08-28T09:00:01+00:00",
            },
            {
                "run_id": "run-123",
                "stage": "quality",
                "status": "success",
                "duration_ms": 10.0,
                "result": {"status": "passed"},
                "error_type": None,
                "recorded_at": "2026-08-28T09:00:02+00:00",
            },
            {
                "run_id": "run-123",
                "stage": "serving",
                "status": "success",
                "duration_ms": 40.5,
                "result": {"advertiser_daily": 1},
                "error_type": None,
                "recorded_at": "2026-08-28T09:00:03+00:00",
            },
        ]


def build_client():
    app = create_app(
        serving_store=FakeServingStore(),
        metrics_store=FakeMetricsStore(),
    )

    return TestClient(app)


def test_pipeline_health_returns_latest_run():
    client = build_client()

    response = client.get("/api/v1/pipeline-health")

    assert response.status_code == 200

    body = response.json()

    assert body["latest_run"]["run_id"] == "run-123"
    assert body["latest_run"]["status"] == "success"
    assert body["latest_run"]["duration_ms"] == 120.5


def test_pipeline_health_returns_stage_breakdown():
    client = build_client()

    response = client.get("/api/v1/pipeline-health")

    body = response.json()

    assert len(body["stages"]) == 4
    assert body["stages"][0]["stage"] == "silver"
    assert body["stages"][-1]["stage"] == "serving"


def test_pipeline_health_returns_recent_runs():
    client = build_client()

    response = client.get("/api/v1/pipeline-health")

    body = response.json()

    assert len(body["recent_runs"]) == 2
    assert body["recent_runs"][1]["status"] == "failed"


def test_pipeline_health_reports_system_health():
    client = build_client()

    response = client.get("/api/v1/pipeline-health")

    body = response.json()

    assert body["system"] == {
        "api": "healthy",
        "serving_database": "ready",
    }


def test_pipeline_health_handles_no_runs():
    class EmptyMetricsStore:
        def list_recent_runs(self, limit=10):
            return []

        def list_stage_runs(self, run_id):
            return []

    app = create_app(
        serving_store=FakeServingStore(),
        metrics_store=EmptyMetricsStore(),
    )

    client = TestClient(app)

    response = client.get("/api/v1/pipeline-health")

    assert response.status_code == 200

    assert response.json() == {
        "system": {
            "api": "healthy",
            "serving_database": "ready",
        },
        "latest_run": None,
        "stages": [],
        "recent_runs": [],
    }
