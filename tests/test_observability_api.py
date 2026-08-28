from fastapi.testclient import TestClient

from src.api.app import create_app


class HealthyServingStore:
    def ping(self):
        return True

    def list_advertiser_daily(self, **kwargs):
        return []

    def list_creative_daily(self, **kwargs):
        return []

    def list_traffic_quality_daily(self, **kwargs):
        return []


class UnhealthyServingStore(HealthyServingStore):
    def ping(self):
        raise RuntimeError("database unavailable")


def test_health_reports_process_alive():
    app = create_app(serving_store=HealthyServingStore())
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_ready_reports_serving_database_available():
    app = create_app(serving_store=HealthyServingStore())
    client = TestClient(app)

    response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "serving_database": "ok",
    }


def test_ready_returns_503_when_serving_database_is_unavailable():
    app = create_app(serving_store=UnhealthyServingStore())
    client = TestClient(app)

    response = client.get("/ready")

    assert response.status_code == 503
    assert response.json() == {
        "status": "not_ready",
        "serving_database": "unavailable",
    }


def test_responses_include_request_id():
    app = create_app(serving_store=HealthyServingStore())
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.headers["x-request-id"]


def test_request_id_is_preserved_when_supplied():
    app = create_app(serving_store=HealthyServingStore())
    client = TestClient(app)

    response = client.get(
        "/health",
        headers={"X-Request-ID": "test-request-123"},
    )

    assert response.headers["x-request-id"] == "test-request-123"


def test_responses_include_duration_header():
    app = create_app(serving_store=HealthyServingStore())
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert "x-process-time-ms" in response.headers
    assert float(response.headers["x-process-time-ms"]) >= 0
