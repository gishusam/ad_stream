from fastapi.testclient import TestClient

from src.api.app import create_app


class FakeServingStore:
    def __init__(self):
        self.advertiser_filters = None
        self.creative_filters = None
        self.traffic_quality_filters = None

    def list_advertiser_daily(
        self,
        event_date=None,
        advertiser_id=None,
    ):
        self.advertiser_filters = {
            "event_date": event_date,
            "advertiser_id": advertiser_id,
        }

        return [
            {
                "event_date": "2026-08-27",
                "advertiser_id": "1458",
                "impressions": 100,
                "total_spend_cny": 12.5,
                "average_bid_cpm": 3.2,
                "average_clearing_cpm": 2.8,
                "total_auction_savings_cny": 2.0,
                "warning_events": 4,
            }
        ]

    def list_creative_daily(
        self,
        event_date=None,
        advertiser_id=None,
        creative_id=None,
    ):
        self.creative_filters = {
            "event_date": event_date,
            "advertiser_id": advertiser_id,
            "creative_id": creative_id,
        }

        return [
            {
                "event_date": "2026-08-27",
                "advertiser_id": "1458",
                "creative_id": "creative-1",
                "impressions": 50,
                "total_spend_cny": 6.25,
                "average_clearing_cpm": 2.8,
                "clicks": 3,
            }
        ]

    def list_traffic_quality_daily(
        self,
        event_date=None,
    ):
        self.traffic_quality_filters = {
            "event_date": event_date,
        }

        return [
            {
                "event_date": "2026-08-27",
                "total_events": 100,
                "valid_events": 96,
                "warning_events": 4,
                "warning_rate": 0.04,
                "quarantined_events": 0,
            }
        ]


def build_client():
    store = FakeServingStore()
    app = create_app(serving_store=store)
    return TestClient(app), store


def test_health():
    client, _ = build_client()

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_list_advertiser_daily():
    client, _ = build_client()

    response = client.get("/api/v1/advertisers/daily")

    assert response.status_code == 200
    assert response.json()[0]["advertiser_id"] == "1458"


def test_advertiser_daily_forwards_filters():
    client, store = build_client()

    response = client.get(
        "/api/v1/advertisers/daily",
        params={
            "event_date": "2026-08-27",
            "advertiser_id": "1458",
        },
    )

    assert response.status_code == 200
    assert store.advertiser_filters == {
        "event_date": "2026-08-27",
        "advertiser_id": "1458",
    }


def test_list_creative_daily():
    client, _ = build_client()

    response = client.get("/api/v1/creatives/daily")

    assert response.status_code == 200
    assert response.json()[0]["creative_id"] == "creative-1"


def test_creative_daily_forwards_filters():
    client, store = build_client()

    response = client.get(
        "/api/v1/creatives/daily",
        params={
            "event_date": "2026-08-27",
            "advertiser_id": "1458",
            "creative_id": "creative-1",
        },
    )

    assert response.status_code == 200
    assert store.creative_filters == {
        "event_date": "2026-08-27",
        "advertiser_id": "1458",
        "creative_id": "creative-1",
    }


def test_list_traffic_quality_daily():
    client, _ = build_client()

    response = client.get("/api/v1/traffic-quality/daily")

    assert response.status_code == 200
    assert response.json()[0]["warning_rate"] == 0.04


def test_traffic_quality_daily_forwards_date_filter():
    client, store = build_client()

    response = client.get(
        "/api/v1/traffic-quality/daily",
        params={"event_date": "2026-08-27"},
    )

    assert response.status_code == 200
    assert store.traffic_quality_filters == {
        "event_date": "2026-08-27",
    }
