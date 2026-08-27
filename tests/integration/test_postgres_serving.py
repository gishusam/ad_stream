import os

import pytest
from dotenv import load_dotenv


load_dotenv(".env.supabase")

DATABASE_URL = os.getenv("SUPABASE_POSTGRES_URL")

pytestmark = pytest.mark.skipif(
    not DATABASE_URL,
    reason="SUPABASE_POSTGRES_URL is not configured",
)


def load_store():
    try:
        from src.serving.postgres_store import PostgresServingStore
    except ModuleNotFoundError:
        pytest.fail("PostgresServingStore is not implemented yet")

    return PostgresServingStore


def test_postgres_store_serves_advertiser_daily():
    PostgresServingStore = load_store()

    store = PostgresServingStore(
        DATABASE_URL,
        schema="serving_test",
    )

    row = {
        "event_date": "2013-10-23",
        "advertiser_id": "2997",
        "impressions": 1000,
        "total_spend_cny": 54.912,
        "average_bid_cpm": 277.0,
        "average_clearing_cpm": 54.912,
        "total_auction_savings_cny": 222.088,
        "warning_events": 1000,
    }

    try:
        store.replace_advertiser_daily([row])

        assert store.list_advertiser_daily() == [row]
    finally:
        store.drop_schema()


def test_postgres_store_serves_creative_daily():
    PostgresServingStore = load_store()

    store = PostgresServingStore(
        DATABASE_URL,
        schema="serving_test",
    )

    row = {
        "event_date": "2013-10-23",
        "advertiser_id": "2997",
        "creative_id": "11908",
        "impressions": 1000,
        "total_spend_cny": 54.912,
        "average_clearing_cpm": 54.912,
        "clicks": 0,
    }

    try:
        store.replace_creative_daily([row])

        assert store.list_creative_daily() == [row]
    finally:
        store.drop_schema()


def test_postgres_store_serves_traffic_quality_daily():
    PostgresServingStore = load_store()

    store = PostgresServingStore(
        DATABASE_URL,
        schema="serving_test",
    )

    row = {
        "event_date": "2013-10-23",
        "total_events": 1000,
        "valid_events": 0,
        "warning_events": 1000,
        "warning_rate": 1.0,
        "quarantined_events": 0,
    }

    try:
        store.replace_traffic_quality_daily([row])

        assert store.list_traffic_quality_daily() == [row]
    finally:
        store.drop_schema()
