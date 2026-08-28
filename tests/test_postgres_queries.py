from unittest.mock import MagicMock, patch

from psycopg.sql import Composed

from src.serving.postgres_store import PostgresServingStore


def build_connection():
    connection = MagicMock()
    context_manager = MagicMock()

    context_manager.__enter__.return_value = connection
    context_manager.__exit__.return_value = False

    result = MagicMock()
    result.fetchall.return_value = []

    connection.execute.return_value = result

    return context_manager, connection


def test_advertiser_query_forwards_filters_as_parameters():
    context_manager, connection = build_connection()

    with patch(
        "src.serving.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresServingStore("postgresql://example")

        store.list_advertiser_daily(
            event_date="2026-08-27",
            advertiser_id="1458",
        )

    query, params = connection.execute.call_args.args

    assert isinstance(query, Composed)
    assert params == [
        "2026-08-27",
        "1458",
    ]


def test_advertiser_query_supports_no_filters():
    context_manager, connection = build_connection()

    with patch(
        "src.serving.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresServingStore("postgresql://example")

        store.list_advertiser_daily()

    _, params = connection.execute.call_args.args

    assert params == []


def test_creative_query_forwards_all_filters_as_parameters():
    context_manager, connection = build_connection()

    with patch(
        "src.serving.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresServingStore("postgresql://example")

        store.list_creative_daily(
            event_date="2026-08-27",
            advertiser_id="1458",
            creative_id="creative-1",
        )

    query, params = connection.execute.call_args.args

    assert isinstance(query, Composed)
    assert params == [
        "2026-08-27",
        "1458",
        "creative-1",
    ]


def test_creative_query_supports_creative_only_filter():
    context_manager, connection = build_connection()

    with patch(
        "src.serving.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresServingStore("postgresql://example")

        store.list_creative_daily(
            creative_id="creative-1",
        )

    _, params = connection.execute.call_args.args

    assert params == ["creative-1"]


def test_traffic_quality_query_forwards_date_as_parameter():
    context_manager, connection = build_connection()

    with patch(
        "src.serving.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresServingStore("postgresql://example")

        store.list_traffic_quality_daily(
            event_date="2026-08-27",
        )

    query, params = connection.execute.call_args.args

    assert isinstance(query, Composed)
    assert params == ["2026-08-27"]


def test_traffic_quality_query_supports_no_filter():
    context_manager, connection = build_connection()

    with patch(
        "src.serving.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresServingStore("postgresql://example")

        store.list_traffic_quality_daily()

    _, params = connection.execute.call_args.args

    assert params == []
