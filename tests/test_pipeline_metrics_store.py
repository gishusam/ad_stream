from unittest.mock import MagicMock, patch

from src.observability.postgres_store import PostgresPipelineMetricsStore


def build_connection():
    connection = MagicMock()
    context_manager = MagicMock()

    context_manager.__enter__.return_value = connection
    context_manager.__exit__.return_value = False

    result = MagicMock()
    result.fetchall.return_value = []
    result.fetchone.return_value = None

    connection.execute.return_value = result

    return context_manager, connection


def test_record_stage_creates_metrics_table_and_writes_record():
    context_manager, connection = build_connection()

    with patch(
        "src.observability.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresPipelineMetricsStore(
            "postgresql://example"
        )

        store.record_stage(
            {
                "run_id": "run-123",
                "stage": "gold",
                "status": "success",
                "duration_ms": 42.5,
                "result": {"advertiser_daily": 1},
                "error_type": None,
            }
        )

    statements = [
        str(call.args[0])
        for call in connection.execute.call_args_list
    ]

    assert len(statements) >= 2


def test_list_recent_runs_returns_rows():
    context_manager, connection = build_connection()

    expected = [
        {
            "run_id": "run-123",
            "status": "success",
            "duration_ms": 120.0,
        }
    ]

    connection.execute.return_value.fetchall.return_value = expected

    with patch(
        "src.observability.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresPipelineMetricsStore(
            "postgresql://example"
        )

        rows = store.list_recent_runs(limit=10)

    assert rows == expected


def test_list_stage_runs_returns_rows():
    context_manager, connection = build_connection()

    expected = [
        {
            "run_id": "run-123",
            "stage": "silver",
            "status": "success",
            "duration_ms": 50.0,
        }
    ]

    connection.execute.return_value.fetchall.return_value = expected

    with patch(
        "src.observability.postgres_store.psycopg.connect",
        return_value=context_manager,
    ):
        store = PostgresPipelineMetricsStore(
            "postgresql://example"
        )

        rows = store.list_stage_runs(
            run_id="run-123"
        )

    assert rows == expected
