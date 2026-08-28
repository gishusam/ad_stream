from pathlib import Path


STORE = Path(
    "src/observability/postgres_store.py"
)


def source():
    return STORE.read_text()


def test_stage_query_selects_latest_observation_per_stage():
    text = source()

    assert "ROW_NUMBER()" in text
    assert "PARTITION BY run_id, stage" in text
    assert "ORDER BY recorded_at DESC, id DESC" in text


def test_stage_query_returns_only_latest_stage_state():
    text = source()

    assert "stage_rank = 1" in text


def test_run_summary_uses_latest_stage_states():
    text = source()

    assert "latest_stage_states" in text


def test_run_status_does_not_use_all_historical_attempts():
    text = source()

    assert "FROM latest_stage_states" in text


def test_run_duration_uses_current_stage_states():
    text = source()

    assert "SUM(duration_ms)" in text
