from pathlib import Path


DAG_PATH = Path(
    "airflow/dags/adstream_pipeline_dag.py"
)


def source():
    return DAG_PATH.read_text()


def test_dag_reads_airflow_runtime_context():
    text = source()

    assert "get_current_context" in text


def test_dag_uses_airflow_run_id():
    text = source()

    assert 'context["run_id"]' in text


def test_dag_creates_pipeline_metrics_store():
    text = source()

    assert "PostgresPipelineMetricsStore" in text


def test_dag_uses_supabase_postgres_for_metrics():
    text = source()

    assert "SUPABASE_POSTGRES_URL" in text


def test_dag_passes_recorder_to_stage_observer():
    text = source()

    assert "recorder=recorder" in text
    assert "metrics_store.record_stage" in text


def test_all_pipeline_stages_receive_run_id():
    text = source()

    for stage in [
        "silver",
        "gold",
        "quality",
        "serving",
    ]:
        assert (
            f'with observe_stage(\n        "{stage}",'
            in text
        )

        assert "run_id=run_id" in text


def test_pipeline_can_run_without_metrics_database():
    text = source()

    assert "if not database_url:" in text
    assert "return run_id, None" in text
