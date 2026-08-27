from pathlib import Path


DAG_PATH = Path("airflow/dags/adstream_pipeline_dag.py")


def test_dag_does_not_run_long_lived_bronze_consumer():
    source = DAG_PATH.read_text()

    assert "BronzeIngestionPipeline" not in source
    assert "AdStreamDataGenerator" not in source


def test_dag_does_not_reference_legacy_silver_or_gold_contracts():
    source = DAG_PATH.read_text()

    legacy_names = [
        "SILVER_LEGITIMATE_PATH",
        "GOLD_REVENUE_PATH",
        "is_fraud",
        "fraud_rate",
    ]

    for name in legacy_names:
        assert name not in source


def test_dag_orchestrates_current_batch_layers():
    source = DAG_PATH.read_text()

    assert "SilverIngestionPipeline" in source
    assert "GoldIngestionPipeline" in source


def test_dag_runs_quality_gate_after_gold():
    source = DAG_PATH.read_text()

    assert "validate_pipeline_results" in source
    assert 'task_id="data_quality_check"' in source
    assert "gold_task >> quality_task" in source


def test_dag_refreshes_serving_after_quality_gate():
    source = DAG_PATH.read_text()

    assert "ServingPipeline" in source
    assert 'task_id="serving_refresh"' in source
    assert "quality_task >> serving_task" in source
