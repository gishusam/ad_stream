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
