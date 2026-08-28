from pathlib import Path


DAG_PATH = Path(
    "airflow/dags/adstream_pipeline_dag.py"
)


def dag_source():
    return DAG_PATH.read_text()


def test_dag_imports_stage_observer():
    source = dag_source()

    assert (
        "from src.observability.stage import observe_stage"
        in source
    )


def test_silver_stage_is_observed():
    source = dag_source()

    assert '"silver"' in source
    assert "with observe_stage(" in source


def test_gold_stage_is_observed():
    source = dag_source()

    assert '"gold"' in source
    assert "with observe_stage(" in source


def test_quality_stage_is_observed():
    source = dag_source()

    assert '"quality"' in source
    assert "with observe_stage(" in source


def test_serving_stage_is_observed():
    source = dag_source()

    assert '"serving"' in source
    assert "with observe_stage(" in source


def test_stage_results_are_recorded():
    source = dag_source()

    assert "_record_result(" in source
    assert "stage," in source
    assert "result," in source
    assert "stage.set_result(" in source


def test_stage_observers_receive_run_id():
    source = dag_source()

    assert "run_id=run_id" in source


def test_stage_observers_receive_metrics_recorder():
    source = dag_source()

    assert "recorder=recorder" in source
    assert "metrics_store.record_stage" in source
