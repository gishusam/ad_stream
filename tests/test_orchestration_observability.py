from pathlib import Path


DAG_PATH = Path(
    "airflow/dags/adstream_pipeline_dag.py"
)


def dag_source():
    return DAG_PATH.read_text()


def test_dag_imports_stage_observer():
    source = dag_source()

    assert (
        "from src.observability.stage "
        "import observe_stage"
    ) in source


def test_silver_stage_is_observed():
    source = dag_source()

    assert 'observe_stage("silver")' in source


def test_gold_stage_is_observed():
    source = dag_source()

    assert 'observe_stage("gold")' in source


def test_quality_stage_is_observed():
    source = dag_source()

    assert 'observe_stage("quality")' in source


def test_serving_stage_is_observed():
    source = dag_source()

    assert 'observe_stage("serving")' in source


def test_stage_results_are_recorded():
    source = dag_source()

    assert "_record_result(stage, result)" in source
    assert 'status="passed"' in source
