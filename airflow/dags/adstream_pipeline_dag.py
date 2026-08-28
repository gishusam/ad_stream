import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import (
    PythonOperator,
    get_current_context,
)

from src.observability.postgres_store import (
    PostgresPipelineMetricsStore,
)
from src.observability.stage import observe_stage


default_args = {
    "owner": "adstream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def _record_result(stage_observation, result):
    if isinstance(result, dict):
        stage_observation.set_result(**result)
    else:
        stage_observation.set_result(
            result_type=type(result).__name__,
        )


def _observability_context():
    context = get_current_context()
    run_id = context["run_id"]

    database_url = os.getenv(
        "SUPABASE_POSTGRES_URL"
    )

    if not database_url:
        return run_id, None

    metrics_store = PostgresPipelineMetricsStore(
        database_url
    )

    return run_id, metrics_store


def run_silver():
    from src.processing.silver_ingestion import (
        SilverIngestionPipeline,
    )

    run_id, metrics_store = _observability_context()

    recorder = (
        metrics_store.record_stage
        if metrics_store is not None
        else None
    )

    with observe_stage(
        "silver",
        run_id=run_id,
        recorder=recorder,
    ) as stage:
        result = SilverIngestionPipeline().run()

        _record_result(
            stage,
            result,
        )

        return result


def run_gold():
    from src.processing.gold_ingestion import (
        GoldIngestionPipeline,
    )

    run_id, metrics_store = _observability_context()

    recorder = (
        metrics_store.record_stage
        if metrics_store is not None
        else None
    )

    with observe_stage(
        "gold",
        run_id=run_id,
        recorder=recorder,
    ) as stage:
        result = GoldIngestionPipeline().run()

        _record_result(
            stage,
            result,
        )

        return result


def run_quality(ti):
    from src.processing.data_quality import (
        validate_pipeline_results,
    )

    silver_result = ti.xcom_pull(
        task_ids="silver_transformation"
    )
    gold_result = ti.xcom_pull(
        task_ids="gold_aggregation"
    )

    run_id, metrics_store = _observability_context()

    recorder = (
        metrics_store.record_stage
        if metrics_store is not None
        else None
    )

    with observe_stage(
        "quality",
        run_id=run_id,
        recorder=recorder,
    ) as stage:
        result = validate_pipeline_results(
            silver_result,
            gold_result,
        )

        if isinstance(result, dict):
            stage.set_result(
                **result,
            )
        else:
            stage.set_result(
                result_type=type(result).__name__,
            )

        return result


def run_serving():
    from src.serving.pipeline import (
        ServingPipeline,
    )

    run_id, metrics_store = _observability_context()

    recorder = (
        metrics_store.record_stage
        if metrics_store is not None
        else None
    )

    with observe_stage(
        "serving",
        run_id=run_id,
        recorder=recorder,
    ) as stage:
        result = ServingPipeline().run()

        _record_result(
            stage,
            result,
        )

        return result


with DAG(
    dag_id="adstream_medallion_pipeline",
    default_args=default_args,
    description=(
        "Build and validate AdStream Silver and Gold "
        "from persisted Bronze data"
    ),
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 27),
    catchup=False,
    tags=[
        "adstream",
        "data-engineering",
        "medallion",
    ],
) as dag:
    silver_task = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver,
    )

    gold_task = PythonOperator(
        task_id="gold_aggregation",
        python_callable=run_gold,
    )

    quality_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_quality,
    )

    serving_task = PythonOperator(
        task_id="serving_refresh",
        python_callable=run_serving,
    )

    silver_task >> gold_task >> quality_task >> serving_task
