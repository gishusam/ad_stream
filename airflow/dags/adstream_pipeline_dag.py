from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "adstream",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


def run_silver():
    from src.processing.silver_ingestion import SilverIngestionPipeline

    return SilverIngestionPipeline().run()


def run_gold():
    from src.processing.gold_ingestion import GoldIngestionPipeline

    return GoldIngestionPipeline().run()


def run_quality(ti):
    from src.processing.data_quality import validate_pipeline_results

    silver_result = ti.xcom_pull(task_ids="silver_transformation")
    gold_result = ti.xcom_pull(task_ids="gold_aggregation")

    return validate_pipeline_results(
        silver_result,
        gold_result,
    )


with DAG(
    dag_id="adstream_medallion_pipeline",
    default_args=default_args,
    description="Build and validate AdStream Silver and Gold from persisted Bronze data",
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 27),
    catchup=False,
    tags=["adstream", "data-engineering", "medallion"],
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

    silver_task >> gold_task >> quality_task
