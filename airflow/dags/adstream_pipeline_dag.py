from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add project root to path so Airflow can find our modules
sys.path.insert(0, "/Users/samwelngugi/Documents/adstream")

from src.utils.logger import get_logger

logger = get_logger("adstream_dag")

# ── Default arguments applied to every task ──────────────────────────────────
# These are the production defaults you'd use at a FAANG company:
# - 1 retry with 5 minute delay before declaring failure
# - Email alerts on failure (disabled locally)
# - No backfill — don't rerun missed schedules automatically

default_args = {
    "owner":            "adstream",
    "depends_on_past":  False,       # don't wait for previous run to succeed
    "retries":          1,           # retry once on failure
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}


# ── Task functions ────────────────────────────────────────────────────────────
# Each function is one pipeline step.
# Airflow calls these functions — it doesn't know or care what's inside.
# That's the separation of concerns: Airflow owns scheduling,
# your pipeline files own the logic.

def run_bronze(**context):
    """
    Bronze ingestion task.
    Generates fresh events and writes to Delta Lake Bronze.
    In production this would read from Kafka consumer.
    Locally we generate directly to keep the DAG self-contained.
    """
    logger.info("airflow_task_started", task="bronze_ingestion")

    from src.processing.bronze_writer import BronzeWriter
    from src.utils.data_generator import AdStreamDataGenerator

    writer = BronzeWriter()
    gen    = AdStreamDataGenerator()
    batch  = gen.generate_impression_batch(200)
    written = writer.write_batch(batch)

    logger.info(
        "airflow_task_complete",
        task="bronze_ingestion",
        rows_written=written,
    )
    return written


def run_silver(**context):
    """
    Silver transformation task.
    Reads Bronze, applies all four transformations, writes Silver.
    Only runs after Bronze succeeds — Airflow enforces this.
    """
    logger.info("airflow_task_started", task="silver_ingestion")

    from src.processing.silver_ingestion import SilverIngestionPipeline

    pipeline = SilverIngestionPipeline()
    pipeline.run()

    logger.info("airflow_task_complete", task="silver_ingestion")


def run_gold(**context):
    """
    Gold aggregation task.
    Reads Silver, computes business metrics, writes Gold.
    Only runs after Silver succeeds.
    """
    logger.info("airflow_task_started", task="gold_ingestion")

    from src.processing.gold_ingestion import GoldIngestionPipeline

    pipeline = GoldIngestionPipeline()
    pipeline.run()

    logger.info("airflow_task_complete", task="gold_ingestion")


def run_data_quality(**context):
    """
    Data quality check task.
    Runs after Gold to verify the full pipeline produced
    expected row counts and no anomalies.
    If this fails, Airflow alerts — Gold exists but is flagged.
    """
    logger.info("airflow_task_started", task="data_quality")

    from src.processing.bronze_writer import get_spark
    from src.processing.silver_transformer import SILVER_LEGITIMATE_PATH
    from src.processing.gold_aggregator import GOLD_REVENUE_PATH

    spark = get_spark()

    # Check Silver has data
    silver_count = (
        spark.read.format("delta")
        .load(SILVER_LEGITIMATE_PATH)
        .count()
    )

    # Check Gold has data
    gold_count = (
        spark.read.format("delta")
        .load(GOLD_REVENUE_PATH)
        .count()
    )

    # Check fraud rate is not suspiciously high
    from src.processing.bronze_writer import BRONZE_PATH
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)
    total      = bronze_df.count()
    fraud      = bronze_df.filter("is_fraud = true").count()
    fraud_rate = fraud / total if total > 0 else 0

    logger.info(
        "data_quality_results",
        silver_rows=silver_count,
        gold_rows=gold_count,
        fraud_rate=round(fraud_rate, 4),
    )

    # Fail the task if quality checks don't pass
    assert silver_count > 0,  "Silver table is empty — pipeline failed"
    assert gold_count > 0,    "Gold table is empty — pipeline failed"
    assert fraud_rate < 0.10, f"Fraud rate {fraud_rate} exceeds 10% threshold"

    logger.info("data_quality_passed", task="data_quality")


# ── DAG definition ────────────────────────────────────────────────────────────
# schedule_interval="@hourly" means run every hour.
# catchup=False means don't backfill missed runs.
# start_date is when the DAG becomes active.

with DAG(
    dag_id="adstream_pipeline",
    default_args=default_args,
    description="AdStream Bronze → Silver → Gold pipeline",
    schedule_interval="@hourly",
    start_date=datetime(2026, 5, 11),
    catchup=False,
    tags=["adstream", "data-engineering", "medallion"],
) as dag:

    # Define tasks
    bronze_task = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze,
    )

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
        python_callable=run_data_quality,
    )

    # Define dependencies — this is the entire DAG structure
    # >> means "then run"
    # Bronze → Silver → Gold → Quality check
    bronze_task >> silver_task >> gold_task >> quality_task