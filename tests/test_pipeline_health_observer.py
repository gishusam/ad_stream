import logging

import pytest

from src.observability.stage import observe_stage


def test_observer_emits_completed_stage_record():
    records = []

    with observe_stage(
        "gold",
        run_id="scheduled__2026-08-28T09:00:00",
        recorder=records.append,
    ) as stage:
        stage.set_result(
            advertiser_daily=1,
            creative_daily=1,
            traffic_quality_daily=1,
        )

    assert len(records) == 1

    record = records[0]

    assert record["run_id"] == "scheduled__2026-08-28T09:00:00"
    assert record["stage"] == "gold"
    assert record["status"] == "success"
    assert record["duration_ms"] >= 0
    assert record["result"] == {
        "advertiser_daily": 1,
        "creative_daily": 1,
        "traffic_quality_daily": 1,
    }
    assert record["error_type"] is None


def test_observer_emits_failed_stage_record():
    records = []

    with pytest.raises(RuntimeError):
        with observe_stage(
            "serving",
            run_id="run-123",
            recorder=records.append,
        ):
            raise RuntimeError("postgres unavailable")

    assert len(records) == 1

    record = records[0]

    assert record["run_id"] == "run-123"
    assert record["stage"] == "serving"
    assert record["status"] == "failed"
    assert record["duration_ms"] >= 0
    assert record["result"] == {}
    assert record["error_type"] == "RuntimeError"


def test_observer_still_works_without_recorder(caplog):
    caplog.set_level(logging.INFO)

    with observe_stage("silver") as stage:
        stage.set_result(written=1000)

    assert any(
        "pipeline_stage_completed" in record.getMessage()
        for record in caplog.records
    )


def test_recorder_failure_does_not_break_pipeline(caplog):
    caplog.set_level(logging.ERROR)

    def broken_recorder(_record):
        raise RuntimeError("metrics database unavailable")

    with observe_stage(
        "quality",
        run_id="run-123",
        recorder=broken_recorder,
    ):
        pass

    assert any(
        "pipeline_stage_metrics_persist_failed"
        in record.getMessage()
        for record in caplog.records
    )
