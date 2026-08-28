import logging

import pytest

from src.observability.stage import observe_stage


def test_observe_stage_logs_start_and_completion(caplog):
    caplog.set_level(logging.INFO)

    with observe_stage("gold") as stage:
        stage.set_result(
            advertiser_daily=1,
            creative_daily=1,
            traffic_quality_daily=1,
        )

    messages = [
        record.getMessage()
        for record in caplog.records
    ]

    assert any(
        "pipeline_stage_started" in message
        and "stage=gold" in message
        for message in messages
    )

    assert any(
        "pipeline_stage_completed" in message
        and "stage=gold" in message
        and "advertiser_daily=1" in message
        and "creative_daily=1" in message
        and "traffic_quality_daily=1" in message
        for message in messages
    )


def test_observe_stage_logs_duration(caplog):
    caplog.set_level(logging.INFO)

    with observe_stage("silver"):
        pass

    completed = [
        record.getMessage()
        for record in caplog.records
        if "pipeline_stage_completed" in record.getMessage()
    ]

    assert len(completed) == 1
    assert "duration_ms=" in completed[0]


def test_observe_stage_logs_failure(caplog):
    caplog.set_level(logging.INFO)

    with pytest.raises(RuntimeError):
        with observe_stage("serving"):
            raise RuntimeError("postgres unavailable")

    messages = [
        record.getMessage()
        for record in caplog.records
    ]

    assert any(
        "pipeline_stage_failed" in message
        and "stage=serving" in message
        and "error_type=RuntimeError" in message
        and "duration_ms=" in message
        for message in messages
    )


def test_observe_stage_reraises_original_exception():
    error = RuntimeError("boom")

    with pytest.raises(RuntimeError) as exc:
        with observe_stage("quality"):
            raise error

    assert exc.value is error


def test_stage_result_can_be_added_incrementally(caplog):
    caplog.set_level(logging.INFO)

    with observe_stage("serving") as stage:
        stage.set_result(advertiser_daily=1)
        stage.set_result(creative_daily=2)

    completed = [
        record.getMessage()
        for record in caplog.records
        if "pipeline_stage_completed" in record.getMessage()
    ][0]

    assert "advertiser_daily=1" in completed
    assert "creative_daily=2" in completed
