"""Pipeline stage timing and result observability."""

import logging
import time
from contextlib import contextmanager


logger = logging.getLogger("adstream.pipeline")


class StageObservation:
    def __init__(self, stage: str):
        self.stage = stage
        self.result = {}

    def set_result(self, **values) -> None:
        self.result.update(values)


def _format_fields(values: dict) -> str:
    if not values:
        return ""

    return " " + " ".join(
        f"{key}={value}"
        for key, value in sorted(values.items())
    )


def _persist_record(recorder, record: dict) -> None:
    if recorder is None:
        return

    try:
        recorder(record)
    except Exception:
        logger.exception(
            "pipeline_stage_metrics_persist_failed "
            "stage=%s run_id=%s",
            record["stage"],
            record.get("run_id"),
        )


@contextmanager
def observe_stage(
    stage: str,
    run_id: str | None = None,
    recorder=None,
):
    observation = StageObservation(stage)
    started = time.perf_counter()

    logger.info(
        "pipeline_stage_started "
        "stage=%s "
        "run_id=%s",
        stage,
        run_id,
    )

    try:
        yield observation

    except Exception as exc:
        duration_ms = (
            time.perf_counter() - started
        ) * 1000

        record = {
            "run_id": run_id,
            "stage": stage,
            "status": "failed",
            "duration_ms": duration_ms,
            "result": dict(observation.result),
            "error_type": type(exc).__name__,
        }

        logger.exception(
            "pipeline_stage_failed "
            "stage=%s "
            "run_id=%s "
            "duration_ms=%.3f "
            "error_type=%s",
            stage,
            run_id,
            duration_ms,
            type(exc).__name__,
        )

        _persist_record(
            recorder,
            record,
        )

        raise

    duration_ms = (
        time.perf_counter() - started
    ) * 1000

    record = {
        "run_id": run_id,
        "stage": stage,
        "status": "success",
        "duration_ms": duration_ms,
        "result": dict(observation.result),
        "error_type": None,
    }

    logger.info(
        "pipeline_stage_completed "
        "stage=%s "
        "run_id=%s "
        "duration_ms=%.3f%s",
        stage,
        run_id,
        duration_ms,
        _format_fields(observation.result),
    )

    _persist_record(
        recorder,
        record,
    )
