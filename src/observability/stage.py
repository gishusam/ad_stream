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


@contextmanager
def observe_stage(stage: str):
    observation = StageObservation(stage)
    started = time.perf_counter()

    logger.info(
        "pipeline_stage_started stage=%s",
        stage,
    )

    try:
        yield observation

    except Exception as exc:
        duration_ms = (
            time.perf_counter() - started
        ) * 1000

        logger.exception(
            "pipeline_stage_failed "
            "stage=%s "
            "duration_ms=%.3f "
            "error_type=%s",
            stage,
            duration_ms,
            type(exc).__name__,
        )

        raise

    duration_ms = (
        time.perf_counter() - started
    ) * 1000

    logger.info(
        "pipeline_stage_completed "
        "stage=%s "
        "duration_ms=%.3f%s",
        stage,
        duration_ms,
        _format_fields(observation.result),
    )
