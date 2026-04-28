import structlog
import logging
import sys


def get_logger(name: str = "adstream") -> structlog.BoundLogger:
    """
    Returns a structured JSON logger.

    Why structured logging?
    In production, logs are ingested by systems like Datadog, Splunk,
    or CloudWatch. Those systems parse JSON — not human-readable strings.
    A log line like:
        {"event": "impression_produced", "topic": "ad.impressions.raw", "count": 1000}
    is searchable, filterable, and alertable. A print() statement is not.
    """

    # Configure standard Python logging underneath
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO
    )

    # Configure structlog to output clean JSON
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    return structlog.get_logger(name)