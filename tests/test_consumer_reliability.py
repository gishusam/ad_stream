from unittest.mock import MagicMock, patch

from src.consumers.base_consumer import BaseConsumer


def test_consumer_uses_extended_max_poll_interval():
    with patch("src.consumers.base_consumer.KafkaConsumer") as kafka_consumer:
        BaseConsumer(
            topics=["test.topic"],
            group_id="test-group",
        )

        kwargs = kafka_consumer.call_args.kwargs
        assert kwargs["max_poll_interval_ms"] == 900000


def test_close_does_not_commit_offsets():
    consumer = BaseConsumer(
        topics=["test.topic"],
        group_id="test-group",
        _skip_connect=True,
    )

    kafka = MagicMock()
    consumer.consumer = kafka

    consumer.close()

    kafka.commit.assert_not_called()
    kafka.close.assert_called_once()
