import pytest
from unittest.mock import MagicMock, patch
from src.producers.base_producer import BaseProducer
from src.producers.impression_producer import ImpressionProducer


class TestBaseProducer:
    """
    We never let tests touch real Kafka.
    We mock the KafkaProducer so tests run fast and work offline.
    This is called "unit testing" — test the logic, not the network.
    """

    @patch("src.producers.base_producer.KafkaProducer")
    def test_producer_connects_on_init(self, mock_kafka):
        producer = BaseProducer()
        mock_kafka.assert_called_once()

    @patch("src.producers.base_producer.KafkaProducer")
    def test_produce_sends_to_correct_topic(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = BaseProducer()
        producer.produce(topic="test.topic", value={"id": "123"}, key="adv_001")

        mock_instance.send.assert_called_once_with(
            topic="test.topic",
            value={"id": "123"},
            key="adv_001",
        )

    @patch("src.producers.base_producer.KafkaProducer")
    def test_produce_batch_sends_all_messages(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = BaseProducer()
        messages = [{"id": str(i)} for i in range(10)]
        producer.produce_batch(topic="test.topic", messages=messages)

        assert mock_instance.send.call_count == 10

    @patch("src.producers.base_producer.KafkaProducer")
    def test_context_manager_closes_producer(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        with BaseProducer() as producer:
            producer.produce(topic="test.topic", value={"id": "1"})

        mock_instance.flush.assert_called()
        mock_instance.close.assert_called()

    @patch("src.producers.base_producer.NoBrokersAvailable", Exception)
    @patch("src.producers.base_producer.KafkaProducer")
    def test_retries_on_connection_failure(self, mock_kafka):
        mock_kafka.side_effect = Exception("broker unavailable")
        with pytest.raises(ConnectionError):
            BaseProducer(max_retries=2, retry_backoff_seconds=0)


class TestImpressionProducer:

    @patch("src.producers.base_producer.KafkaProducer")
    def test_produces_valid_impression(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = ImpressionProducer()
        event = producer.produce_impression()

        assert "impression_id" in event
        assert "bid_price" in event
        assert event["bid_price"] > 0
        mock_instance.send.assert_called_once()

    @patch("src.producers.base_producer.KafkaProducer")
    def test_impression_uses_correct_topic(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = ImpressionProducer()
        producer.produce_impression()

        call_kwargs = mock_instance.send.call_args
        assert call_kwargs.kwargs["topic"] == "ad.impressions.raw"

    @patch("src.producers.base_producer.KafkaProducer")
    def test_impression_partitioned_by_advertiser(self, mock_kafka):
        mock_instance = MagicMock()
        mock_kafka.return_value = mock_instance

        producer = ImpressionProducer()
        event = producer.produce_impression()

        call_kwargs = mock_instance.send.call_args
        assert call_kwargs.kwargs["key"] == event["advertiser_id"]