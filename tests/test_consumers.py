import pytest
from unittest.mock import MagicMock, patch
from src.consumers.base_consumer import BaseConsumer
from src.consumers.impression_consumer import ImpressionConsumer


# ── helpers that skip real Kafka connection ──────────────────────────────────

def make_base_consumer(**kwargs):
    return BaseConsumer(
        topics=["test.topic"],
        group_id="test-group",
        _skip_connect=True,
        **kwargs
    )

def make_impression_consumer():
    c = ImpressionConsumer.__new__(ImpressionConsumer)
    # Manually set all attributes BaseConsumer.__init__ would set
    c.topics = ["ad.impressions.raw"]
    c.group_id = "adstream-bronze-ingestion"
    c.bootstrap_servers = "localhost:9092"
    c.max_retries = 3
    c.retry_backoff_seconds = 2.0
    c.batch_size = 100
    c.consumer = MagicMock()
    c._running = False
    c.fraud_count = 0
    c.valid_count = 0
    c.invalid_count = 0
    return c


# ── BaseConsumer tests ───────────────────────────────────────────────────────

class TestBaseConsumer:

    def test_consumer_initialises_without_connecting(self):
        consumer = make_base_consumer()
        assert consumer.consumer is None
        assert consumer.group_id == "test-group"

    def test_commit_calls_kafka_commit(self):
        consumer = make_base_consumer()
        consumer.consumer = MagicMock()
        consumer.commit()
        consumer.consumer.commit.assert_called_once()

    def test_close_commits_and_closes(self):
        consumer = make_base_consumer()
        consumer.consumer = MagicMock()
        consumer.close()
        consumer.consumer.commit.assert_called()
        consumer.consumer.close.assert_called()

    def test_poll_returns_empty_list_when_no_messages(self):
        consumer = make_base_consumer()
        consumer.consumer = MagicMock()
        consumer.consumer.poll.return_value = {}
        result = consumer.poll_batch()
        assert result == []

    def test_poll_returns_messages(self):
        consumer = make_base_consumer()
        mock_msg = MagicMock()
        mock_msg.value = {"impression_id": "abc"}
        mock_msg.topic = "test.topic"
        mock_msg.partition = 0
        mock_msg.offset = 1
        consumer.consumer = MagicMock()
        consumer.consumer.poll.return_value = {
            MagicMock(): [mock_msg]
        }
        result = consumer.poll_batch()
        assert len(result) == 1
        assert result[0]["impression_id"] == "abc"


# ── ImpressionConsumer tests ─────────────────────────────────────────────────

class TestImpressionConsumer:

    def test_valid_event_is_parsed(self):
        consumer = make_impression_consumer()
        raw = [{
            "impression_id":  "abc-123",
            "user_id":        "user-001",
            "advertiser_id":  "adv_001",
            "campaign_id":    "camp_001",
            "content_id":     "cnt_001",
            "bid_price":      0.45,
            "currency":       "USD",
            "country_code":   "US",
            "device_type":    "mobile",
            "ad_format":      "video",
            "timestamp":      "2026-04-27T12:00:00Z",
            "is_fraud":       False,
        }]
        valid, invalid = consumer.parse_batch(raw)
        assert len(valid) == 1
        assert len(invalid) == 0
        assert consumer.valid_count == 1

    def test_invalid_event_is_quarantined(self):
        consumer = make_impression_consumer()
        raw = [{"bad_field": "garbage"}]
        valid, invalid = consumer.parse_batch(raw)
        assert len(valid) == 0
        assert len(invalid) == 1
        assert consumer.invalid_count == 1

    def test_fraud_event_is_counted(self):
        consumer = make_impression_consumer()
        raw = [{
            "impression_id":  "fraud-123",
            "user_id":        "bot_001",
            "advertiser_id":  "adv_001",
            "campaign_id":    "camp_001",
            "content_id":     "cnt_001",
            "bid_price":      12.50,
            "currency":       "USD",
            "country_code":   "US",
            "device_type":    "mobile",
            "ad_format":      "video",
            "timestamp":      "2026-04-27T12:00:00Z",
            "is_fraud":       True,
        }]
        valid, invalid = consumer.parse_batch(raw)
        assert len(valid) == 1
        assert consumer.fraud_count == 1

    def test_mixed_batch_counted_correctly(self):
        consumer = make_impression_consumer()
        raw = [
            {
                "impression_id": "good-1", "user_id": "u1",
                "advertiser_id": "adv_001", "campaign_id": "camp_001",
                "content_id": "cnt_001", "bid_price": 0.45,
                "currency": "USD", "country_code": "US",
                "device_type": "mobile", "ad_format": "video",
                "timestamp": "2026-04-27T12:00:00Z", "is_fraud": False,
            },
            {"bad": "data"},
        ]
        valid, invalid = consumer.parse_batch(raw)
        assert len(valid) == 1
        assert len(invalid) == 1

    def test_fraud_rate_calculation(self):
        consumer = make_impression_consumer()
        consumer.valid_count = 100
        consumer.fraud_count = 3
        stats = consumer.get_stats()
        assert stats["fraud_rate"] == 0.03

    def test_zero_valid_count_returns_zero_fraud_rate(self):
        consumer = make_impression_consumer()
        stats = consumer.get_stats()
        assert stats["fraud_rate"] == 0.0