import pytest
from datetime import datetime
from src.models.events import ImpressionEvent, PaymentEvent, EngagementEvent
from src.utils.data_generator import AdStreamDataGenerator



class TestImpressionEvent:

    def test_valid_impression_is_created(self):
        event = ImpressionEvent(
            user_id="user_001",
            advertiser_id="adv_001",
            campaign_id="camp_001",
            content_id="cnt_001",
            bid_price=0.45,
            currency="USD",
            country_code="ke",        # lowercase — validator should uppercase it
            device_type="mobile",
            ad_format="video"
        )
        assert event.impression_id is not None
        assert event.country_code == "KE"         # validator fired
        assert event.bid_price == 0.45
        assert event.is_fraud is False
        assert isinstance(event.timestamp, datetime)

    def test_negative_bid_price_is_rejected(self):
        with pytest.raises(Exception):
            ImpressionEvent(
                user_id="user_001",
                advertiser_id="adv_001",
                campaign_id="camp_001",
                content_id="cnt_001",
                bid_price=-0.50,       # invalid
                currency="USD",
                country_code="US",
                device_type="mobile",
                ad_format="banner"
            )

    def test_zero_bid_price_is_rejected(self):
        with pytest.raises(Exception):
            ImpressionEvent(
                user_id="user_001",
                advertiser_id="adv_001",
                campaign_id="camp_001",
                content_id="cnt_001",
                bid_price=0.0,         # invalid
                currency="USD",
                country_code="US",
                device_type="mobile",
                ad_format="banner"
            )

    def test_invalid_country_code_is_rejected(self):
        with pytest.raises(Exception):
            ImpressionEvent(
                user_id="user_001",
                advertiser_id="adv_001",
                campaign_id="camp_001",
                content_id="cnt_001",
                bid_price=0.45,
                currency="USD",
                country_code="KENYA",  # invalid — must be 2 chars
                device_type="mobile",
                ad_format="banner"
            )

    def test_each_impression_gets_unique_id(self):
        e1 = ImpressionEvent(
            user_id="u1", advertiser_id="a1", campaign_id="c1",
            content_id="cnt1", bid_price=0.10, currency="USD",
            country_code="US", device_type="mobile", ad_format="banner"
        )
        e2 = ImpressionEvent(
            user_id="u1", advertiser_id="a1", campaign_id="c1",
            content_id="cnt1", bid_price=0.10, currency="USD",
            country_code="US", device_type="mobile", ad_format="banner"
        )
        assert e1.impression_id != e2.impression_id


class TestPaymentEvent:

    def test_valid_payment_is_created(self):
        event = PaymentEvent(
            advertiser_id="adv_001",
            campaign_id="camp_001",
            amount=1500.00,
            currency="USD",
            impression_batch_id="batch_001",
            payment_status="settled"
        )
        assert event.payment_id is not None
        assert event.amount == 1500.00

    def test_negative_amount_is_rejected(self):
        with pytest.raises(Exception):
            PaymentEvent(
                advertiser_id="adv_001",
                campaign_id="camp_001",
                amount=-500.00,        # invalid
                currency="USD",
                impression_batch_id="batch_001",
                payment_status="settled"
            )


class TestEngagementEvent:

    def test_valid_engagement_is_created(self):
        event = EngagementEvent(
            user_id="user_001",
            content_id="cnt_001",
            impression_id="imp_001",
            watch_duration_seconds=120,
            completion_rate=0.85,
            clicked=False
        )
        assert event.engagement_id is not None
        assert event.completion_rate == 0.85

    def test_completion_rate_above_1_is_rejected(self):
        with pytest.raises(Exception):
            EngagementEvent(
                user_id="user_001",
                content_id="cnt_001",
                impression_id="imp_001",
                watch_duration_seconds=120,
                completion_rate=1.5,   # invalid
                clicked=False
            )

class TestAdStreamDataGenerator:

    def setup_method(self):
        """Runs before every test — gives each test a fresh generator."""
        self.generator = AdStreamDataGenerator()

    def test_generates_valid_impression(self):
        event = self.generator.generate_impression()
        assert event.impression_id is not None
        assert event.bid_price > 0
        assert len(event.country_code) == 2

    def test_generates_valid_payment(self):
        event = self.generator.generate_payment()
        assert event.payment_id is not None
        assert event.amount > 0
        assert event.payment_status in ["settled", "pending", "failed", "disputed"]

    def test_generates_valid_engagement(self):
        event = self.generator.generate_engagement()
        assert event.engagement_id is not None
        assert 0.0 <= event.completion_rate <= 1.0

    def test_impression_pool_tracks_ids(self):
        for _ in range(10):
            self.generator.generate_impression()
        assert len(self.generator._impression_pool) == 10

    def test_engagement_links_to_real_impression(self):
        # Generate impressions first to populate the pool
        for _ in range(5):
            self.generator.generate_impression()
        engagement = self.generator.generate_engagement()
        assert engagement.impression_id in self.generator._impression_pool

    def test_batch_generation(self):
        batch = self.generator.generate_impression_batch(100)
        assert len(batch) == 100
        # Every event in the batch must be valid
        assert all(e.bid_price > 0 for e in batch)

    def test_fraud_events_have_bot_user_ids(self):
        # Generate enough impressions that we're statistically guaranteed
        # to get at least one fraud event (3% rate, 500 events)
        events = self.generator.generate_impression_batch(500)
        fraud_events = [e for e in events if e.is_fraud]
        assert len(fraud_events) > 0
        assert all(e.user_id.startswith("bot_") for e in fraud_events)