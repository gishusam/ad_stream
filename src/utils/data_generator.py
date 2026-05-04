from faker import Faker
from src.models.events import ImpressionEvent, PaymentEvent, EngagementEvent
from src.utils.logger import get_logger
import random
import uuid

fake = Faker()
logger = get_logger("data_generator")

# ── Static reference data ──────────────────────────────────────────────────
# In production these come from your user profiles database and content CMS.
# Locally we simulate a realistic fixed pool — same advertisers, campaigns,
# and content IDs appear repeatedly, just like real platform data.

ADVERTISERS = [f"adv_{str(i).zfill(3)}" for i in range(1, 21)]     # 20 advertisers
CAMPAIGNS   = [f"camp_{str(i).zfill(3)}" for i in range(1, 51)]    # 50 campaigns
CONTENT_IDS = [f"cnt_{str(i).zfill(3)}" for i in range(1, 101)]    # 100 pieces of content
COUNTRIES   = ["US", "GB", "KE", "DE", "FR", "NG", "ZA", "IN", "BR", "CA"]
DEVICES     = ["mobile", "desktop", "tablet", "ctv"]
AD_FORMATS  = ["banner", "video", "native", "audio"]
CURRENCIES  = ["USD", "EUR", "GBP", "KES"]

# Fraud rate — 3% of impressions are fraudulent, matching industry average
FRAUD_RATE = 0.03


class AdStreamDataGenerator:
    """
    Generates realistic streaming data for AdStream.

    Design decision: we use a class so the generator holds state.
    For example, it tracks which impressions have been created so
    PaymentEvents and EngagementEvents can reference real impression IDs.
    A plain function can't do this cleanly.
    """

    def __init__(self):
        self._impression_pool: list[str] = []   # stores recent impression IDs
        self._batch_pool: list[str] = []        # stores recent batch IDs
        logger.info("data_generator_initialised",
                    advertisers=len(ADVERTISERS),
                    campaigns=len(CAMPAIGNS),
                    content_items=len(CONTENT_IDS))

    def generate_impression(self) -> ImpressionEvent:
        """
        Generate one realistic impression event.

        Fraud simulation: 3% of impressions are flagged.
        Fraudulent impressions have suspiciously high bid prices
        and cluster around a small set of user IDs — a real fraud signal.
        """
        is_fraud = random.random() < FRAUD_RATE

        if is_fraud:
            # Fraud pattern: abnormally high bids from bot user IDs
            user_id = f"bot_{random.randint(1, 10):03d}"
            bid_price = round(random.uniform(5.0, 15.0), 6)   # unusually high
        else:
            user_id = f"user_{random.randint(0, 999):06d}"
            bid_price = round(random.uniform(0.01, 2.50), 6)  # normal range

        impression = ImpressionEvent(
            user_id=user_id,
            advertiser_id=random.choice(ADVERTISERS),
            campaign_id=random.choice(CAMPAIGNS),
            content_id=random.choice(CONTENT_IDS),
            bid_price=bid_price,
            currency=random.choice(CURRENCIES),
            country_code=random.choice(COUNTRIES),
            device_type=random.choice(DEVICES),
            ad_format=random.choice(AD_FORMATS),
            is_fraud=is_fraud
        )

        # Track this impression ID so payments/engagements can link back to it
        self._impression_pool.append(impression.impression_id)

        # Keep pool bounded — only remember last 10,000 impressions
        if len(self._impression_pool) > 10_000:
            self._impression_pool.pop(0)

        return impression

    def generate_payment(self) -> PaymentEvent:
        """
        Generate one payment event.

        Payments reference a batch of impressions — one payment covers
        multiple impressions for the same advertiser/campaign combination.
        This is why reconciliation needs the 6-hour watermark join.
        """
        batch_id = str(uuid.uuid4())
        self._batch_pool.append(batch_id)

        return PaymentEvent(
            advertiser_id=random.choice(ADVERTISERS),
            campaign_id=random.choice(CAMPAIGNS),
            amount=round(random.uniform(100.0, 50_000.0), 2),
            currency=random.choice(CURRENCIES),
            impression_batch_id=batch_id,
            payment_status=random.choices(
                ["settled", "pending", "failed", "disputed"],
                weights=[70, 20, 7, 3]      # realistic distribution
            )[0]
        )

    def generate_engagement(self) -> EngagementEvent:
        """
        Generate one engagement event linked to a real impression.

        If no impressions exist yet, generates a placeholder impression ID.
        """
        impression_id = (
            random.choice(self._impression_pool)
            if self._impression_pool
            else str(uuid.uuid4())
        )

        watch_duration = random.randint(0, 1800)    # 0 to 30 minutes
        completion_rate = round(
            min(watch_duration / 1800, 1.0), 4
        )

        return EngagementEvent(
            user_id=fake.uuid4(),
            content_id=random.choice(CONTENT_IDS),
            impression_id=impression_id,
            watch_duration_seconds=watch_duration,
            completion_rate=completion_rate,
            clicked=random.random() < 0.05     # 5% click-through rate
        )

    def generate_impression_batch(self, size: int) -> list[ImpressionEvent]:
        """Generate a batch of impressions in one call."""
        return [self.generate_impression() for _ in range(size)]