from src.consumers.base_consumer import BaseConsumer
from src.models.events import ImpressionEvent
from src.utils.logger import get_logger
from pydantic import ValidationError

logger = get_logger("impression_consumer")

IMPRESSION_TOPIC = "ad.impressions.raw"
CONSUMER_GROUP   = "adstream-bronze-ingestion"


class ImpressionConsumer(BaseConsumer):
    """
    Reads impression events from Kafka and validates them.

    Responsibilities:
    1. Read raw JSON messages from Kafka
    2. Deserialise into ImpressionEvent objects (validates schema)
    3. Separate valid events from invalid ones
    4. Flag fraud events with a log alert before passing downstream

    What it does NOT do:
    - Write to Delta Lake (that's bronze_writer.py's job)
    - Commit offsets (that's done after successful Delta write)
    - Transform or clean data (that's Silver layer, Week 3)

    Single responsibility principle — each class does one thing.
    """

    def __init__(self, **kwargs):
        super().__init__(
            topics=[IMPRESSION_TOPIC],
            group_id=CONSUMER_GROUP,
            **kwargs,
        )
        self.fraud_count = 0
        self.valid_count = 0
        self.invalid_count = 0
        logger.info(
            "impression_consumer_ready",
            topic=IMPRESSION_TOPIC,
            group_id=CONSUMER_GROUP,
        )

    def parse_batch(self, raw_messages: list[dict]) -> tuple[list[ImpressionEvent], list[dict]]:
        """
        Parse and validate a batch of raw Kafka messages.

        Returns:
            valid_events:   list of ImpressionEvent objects — safe to write
            invalid_events: list of raw dicts that failed validation — quarantine

        Why separate valid from invalid instead of crashing?
        In production, one malformed event should never stop the pipeline.
        Invalid events go to a quarantine table for investigation.
        Valid events continue to Bronze. The pipeline never stops.
        """
        valid_events   = []
        invalid_events = []

        for raw in raw_messages:
            try:
                event = ImpressionEvent(**raw)

                # Fraud alert — log immediately, still write to Bronze
                # Bronze is the raw truth — we write everything including fraud
                # Fraud filtering happens in Silver layer
                if event.is_fraud:
                    self.fraud_count += 1
                    logger.warning(
                        "fraud_event_detected",
                        impression_id=event.impression_id,
                        user_id=event.user_id,
                        bid_price=event.bid_price,
                        advertiser_id=event.advertiser_id,
                    )

                valid_events.append(event)
                self.valid_count += 1

            except ValidationError as e:
                self.invalid_count += 1
                logger.error(
                    "invalid_event_rejected",
                    raw_event=raw,
                    error=str(e),
                )
                invalid_events.append(raw)

        if valid_events:
            logger.info(
                "batch_parsed",
                valid=len(valid_events),
                invalid=len(invalid_events),
                fraud_in_batch=sum(1 for e in valid_events if e.is_fraud),
            )

        return valid_events, invalid_events

    def get_stats(self) -> dict:
        """Return running totals — useful for monitoring."""
        return {
            "valid_count":   self.valid_count,
            "invalid_count": self.invalid_count,
            "fraud_count":   self.fraud_count,
            "fraud_rate":    round(
                self.fraud_count / self.valid_count, 4
            ) if self.valid_count > 0 else 0.0,
        }