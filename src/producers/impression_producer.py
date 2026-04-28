from src.producers.base_producer import BaseProducer
from src.utils.data_generator import AdStreamDataGenerator
from src.utils.logger import get_logger
import time

logger = get_logger("impression_producer")

IMPRESSION_TOPIC = "ad.impressions.raw"


class ImpressionProducer(BaseProducer):
    """
    Produces ad impression events to Kafka.

    Inherits all connection, retry, and error handling from BaseProducer.
    This class only cares about one thing: impression-specific logic.

    This is the Open/Closed principle in practice — BaseProducer is
    closed for modification but open for extension. You extend it,
    you don't rewrite it.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.generator = AdStreamDataGenerator()
        logger.info("impression_producer_ready", topic=IMPRESSION_TOPIC)

    def produce_impression(self) -> dict:
        """Generate and send one impression event. Returns the event dict."""
        event = self.generator.generate_impression()

        # Convert Pydantic model to dict for Kafka serialisation
        # model_dump() is the Pydantic v2 method (replaces .dict())
        event_dict = event.model_dump(mode="json")

        self.produce(
            topic=IMPRESSION_TOPIC,
            value=event_dict,
            key=event.advertiser_id,   # partition by advertiser
        )

        return event_dict

    def run_continuous(self, events_per_second: int = 100, duration_seconds: int = 60):
        """
        Produce impressions continuously at a controlled rate.

        Args:
            events_per_second: target throughput
            duration_seconds:  how long to run (None = run forever)

        Rate control: we calculate how long each event should take,
        then sleep the remainder. Simple but effective for local dev.
        In production you'd use a proper rate limiter.
        """
        interval = 1.0 / events_per_second
        total_sent = 0
        start_time = time.time()
        log_every = events_per_second * 5   # log every 5 seconds worth

        logger.info(
            "continuous_produce_started",
            topic=IMPRESSION_TOPIC,
            events_per_second=events_per_second,
            duration_seconds=duration_seconds,
        )

        try:
            while True:
                elapsed = time.time() - start_time

                if duration_seconds and elapsed >= duration_seconds:
                    break

                loop_start = time.time()
                self.produce_impression()
                total_sent += 1

                # Log progress every 5 seconds
                if total_sent % log_every == 0:
                    actual_rate = total_sent / elapsed if elapsed > 0 else 0
                    logger.info(
                        "produce_progress",
                        total_sent=total_sent,
                        elapsed_seconds=round(elapsed, 1),
                        actual_rate=round(actual_rate, 1),
                        target_rate=events_per_second,
                    )

                # Sleep to maintain target rate
                loop_duration = time.time() - loop_start
                sleep_time = interval - loop_duration
                if sleep_time > 0:
                    time.sleep(sleep_time)

        except KeyboardInterrupt:
            logger.info("produce_interrupted_by_user", total_sent=total_sent)
        finally:
            self.flush()
            logger.info(
                "continuous_produce_finished",
                total_sent=total_sent,
                duration_seconds=round(time.time() - start_time, 1),
            )