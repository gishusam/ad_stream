from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from src.utils.logger import get_logger
import json
import time

logger = get_logger("base_consumer")


class BaseConsumer:
    """
    Base Kafka consumer that every specific consumer inherits from.

    Key design decisions:
    1. Manual offset commit — we NEVER auto-commit. Offsets are only
       committed after successful downstream writes. This guarantees
       at-least-once delivery. If we crash, Kafka replays from the
       last committed offset.

    2. Consumer group — every consumer belongs to a group. Kafka tracks
       offsets per group. If we restart, we resume exactly where we stopped.

    3. Graceful shutdown — the running flag lets us stop cleanly without
       losing buffered events or corrupting offset state.
    """

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        bootstrap_servers: str = "localhost:9092",
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
        batch_size: int = 100,
        _skip_connect: bool = False,
    ):
        self.topics = topics
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.batch_size = batch_size
        self.consumer = None
        self._running = False
       
        if not _skip_connect:
         self._connect()

    def _connect(self):
        """
        Connect to Kafka with retry logic.
        Same pattern as BaseProducer — never assume Kafka is ready.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    *self.topics,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset="earliest",   # start from beginning if no offset
                    enable_auto_commit=False,        # MANUAL commit only
                    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                    max_poll_records=self.batch_size,
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                )
                logger.info(
                    "kafka_consumer_connected",
                    topics=self.topics,
                    group_id=self.group_id,
                    attempt=attempt,
                )
                return

            except NoBrokersAvailable:
                logger.warning(
                    "kafka_not_available",
                    attempt=attempt,
                    max_retries=self.max_retries,
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_backoff_seconds)
                else:
                    raise ConnectionError(
                        f"Could not connect to Kafka at {self.bootstrap_servers} "
                        f"after {self.max_retries} attempts. Is Docker running?"
                    )

    def poll_batch(self) -> list[dict]:
        """
        Poll Kafka for up to batch_size messages.

        Returns a list of deserialised message values (dicts).
        Does NOT commit offsets — that happens after successful processing.

        Why poll instead of iterate?
        The iterator blocks forever. poll() lets us control the loop,
        check the running flag, and shut down cleanly.
        """
        if not self.consumer:
            raise RuntimeError("Consumer not initialised.")

        records = self.consumer.poll(timeout_ms=1000)
        messages = []

        for topic_partition, msgs in records.items():
            for msg in msgs:
                messages.append(msg.value)
                logger.debug(
                    "message_received",
                    topic=msg.topic,
                    partition=msg.partition,
                    offset=msg.offset,
                )

        return messages

    def commit(self):
        """
        Commit current offsets to Kafka.

        ONLY call this after you have successfully written
        the batch to Delta Lake. Never before.
        """
        if self.consumer:
            self.consumer.commit()
            logger.debug("offsets_committed")

    def close(self):
        """Clean shutdown — commit final offsets and close connection."""
        self._running = False
        if self.consumer:
            self.consumer.commit()
            self.consumer.close()
            logger.info("consumer_closed", group_id=self.group_id)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()