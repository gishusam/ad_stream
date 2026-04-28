import json
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from src.utils.logger import get_logger
from typing import Callable
import time

logger = get_logger("base_producer")


class BaseProducer:
    """
    Base Kafka producer that every specific producer inherits from.

    Design decision: we wrap KafkaProducer in our own class so that:
    1. Connection logic lives in one place
    2. Error handling is consistent across all producers
    3. Retry logic is not duplicated in every producer
    4. Tests can mock this class cleanly

    This is the same pattern used at FAANG — nobody talks to Kafka
    directly from business logic. There is always an abstraction layer.
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        max_retries: int = 3,
        retry_backoff_seconds: float = 2.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.max_retries = max_retries
        self.retry_backoff_seconds = retry_backoff_seconds
        self.producer = None
        self._connect()

    def _connect(self):
        """
        Establish connection to Kafka with retry logic.

        Why retry? Kafka brokers take time to start. In Docker,
        your Python code might start before Kafka is fully ready.
        Without retries, you get a crash. With retries, you get
        a working system. This is the difference between a script
        and production code.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",           # wait for all replicas to confirm
                    retries=3,            # kafka-level retries on send failure
                    max_block_ms=10000,   # wait max 10s if kafka buffer is full
                    request_timeout_ms=30000,
                )
                logger.info(
                    "kafka_connected",
                    bootstrap_servers=self.bootstrap_servers,
                    attempt=attempt,
                )
                return

            except NoBrokersAvailable:
                logger.warning(
                    "kafka_not_available",
                    attempt=attempt,
                    max_retries=self.max_retries,
                    retry_in_seconds=self.retry_backoff_seconds,
                )
                if attempt < self.max_retries:
                    time.sleep(self.retry_backoff_seconds)
                else:
                    raise ConnectionError(
                        f"Could not connect to Kafka at {self.bootstrap_servers} "
                        f"after {self.max_retries} attempts. "
                        f"Is Docker running? Try: make up"
                    )

    def produce(
        self,
        topic: str,
        value: dict,
        key: str = None,
        on_delivery: Callable = None,
    ) -> None:
        """
        Send one message to a Kafka topic.

        Args:
            topic:       Kafka topic name
            value:       message payload as a dict (will be JSON serialised)
            key:         optional partition key — events with the same key
                         always go to the same partition. We use advertiser_id
                         so all impressions for one advertiser are ordered.
            on_delivery: optional callback fired when Kafka confirms delivery
        """
        if self.producer is None:
            raise RuntimeError("Producer not initialised. Call _connect() first.")

        try:
            future = self.producer.send(
                topic=topic,
                value=value,
                key=key,
            )

            if on_delivery:
                future.add_callback(on_delivery)

            future.add_errback(self._on_error)

        except KafkaError as e:
            logger.error(
                "kafka_produce_failed",
                topic=topic,
                key=key,
                error=str(e),
            )
            raise

    def produce_batch(self, topic: str, messages: list[dict], key_field: str = None):
        """
        Send a batch of messages efficiently.

        Why batch? Kafka is optimised for batches. Sending 1000 individual
        messages is slower than sending one batch of 1000. In production,
        always batch when throughput matters.
        """
        sent = 0
        failed = 0

        for msg in messages:
            try:
                key = str(msg.get(key_field)) if key_field else None
                self.produce(topic=topic, value=msg, key=key)
                sent += 1
            except KafkaError:
                failed += 1

        # flush() blocks until all pending messages are delivered
        self.flush()

        logger.info(
            "batch_produced",
            topic=topic,
            sent=sent,
            failed=failed,
            total=len(messages),
        )

    def flush(self, timeout: float = 30.0):
        """Force all buffered messages to be sent immediately."""
        if self.producer:
            self.producer.flush(timeout=timeout)

    def close(self):
        """Clean shutdown — always call this when done."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("producer_closed")

    def _on_error(self, e: Exception):
        logger.error("kafka_delivery_failed", error=str(e))

    def __enter__(self):
        """Allows using producer as a context manager: with BaseProducer() as p:"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Automatically closes producer when context manager exits."""
        self.close()