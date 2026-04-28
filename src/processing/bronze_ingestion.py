from src.consumers.impression_consumer import ImpressionConsumer
from src.processing.bronze_writer import BronzeWriter
from src.utils.logger import get_logger
import signal
import sys

logger = get_logger("bronze_ingestion")


class BronzeIngestionPipeline:
    """
    Orchestrates the full Bronze ingestion flow:
    Kafka → Consumer → Validate → Write to Delta → Commit offset

    This is the file you run to start the pipeline.
    It ties consumer and writer together with the correct
    commit ordering — write first, commit second. Always.

    Signal handling: catches Ctrl+C and SIGTERM for graceful shutdown.
    In production, Kubernetes sends SIGTERM before killing a container.
    Without this handler, you'd lose buffered events on every deploy.
    """

    def __init__(self):
        self.consumer = ImpressionConsumer()
        self.writer   = BronzeWriter()
        self.total_written = 0
        self._setup_signal_handlers()
        logger.info("bronze_ingestion_pipeline_ready")

    def _setup_signal_handlers(self):
        """Catch Ctrl+C and SIGTERM for graceful shutdown."""
        signal.signal(signal.SIGINT,  self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

    def _shutdown(self, signum, frame):
        logger.info("shutdown_signal_received", signal=signum)
        self.consumer.close()
        logger.info(
            "pipeline_shutdown_complete",
            total_written=self.total_written,
            stats=self.consumer.get_stats(),
        )
        sys.exit(0)

    def run(self):
        """
        Main loop — runs until interrupted.

        Order of operations per batch:
        1. Poll Kafka for up to batch_size messages
        2. Validate and parse into ImpressionEvent objects
        3. Write valid events to Delta Lake Bronze
        4. ONLY THEN commit offsets to Kafka
        5. Repeat

        If step 3 fails, step 4 never runs.
        Kafka replays the batch on next startup.
        No data is lost.
        """
        logger.info("pipeline_started")
        self.consumer._running = True

        with self.consumer:
            while True:
                # Step 1 — poll
                raw_messages = self.consumer.poll_batch()

                if not raw_messages:
                    continue

                # Step 2 — validate
                valid_events, invalid_events = self.consumer.parse_batch(raw_messages)

                # Step 3 — write to Delta
                if valid_events:
                    written = self.writer.write_batch(valid_events)
                    self.total_written += written

                # Step 4 — commit AFTER successful write
                self.consumer.commit()

                logger.info(
                    "batch_complete",
                    total_written=self.total_written,
                    stats=self.consumer.get_stats(),
                )


if __name__ == "__main__":
    pipeline = BronzeIngestionPipeline()
    pipeline.run()