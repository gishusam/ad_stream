from __future__ import annotations

import argparse
import time
from collections.abc import Iterable
from typing import Protocol

from src.models.events import ImpressionEvent
from src.sources.ipinyou import IPinYouLogReader, IPinYouRawImpressionReader


IMPRESSION_TOPIC = "ad.impressions.raw"


class EventSink(Protocol):
    def produce(self, topic: str, value: dict, key: str | None = None) -> None: ...
    def flush(self) -> None: ...


def build_reader(log_path: str, advertiser_id: str | None = None):
    """Select the original compressed impression reader or formatted-log reader."""
    path = str(log_path)
    if path.endswith(".bz2"):
        return IPinYouRawImpressionReader(path, advertiser_id=advertiser_id)
    if advertiser_id is not None:
        raise ValueError("--advertiser-id is only supported for original .bz2 impression logs")
    return IPinYouLogReader(path)


class IPinYouReplayProducer:
    """Replay real historical RTB impressions through the existing Kafka topic."""

    def __init__(
        self,
        sink: EventSink,
        topic: str = IMPRESSION_TOPIC,
        sleep_fn=time.sleep,
    ):
        self.sink = sink
        self.topic = topic
        self.sleep_fn = sleep_fn

    def replay(
        self,
        events: Iterable[ImpressionEvent],
        *,
        limit: int | None = None,
        events_per_second: float | None = None,
    ) -> int:
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")
        if events_per_second is not None and events_per_second <= 0:
            raise ValueError("events_per_second must be > 0")

        interval = 1.0 / events_per_second if events_per_second else None
        sent = 0

        for event in events:
            if limit is not None and sent >= limit:
                break

            if sent > 0 and interval is not None:
                self.sleep_fn(interval)

            self.sink.produce(
                topic=self.topic,
                value=event.model_dump(mode="json"),
                key=event.advertiser_id,
            )
            sent += 1

        self.sink.flush()
        return sent


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Replay formatted iPinYou RTB impressions into AdStream Kafka."
    )
    parser.add_argument("log_path", help="Path to iPinYou train.log.txt or test.log.txt")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--advertiser-id", help="Filter original .bz2 logs to one advertiser before applying --limit")
    parser.add_argument("--events-per-second", type=float, default=100.0)
    args = parser.parse_args(argv)

    # Lazy import keeps parsing/replay logic independently testable without Kafka.
    from src.producers.base_producer import BaseProducer

    with BaseProducer(bootstrap_servers=args.bootstrap_servers) as sink:
        replay = IPinYouReplayProducer(sink=sink)
        sent = replay.replay(
            build_reader(args.log_path, advertiser_id=args.advertiser_id),
            limit=args.limit,
            events_per_second=args.events_per_second,
        )

    print(f"replayed_events={sent} source=ipinyou topic={IMPRESSION_TOPIC}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
