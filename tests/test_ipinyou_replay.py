from pathlib import Path

from src.producers.ipinyou_replay_producer import IPinYouReplayProducer
from src.sources.ipinyou import IPinYouLogReader


FIXTURE = Path(__file__).parent / "fixtures" / "ipinyou_train_sample.tsv"


class RecordingSink:
    def __init__(self):
        self.messages = []
        self.flush_calls = 0

    def produce(self, topic, value, key=None):
        self.messages.append((topic, value, key))

    def flush(self):
        self.flush_calls += 1


def test_replay_sends_real_events_to_existing_impression_topic():
    sink = RecordingSink()
    replay = IPinYouReplayProducer(sink=sink)

    sent = replay.replay(IPinYouLogReader(FIXTURE), limit=2)

    assert sent == 2
    assert sink.flush_calls == 1
    assert [message[0] for message in sink.messages] == [
        "ad.impressions.raw",
        "ad.impressions.raw",
    ]
    assert sink.messages[0][2] == "1458"
    assert sink.messages[0][1]["source_dataset"] == "ipinyou"
    assert sink.messages[0][1]["paying_price"] == 55.0
    assert sink.messages[0][1]["is_fraud"] is False


def test_replay_rate_control_sleeps_between_events_not_after_last_event():
    sink = RecordingSink()
    sleeps = []
    replay = IPinYouReplayProducer(sink=sink, sleep_fn=sleeps.append)

    sent = replay.replay(
        IPinYouLogReader(FIXTURE),
        limit=2,
        events_per_second=4,
    )

    assert sent == 2
    assert sleeps == [0.25]
