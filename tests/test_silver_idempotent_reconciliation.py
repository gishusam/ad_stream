from src.processing.silver_ingestion import (
    _count_persisted_batch,
)


class FakeFrame:
    def __init__(self, keys):
        self.keys = list(keys)

    def select(self, key):
        return FakeFrame(self.keys)

    def dropDuplicates(self, keys):
        return FakeFrame(list(dict.fromkeys(self.keys)))

    def join(self, other, key, how):
        assert how == "inner"

        persisted = set(other.keys)

        return FakeFrame(
            [
                value
                for value in self.keys
                if value in persisted
            ]
        )

    def count(self):
        return len(self.keys)


def test_count_persisted_batch_accepts_idempotent_rerun():
    expected_batch = FakeFrame(
        ["event-1", "event-2", "event-3"]
    )

    persisted_table = FakeFrame(
        [
            "older-event",
            "event-1",
            "event-2",
            "event-3",
        ]
    )

    assert (
        _count_persisted_batch(
            expected_batch,
            persisted_table,
            "event_id",
        )
        == 3
    )


def test_count_persisted_batch_detects_partial_persistence():
    expected_batch = FakeFrame(
        ["event-1", "event-2", "event-3"]
    )

    persisted_table = FakeFrame(
        ["event-1", "event-3"]
    )

    assert (
        _count_persisted_batch(
            expected_batch,
            persisted_table,
            "event_id",
        )
        == 2
    )
