from datetime import date

import pytest


class FakeRow:
    def __init__(self, **values):
        self.values = values

    def asDict(self):
        return self.values


class FakeDataFrame:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class FakeGoldBackend:
    def read_advertiser_daily(self):
        return FakeDataFrame([
            FakeRow(
                event_date=date(2013, 10, 23),
                advertiser_id="2997",
                impressions=1000,
                total_spend_cny=54.912,
                average_bid_cpm=277.0,
                average_clearing_cpm=54.912,
                total_auction_savings_cny=222.088,
                warning_events=1000,
            )
        ])

    def read_creative_daily(self):
        return FakeDataFrame([
            FakeRow(
                event_date=date(2013, 10, 23),
                advertiser_id="2997",
                creative_id="11908",
                impressions=1000,
                total_spend_cny=54.912,
                average_clearing_cpm=54.912,
                clicks=0,
            )
        ])

    def read_traffic_quality_daily(self):
        return FakeDataFrame([
            FakeRow(
                event_date=date(2013, 10, 23),
                total_events=1000,
                valid_events=0,
                warning_events=1000,
                warning_rate=1.0,
                quarantined_events=0,
            )
        ])


class RecordingStore:
    def __init__(self):
        self.advertiser_rows = None
        self.creative_rows = None
        self.quality_rows = None

    def replace_advertiser_daily(self, rows):
        self.advertiser_rows = rows

    def replace_creative_daily(self, rows):
        self.creative_rows = rows

    def replace_traffic_quality_daily(self, rows):
        self.quality_rows = rows


def load_refresh_job():
    try:
        from src.serving.refresh_job import ServingRefreshJob
    except ModuleNotFoundError:
        pytest.fail("ServingRefreshJob is not implemented yet")

    return ServingRefreshJob


def test_refresh_job_materializes_all_gold_tables():
    ServingRefreshJob = load_refresh_job()

    store = RecordingStore()
    job = ServingRefreshJob(
        gold_backend=FakeGoldBackend(),
        serving_store=store,
    )

    result = job.run()

    assert result == {
        "advertiser_daily": 1,
        "creative_daily": 1,
        "traffic_quality_daily": 1,
    }

    assert store.advertiser_rows[0]["advertiser_id"] == "2997"
    assert store.creative_rows[0]["creative_id"] == "11908"
    assert store.quality_rows[0]["total_events"] == 1000
