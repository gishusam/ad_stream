from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from src.processing.gold_aggregator import GoldAggregator


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("AdStream-GoldV2-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_advertiser_daily_uses_clearing_price_derived_spend(spark):
    silver = spark.createDataFrame(
        [
            (
                "event-1",
                date(2013, 10, 23),
                "2997",
                Decimal("30.000000"),
                Decimal("18.000000"),
                Decimal("0.018000000"),
                Decimal("12.000000"),
                "WARNING",
            ),
            (
                "event-2",
                date(2013, 10, 23),
                "2997",
                Decimal("40.000000"),
                Decimal("20.000000"),
                Decimal("0.020000000"),
                Decimal("20.000000"),
                "VALID",
            ),
        ],
        [
            "event_id",
            "event_date",
            "advertiser_id",
            "bid_price_cpm",
            "clearing_price_cpm",
            "impression_spend_cny",
            "auction_savings_cpm",
            "data_quality_status",
        ],
    )

    result = GoldAggregator().compute_advertiser_daily(silver)
    row = result.collect()[0]

    assert row.event_date == date(2013, 10, 23)
    assert row.advertiser_id == "2997"
    assert row.impressions == 2
    assert float(row.total_spend_cny) == pytest.approx(0.038)
    assert float(row.average_bid_cpm) == pytest.approx(35.0)
    assert float(row.average_clearing_cpm) == pytest.approx(19.0)
    assert float(row.total_auction_savings_cny) == pytest.approx(0.032)
    assert row.warning_events == 1
