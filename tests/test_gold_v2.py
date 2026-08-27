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


def test_creative_daily_preserves_null_click_semantics(spark):
    silver = spark.createDataFrame(
        [
            (
                "event-1",
                date(2013, 10, 23),
                "2997",
                "creative-a",
                Decimal("0.018000000"),
                Decimal("18.000000"),
                True,
            ),
            (
                "event-2",
                date(2013, 10, 23),
                "2997",
                "creative-a",
                Decimal("0.020000000"),
                Decimal("20.000000"),
                False,
            ),
            (
                "event-3",
                date(2013, 10, 23),
                "2997",
                "creative-a",
                Decimal("0.015000000"),
                Decimal("15.000000"),
                None,
            ),
        ],
        [
            "event_id",
            "event_date",
            "advertiser_id",
            "creative_id",
            "impression_spend_cny",
            "clearing_price_cpm",
            "clicked",
        ],
    )

    result = GoldAggregator().compute_creative_daily(silver)
    row = result.collect()[0]

    assert row.event_date == date(2013, 10, 23)
    assert row.advertiser_id == "2997"
    assert row.creative_id == "creative-a"
    assert row.impressions == 3
    assert float(row.total_spend_cny) == pytest.approx(0.053)
    assert float(row.average_clearing_cpm) == pytest.approx(
        17.666666,
        rel=1e-5,
    )
    assert row.clicks == 1

    assert "ctr" not in result.columns


def test_traffic_quality_daily_reconciles_silver_and_quarantine(spark):
    silver = spark.createDataFrame(
        [
            (
                "event-1",
                date(2013, 10, 23),
                "VALID",
            ),
            (
                "event-2",
                date(2013, 10, 23),
                "WARNING",
            ),
            (
                "event-3",
                date(2013, 10, 23),
                "WARNING",
            ),
        ],
        [
            "event_id",
            "event_date",
            "data_quality_status",
        ],
    )

    quarantine = spark.createDataFrame(
        [
            (
                "quarantine-1",
                date(2013, 10, 23),
            ),
        ],
        [
            "quarantine_id",
            "event_date",
        ],
    )

    result = GoldAggregator().compute_traffic_quality_daily(
        silver,
        quarantine,
    )

    row = result.collect()[0]

    assert row.event_date == date(2013, 10, 23)
    assert row.valid_events == 1
    assert row.warning_events == 2
    assert row.quarantined_events == 1
    assert row.total_events == 4
    assert float(row.warning_rate) == pytest.approx(0.5)
