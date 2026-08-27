from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from src.processing.silver_transformer import SilverTransformer


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("AdStream-SilverV2-Tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_repeated_source_bid_id_at_different_timestamps_is_preserved(spark):
    bronze = spark.createDataFrame(
        [
            (
                "4fa883601704fcea594fd2c17a1560f9",
                "ipinyou",
                "4fa883601704fcea594fd2c17a1560f9",
                datetime(2013, 10, 23, 17, 10, 5, 542000),
            ),
            (
                "4fa883601704fcea594fd2c17a1560f9",
                "ipinyou",
                "4fa883601704fcea594fd2c17a1560f9",
                datetime(2013, 10, 23, 17, 10, 11, 359000),
            ),
        ],
        [
            "impression_id",
            "source_dataset",
            "source_bid_id",
            "timestamp",
        ],
    )

    result = SilverTransformer().deduplicate(bronze)

    assert result.count() == 2


def test_event_id_is_deterministic_for_same_source_event(spark):
    bronze = spark.createDataFrame(
        [
            (
                "ipinyou",
                "bid-123",
                datetime(2013, 10, 23, 17, 10, 5, 542000),
            ),
            (
                "ipinyou",
                "bid-123",
                datetime(2013, 10, 23, 17, 10, 5, 542000),
            ),
        ],
        [
            "source_dataset",
            "source_bid_id",
            "timestamp",
        ],
    )

    result = SilverTransformer().add_event_identity(bronze)

    event_ids = [
        row.event_id
        for row in result.select("event_id").collect()
    ]

    assert len(set(event_ids)) == 1
    assert len(event_ids[0]) == 64


def test_normalize_fields_uses_canonical_rtb_names(spark):
    bronze = spark.createDataFrame(
        [
            (
                datetime(2013, 10, 23, 17, 10, 5),
                "creative-123",
                30.0,
                18.0,
            ),
        ],
        [
            "timestamp",
            "content_id",
            "bid_price",
            "paying_price",
        ],
    )

    result = SilverTransformer().normalize_fields(bronze)

    assert "event_timestamp" in result.columns
    assert "creative_id" in result.columns
    assert "bid_price_cpm" in result.columns
    assert "clearing_price_cpm" in result.columns

    assert "timestamp" not in result.columns
    assert "content_id" not in result.columns
    assert "bid_price" not in result.columns
    assert "paying_price" not in result.columns


def test_derive_economics_uses_fixed_precision_cpm_semantics(spark):
    from decimal import Decimal

    bronze = spark.createDataFrame(
        [
            (
                30.0,
                18.0,
                "CPM",
                "CNY",
            ),
        ],
        [
            "bid_price_cpm",
            "clearing_price_cpm",
            "pricing_basis",
            "currency",
        ],
    )

    result = SilverTransformer().derive_economics(bronze)

    row = result.first()
    types = {
        field.name: field.dataType.simpleString()
        for field in result.schema.fields
    }

    assert types["bid_price_cpm"] == "decimal(18,6)"
    assert types["clearing_price_cpm"] == "decimal(18,6)"
    assert types["impression_spend_cny"] == "decimal(18,9)"
    assert types["auction_savings_cpm"] == "decimal(18,6)"

    assert row.bid_price_cpm == Decimal("30.000000")
    assert row.clearing_price_cpm == Decimal("18.000000")
    assert row.impression_spend_cny == Decimal("0.018000000")
    assert row.auction_savings_cpm == Decimal("12.000000")
