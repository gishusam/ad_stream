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


def test_invalid_event_is_routed_to_quarantine(spark):
    bronze = spark.createDataFrame(
        [
            (
                "ipinyou",
                "bid-invalid",
                datetime(2013, 10, 23, 17, 10, 5),
                None,                  # advertiser_id missing
                "creative-123",
                30.0,
                18.0,
                "CPM",
                "exchange-1",
                "slot-1",
                "user-1",
                "desktop",
                "banner",
            ),
            (
                "ipinyou",
                "bid-valid",
                datetime(2013, 10, 23, 17, 10, 6),
                "2997",
                "creative-456",
                30.0,
                18.0,
                "CPM",
                "exchange-1",
                "slot-2",
                "user-2",
                "desktop",
                "banner",
            ),
        ],
        [
            "source_dataset",
            "source_bid_id",
            "event_timestamp",
            "advertiser_id",
            "creative_id",
            "bid_price_cpm",
            "clearing_price_cpm",
            "pricing_basis",
            "ad_exchange",
            "slot_id",
            "user_id",
            "device_type",
            "ad_format",
        ],
    )

    usable, quarantine = SilverTransformer().classify_quality(bronze)

    assert usable.count() == 1
    assert quarantine.count() == 1

    invalid = quarantine.first()

    assert invalid.source_bid_id == "bid-invalid"
    assert invalid.data_quality_status == "INVALID"
    assert "missing_advertiser_id" in invalid.quality_issues


def test_warning_event_stays_usable(spark):
    bronze = spark.createDataFrame(
        [
            (
                "ipinyou",
                "bid-warning",
                datetime(2013, 10, 23, 17, 10, 5),
                "2997",
                "creative-123",
                20.0,
                25.0,          # clearing price > bid price
                "CPM",
                None,          # missing exchange
                "slot-1",
                "user-1",
                "desktop",
                "banner",
            ),
        ],
        """
        source_dataset string,
        source_bid_id string,
        event_timestamp timestamp,
        advertiser_id string,
        creative_id string,
        bid_price_cpm double,
        clearing_price_cpm double,
        pricing_basis string,
        ad_exchange string,
        slot_id string,
        user_id string,
        device_type string,
        ad_format string
        """,
    )

    usable, quarantine = SilverTransformer().classify_quality(bronze)

    assert usable.count() == 1
    assert quarantine.count() == 0

    row = usable.first()

    assert row.data_quality_status == "WARNING"
    assert "clearing_price_exceeds_bid" in row.quality_issues
    assert "missing_ad_exchange" in row.quality_issues


@pytest.mark.parametrize(
    ("overrides", "expected_issue"),
    [
        ({"source_dataset": None}, "missing_source_dataset"),
        ({"source_bid_id": None}, "missing_source_bid_id"),
        ({"event_timestamp": None}, "missing_event_timestamp"),
        ({"creative_id": None}, "missing_creative_id"),
        ({"bid_price_cpm": None}, "missing_bid_price"),
        ({"bid_price_cpm": -1.0}, "negative_bid_price"),
        ({"clearing_price_cpm": -1.0}, "negative_clearing_price"),
        ({"pricing_basis": None}, "missing_pricing_basis"),
        ({"pricing_basis": "CPC"}, "unsupported_pricing_basis"),
    ],
)
def test_structurally_invalid_events_are_quarantined(
    spark,
    overrides,
    expected_issue,
):
    values = {
        "source_dataset": "ipinyou",
        "source_bid_id": "bid-123",
        "event_timestamp": datetime(2013, 10, 23, 17, 10, 5),
        "advertiser_id": "2997",
        "creative_id": "creative-123",
        "bid_price_cpm": 30.0,
        "clearing_price_cpm": 18.0,
        "pricing_basis": "CPM",
        "ad_exchange": "exchange-1",
        "slot_id": "slot-1",
        "user_id": "user-1",
        "device_type": "desktop",
        "ad_format": "banner",
    }
    values.update(overrides)

    bronze = spark.createDataFrame(
        [tuple(values.values())],
        """
        source_dataset string,
        source_bid_id string,
        event_timestamp timestamp,
        advertiser_id string,
        creative_id string,
        bid_price_cpm double,
        clearing_price_cpm double,
        pricing_basis string,
        ad_exchange string,
        slot_id string,
        user_id string,
        device_type string,
        ad_format string
        """,
    )

    usable, quarantine = SilverTransformer().classify_quality(bronze)

    assert usable.count() == 0
    assert quarantine.count() == 1

    row = quarantine.first()

    assert row.data_quality_status == "INVALID"
    assert expected_issue in row.quality_issues
