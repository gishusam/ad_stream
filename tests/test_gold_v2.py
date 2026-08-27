from datetime import date
from decimal import Decimal

import pytest
from src.processing.bronze_writer import get_spark

from src.processing.gold_aggregator import GoldAggregator


@pytest.fixture(scope="module")
def spark():
    session = get_spark("delta")
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


def test_delta_gold_backend_rebuild_overwrites_previous_snapshot(
    spark,
    tmp_path,
):
    from src.storage.gold import DeltaGoldBackend

    backend = DeltaGoldBackend(
        spark=spark,
        advertiser_path=str(tmp_path / "advertiser_daily"),
        creative_path=str(tmp_path / "creative_daily"),
        quality_path=str(tmp_path / "traffic_quality_daily"),
    )

    advertiser_v1 = spark.createDataFrame(
        [
            (
                date(2013, 10, 23),
                "2997",
                10,
            ),
        ],
        [
            "event_date",
            "advertiser_id",
            "impressions",
        ],
    )

    advertiser_v2 = spark.createDataFrame(
        [
            (
                date(2013, 10, 23),
                "2997",
                25,
            ),
        ],
        [
            "event_date",
            "advertiser_id",
            "impressions",
        ],
    )

    creative = spark.createDataFrame(
        [
            (
                date(2013, 10, 23),
                "2997",
                "creative-a",
                10,
            ),
        ],
        [
            "event_date",
            "advertiser_id",
            "creative_id",
            "impressions",
        ],
    )

    quality = spark.createDataFrame(
        [
            (
                date(2013, 10, 23),
                10,
            ),
        ],
        [
            "event_date",
            "total_events",
        ],
    )

    backend.write(
        advertiser_v1,
        creative,
        quality,
    )

    backend.write(
        advertiser_v2,
        creative,
        quality,
    )

    rows = backend.read_advertiser_daily().collect()

    assert len(rows) == 1
    assert rows[0].impressions == 25


def test_iceberg_gold_backend_rebuilds_three_tables(spark):
    from src.storage.gold import IcebergGoldBackend

    class RecordingSpark:
        def __init__(self):
            self.queries = []

        def sql(self, query):
            self.queries.append(
                " ".join(query.split())
            )

    fake_spark = RecordingSpark()

    backend = IcebergGoldBackend(fake_spark)

    advertiser = spark.createDataFrame(
        [
            (
                date(2013, 10, 23),
                "2997",
                10,
            ),
        ],
        [
            "event_date",
            "advertiser_id",
            "impressions",
        ],
    )

    creative = spark.createDataFrame(
        [
            (
                date(2013, 10, 23),
                "2997",
                "creative-a",
                10,
            ),
        ],
        [
            "event_date",
            "advertiser_id",
            "creative_id",
            "impressions",
        ],
    )

    quality = spark.createDataFrame(
        [
            (
                date(2013, 10, 23),
                10,
            ),
        ],
        [
            "event_date",
            "total_events",
        ],
    )

    backend.write(
        advertiser,
        creative,
        quality,
    )

    sql = "\n".join(fake_spark.queries)

    assert (
        "CREATE NAMESPACE IF NOT EXISTS supabase.gold"
        in sql
    )

    for table in [
        "advertiser_daily",
        "creative_daily",
        "traffic_quality_daily",
    ]:
        assert (
            f"DROP TABLE IF EXISTS supabase.gold.{table}"
            in sql
        )
        assert (
            f"CREATE TABLE supabase.gold.{table}"
            in sql
        )
        assert (
            f"INSERT INTO supabase.gold.{table}"
            in sql
        )

    namespace_index = next(
        i
        for i, query in enumerate(fake_spark.queries)
        if "CREATE NAMESPACE IF NOT EXISTS supabase.gold" in query
    )

    first_drop_index = next(
        i
        for i, query in enumerate(fake_spark.queries)
        if "DROP TABLE IF EXISTS" in query
    )

    assert namespace_index < first_drop_index


def test_gold_pipeline_builds_three_tables_from_canonical_silver(
    spark,
    monkeypatch,
):
    from src.processing import gold_ingestion

    silver_df = spark.createDataFrame(
        [
            (
                "event-1",
                date(2013, 10, 23),
                "2997",
                "creative-a",
                Decimal("30.000000"),
                Decimal("18.000000"),
                Decimal("0.018000000"),
                Decimal("12.000000"),
                True,
                "WARNING",
            ),
            (
                "event-2",
                date(2013, 10, 23),
                "2997",
                "creative-a",
                Decimal("40.000000"),
                Decimal("20.000000"),
                Decimal("0.020000000"),
                Decimal("20.000000"),
                None,
                "VALID",
            ),
        ],
        [
            "event_id",
            "event_date",
            "advertiser_id",
            "creative_id",
            "bid_price_cpm",
            "clearing_price_cpm",
            "impression_spend_cny",
            "auction_savings_cpm",
            "clicked",
            "data_quality_status",
        ],
    )

    quarantine_df = spark.createDataFrame(
        [],
        "quarantine_id string, event_date date",
    )

    class FakeSilverBackend:
        def read_silver(self):
            return silver_df

        def read_quarantine(self):
            return quarantine_df

    class FakeGoldBackend:
        def __init__(self):
            self.tables = None

        def write(
            self,
            advertiser_df,
            creative_df,
            quality_df,
        ):
            self.tables = (
                advertiser_df,
                creative_df,
                quality_df,
            )

    fake_gold = FakeGoldBackend()

    monkeypatch.setattr(
        gold_ingestion,
        "build_silver_backend",
        lambda spark, backend_name: FakeSilverBackend(),
    )

    monkeypatch.setattr(
        gold_ingestion,
        "build_gold_backend",
        lambda spark, backend_name: fake_gold,
    )

    pipeline = gold_ingestion.GoldIngestionPipeline(
        spark=spark,
        backend_name="delta",
    )

    result = pipeline.run()

    assert result == {
        "silver": 2,
        "quarantine": 0,
        "advertiser_daily": 1,
        "creative_daily": 1,
        "traffic_quality_daily": 1,
    }

    advertiser, creative, quality = fake_gold.tables

    assert advertiser.collect()[0].impressions == 2
    assert creative.collect()[0].impressions == 2

    quality_row = quality.collect()[0]
    assert quality_row.total_events == 2
    assert quality_row.valid_events == 1
    assert quality_row.warning_events == 1
    assert quality_row.quarantined_events == 0


def test_gold_pipeline_treats_missing_delta_quarantine_as_empty(
    spark,
    monkeypatch,
    tmp_path,
):
    from src.processing import gold_ingestion
    from src.storage.silver import DeltaSilverBackend

    silver_df = spark.createDataFrame(
        [
            (
                "event-1",
                date(2013, 10, 23),
                "2997",
                "creative-a",
                Decimal("30.000000"),
                Decimal("18.000000"),
                Decimal("0.018000000"),
                Decimal("12.000000"),
                None,
                "WARNING",
            ),
        ],
        """
        event_id string,
        event_date date,
        advertiser_id string,
        creative_id string,
        bid_price_cpm decimal(18,6),
        clearing_price_cpm decimal(18,6),
        impression_spend_cny decimal(18,9),
        auction_savings_cpm decimal(18,6),
        clicked boolean,
        data_quality_status string
        """,
    )

    silver_path = str(tmp_path / "silver")
    quarantine_path = str(tmp_path / "missing_quarantine")

    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .save(silver_path)
    )

    silver_backend = DeltaSilverBackend(
        spark,
        silver_path=silver_path,
        quarantine_path=quarantine_path,
    )

    class FakeGoldBackend:
        def write(
            self,
            advertiser_df,
            creative_df,
            quality_df,
        ):
            pass

    monkeypatch.setattr(
        gold_ingestion,
        "build_silver_backend",
        lambda spark, backend_name: silver_backend,
    )

    monkeypatch.setattr(
        gold_ingestion,
        "build_gold_backend",
        lambda spark, backend_name: FakeGoldBackend(),
    )

    pipeline = gold_ingestion.GoldIngestionPipeline(
        spark=spark,
        backend_name="delta",
    )

    result = pipeline.run()

    assert result["silver"] == 1
    assert result["quarantine"] == 0
    assert result["traffic_quality_daily"] == 1
