from pathlib import Path


def test_bronze_schema_preserves_real_rtb_auction_fields():
    text = Path("src/processing/bronze_writer.py").read_text()

    for field in (
        "paying_price",
        "pricing_basis",
        "clicked",
        "source_dataset",
        "source_bid_id",
        "ad_exchange",
        "slot_id",
        "source_user_agent",
    ):
        assert f'StructField("{field}"' in text


def test_silver_reference_values_accept_ipinyou_country_and_currency():
    text = Path("src/processing/silver_transformer.py").read_text()

    assert '"CN"' in text
    assert '"CNY"' in text
