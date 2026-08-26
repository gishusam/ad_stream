from pathlib import Path

from src.sources.ipinyou import IPinYouLogReader


FIXTURE = Path(__file__).parent / "fixtures" / "ipinyou_train_sample.tsv"


def test_reader_maps_real_rtb_fields_without_inventing_fraud():
    event = next(iter(IPinYouLogReader(FIXTURE)))

    assert event.impression_id == "72879b068fec2d3c2afd51"
    assert event.user_id == "Vhk7ZAnyPIc9tbE"
    assert event.advertiser_id == "1458"
    assert event.campaign_id == "ipinyou-1458"
    assert event.content_id == "48f2e9ba1570a5e1dd653caa"
    assert event.bid_price == 300.0
    assert event.paying_price == 55.0
    assert event.currency == "CNY"
    assert event.pricing_basis == "CPM"
    assert event.clicked is False
    assert event.is_fraud is False
    assert event.source_dataset == "ipinyou"
    assert event.source_bid_id == "72879b068fec2d3c2afd51"
    assert event.ad_exchange == "1"
    assert event.slot_id == "mm_34955955_11267874_10048459"
    assert event.device_type == "desktop"
    assert event.country_code == "CN"
    assert event.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f") == "2013-06-06 00:01:05.500000"


def test_reader_preserves_file_order_and_maps_mobile_user_agents():
    events = list(IPinYouLogReader(FIXTURE))

    assert [event.impression_id for event in events] == [
        "72879b068fec2d3c2afd51",
        "bid-mobile-2",
    ]
    assert events[1].clicked is True
    assert events[1].device_type == "mobile"


def test_reader_limit_stops_without_loading_entire_file():
    events = list(IPinYouLogReader(FIXTURE).iter_events(limit=1))

    assert len(events) == 1
    assert events[0].impression_id == "72879b068fec2d3c2afd51"
