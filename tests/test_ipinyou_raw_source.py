import bz2
from pathlib import Path

from src.sources.ipinyou import IPinYouRawImpressionReader

RAW_2821 = "\t".join([
    "7281a6617675e62f6246fe18613003e9", "20131023111700562", "1", "C8BGsUC10o",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
    "222.187.198.*", "80", "93", "4", "d49ed6ed0072ba1291d16e2da5acfe71",
    "d5a63c3e6ef51c081440e142e8aa9c14", "null", "9223372032560761703", "960", "90",
    "FirstView", "Na", "0", "10717", "294", "230", "null", "2821",
    "10057,10048,10059"
])


def _write_bz2(path: Path, lines: list[str]) -> None:
    with bz2.open(path, "wt", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def test_raw_reader_maps_original_impression_schema(tmp_path):
    path = tmp_path / "imp.20131023.txt.bz2"
    _write_bz2(path, [RAW_2821])

    event = next(iter(IPinYouRawImpressionReader(path)))

    assert event.impression_id == "7281a6617675e62f6246fe18613003e9"
    assert event.advertiser_id == "2821"
    assert event.content_id == "10717"
    assert event.bid_price == 294.0
    assert event.paying_price == 230.0
    assert event.ad_exchange == "4"
    assert event.slot_id == "9223372032560761703"
    assert event.clicked is None
    assert event.is_fraud is False
    assert event.source_dataset == "ipinyou"
    assert event.timestamp.strftime("%Y-%m-%d %H:%M:%S.%f") == "2013-10-23 11:17:00.562000"


def test_raw_reader_filters_advertiser_before_limit(tmp_path):
    path = tmp_path / "imp.20131023.txt.bz2"
    cols = RAW_2821.split("\t")
    cols[0] = "real-2997-a"
    cols[22] = "2997"
    row_2997_a = "\t".join(cols)
    cols[0] = "real-2997-b"
    row_2997_b = "\t".join(cols)
    _write_bz2(path, [RAW_2821, row_2997_a, row_2997_b])

    events = list(
        IPinYouRawImpressionReader(path, advertiser_id="2997").iter_events(limit=1)
    )

    assert [event.impression_id for event in events] == ["real-2997-a"]
    assert events[0].advertiser_id == "2997"
