from src.producers.ipinyou_replay_producer import build_reader
from src.sources.ipinyou import IPinYouRawImpressionReader


def test_build_reader_uses_raw_bz2_reader_for_original_impression_logs(tmp_path):
    path = tmp_path / "imp.20131023.txt.bz2"

    reader = build_reader(path, advertiser_id="2997")

    assert isinstance(reader, IPinYouRawImpressionReader)
    assert reader.advertiser_id == "2997"
