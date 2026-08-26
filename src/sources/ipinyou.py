from __future__ import annotations

import csv
from datetime import datetime
from pathlib import Path
from typing import Iterator

from src.models.events import ImpressionEvent


REQUIRED_COLUMNS = {
    "click",
    "bidid",
    "timestamp",
    "ipinyouid",
    "useragent",
    "adexchange",
    "slotid",
    "creative",
    "bidprice",
    "payprice",
    "advertiser",
}


def _parse_timestamp(raw: str) -> datetime:
    """Parse the iPinYou YYYYMMDDHHMMSSmmm timestamp without inventing a timezone."""
    value = raw.strip()
    if value.endswith(".0"):
        value = value[:-2]
    if len(value) != 17 or not value.isdigit():
        raise ValueError(f"Unsupported iPinYou timestamp: {raw!r}")
    return datetime.strptime(value, "%Y%m%d%H%M%S%f")


def _device_type(user_agent: str) -> str:
    normalized = user_agent.lower()
    if "ipad" in normalized or "tablet" in normalized:
        return "tablet"
    if any(token in normalized for token in ("android", "iphone", "mobile")):
        return "mobile"
    return "desktop"


class IPinYouLogReader:
    """Stream formatted iPinYou ``train.log.txt`` / ``test.log.txt`` rows as AdStream events.

    The reader preserves file order and does not load the full dataset into memory.
    iPinYou prices are kept in the source-native RMB/CPM units documented by the
    dataset. No fraud label is inferred or fabricated in this ingestion layer.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def __iter__(self) -> Iterator[ImpressionEvent]:
        return self.iter_events()

    def iter_events(self, limit: int | None = None) -> Iterator[ImpressionEvent]:
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")

        with self.path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            if reader.fieldnames is None:
                raise ValueError("iPinYou log is missing a header row")

            missing = REQUIRED_COLUMNS.difference(reader.fieldnames)
            if missing:
                names = ", ".join(sorted(missing))
                raise ValueError(f"iPinYou log is missing required columns: {names}")

            emitted = 0
            for row in reader:
                if limit is not None and emitted >= limit:
                    break

                advertiser = row["advertiser"].strip()
                bid_id = row["bidid"].strip()
                user_id = row["ipinyouid"].strip() or f"anonymous-{bid_id}"
                user_agent = row["useragent"].strip()

                yield ImpressionEvent(
                    impression_id=bid_id,
                    user_id=user_id,
                    advertiser_id=advertiser,
                    campaign_id=f"ipinyou-{advertiser}",
                    content_id=row["creative"].strip(),
                    bid_price=float(row["bidprice"]),
                    paying_price=float(row["payprice"]),
                    pricing_basis="CPM",
                    currency="CNY",
                    country_code="CN",
                    device_type=_device_type(user_agent),
                    ad_format="banner",
                    timestamp=_parse_timestamp(row["timestamp"]),
                    clicked=row["click"].strip() == "1",
                    is_fraud=False,
                    source_dataset="ipinyou",
                    source_bid_id=bid_id,
                    ad_exchange=row["adexchange"].strip() or None,
                    slot_id=row["slotid"].strip() or None,
                    source_user_agent=user_agent or None,
                )
                emitted += 1
