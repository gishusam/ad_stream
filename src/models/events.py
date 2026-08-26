from pydantic import BaseModel, Field, field_validator
from datetime import datetime, timezone
from typing import Literal
import uuid


class ImpressionEvent(BaseModel):
    """
    Represents a single ad impression — the core business event.
    Every time an ad is shown to a user, one of these is created.
    """
    impression_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    advertiser_id: str
    campaign_id: str
    content_id: str
    bid_price: float
    currency: Literal["USD", "EUR", "GBP", "KES", "CNY"]
    country_code: str
    device_type: Literal["mobile", "desktop", "tablet", "ctv"]
    ad_format: Literal["banner", "video", "native", "audio"]
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    is_fraud: bool = False

    # Optional source fields preserve real auction semantics without breaking
    # the synthetic generator used for tests and load simulation.
    paying_price: float | None = None
    pricing_basis: Literal["CPM", "CPC", "CPA"] | None = None
    clicked: bool | None = None
    source_dataset: str | None = None
    source_bid_id: str | None = None
    ad_exchange: str | None = None
    slot_id: str | None = None
    source_user_agent: str | None = None

    @field_validator("bid_price")
    @classmethod
    def bid_price_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError(f"bid_price must be positive, got {v}")
        return round(v, 6)

    @field_validator("paying_price")
    @classmethod
    def paying_price_must_be_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError(f"paying_price must be non-negative, got {v}")
        return round(v, 6) if v is not None else None

    @field_validator("country_code")
    @classmethod
    def country_code_must_be_valid(cls, v):
        if len(v) != 2:
            raise ValueError(f"country_code must be 2 characters, got '{v}'")
        return v.upper()


class PaymentEvent(BaseModel):
    """
    Represents a payment from an advertiser.
    Arrives hours after the impression — this is what we reconcile.
    """
    payment_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    advertiser_id: str
    campaign_id: str
    amount: float
    currency: Literal["USD", "EUR", "GBP", "KES"]
    impression_batch_id: str      # links back to a batch of impressions
    payment_status: Literal["pending", "settled", "failed", "disputed"]
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("amount")
    @classmethod
    def amount_must_be_positive(cls, v):
        if v <= 0:
            raise ValueError(f"amount must be positive, got {v}")
        return round(v, 2)


class EngagementEvent(BaseModel):
    """
    Represents user interaction with content — watch time, completion, clicks.
    Used to calculate CPM and content monetisation efficiency.
    """
    engagement_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    content_id: str
    impression_id: str            # links back to the impression that drove this
    watch_duration_seconds: int
    completion_rate: float        # 0.0 to 1.0
    clicked: bool
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("completion_rate")
    @classmethod
    def completion_rate_must_be_valid(cls, v):
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"completion_rate must be between 0 and 1, got {v}")
        return round(v, 4)