# AdStream Silver v2 Design

## Goal

Convert Bronze source truth into canonical, trustworthy RTB impression events
that Gold can safely aggregate.

## Outputs

- `silver.impressions` — analytically usable VALID/WARNING events
- `silver.quarantine` — structurally unusable INVALID events

## Remove legacy behavior

Silver v2 removes:

- deduplication on `impression_id`
- generated user-profile enrichment
- legitimate/fraud table split
- synthetic fraud assumptions
- arbitrary synthetic bid thresholds
- Delta-only persistence

## Canonical schema

Identity:
- event_id
- source_dataset
- source_bid_id

Time:
- event_timestamp
- event_date
- ingestion_date

Business:
- advertiser_id
- campaign_id
- creative_id
- user_id
- ad_exchange
- slot_id
- device_type
- ad_format
- country_code

Economics:
- currency
- pricing_basis
- bid_price_cpm
- clearing_price_cpm
- impression_spend_cny
- auction_savings_cpm

Engagement:
- clicked

Quality:
- data_quality_status
- quality_issues

## Identity

`source_bid_id` is lineage, not a unique event key.

`event_id` is deterministic SHA-256 of:

source_dataset + source_bid_id + event_timestamp

Same source bid ID at different timestamps must remain separate events.

## Economics

For CPM events:

impression_spend_cny = clearing_price_cpm / 1000

auction_savings_cpm = bid_price_cpm - clearing_price_cpm

Money fields use fixed-precision decimal types.

## Quality

VALID:
- structurally usable with no anomaly

WARNING:
- usable but suspicious, e.g. clearing price > bid price
- missing exchange/slot
- unknown device/format

INVALID:
- missing source identity
- missing timestamp
- missing advertiser/creative
- missing or negative bid price
- negative clearing price
- unsupported pricing basis

Only INVALID events go to quarantine.

`clicked = NULL` is valid for raw impression logs.

## Incremental behavior

Normal runs are incremental and idempotent on `event_id`.

Replaying the same Bronze event must not create duplicate Silver rows.

Bronze remains the immutable source for rebuilds/backfills.

## Storage

Default:
- Delta local

Explicit cloud:
- Iceberg / Supabase

Cloud tables:
- `supabase.silver.impressions`
- `supabase.silver.quarantine`

## Reconciliation

For each processed scope:

Bronze processed = Silver usable + Silver quarantine

No silent row loss.

## Gold

Gold v1 depends on retired synthetic Silver semantics.

It must not silently consume Silver v2 until Gold is redesigned.
