# AdStream Gold v2 Design

## Purpose
Gold v2 converts trusted Silver RTB events into three business-ready daily aggregates.

Gold reads:
- `silver.impressions`
- `silver.quarantine` only for quality reconciliation

Gold does not invent source semantics and does not perform fraud detection.

## Architecture
```text
silver.impressions
        |
        v
   GoldAggregator
        |
        +-------------------+-------------------+
        |                   |                   |
        v                   v                   v
advertiser_daily      creative_daily    traffic_quality_daily
        |                   |                   |
        +-------------------+-------------------+
                            |
                            v
                    Delta or Iceberg
```

## advertiser_daily

Columns:
- event_date
- advertiser_id
- impressions
- total_spend_cny
- average_bid_cpm
- average_clearing_cpm
- total_auction_savings_cny
- warning_events

Rules:
- impressions = count of Silver events
- total_spend_cny = SUM(impression_spend_cny)
- average_bid_cpm = AVG(bid_price_cpm)
- average_clearing_cpm = AVG(clearing_price_cpm)
- total_auction_savings_cny = SUM(auction_savings_cpm / 1000)
- warning_events = count where status is WARNING

Bid price is never treated as spend.

## creative_daily

Columns:
- event_date
- advertiser_id
- creative_id
- impressions
- total_spend_cny
- average_clearing_cpm
- clicks

Rules:
- impressions = count of Silver events
- total_spend_cny = SUM(impression_spend_cny)
- average_clearing_cpm = AVG(clearing_price_cpm)
- clicks = count where clicked = TRUE

NULL clicked means click information was unavailable.
Gold v2 does not calculate CTR.

## traffic_quality_daily

Columns:
- event_date
- total_events
- valid_events
- warning_events
- warning_rate
- quarantined_events

Rules:
- valid_events = Silver VALID rows
- warning_events = Silver WARNING rows
- quarantined_events = quarantine rows
- total_events = valid + warning + quarantined
- warning_rate = warning_events / total_events

## Storage

Delta:
- data/gold/advertiser_daily
- data/gold/creative_daily
- data/gold/traffic_quality_daily

Iceberg:
- supabase.gold.advertiser_daily
- supabase.gold.creative_daily
- supabase.gold.traffic_quality_daily

Gold is rebuilt from the current Silver snapshot and overwrites previous Gold results.

## Verification

Verify:
1. advertiser formulas
2. creative formulas
3. NULL click semantics
4. quality reconciliation
5. Delta persistence
6. Iceberg persistence
7. unchanged Silver produces identical Gold on rerun

## Out of Scope

- fraud analytics
- CTR
- campaign aggregates
- hourly aggregates
- Airflow
- dashboard
- APIs
- observability
- streaming Gold
- extra dimensions
