# Real RTB Replay Design

## Goal

Replace synthetic events as AdStream's primary demonstration input with real historical iPinYou RTB records replayed through the existing Kafka → consumer → Delta Bronze path. Keep `AdStreamDataGenerator` for unit tests and load simulation only.

## Scope of this slice

This slice stops at Bronze. It does not yet rewrite Gold revenue semantics, the Airflow DAG, the dashboard, or fraud/anomaly logic. Those are follow-up slices after real source provenance is established.

## Data contract

`IPinYouLogReader` streams formatted `train.log.txt` / `test.log.txt` files without loading the dataset into memory. Each row maps to the existing `ImpressionEvent`, extended with optional real-auction provenance fields: `paying_price`, `pricing_basis`, `clicked`, `source_dataset`, `source_bid_id`, `ad_exchange`, `slot_id`, and `source_user_agent`.

The source's documented price unit is preserved as CNY/RMB CPM. No exchange-rate conversion is performed in ingestion. iPinYou records receive `country_code="CN"`, `pricing_basis="CPM"`, and `is_fraud=False` because fraud is not a ground-truth label supplied by this integration.

## Replay

`IPinYouReplayProducer` uses composition around the existing producer interface. It sends canonical event dictionaries to `ad.impressions.raw`, keyed by advertiser ID, preserving file order. A configurable EPS limiter makes historical data behave like a stream while remaining deterministic enough for local demonstrations.

## Storage

The Delta Bronze schema gains nullable columns for the new source fields so the real auction semantics survive Kafka consumption. Existing generated events remain compatible because those fields default to null. Silver's validation reference values gain `CN` and `CNY` so real iPinYou events are not quarantined solely because of origin/currency.

## Dataset handling

The dataset is not bundled or automatically downloaded. `data/external/` is gitignored. Tests use a tiny schema-compatible fixture only; it is test data, not the production demonstration source.

## Verification

Unit tests cover field mapping, ordering, rate control, no fabricated fraud label, and Bronze/Silver storage-contract changes. The end-to-end acceptance criterion is a manually downloaded real iPinYou row replayed to Kafka and persisted by the existing Bronze consumer.
