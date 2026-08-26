# AdStream Bronze Storage Backend Design

**Date:** 2026-08-26
**Status:** Approved for implementation

## 1. Context

AdStream currently writes validated impression events to a local Delta Lake
table at:

    data/bronze/impressions

The real iPinYou replay pipeline has now been verified end-to-end through
Kafka and local Bronze.

Cloud persistence has also been verified using:

- Apache Spark 3.5
- Apache Iceberg 1.6.1
- Supabase Analytics
- Supabase Iceberg REST Catalog
- table: `supabase.bronze.impressions`

The cloud table contains the same 1,000 Bronze rows as the local Delta table,
including the repeated iPinYou `source_bid_id` present in the original source.

Supabase Files storage is not used as a Delta backend because Hadoop S3A was
not compatible with the Supabase Files S3 gateway in testing.

## 2. Goal

Allow the existing Bronze ingestion pipeline to write to either:

1. local Delta Lake for development; or
2. Supabase Iceberg for deliberate cloud/portfolio runs.

The Kafka consumer and validation flow must not need to know which storage
technology is selected.

## 3. Backend Selection

Storage is selected explicitly with:

    ADSTREAM_STORAGE_BACKEND

Supported values:

    delta
    iceberg

If the variable is missing, the default is:

    delta

This is intentional.

A developer who has Supabase credentials loaded must not accidentally write
test data to cloud storage merely because credentials happen to exist.

Examples:

    # local development
    python -m src.processing.bronze_ingestion

    # deliberate cloud ingestion
    ADSTREAM_STORAGE_BACKEND=iceberg \
      python -m src.processing.bronze_ingestion

Unknown backend values must fail immediately with a clear configuration
error.

## 4. Architecture

    Kafka
      |
      v
    ImpressionConsumer
      |
      v
    Validation
      |
      v
    BronzeWriter
      |
      +----------------------+
      |                      |
      v                      v
    Delta backend         Iceberg backend
    local filesystem      Supabase Analytics
      |                      |
      v                      v
    data/bronze/          supabase.bronze.
    impressions           impressions

`BronzeWriter` owns event-to-DataFrame preparation.

Storage-specific behavior is delegated to a backend implementation.

## 5. Components

### Spark configuration

Spark session construction must be separated from storage-writing logic.

Local Delta mode requires:

- Delta Spark extension
- Delta catalog
- constrained local resources

Cloud Iceberg mode requires:

- Iceberg Spark extension
- Iceberg REST catalog
- Iceberg AWS bundle
- Supabase catalog URI
- Supabase S3 endpoint
- Supabase S3 credentials
- Supabase catalog token
- AWS region

Secrets must only come from environment variables and must never be logged.

### Delta Bronze backend

Responsibilities:

- write append-only batches to `data/bronze/impressions`
- partition by `ingestion_date`
- read the local Bronze table

This remains the default development backend.

### Iceberg Bronze backend

Responsibilities:

- use `supabase.bronze.impressions`
- create the `bronze` namespace if necessary
- create the table separately from inserting data
- append validated batches
- read the cloud Bronze table

The implementation must not use staged CTAS / DataFrameWriterV2 `.create()`
for data writes because the current Supabase Iceberg REST catalog does not
support stage-create.

### BronzeWriter

Responsibilities:

- accept validated `ImpressionEvent` objects
- convert events into the explicit Spark schema
- add `ingestion_date`
- delegate write/read operations to the selected backend

It must not contain backend-specific Delta or Iceberg write logic.

## 6. Data Semantics

Bronze preserves source truth.

`source_bid_id` is a source lineage identifier and is not assumed to be a
unique event key.

The iPinYou source contains at least one repeated `source_bid_id` where the
records have different timestamps.

Bronze must not deduplicate records solely by `source_bid_id`.

AdStream's event identity remains separate from the source bid identifier.

## 7. Delivery Semantics

The existing Kafka flow commits offsets only after a successful Bronze write.

That ordering must remain unchanged:

    consume
      -> validate
      -> Bronze write succeeds
      -> Kafka offset commit

The current documentation suggesting that Delta's transaction log
automatically prevents duplicate application-level events on replay is
incorrect and must be removed.

Delta provides transactional table writes, but an append retried by the
application can still create duplicate business events unless explicit
idempotency logic is implemented.

This change does not add such deduplication.

## 8. Failure Behaviour

### Delta mode

Failure to write local Delta:

- raise the error
- do not commit the Kafka offset

### Iceberg mode

Missing required Supabase configuration:

- fail during startup/configuration
- identify the missing variable
- never silently fall back to Delta

Failure during Iceberg append:

- raise the error
- do not commit the Kafka offset

### Invalid backend

Any value other than `delta` or `iceberg`:

- fail fast
- show the accepted backend values

## 9. Testing

Tests must cover at minimum:

1. default backend resolves to `delta`
2. explicit `delta` resolves to local backend
3. explicit `iceberg` resolves to cloud backend
4. invalid backend fails clearly
5. Iceberg configuration validates required environment variables
6. BronzeWriter delegates writes to its backend
7. empty batches remain no-ops
8. ingestion_date is still added
9. Delta backend uses append + partitioning
10. Iceberg backend inserts into the existing Iceberg table
11. writer reads through the selected backend
12. existing consumer offset-commit behaviour remains unchanged

Cloud credentials are not required for the unit test suite.

Live Supabase verification remains a separate integration/smoke test.

## 10. Documentation Updates

Update terminology that currently assumes all Bronze storage is Delta.

Examples include:

- `bronze_writer.py`
- `bronze_ingestion.py`
- consumer comments/docstrings
- `docs/data/IPINYOU.md`

Document:

- Delta = local development backend
- Iceberg/Supabase = cloud portfolio backend
- `source_bid_id` is not guaranteed unique
- Bronze preserves source duplicates
- Kafka offsets are committed after successful Bronze persistence

Also correct the `local[2]` comment: it uses two local worker threads, not all
CPU cores.

## 11. Scope

Included:

- configurable Bronze storage backend
- Spark configuration required for both backends
- unit tests
- Bronze documentation corrections
- cloud Iceberg Bronze support

Not included:

- Silver redesign
- Gold redesign
- automatic deduplication
- exactly-once Kafka semantics
- changing the iPinYou event schema
- replacing `content_id` with `creative_id`
- production orchestration

Silver will be addressed after Bronze storage is complete and verified.

## 12. Success Criteria

The change is complete when:

- the full existing unit test suite passes
- local Delta ingestion still works without Supabase credentials
- `ADSTREAM_STORAGE_BACKEND=iceberg` targets
  `supabase.bronze.impressions`
- backend selection is explicit and tested
- Kafka offsets remain uncommitted on failed persistence
- no credentials are committed
- the real Supabase Bronze table remains readable
