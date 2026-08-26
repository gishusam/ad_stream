# Configurable Bronze Storage Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make AdStream Bronze persistence explicitly selectable between local Delta and Supabase Iceberg while keeping Delta as the safe default.

**Architecture:** `BronzeWriter` continues to prepare validated impression DataFrames but delegates persistence to a selected backend. Spark configuration is backend-aware. Delta remains local/default; Iceberg targets `supabase.bronze.impressions`.

**Tech Stack:** Python 3.13, PySpark 3.5.0, Delta Lake 3.0.0, Apache Iceberg 1.6.1, Supabase Analytics

**Spec:** `docs/superpowers/specs/2026-08-26-bronze-storage-backend-design.md`

## Global Constraints

- Default backend is `delta`.
- Cloud backend requires explicit `ADSTREAM_STORAGE_BACKEND=iceberg`.
- Never select cloud merely because credentials exist.
- Never log credentials.
- Kafka offsets remain committed only after successful Bronze persistence.
- Bronze does not deduplicate on `source_bid_id`.
- Do not redesign Silver or Gold.
- Do not stage unrelated dashboard, Docker, `.gitignore`, or `scripts/` changes.

---

### Task 1: Backend selection

**Files:**
- Modify: `src/processing/bronze_writer.py`
- Test: `tests/test_bronze_storage_backend.py`

**Produces:**
- `resolve_bronze_backend_name(env=None) -> str`
- accepted values: `delta`, `iceberg`
- default: `delta`
- invalid values raise `ValueError`

Steps:
1. Write failing backend-selection tests.
2. Run them and observe RED.
3. Implement the smallest resolver.
4. Re-run tests and observe GREEN.

### Task 2: Backend-specific Spark configuration

**Files:**
- Create: `src/processing/spark.py`
- Modify: `src/processing/bronze_writer.py`
- Test: `tests/test_bronze_storage_backend.py`

**Produces:**
- Delta Spark configuration for local mode.
- Iceberg REST catalog configuration for cloud mode.
- Required Supabase environment validation.
- `get_spark()` remains available from `bronze_writer.py` for compatibility.

Steps:
1. Add failing configuration tests.
2. Observe RED.
3. Implement environment validation and backend-aware Spark builders.
4. Observe GREEN.

### Task 3: Bronze persistence backends

**Files:**
- Create: `src/storage/__init__.py`
- Create: `src/storage/bronze.py`
- Modify: `src/processing/bronze_writer.py`
- Test: `tests/test_bronze_storage_backend.py`

**Produces:**
- `DeltaBronzeBackend`
- `IcebergBronzeBackend`
- backend factory
- common `write(df)` / `read()` behavior

Delta:
- append to `data/bronze/impressions`
- partition by `ingestion_date`

Iceberg:
- target `supabase.bronze.impressions`
- create namespace if needed
- create table separately if missing
- append with a separate INSERT
- never use staged CTAS

Steps:
1. Add failing backend tests.
2. Observe RED.
3. Implement minimal backends.
4. Observe GREEN.

### Task 4: BronzeWriter delegation

**Files:**
- Modify: `src/processing/bronze_writer.py`
- Test: `tests/test_bronze_storage_backend.py`

**Produces:**
- existing explicit impression schema retained
- existing event conversion retained
- `ingestion_date` retained
- empty batches remain no-ops
- persistence delegated to selected backend
- reads delegated to selected backend

Steps:
1. Add failing delegation tests.
2. Observe RED.
3. Refactor writer without changing event semantics.
4. Observe GREEN.

### Task 5: Documentation correctness

**Files:**
- Modify: `src/processing/bronze_ingestion.py`
- Modify: `src/consumers/impression_consumer.py`
- Modify: `src/consumers/base_consumer.py`
- Modify: `docs/data/IPINYOU.md`
- Modify: `src/processing/bronze_writer.py`

Corrections:
- Delta is not automatically application-level idempotent.
- `source_bid_id` is not guaranteed unique.
- Bronze preserves source duplicates.
- `local[2]` means two local worker threads.
- terminology says Bronze persistence rather than always Delta where appropriate.

### Task 6: Verification and commit

Run:
- focused Bronze tests
- full `pytest -q`
- `python -m compileall -q src`
- local Delta verification
- cloud Iceberg read verification
- `git diff --check`
- inspect staged files explicitly

Commit only Bronze implementation files and this plan.
