# Real RTB Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replay real iPinYou RTB logs through AdStream's existing Kafka ingestion path while preserving real auction provenance into Delta Bronze.

**Architecture:** A streaming file reader maps formatted iPinYou rows to the canonical `ImpressionEvent`. A replay producer sends those events to `ad.impressions.raw`; the existing consumer and Bronze writer continue downstream. Optional source fields keep the old synthetic generator backward-compatible.

**Tech Stack:** Python 3, Pydantic 2, kafka-python, PySpark 3.5, Delta Lake 3.0, pytest.

**Spec:** `docs/superpowers/specs/2026-08-26-real-rtb-replay-design.md`

## Global Constraints

- Do not bundle or auto-download the iPinYou dataset.
- Do not infer or fabricate a fraud label from iPinYou rows.
- Preserve `bidprice` and `payprice` as source-native CNY/RMB CPM values in this slice.
- Keep the existing synthetic generator usable for tests/load simulation.
- Stop this slice at Bronze; Gold semantics are a separate follow-up.

---

### Task 1: Extend the canonical impression contract

**Files:**
- Modify: `src/models/events.py`
- Test: `tests/test_ipinyou_source.py`

**Interfaces:**
- Produces: `ImpressionEvent` with nullable `paying_price`, `pricing_basis`, `clicked`, `source_dataset`, `source_bid_id`, `ad_exchange`, `slot_id`, `source_user_agent`.

- [ ] Write a failing parser test that expects `currency="CNY"`, `paying_price=55.0`, `pricing_basis="CPM"`, and `is_fraud=False`.
- [ ] Run `python -m pytest -q tests/test_ipinyou_source.py` and confirm the feature is missing.
- [ ] Add the optional fields and allow `CNY` on `ImpressionEvent`.
- [ ] Add a non-negative validator for `paying_price`.
- [ ] Re-run the focused test.

### Task 2: Add the iPinYou streaming reader

**Files:**
- Create: `src/sources/__init__.py`
- Create: `src/sources/ipinyou.py`
- Create: `tests/fixtures/ipinyou_train_sample.tsv`
- Test: `tests/test_ipinyou_source.py`

**Interfaces:**
- Produces: `IPinYouLogReader(path).iter_events(limit=None) -> Iterator[ImpressionEvent]`.

- [ ] Test field mapping, file-order preservation, mobile/desktop mapping, and `limit`.
- [ ] Implement a `csv.DictReader(..., delimiter="\t")` iterator with required-column validation.
- [ ] Parse the 17-digit source timestamp without inventing a timezone.
- [ ] Map `creative→content_id`, `advertiser→advertiser_id`, `bidprice→bid_price`, `payprice→paying_price`, `click→clicked`.
- [ ] Run `python -m pytest -q tests/test_ipinyou_source.py`.

### Task 3: Add Kafka replay

**Files:**
- Create: `src/producers/ipinyou_replay_producer.py`
- Test: `tests/test_ipinyou_replay.py`

**Interfaces:**
- Consumes: any iterable of `ImpressionEvent` and an `EventSink` implementing `produce()` + `flush()`.
- Produces: `IPinYouReplayProducer.replay(...) -> int` number of events sent.

- [ ] Write a recording-sink test asserting topic `ad.impressions.raw`, advertiser partition key and preserved paying price.
- [ ] Write a rate-control test asserting a 4 EPS stream sleeps 0.25 seconds between records only.
- [ ] Implement replay using composition rather than importing Kafka at module-import time.
- [ ] Add a CLI that lazily constructs the existing `BaseProducer`.
- [ ] Run `python -m pytest -q tests/test_ipinyou_replay.py`.

### Task 4: Persist real source semantics in Bronze

**Files:**
- Modify: `src/processing/bronze_writer.py`
- Modify: `src/processing/silver_transformer.py`
- Test: `tests/test_real_rtb_storage_contract.py`

**Interfaces:**
- Bronze adds nullable columns matching the optional `ImpressionEvent` fields.

- [ ] Write a failing storage-contract test for all new Bronze fields and for `CN`/`CNY` validation.
- [ ] Add the nullable Spark schema fields.
- [ ] Add `CN` and `CNY` to Silver's allowed reference values.
- [ ] Run the focused tests, then `python -m compileall -q src`.

### Task 5: Protect and document external data

**Files:**
- Create/modify: `.gitignore`
- Create: `docs/data/IPINYOU.md`

- [ ] Ignore `data/external/`, Python caches, local Airflow runtime files and test caches.
- [ ] Document manual dataset acquisition, expected formatted path and replay command.
- [ ] Verify `git status --short` cannot stage the real dataset path.

### Acceptance checkpoint

After the user has the real dataset file locally:

```bash
make up
python -m src.producers.ipinyou_replay_producer data/external/ipinyou/1458/train.log.txt --limit 1000 --events-per-second 100
python -m src.processing.bronze_ingestion
```

Then inspect Bronze and confirm at least one persisted event has `source_dataset=ipinyou`, a non-null `source_bid_id`, `paying_price`, and `clicked` value.
