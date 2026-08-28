# AdStream

**An end-to-end data engineering platform for replaying real-world RTB advertising events through a production-style medallion pipeline, serving analytics through FastAPI, and exposing both business metrics and pipeline health in an operational dashboard.**

> **Portfolio scope:** AdStream demonstrates data ingestion, distributed processing, lakehouse storage, data quality, orchestration, analytical serving, API design, observability, testing, and CI. It is not a live demand-side platform (DSP) and does not place bids on production ad exchanges.

## Overview

AdStream turns historical real-time bidding (RTB) advertising logs into a reproducible event-driven analytics pipeline.

The project uses the public **iPinYou Global RTB Bidding Algorithm Competition Dataset** as its primary demonstration source. Events can be replayed through Kafka, persisted to a Bronze layer, transformed and validated with PySpark, aggregated into Gold analytical products, refreshed into PostgreSQL for low-latency querying, and exposed through a FastAPI API and browser dashboard.

The architecture separates analytical storage from serving storage:

- **Delta Lake** for local development.
- **Apache Iceberg on Supabase Analytics** for the cloud lakehouse path.
- **Supabase PostgreSQL** for serving and observability.
- **FastAPI** for query and health endpoints.
- **Apache Airflow** for Silver → Gold → Quality → Serving orchestration.
- **GitHub Actions** for automated validation.

## Architecture

```text
iPinYou RTB Dataset
        │
        ▼
 Kafka Event Replay
 ad.impressions.raw
        │
        ▼
┌──────────────────┐
│ BRONZE           │
│ Delta / Iceberg  │
└────────┬─────────┘
         ▼
┌──────────────────┐
│ SILVER           │──────► Quarantine
│ Canonical schema │
│ Validation       │
└────────┬─────────┘
         ▼
┌──────────────────────────┐
│ GOLD                     │
│ advertiser_daily         │
│ creative_daily           │
│ traffic_quality_daily    │
└────────┬─────────────────┘
         ▼
   Data Quality Gate
         │
         ▼
┌──────────────────┐
│ SERVING          │
│ Supabase Postgres│
└────────┬─────────┘
         ▼
      FastAPI
      ↙     ↘
 Analytics  Pipeline Health
```

Airflow runs the analytical refresh hourly:

```text
Silver Transformation → Gold Aggregation → Data Quality Check → Serving Refresh
```

Each stage can persist its run ID, status, duration, result metadata and error type to PostgreSQL.

## Data Source — iPinYou RTB

AdStream's primary demonstration source is the **iPinYou Global RTB Bidding Algorithm Competition Dataset**, built from real advertising campaigns run through the iPinYou DSP platform.

The dataset originates from iPinYou's 2013 Global RTB Bidding Algorithm Competition and contains processed bidding, impression, click and conversion logs. It is a benchmark dataset for computational-advertising research including CTR estimation and bid optimisation.

**Dataset paper:** Hairen Liao, Lingxiao Peng, Zhenchuan Liu and Xuehua Shen, *iPinYou Global RTB Bidding Algorithm Competition Dataset*  
https://contest.ipinyou.com/ipinyou-dataset.pdf

The repository deliberately does **not** commit or automatically download the full dataset. Acquisition is manual because the source is large and should be obtained from the publisher/research mirrors.

See [`docs/data/IPINYOU.md`](docs/data/IPINYOU.md) for repository-specific ingestion notes.

### Fields used by AdStream

The ingestion layer preserves the RTB attributes needed downstream, including:

- bid ID / source lineage
- timestamp
- anonymised iPinYou user ID
- user-agent
- advertiser ID
- creative ID
- ad exchange
- ad slot ID
- bidding price
- paying / clearing price
- click outcome when available

Canonical pricing is represented as `CPM` in `CNY`. Source-native bid and paying prices are retained rather than relabelled as USD revenue.

### Supported source formats

AdStream supports both:

1. formatted `train.log.txt` / `test.log.txt` files; and
2. original 24-column iPinYou impression logs, including `.bz2`.

Example raw input:

```text
data/external/ipinyou/raw/imp.20131023.txt.bz2
```

The raw reader streams records and can filter by advertiser before applying a limit, making bounded development replays practical.

### Source-integrity rules

AdStream avoids inventing labels not supported by the dataset:

- raw impression logs do not contain click outcomes, so `clicked` remains unknown there;
- no fraud label is inferred from iPinYou ingestion;
- source bid IDs are retained for lineage;
- Bronze preserves source records rather than assuming bid ID is always unique at impression-event granularity.

## Event Replay

A bounded slice of real RTB events can be replayed into Kafka:

```bash
python -m src.producers.ipinyou_replay_producer \
  data/external/ipinyou/raw/imp.20131023.txt.bz2 \
  --advertiser-id 2997 \
  --limit 1000 \
  --events-per-second 100
```

Events are published to `ad.impressions.raw`.

This lets a historical benchmark behave like an event stream without claiming a live ad-exchange integration.

## Medallion Pipeline

### Bronze — source-preserving ingestion

Bronze is the persisted raw-event layer.

| Backend | Purpose | Location |
|---|---|---|
| Delta Lake | default local development | `data/bronze/impressions` |
| Apache Iceberg | cloud lakehouse | `supabase.bronze.impressions` |

Select Iceberg with:

```bash
export ADSTREAM_STORAGE_BACKEND=iceberg
```

Kafka offsets are committed only after successful Bronze persistence.

### Silver — canonical and validated events

Silver transforms Bronze into the canonical analytical schema and separates unusable records into quarantine.

The pipeline reconciles:

```text
bronze rows = silver rows + quarantine rows
```

Silver persistence is idempotent. AdStream distinguishes:

- **inserted rows** — physical inserts performed in this execution;
- **written rows** — expected current-batch rows confirmed in persisted storage.

A valid replay can therefore report:

```text
silver:           1000
inserted_silver:     0
written_silver:   1000
```

This verifies persisted state without treating a zero-insert idempotent rerun as a failed write.

### Gold — analytical products

| Data product | Purpose |
|---|---|
| `advertiser_daily` | advertiser impressions, spend, bid/clearing CPM and auction savings |
| `creative_daily` | creative impressions, spend, clearing CPM and clicks |
| `traffic_quality_daily` | total, valid, warning and quarantined event monitoring |

With Iceberg selected, Gold is stored under the `supabase.gold` namespace.

### Data Quality Gate

Before Serving refresh, Airflow checks:

- Bronze = Silver + Quarantine
- expected Silver rows exist after persistence
- expected Quarantine rows exist after persistence
- Gold inputs agree with Silver results
- required Gold outputs are non-empty

A failed quality gate blocks the downstream serving refresh.

## Serving Layer

Gold aggregates are refreshed into **Supabase PostgreSQL** for query-oriented serving. This keeps the browser/API path independent of Spark execution and gives the application a conventional low-latency relational store.

Runtime connection:

```text
SUPABASE_POSTGRES_URL
```

Secrets must not be committed.

## Query API

FastAPI is implemented in `src/api/app.py`.

### Health and operations

| Endpoint | Purpose |
|---|---|
| `GET /health` | API liveness |
| `GET /ready` | serving-database readiness |
| `GET /api/v1/pipeline-health` | system and pipeline health |

### Analytics

| Endpoint | Filters |
|---|---|
| `GET /api/v1/advertisers/daily` | `event_date`, `advertiser_id` |
| `GET /api/v1/creatives/daily` | `event_date`, `advertiser_id`, `creative_id` |
| `GET /api/v1/traffic-quality/daily` | `event_date` |

Responses include `X-Request-ID` and `X-Process-Time-Ms`. A caller-supplied request ID is preserved for log correlation.

## Pipeline Observability

Airflow stages persist operational telemetry to PostgreSQL:

- Airflow `run_id`
- stage
- status
- duration
- result metadata
- exception type
- timestamp

Observed stages:

```text
silver
gold
quality
serving
```

### Retry-aware health

Historical failures are retained, but current run health uses the **latest observation per stage**.

```text
Quality attempt 1 → failed
Quality attempt 2 → failed
Quality attempt 3 → success
                    └──── current stage state
```

This keeps failure history while allowing a recovered Airflow run to report healthy.

## Dashboard

The browser dashboard has two primary views.

### Analytics

Business-facing Gold metrics include:

- Total Spend
- Impressions
- Auction Savings
- Warning / Traffic Quality
- Creative Performance

### Pipeline Health

Operational visibility includes:

- API health
- Serving DB readiness
- latest run ID and status
- run duration
- Silver / Gold / Quality / Serving status
- stage durations and result metadata
- recent pipeline runs

The dashboard consumes FastAPI rather than querying Iceberg or PostgreSQL directly.

> Add final screenshots under `docs/images/` and embed them here once captured.

## Orchestration

DAG: `adstream_medallion_pipeline`  
Schedule: `@hourly`  
Definition: `airflow/dags/adstream_pipeline_dag.py`

Tasks have one retry with a five-minute retry delay. Metrics use Airflow's runtime `run_id`, so retries remain associated with the same logical run.

## Runtime Configuration

Local analytical storage:

```bash
export ADSTREAM_STORAGE_BACKEND=delta
```

Cloud Iceberg:

```bash
export ADSTREAM_STORAGE_BACKEND=iceberg
```

Iceberg requires:

```text
SUPABASE_PROJECT_REF
SUPABASE_CATALOG_TOKEN
SUPABASE_ICEBERG_WAREHOUSE
SUPABASE_S3_ACCESS_KEY
SUPABASE_S3_SECRET_KEY
SUPABASE_S3_REGION
```

Serving/API access additionally requires:

```text
SUPABASE_POSTGRES_URL
```

## Technology Stack

| Area | Technology |
|---|---|
| Language | Python 3.11+ |
| Event streaming | Apache Kafka |
| Processing | PySpark 3.5 |
| Local lakehouse | Delta Lake |
| Cloud lakehouse | Apache Iceberg / Supabase Analytics |
| Orchestration | Apache Airflow |
| Serving | PostgreSQL / Supabase |
| API | FastAPI + Uvicorn |
| Models | Pydantic |
| Database driver | psycopg |
| Logging | structlog / Python logging |
| Testing | pytest |
| CI | GitHub Actions |
| Dashboard | HTML / CSS / JavaScript |

## Repository Structure

```text
ad_stream/
├── .github/workflows/ci.yml
├── airflow/dags/adstream_pipeline_dag.py
├── dashboard/
├── docs/
│   └── data/IPINYOU.md
├── src/
│   ├── api/
│   ├── config/
│   ├── consumers/
│   ├── models/
│   ├── observability/
│   ├── processing/
│   ├── producers/
│   ├── serving/
│   ├── sources/
│   ├── storage/
│   └── utils/
├── tests/
├── pyproject.toml
└── requirements.txt
```

## Running Locally

### Prerequisites

- Python 3.11+
- Java 17 for Spark
- pip / virtual environment
- Kafka for event replay
- Airflow for orchestration
- Supabase credentials only for cloud/serving execution

### Install

```bash
git clone https://github.com/gishusam/ad_stream.git
cd ad_stream

python3 -m venv venv
source venv/bin/activate

python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
pip install -e .
```

### Test

```bash
pytest -q
```

CI uses `ADSTREAM_STORAGE_BACKEND=delta`, so repository validation does not require cloud secrets.

### Start the Query API

With `SUPABASE_POSTGRES_URL` configured:

```bash
uvicorn src.api.app:create_app \
  --factory \
  --host 127.0.0.1 \
  --port 8010
```

Smoke test:

```bash
curl http://127.0.0.1:8010/health
curl http://127.0.0.1:8010/ready
curl http://127.0.0.1:8010/api/v1/pipeline-health
```

### Start the dashboard

In another terminal:

```bash
python -m http.server 8088 -d dashboard
```

Open:

```text
http://localhost:8088/adstream_dashboard.html
```

## Testing and CI

The repository tests:

- event models
- Kafka producers and consumers
- iPinYou formatted/raw readers
- Bronze storage backends
- Silver transformations
- Silver idempotent reconciliation
- Gold aggregation
- data-quality gates
- Airflow orchestration contracts
- serving refresh
- PostgreSQL queries
- FastAPI endpoints
- readiness and request observability
- stage observability
- retry-aware pipeline-health aggregation
- dashboard/API integration
- CI configuration

GitHub Actions runs on pushes to `main` and `feature/**`, and on pull requests targeting `main`. CI uses Python 3.11, Java 17 and Delta storage.

## Key Engineering Decisions

1. **Historical data, streaming architecture** — iPinYou is replayed through Kafka; AdStream does not claim a live ad-exchange feed.
2. **Source truth over invented labels** — unknown click/fraud values are not fabricated.
3. **Portable analytical storage** — the same processing design supports local Delta and cloud Iceberg.
4. **Persisted-state idempotency** — physical inserts are separated from successful persisted-batch reconciliation.
5. **Quality before serving** — serving refresh is blocked when cross-layer validation fails.
6. **Lakehouse vs serving separation** — Iceberg handles analytical persistence; PostgreSQL handles application queries.
7. **Observability as data** — pipeline telemetry is persisted, not limited to console logs.
8. **Retry-aware health** — failed attempts remain historical evidence while latest-stage state represents current health.

## Project Scope

AdStream demonstrates:

- real-world dataset integration
- event-driven ingestion and replay
- medallion architecture
- PySpark transformations
- Delta Lake and Apache Iceberg
- idempotent processing
- quarantine handling
- reconciliation and data-quality gates
- Airflow orchestration
- analytical data products
- PostgreSQL serving
- REST APIs
- liveness/readiness checks
- request correlation and latency instrumentation
- persisted pipeline observability
- retry-aware health reporting
- operational dashboards
- automated testing and CI

It intentionally does **not** claim to implement:

- a production DSP bidding engine
- live ad-exchange connectivity
- production-scale auction throughput
- ML bid optimisation
- fraud classification from iPinYou labels

## Data-flow Note

Because iPinYou is historical, dates shown in the Analytics dashboard correspond to the underlying advertising events, not the date the AdStream pipeline was executed.

## Further Reading

- iPinYou dataset paper: https://contest.ipinyou.com/ipinyou-dataset.pdf
- Repository data notes: [`docs/data/IPINYOU.md`](docs/data/IPINYOU.md)
- iPinYou formatter: https://github.com/wnzhang/make-ipinyou-data
- RTB benchmarking paper: https://arxiv.org/abs/1407.7073

## Status

The current implementation covers the demonstration path from RTB source ingestion through Bronze/Silver/Gold processing, data-quality validation, serving, API access, analytics, and pipeline observability.

Presentation-oriented next steps are intentionally lightweight:

- add final Analytics and Pipeline Health screenshots
- optionally publish a read-only hosted demo
- expand deployment documentation only if the project is deployed beyond local development
