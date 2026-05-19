# RetailPulse — GCP Data Engineering Portfolio Project

> End-to-end data pipeline on Google Cloud Platform, built with a medallion architecture (Bronze → Silver → Gold) in the e-commerce domain.

![Python](https://img.shields.io/badge/Python-3.11-blue)
![BigQuery](https://img.shields.io/badge/BigQuery-GCP-orange)
![dbt](https://img.shields.io/badge/dbt-1.11-red)
![Airflow](https://img.shields.io/badge/Airflow-Docker-green)
![Beam](https://img.shields.io/badge/Apache%20Beam-Streaming-purple)

---

## Overview

RetailPulse simulates a production-grade e-commerce data platform on GCP. It ingests order, product, and customer data through both batch and streaming pipelines, transforms it through layered dbt models, validates it with 49 automated tests, and serves it to a business dashboard.

**Dashboard:** [Looker Studio — RetailPulse Analytics](#) ← _coming soon_

---

## Architecture
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│         DummyJSON API          │        Python Faker            │
│         (Batch — daily)        │       (Streaming — live)       │
└──────────────┬─────────────────┴───────────────┬────────────────┘
│                                 │
▼                                 ▼
┌──────────────────────────┐      ┌──────────────────────────────┐
│     Cloud Storage (GCS)  │      │         Cloud Pub/Sub        │
│   Bronze data lake       │      │        orders-stream topic   │
│   date-partitioned JSON  │      └──────────────┬───────────────┘
└──────────────┬───────────┘                     │
│                                 ▼
▼                    ┌────────────────────────────┐
┌──────────────────────────┐        │     Apache Beam pipeline   │
│  BigQuery Bronze Layer   │        │     (DirectRunner / local) │
│  raw_orders              │        └──────────────┬─────────────┘
│  raw_products            │                       │
│  raw_customers           │◄──────────────────────┘
│  streaming_orders        │     writes to BigQuery directly
└──────────────┬───────────┘
│  Airflow DAG orchestrates
▼
┌──────────────────────────┐
│  BigQuery Silver Layer   │
│  stg_orders              │
│  stg_products            │  ◄── dbt staging models
│  stg_customers           │      (cleaned, typed, deduped)
│  int_order_items         │
└──────────────┬───────────┘
│
▼
┌──────────────────────────┐
│  BigQuery Gold Layer     │
│  dim_customers           │
│  dim_products            │  ◄── dbt mart models
│  fct_orders              │      (aggregated, business-ready)
│  mart_revenue_by_category│
└──────────────┬───────────┘
│
▼
┌──────────────────────────┐
│   Looker Studio Dashboard│
│   Revenue by Category    │
│   Top Products           │
│   Order Status           │
│   Customer Geography     │
└──────────────────────────┘
---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Ingestion scripts and streaming publisher |
| Google Cloud Storage | Bronze data lake — raw JSON files |
| BigQuery | Data warehouse — Bronze, Silver, Gold layers |
| dbt-bigquery | Silver and Gold transformations (8 models, 49 tests) |
| Apache Airflow (Docker) | Batch pipeline orchestration |
| Apache Beam (DirectRunner) | Streaming pipeline — Pub/Sub to BigQuery |
| Cloud Pub/Sub | Message queue for real-time order events |
| Looker Studio | Business intelligence dashboard |
| GitHub | Version control |

---

## Project Structure
retailpulse-gcp/
├── ingestion/
│   ├── extract_orders.py        # Pulls orders + products from DummyJSON API → GCS
│   └── extract_customers.py     # Pulls customers from RandomUser API → GCS
│   └── load_to_bq.py            # Loads GCS JSON → BigQuery Bronze tables
├── dbt/
│   ├── models/
│   │   ├── staging/             # Silver layer — stg_orders, stg_products, stg_customers
│   │   ├── intermediate/        # int_order_items — unnests products JSON array
│   │   └── marts/               # Gold layer — dim_, fct_, mart_*
│   ├── tests/                   # Custom singular SQL tests
│   │   ├── assert_no_duplicate_orders_in_gold.sql
│   │   ├── assert_revenue_matches_order_items.sql
│   │   └── assert_streaming_orders_have_valid_amounts.sql
│   ├── macros/
│   │   └── generate_schema_name.sql  # Prevents BigQuery schema name doubling
│   └── dbt_project.yml
├── dags/
│   └── retailpulse_dag.py       # Airflow DAG — extract → bronze → dbt run → dbt test
├── streaming/
│   ├── publish_events.py        # Generates fake orders with Faker → Pub/Sub
│   └── beam_pipeline.py         # Reads Pub/Sub → parses JSON → writes to BigQuery
├── docker-compose.yaml          # Airflow multi-container setup (webserver, scheduler, worker)
├── .env.example                 # Environment variable template (no secrets)
└── README.md

---

## Key Design Decisions

**1. Medallion architecture**
Bronze stores raw data exactly as received — never modified. Silver cleans and types. Gold aggregates for business use. This separation means any layer can be rebuilt independently without touching the others.

**2. No transformation at Bronze**
The `products` column in `raw_orders` is stored as a raw JSON string and only unnested at the Silver layer (`int_order_items`). This preserves the original payload and defers schema decisions to the transformation layer.

**3. Idempotency via DELETE + INSERT**
All Bronze loads use a DELETE + INSERT pattern scoped to `run_date`. Running the pipeline twice on the same day produces identical results — no duplicates, no gaps.

**4. dbt test philosophy**
49 tests across 3 tiers: schema tests (not_null, unique, accepted_values), dbt_utils expression tests (business logic), and custom singular SQL tests (cross-layer consistency). Two tests are deliberately set to `severity: warn` rather than error — 30 orders from the source API have null dates, which is a real data quality issue worth surfacing, not hiding.

**5. Streaming via Pub/Sub + Beam**
The streaming pipeline uses the same BigQuery Bronze dataset as the batch pipeline. Apache Beam's DirectRunner is used locally; switching to Dataflow requires only a single runner flag change — the pipeline code is identical.

**6. Docker for Airflow on Apple Silicon**
Local pip-installed Airflow causes SIGSEGV crashes on Apple Silicon (M-series chips) due to gunicorn worker issues. Docker Desktop resolves this completely and better mirrors a production Cloud Composer environment.

---

## Setup Instructions

### Prerequisites
- GCP project with BigQuery, GCS, and Pub/Sub enabled
- Service account with BigQuery Admin, Storage Admin, Pub/Sub Admin roles
- Python 3.11
- Docker Desktop
- dbt-bigquery (`pip install dbt-bigquery`)

### 1. Clone the repo
```bash
git clone https://github.com/Gitprashanth/retailpulse-gcp.git
cd retailpulse-gcp
```

### 2. Configure environment variables
```bash
cp .env.example .env
# Edit .env with your GCP project ID, bucket name, and dataset names
```

### 3. Run batch ingestion
```bash
cd ingestion
python extract_orders.py
python extract_customers.py
python load_to_bq.py
```

### 4. Run dbt transformations
```bash
cd dbt
dbt deps
dbt run
dbt test
```

### 5. Start Airflow
```bash
docker-compose up -d
# Open http://localhost:8080
# Enable the retailpulse_dag DAG
```

### 6. Run the streaming pipeline
```bash
# Terminal 1 — start the Beam pipeline
cd streaming
python beam_pipeline.py

# Terminal 2 — publish events
python publish_events.py
```

---

## Data Quality Results

| Metric | Result |
|---|---|
| Total dbt tests | 49 |
| Passing | 47 |
| Warnings (known source data issues) | 2 |
| Errors | 0 |

The 2 warnings track null `order_date` values present in the source API — surfaced deliberately through all three medallion layers rather than silently dropped.

---

## GCP Resources

| Resource | Name |
|---|---|
| Project | `retailpulse-gcp` |
| Region | `asia-south1` (Mumbai) |
| GCS Bucket | `retailpulse-raw-prashanth07d91a0428` |
| BigQuery Datasets | `retailpulse_bronze`, `retailpulse_silver`, `retailpulse_gold` |
| Pub/Sub Topic | `orders-stream` |
| Pub/Sub Subscription | `orders-stream-sub` |