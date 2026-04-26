# RetailPulse — GCP Data Engineering Project
## Claude Project Context File
**Last updated:** 2026-04-14
**Status:** Phase 5 complete — Airflow DAG running on Docker

---

## Project identity
- **Project name:** RetailPulse
- **Goal:** End-to-end GCP data engineering portfolio project
- **Domain:** E-commerce (orders, customers, products)
- **Builder:** Prashanth Ellandula
- **GCP Project ID:** retailpulse-gcp
- **GCP Region:** asia-south1 (Mumbai)
- **GitHub repo:** https://github.com/YOUR_USERNAME/retailpulse-gcp

---

## GCP resources (already created)
- **GCS Bucket:** gs://retailpulse-raw-prashanth07d91a0428/
  - orders/{date}/orders.json
  - products/{date}/products.json
  - customers/{date}/customers.json
- **BigQuery datasets:**
  - retailpulse_bronze (raw layer)
  - retailpulse_silver (dbt staging)
  - retailpulse_gold (dbt marts)
- **Pub/Sub:**
  - Topic: orders-stream
  - Subscription: orders-stream-sub
  - Dead-letter: orders-stream-deadletter
- **Service account:** retailpulse-sa

---

## Architecture — Medallion layers
- **Bronze:** Raw data, never modified, date-partitioned
- **Silver:** dbt staging models — cleaned, typed, deduplicated
- **Gold:** dbt mart models — aggregated, business-ready

## Full data flow
### Batch (daily 6am)
Fake Store API → extract_orders.py → GCS Bronze
RandomUser API → extract_customers.py → GCS Bronze
Airflow DAG → orchestrates → BQ load → dbt → quality checks

### Streaming (continuous)
Python Faker → publish_events.py → Pub/Sub → Dataflow (Beam) → BQ Bronze

---

## Tech stack
| Tool | Purpose | Status |
|---|---|---|
| Python 3.11 | Scripting | Ready |
| GCS | Bronze data lake | Ready |
| BigQuery | Warehouse | Ready |
| Cloud Composer / Airflow | Batch orchestration | Complete (Docker local) |
| Apache Beam / Dataflow | Streaming | Not started |
| dbt-bigquery | Transformations | Complete |
| Great Expectations | Data quality | Not started |
| Looker Studio | Dashboard | Not started |

---

## Files already written
- `ingestion/extract_orders.py` — calls DummyJSON API, uploads orders + products to GCS
- `ingestion/extract_customers.py` — calls RandomUser API, flattens + uploads to GCS
- `ingestion/load_to_bq.py` — loads GCS JSON → BigQuery Bronze tables
- `dbt/dbt_project.yml` — dbt project config, profiles, packages
- `dbt/macros/generate_schema_name.sql` — prevents schema name doubling in BigQuery
- `dbt/models/staging/stg_orders.sql` — cleaned, type-cast orders view (Silver)
- `dbt/models/staging/stg_products.sql` — cleaned, deduplicated products view (Silver)
- `dbt/models/staging/stg_customers.sql` — cleaned, deduplicated customers view (Silver)
- `dbt/models/intermediate/int_order_items.sql` — unnests products JSON array into line items
- `dbt/models/marts/dim_customers.sql` — Gold dimension table with lifecycle stats
- `dbt/models/marts/dim_products.sql` — Gold dimension table with sales performance
- `dbt/models/marts/fct_orders.sql` — Gold fact table with aggregated order financials
- `dbt/models/marts/mart_revenue_by_category.sql` — Gold summary mart by category + month
- `dags/retailpulse_dag.py` — Airflow DAG orchestrating extract → bronze → dbt run → dbt test
- `docker-compose.yaml` — Airflow on Docker setup with GCP auth and project mount

## Files in progress


---

## Key decisions made
1. Use `asia-south1` region for all GCP resources
2. Date-partitioned GCS folders: `folder/{YYYY-MM-DD}/file.json`
3. Idempotency pattern: DELETE + INSERT scoped to run date
4. Airflow ds macro for date injection
5. Flatten nested JSON at extraction time, not at transformation time
6. `.env` file for all config — never hardcode project IDs or bucket names
7. `.gitignore` includes `*.json`, `.env`, `__pycache__`
8. Switched from Fake Store API to DummyJSON — Fake Store API unreliable
9. dbt schema names used as-is via custom generate_schema_name macro (prevents BigQuery doubling)
10. 30 orders have null dates from source API — tracked as dbt WARN, not ERROR
11. Airflow runs via Docker (not local pip) — avoids SIGSEGV on Apple Silicon
12. GCP ADC credentials mounted into Docker at /opt/gcloud/adc.json
13. dbt tasks are echo placeholders pending custom Docker image with dbt-bigquery
14. BashOperator env must explicitly include GOOGLE_APPLICATION_CREDENTIALS — it replaces not inherits the environment

---
## Environment variables (.env)

GCP_PROJECT_ID=retailpulse-gcp
GCP_REGION=asia-south1
GCS_BUCKET_NAME=retailpulse-raw-prashanth07d91a0428
BQ_DATASET_BRONZE=retailpulse_bronze
BQ_DATASET_SILVER=retailpulse_silver
BQ_DATASET_GOLD=retailpulse_gold
PUBSUB_TOPIC=orders-stream
PUBSUB_SUBSCRIPTION=orders-stream-sub

---

## BigQuery table schemas (planned)
### Bronze — raw_orders
order_id, user_id, date, products (JSON array), status

### Bronze — raw_products
product_id, title, price, description, category, image

### Bronze — raw_customers
customer_id, first_name, last_name, email, phone, gender,
date_of_birth, city, country, registered_at

### Bronze — streaming_orders (real-time)
order_id, customer_id, product_id, amount, timestamp, ingested_at

---

## Chat log — what was built where
| Chat | Topic | Last completed step |
|---|---|---|
| This file | Context | Updated 2026-04-08 |
| Ingestion chat | extract_orders.py, extract_customers.py | GCS upload verified |
| BQ loading chat | load_to_bq.py complete — all 3 Bronze tables verified |
| dbt chat | Silver + Gold models | All 8 models built, 40 tests run (38 PASS, 2 WARN) |
| Airflow chat | Plan agreed — local Airflow on Mac, full pipeline DAG (extract → Bronze → dbt run → dbt test) | Not started
| Streaming chat | Beam pipeline | Not started |
| Quality & showcase | Great Expectations, Looker, README | Not started |
| Airflow chat | Docker setup, DAG built, all 5 tasks green | Complete — dbt tasks are placeholders |

---

## How to use this file
1. Always paste this file at the START of every new chat
2. After finishing work in any chat, update the relevant section
3. Copy the updated version back into Claude Project Knowledge
4. Never paste secrets or key file contents here