import os
import json
from datetime import datetime
from google.cloud import bigquery, storage
from dotenv import load_dotenv
import pandas as pd
from io import StringIO

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_DATASET_BRONZE")
RUN_DATE = os.getenv("RUN_DATE", datetime.today().strftime("%Y-%m-%d"))

client = bigquery.Client(project=PROJECT_ID)

def get_table_schema(table_name):
    schemas = {
        "raw_orders": [
            bigquery.SchemaField("order_id",   "INTEGER"),
            bigquery.SchemaField("user_id",    "INTEGER"),
            bigquery.SchemaField("date",       "TIMESTAMP"),
            bigquery.SchemaField("products",   "JSON"),
            bigquery.SchemaField("status",     "STRING"),  # derived below
        ],
        "raw_products": [
            bigquery.SchemaField("product_id",   "INTEGER"),
            bigquery.SchemaField("title",        "STRING"),
            bigquery.SchemaField("price",        "FLOAT"),
            bigquery.SchemaField("description",  "STRING"),
            bigquery.SchemaField("category",     "STRING"),
            bigquery.SchemaField("image",        "STRING"),
        ],
        "raw_customers": [
            bigquery.SchemaField("customer_id",    "STRING"),
            bigquery.SchemaField("first_name",     "STRING"),
            bigquery.SchemaField("last_name",      "STRING"),
            bigquery.SchemaField("email",          "STRING"),
            bigquery.SchemaField("phone",          "STRING"),
            bigquery.SchemaField("gender",         "STRING"),
            bigquery.SchemaField("date_of_birth",  "TIMESTAMP"),
            bigquery.SchemaField("city",           "STRING"),
            bigquery.SchemaField("country",        "STRING"),
            bigquery.SchemaField("registered_at",  "TIMESTAMP"),
        ],
    }
    return schemas[table_name]   

# def load_gcs_to_bq(gcs_folder, filename, table_name):
#     """Load a JSON file from GCS into a BigQuery Bronze table."""
#     gcs_path = f"{gcs_folder}/{RUN_DATE}/{filename}"
#     table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"

#     print(f"\nLoading gs://{BUCKET_NAME}/{gcs_path}")
#     print(f"  -> {table_ref}")

#     # Source files are JSON arrays — download and parse before loading
#     storage_client = storage.Client(project=PROJECT_ID)
#     blob = storage_client.bucket(BUCKET_NAME).blob(gcs_path)
#     rows = json.loads(blob.download_as_text())

#     job_config = bigquery.LoadJobConfig(
#         schema=get_table_schema(table_name),
#         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
#         ignore_unknown_values=True,
#     )

#     load_job = client.load_table_from_json(rows, table_ref, job_config=job_config)
#     load_job.result()

#     table = client.get_table(table_ref)
#     print(f"  Loaded {table.num_rows} rows into {table_ref}")


def load_gcs_to_bq(gcs_folder, filename, table_name):
    """Load a JSON file from GCS into a BigQuery Bronze table."""
    from google.cloud import storage as gcs

    gcs_client = gcs.Client()
    blob = gcs_client.bucket(BUCKET_NAME).blob(
        f"{gcs_folder}/{RUN_DATE}/{filename}"
    )
    content = blob.download_as_text()
    data = json.loads(content)

    # Normalize orders: rename id fields to match schema
    if table_name == "raw_orders":
        for row in data:
            row["order_id"] = row.pop("id", None)
            row["user_id"]  = row.pop("userId", None)
            row["products"] = json.dumps(row.pop("products", []))
            row["status"]   = "unknown"  # Fake Store API has no status field

    if table_name == "raw_products":
        for row in data:
            row["product_id"] = row.pop("id", None)
            row.pop("rating", None)  # drop nested rating field

    table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{table_name}"
    print(f"\nLoading {len(data)} rows → {table_ref}")

    job_config = bigquery.LoadJobConfig(
        schema=get_table_schema(table_name),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
    )

    ndjson = "\n".join(json.dumps(row) for row in data)

    load_job = client.load_table_from_file(
        StringIO(ndjson),
        table_ref,
        job_config=job_config,
    )
    load_job.result()

    table = client.get_table(table_ref)
    print(f"  ✅ {table.num_rows} rows loaded into {table_ref}")

def run():
    print(f"---- Load GCS -> BigQuery Bronze | Run date: {RUN_DATE} ---")

    load_gcs_to_bq("orders", "orders.json", "raw_orders")
    load_gcs_to_bq("products",  "products.json",  "raw_products")
    load_gcs_to_bq("customers", "customers.json", "raw_customers")

    print("\n--- Load complete ---")


if __name__ == "__main__":
    run()