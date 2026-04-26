import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ID      = os.getenv("GCP_PROJECT_ID")
SUBSCRIPTION    = os.getenv("PUBSUB_SUBSCRIPTION")
BQ_DATASET      = os.getenv("BQ_DATASET_BRONZE")
BQ_TABLE        = "streaming_orders"

STREAMING_ORDERS_SCHEMA = {
    "fields": [
        {"name": "order_id",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING",    "mode": "REQUIRED"},
        {"name": "product_id",  "type": "STRING",    "mode": "REQUIRED"},
        {"name": "amount",      "type": "FLOAT",     "mode": "REQUIRED"},
        {"name": "status",      "type": "STRING",    "mode": "NULLABLE"},
        {"name": "event_time",  "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

class ParseAndEnrich(beam.DoFn):

    def process(self, element):
        try: 
            message_str = element.decode("utf-8")
            order = json.loads(message_str)
            order["ingested_at"] = datetime.utcnow().isoformat()
            yield order
        except Exception as e:
            print(f"Failed to parse message{e}")

def run():
    options = PipelineOptions(
        project=PROJECT_ID,
        temp_location=f"gs://{os.getenv('GCS_BUCKET_NAME')}/temp",
        region=os.getenv("GCP_REGION"),
    )
    options.view_as(StandardOptions).streaming = True

    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION}"
    bq_table_spec     = f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}"

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from PubSub"  >> beam.io.ReadFromPubSub(subscription=subscription_path)
            | "Parse and enrich"  >> beam.ParDo(ParseAndEnrich())
            | "Write to BigQuery" >> WriteToBigQuery(
                bq_table_spec,
                schema=STREAMING_ORDERS_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    run()