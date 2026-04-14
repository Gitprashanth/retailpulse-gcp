from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

PROJECT_ROOT = "/opt/retailpulse"
VENV_PYTHON  = "python"
DBT_BIN      = "dbt"

# Base env passed to every task — includes GCP auth
BASE_ENV = {
    "PATH": "/home/airflow/.local/bin:/usr/local/bin:/usr/bin:/bin",
    "GOOGLE_APPLICATION_CREDENTIALS": "/opt/gcloud/adc.json",
    "GOOGLE_CLOUD_PROJECT": "retailpulse-gcp",
    "GCP_PROJECT_ID": "retailpulse-gcp",
    "GCS_BUCKET_NAME": "retailpulse-raw-prashanth07d91a0428",
    "BQ_DATASET_BRONZE": "retailpulse_bronze",
    "BQ_DATASET_SILVER": "retailpulse_silver",
    "BQ_DATASET_GOLD": "retailpulse_gold",
}

default_args = {
    "owner": "prashanth",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="retailpulse_daily",
    description="RetailPulse — extract → bronze → dbt run → dbt test",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    default_args=default_args,
    tags=["retailpulse", "batch"],
) as dag:

    extract_orders = BashOperator(
        task_id="extract_orders",
        bash_command=f"python {PROJECT_ROOT}/ingestion/extract_orders.py",
        env={**BASE_ENV, "RUN_DATE": "{{ ds }}"},
    )

    extract_customers = BashOperator(
        task_id="extract_customers",
        bash_command=f"python {PROJECT_ROOT}/ingestion/extract_customers.py",
        env={**BASE_ENV, "RUN_DATE": "{{ ds }}"},
    )

    load_bronze = BashOperator(
        task_id="load_to_bronze",
        bash_command=f"python {PROJECT_ROOT}/ingestion/load_to_bq.py",
        env={**BASE_ENV, "RUN_DATE": "{{ ds }}"},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="echo dbt-run-placeholder",
        env=BASE_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="echo dbt-test-placeholder",
        env=BASE_ENV,
    )

    [extract_orders, extract_customers] >> load_bronze >> dbt_run >> dbt_test
