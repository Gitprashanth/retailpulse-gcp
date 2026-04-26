import requests
import json
import os
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
RUN_DATE = os.getenv("RUN_DATE", datetime.today().strftime("%Y-%m-%d"))

def fetch_customers(count = 200):
    """Fetch synthetic customer data from RandomUser API"""
    print(f"Fetching {count} customers from RandomUser API")
    url = f"https://randomuser.me/api/?results={count}&nat=us,gb,in"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    raw = response.json()

    # Flatten to only the required fields
    customers = []
    for user in raw["results"]:
        customers.append({
            "customer_id":  user["login"]["uuid"],
            "first_name":   user["name"]["first"],
            "last_name":    user["name"]["last"],
            "email":        user["email"],
            "phone":        user["phone"],
            "gender":       user["gender"],
            "date_of_birth": user["dob"]["date"],
            "city":         user["location"]["city"],
            "country":      user["location"]["country"],
            "registered_at": user["registered"]["date"]
        })

    print(f"Fetched and flattened {len(customers)} customers")
    return customers

def upload_to_gcs(data, folder, filename):
    """Upload JSON data to CGS Bronze bucket"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    blob_path = f"{folder}/{RUN_DATE}/{filename}"
    blob = bucket.blob(blob_path)

    blob.upload_from_string(
        data=json.dumps(data,indent=2),
        content_type= "application/json"
    )
    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_path}")
    return blob_path

def run():
    print(f"---Extract Customers | Run date ; {RUN_DATE} ---")
    customers = fetch_customers(count=200)
    upload_to_gcs(customers, "customers", "customers.json")
    print("--- Customer extraction complete ---")

if __name__ == "__main__":
    run()