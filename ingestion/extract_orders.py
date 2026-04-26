import requests
import json
import os
from datetime import datetime
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID=os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
RUN_DATE = os.getenv("RUN_DATE", datetime.today().strftime("%Y-%m-%d"))
def fetch_orders():
    """Fetch orders from Fake Store API"""
    print("Fetching orders from DummyJSON API...")
    url = "https://dummyjson.com/carts"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    # orders = response.json()
    orders = response.json()["carts"]  # DummyJSON wraps results in a key
    print(f"Fetched {len(orders)} orders")
    return orders

def fetch_products():
    """Fetching products from Fakestore API"""
    # print("Fetching products from Fakestore API..")
    # url = "https://fakestoreapi.com/products"
    print("Fetching products from DummyJSON API...")
    url = "https://dummyjson.com/products?limit=100"
    response = requests.get(url,timeout=30)
    response.raise_for_status()
    # products = response.json()
    products = response.json()["products"]  # DummyJSON wraps in a key
    print(f"Fetched {len(products)} Products")
    return products

def upload_to_gcs(data,folder,filename):
    """Upload JSON data to GCS Bronze Bucket"""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    #Date- Partitioned path : Orders/2024-01-15/orders.json
    blob_path = f"{folder}/{RUN_DATE}/{filename}"
    blob=bucket.blob(blob_path)

    blob.upload_from_string(
        data=json.dumps(data, indent=2),
        content_type="application/json"
    )
    print(f"Uploaded to gs://{BUCKET_NAME}/{blob_path}")
    return blob_path

def run():
    print(f"--- Extract orders and Products | Run date: {RUN_DATE} ---")

    #Extract
    orders = fetch_orders()
    products = fetch_products()

    #Load to GCS Bronze
    upload_to_gcs(orders,"orders","orders.json")
    upload_to_gcs(products,"products","products.json")

    print("--- Extraction Complete ---")

if __name__ == "__main__":
    run()