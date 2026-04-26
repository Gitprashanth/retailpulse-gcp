import json
import random
import time
from datetime import datetime

from faker import Faker
from google import pubsub_v1
from dotenv import load_dotenv
import os

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC")
DELAY_SEC = 2

fake = Faker()

def generate_order():
    return{
        "order_id"              :str(fake.uuid4()),
        "customer_id"           :str(fake.uuid4()),
        "product_id"            :str(random.randint(1,100)),
        "amount"                :round(random.uniform(5.0,500.0),2),
        "status"                :random.choice(["pending", "confirmed", "cancelled"]),
        "event_time"            :datetime.utcnow().isoformat()
        }

def publish_message(publisher, topic_path, order):
    message_bytes = json.dumps(order).encode("utf-8")
    future = publisher.publish(
        request={
            "topic": topic_path,
            "messages": [{"data": message_bytes}]
        }
    )
    # future.result()
    print(f"Published: order_id={order['order_id']}  amount={order['amount']}")

def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID,TOPIC_ID)

    print(f"publishing to: {topic_path}")
    print("Press ctrl+C to stop\n")

    try:
        while True:
            order = generate_order()
            publish_message(publisher, topic_path, order)
            time.sleep(DELAY_SEC)
    except KeyboardInterrupt:
        print("\n stopped.")

if __name__=="__main__":
    main()