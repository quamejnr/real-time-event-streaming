"""
Kafka producer that generates sample order events.
Publishes random orders to the 'order_created' Kafka topic every second.
"""
import json
import time
import random
import uuid
from confluent_kafka import Producer

def main():
    p = Producer({"bootstrap.servers": "kafka:9092"})
    while True:
        try:
            data = {
                "order_id": str(uuid.uuid4()), 
                "amount": round(random.uniform(10.0, 100.0), 2)
            }

            p.produce("order_created", value=json.dumps(data).encode("utf-8"))
            print(f"{data['order_id']} sent")
            p.flush()
            time.sleep(1)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)

if __name__ == "__main__":
    main()
