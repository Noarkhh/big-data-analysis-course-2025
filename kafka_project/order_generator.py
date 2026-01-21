import json
import time
import random
import uuid
from kafka import KafkaProducer

KAFKA_BROKER = "localhost:9092"
TOPIC_ORDERS = "orders"

# Define the Schema for Orders
ORDER_SCHEMA = {
    "type": "struct",
    "fields": [
        {"type": "string", "optional": False, "field": "order_id"},
        {"type": "string", "optional": False, "field": "client_id"},
        {"type": "float", "optional": False, "field": "price"},
        {"type": "string", "optional": False, "field": "status"},
        {
            "type": "int64",
            "optional": False,
            "name": "org.apache.kafka.connect.data.Timestamp",
            "field": "timestamp",
        },
    ],
    "optional": False,
    "name": "orders",
}


def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "client_id": f"client_{random.randint(1, 100)}",
        "price": round(random.uniform(10.0, 500.0), 2),
        "status": "received",
        "timestamp": int(time.time() * 1000),  # Connect likes milliseconds
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Starting Order Generator...")

    try:
        while True:
            data = generate_order()
            # WRAP WITH SCHEMA
            payload = {"schema": ORDER_SCHEMA, "payload": data}

            producer.send(TOPIC_ORDERS, payload)
            print(f"Sent order: {data['order_id']}")
            time.sleep(0.05)
    except KeyboardInterrupt:
        pass
    finally:
        producer.close()


if __name__ == "__main__":
    main()

