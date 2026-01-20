import json
import time
import random
import uuid
from kafka import KafkaProducer

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_ORDERS = "orders"


def generate_order():
    return {
        "order_id": str(uuid.uuid4()),
        "client_id": f"client_{random.randint(1, 100)}",
        "price": round(random.uniform(10.0, 500.0), 2),
        "status": "received",  # Initial status
        "timestamp": time.time(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"Starting Order Generator connected to {KAFKA_BROKER}...")

    try:
        while True:
            order = generate_order()
            producer.send(TOPIC_ORDERS, order)
            print(f"Sent order: {order['order_id']}")
            time.sleep(random.uniform(0.02, 0.05))  # Send a new order every few seconds
    except KeyboardInterrupt:
        print("Stopping generator...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

