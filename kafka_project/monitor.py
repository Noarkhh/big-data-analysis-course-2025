import json
from kafka import KafkaConsumer

KAFKA_BROKER = "localhost:9092"
TOPIC_ORDERS = "orders"
TOPIC_TRUCK_STATUS = "truck_status"


def main():
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="monitor_group",
    )

    # Subscribe to both topics
    consumer.subscribe([TOPIC_ORDERS, TOPIC_TRUCK_STATUS])

    print("Monitoring system events...")

    for message in consumer:
        topic = message.topic
        data = message.value

        if topic == TOPIC_ORDERS:
            print(f"[ORDER] ID: {data['order_id']} | Status: {data['status']}")
        elif topic == TOPIC_TRUCK_STATUS:
            print(f"[TRUCK] ID: {data['truck_id']} | Pos: {data['position']}")


if __name__ == "__main__":
    main()

