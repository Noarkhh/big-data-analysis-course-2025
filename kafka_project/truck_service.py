import json
import time
import threading
import random
import math
from kafka import KafkaConsumer, KafkaProducer

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
TOPIC_ORDERS = "orders"
TOPIC_TRUCK_STATUS = "truck_status"

# Map settings
MAP_SIZE = 100
DISPATCH_CENTERS = [
    {"x": 10, "y": 10},  # Center 1 (Bottom Left)
    {"x": 10, "y": 90},  # Center 2 (Top Left)
    {"x": 90, "y": 10},  # Center 3 (Bottom Right)
    {"x": 90, "y": 90},  # Center 4 (Top Right)
    {"x": 50, "y": 50},  # Center 5 (Middle)
]

producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def simulate_truck_delivery(truck_id, order):
    """Simulates a truck traveling from a hub to a random destination."""

    # 1. Assign Start (Dispatch Center) and End (Customer)
    start_pos = random.choice(DISPATCH_CENTERS).copy()
    destination = {"x": random.randint(0, MAP_SIZE), "y": random.randint(0, MAP_SIZE)}

    current_pos = start_pos.copy()

    print(f"🚛 Truck {truck_id} starting at {current_pos} -> Dest {destination}")

    # Update Order -> In Progress
    order["status"] = "in_progress"
    producer.send(TOPIC_ORDERS, order)

    # 2. Movement Loop
    while current_pos["x"] != destination["x"] or current_pos["y"] != destination["y"]:

        # Calculate distance remaining
        dx = destination["x"] - current_pos["x"]
        dy = destination["y"] - current_pos["y"]

        # Determine Move Distance (Speed): Random between 2 and 5 units
        step_size = random.randint(2, 5)

        # LOGIC: Move along grid lines (Manhattan)
        # We prioritize the axis with the larger distance to cover,
        # but sometimes switch it up to look less robotic.
        move_x = False

        if abs(dx) > abs(dy):
            move_x = True
        elif abs(dy) > abs(dx):
            move_x = False
        else:
            # If distances are equal, pick random axis
            move_x = random.choice([True, False])

        # Execute Move
        if move_x and dx != 0:
            # Don't overshoot: min(step, distance_remaining)
            actual_step = min(step_size, abs(dx))
            direction = 1 if dx > 0 else -1
            current_pos["x"] += actual_step * direction
        elif dy != 0:
            actual_step = min(step_size, abs(dy))
            direction = 1 if dy > 0 else -1
            current_pos["y"] += actual_step * direction

        # Send Status Update
        truck_status = {
            "truck_id": truck_id,
            "position": current_pos,
            "timestamp": time.time(),
            "status": "shipping",
            "current_order_id": order["order_id"],
            "destination": destination,  # Optional: useful for debugging/viz
        }
        producer.send(TOPIC_TRUCK_STATUS, truck_status)

        # Simulation Tick
        time.sleep(0.1)  # Update every 0.5 seconds

    # 3. Delivery Complete
    # Send final "delivered" status (optional, but good for logs)
    # The visualizer handles removal via the Order topic, but we keep this for consistency.
    final_status = {
        "truck_id": truck_id,
        "position": current_pos,
        "timestamp": time.time(),
        "status": "delivered",
        "current_order_id": order["order_id"],
    }
    producer.send(TOPIC_TRUCK_STATUS, final_status)

    # Update Order -> Shipped (Triggers Visualizer Cleanup)
    order["status"] = "shipped"
    producer.send(TOPIC_ORDERS, order)

    print(f"✅ Truck {truck_id} finished delivery at {current_pos}.")


def main():
    consumer = KafkaConsumer(
        TOPIC_ORDERS,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="truck_service_group",
    )

    print("Truck Service Listening (Grid Movement Mode)...")

    for message in consumer:
        order = message.value

        if order.get("status") == "received":
            truck_id = f"truck_{random.randint(1000, 9999)}"
            t = threading.Thread(target=simulate_truck_delivery, args=(truck_id, order))
            t.start()


if __name__ == "__main__":
    main()

