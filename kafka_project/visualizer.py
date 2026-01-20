import json
import threading
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_TRUCK_STATUS = "truck_status"
TOPIC_ORDERS = "orders"

# Shared state
truck_data = {}  # { 'truck_id': {'x': int, 'y': int, 'color': ...} }
order_to_truck_map = {}  # { 'order_id': 'truck_id' }

# NEW: Keep track of trucks that have finished to prevent "ghosting"
finished_trucks = set()

lock = threading.Lock()


def kafka_listener():
    """Consumes both topics to correlate movement with order completion."""
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        group_id="viz_group_race_fixed",  # Changed group ID to reset offset
        auto_offset_reset="latest",
    )

    consumer.subscribe([TOPIC_TRUCK_STATUS, TOPIC_ORDERS])
    print(f"🎨 Visualizer listening on {TOPIC_TRUCK_STATUS} and {TOPIC_ORDERS}...")

    for message in consumer:
        topic = message.topic
        data = message.value

        with lock:
            # --- CASE 1: Truck Movement Update ---
            if topic == TOPIC_TRUCK_STATUS:
                t_id = data["truck_id"]
                o_id = data.get("current_order_id")

                # CRITICAL FIX:
                # If this truck is already marked as finished, ignore this message.
                # This handles the race condition where a late "position update"
                # arrives after the "order shipped" event.
                if t_id in finished_trucks:
                    continue

                x = data["position"]["x"]
                y = data["position"]["y"]

                # Link order -> truck
                if o_id:
                    order_to_truck_map[o_id] = t_id

                # Update/Create truck
                if t_id not in truck_data:
                    h = hash(t_id)
                    color = (
                        (h & 0xFF) / 255.0,
                        ((h >> 8) & 0xFF) / 255.0,
                        ((h >> 16) & 0xFF) / 255.0,
                    )
                    truck_data[t_id] = {"x": x, "y": y, "color": color}
                else:
                    truck_data[t_id]["x"] = x
                    truck_data[t_id]["y"] = y

            # --- CASE 2: Order Status Update ---
            elif topic == TOPIC_ORDERS:
                status = data.get("status")
                o_id = data.get("order_id")

                # If order is done, clean up
                if status == "shipped" and o_id in order_to_truck_map:
                    t_id = order_to_truck_map[o_id]

                    # 1. Remove from drawing map
                    if t_id in truck_data:
                        del truck_data[t_id]
                        print(f"✅ Order {o_id} shipped. Truck {t_id} removed.")

                    # 2. Add to ignore list (Fixes Race Condition)
                    finished_trucks.add(t_id)

                    # 3. Cleanup memory
                    del order_to_truck_map[o_id]


def update_plot(frame):
    """Refreshes the Matplotlib window."""
    plt.cla()
    plt.xlim(0, 100)
    plt.ylim(0, 100)
    plt.title("Real-Time Truck Logistics Map")
    plt.xlabel("East-West Position")
    plt.ylabel("North-South Position")
    plt.grid(True, linestyle="--", alpha=0.5)

    with lock:
        if not truck_data:
            plt.text(50, 50, "Waiting for orders...", ha="center", color="gray")
            return

        x_vals = []
        y_vals = []
        colors = []
        labels = []

        for t_id, info in truck_data.items():
            x_vals.append(info["x"])
            y_vals.append(info["y"])
            colors.append(info["color"])
            labels.append(t_id)

        plt.scatter(x_vals, y_vals, c=colors, s=100, edgecolors="black")

        for i, label in enumerate(labels):
            plt.annotate(
                label,
                (x_vals[i], y_vals[i]),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
            )


def main():
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()

    fig = plt.figure(figsize=(8, 6))
    ani = FuncAnimation(fig, update_plot, interval=50)
    plt.show()


if __name__ == "__main__":
    main()

