import json
import threading
import time
import collections
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_ORDERS = 'orders'
TOPIC_TRUCK_STATUS = 'truck_status'

# Limit history for the heatmap to keep it responsive
HEATMAP_HISTORY_STEPS = 5000 
# Window size for calculating the moving average of delivery times
AVG_WINDOW_SIZE = 50 

# --- Shared State ---
lock = threading.Lock()

# Metric 1: Delivery Times
# Map {order_id: start_timestamp} to calculate duration
order_start_times = {}
# List of (timestamp_completed, duration_seconds)
delivery_history = [] 
# List of (timestamp, current_rolling_avg) for plotting
avg_delivery_over_time = [] 

# Metric 2: Heatmap Data
# Deque stores (x, y) tuples of recent truck positions
truck_position_history = collections.deque(maxlen=HEATMAP_HISTORY_STEPS)

def get_payload(msg_value):
    """Helper to unwrap JDBC schema if present."""
    if isinstance(msg_value, dict) and 'payload' in msg_value:
        return msg_value['payload']
    return msg_value

def kafka_consumer_thread():
    """Background thread acting as the Stream Processor."""
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='analytics_dashboard_v1',
        auto_offset_reset='latest'
    )
    
    consumer.subscribe([TOPIC_ORDERS, TOPIC_TRUCK_STATUS])
    print("📊 Analytics Service Listening...")

    for message in consumer:
        try:
            data = get_payload(message.value)
            topic = message.topic
            
            with lock:
                # --- PROCESS TRUCK MOVEMENT (For Heatmap) ---
                if topic == TOPIC_TRUCK_STATUS:
                    # Ignore 'delivered' events for the heatmap to avoid huge dots at destinations
                    # We only care about movement ('shipping')
                    if data.get('status') == 'shipping':
                        x = data.get('pos_x')
                        y = data.get('pos_y')
                        if x is not None and y is not None:
                            truck_position_history.append((x, y))

                # --- PROCESS ORDERS (For Delivery Time) ---
                elif topic == TOPIC_ORDERS:
                    status = data.get('status')
                    o_id = data.get('order_id')
                    ts = data.get('timestamp') # This is in ms
                    
                    if status == 'received':
                        # Record start time
                        order_start_times[o_id] = ts
                    
                    elif status == 'shipped':
                        # Calculate Duration
                        start_ts = order_start_times.pop(o_id, None)
                        if start_ts and ts:
                            duration_ms = ts - start_ts
                            duration_sec = duration_ms / 1000.0
                            
                            # Add to history
                            delivery_history.append(duration_sec)
                            
                            # Calculate Rolling Average (last N items)
                            recent_times = delivery_history[-AVG_WINDOW_SIZE:]
                            current_avg = sum(recent_times) / len(recent_times)
                            
                            # Record for plotting (using simple counter as X-axis or system time)
                            avg_delivery_over_time.append(current_avg)

        except Exception as e:
            print(f"Error processing stream: {e}")

def update_charts(frame):
    """Refreshes the Matplotlib window."""
    with lock:
        # Don't update if no data to avoid errors
        if not truck_position_history and not avg_delivery_over_time:
            return

        # --- Plot 1: Average Delivery Time ---
        ax_line.cla()
        ax_line.set_title(f"Avg Delivery Time (Rolling last {AVG_WINDOW_SIZE})")
        ax_line.set_ylabel("Seconds")
        ax_line.set_xlabel("Total Deliveries Completed")
        ax_line.grid(True, linestyle='--', alpha=0.5)
        
        if avg_delivery_over_time:
            ax_line.plot(avg_delivery_over_time, color='blue', linewidth=2, label='Avg Duration')
            # Show current value
            curr_val = avg_delivery_over_time[-1]
            ax_line.text(len(avg_delivery_over_time)-1, curr_val, f"{curr_val:.2f}s", fontsize=10, fontweight='bold')

        # --- Plot 2: Traffic Heatmap ---
        ax_heat.cla()
        ax_heat.set_title(f"Traffic Density (Last {HEATMAP_HISTORY_STEPS} Steps)")
        ax_heat.set_xlim(0, 100)
        ax_heat.set_ylim(0, 100)
        
        if len(truck_position_history) > 10:
            # Convert deque to numpy array for fast processing
            data_arr = np.array(truck_position_history)
            x = data_arr[:, 0]
            y = data_arr[:, 1]
            
            # Create 2D Histogram (Heatmap)
            # bins=50 gives us a nice granular grid (100/50 = 2 units per block)
            h = ax_heat.hist2d(x, y, bins=50, range=[[0, 100], [0, 100]], cmap='inferno')

def main():
    # 1. Start Consumer Thread
    t = threading.Thread(target=kafka_consumer_thread, daemon=True)
    t.start()

    # 2. Setup Dashboard Layout
    global ax_line, ax_heat
    fig = plt.figure(figsize=(10, 8))
    
    # 2 rows, 1 column
    ax_line = fig.add_subplot(2, 1, 1) # Top
    ax_heat = fig.add_subplot(2, 1, 2) # Bottom
    
    plt.tight_layout(pad=3.0)

    # 3. Start Animation (Update every 200ms)
    ani = FuncAnimation(fig, update_charts, interval=200)
    plt.show()

if __name__ == "__main__":
    main()