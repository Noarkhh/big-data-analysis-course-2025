import json
import threading
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKER = 'localhost:9092'
TOPIC_TRUCK_STATUS = 'truck_status'
TOPIC_ORDERS = 'orders'

# Shared state
truck_data = {}          
order_to_truck_map = {}  
finished_trucks = set() 
lock = threading.Lock()

def kafka_listener():
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='viz_group_fast_schema', 
        auto_offset_reset='latest'
    )
    
    consumer.subscribe([TOPIC_TRUCK_STATUS, TOPIC_ORDERS])
    print(f"🎨 Visualizer listening (High Frequency Mode)...")
    
    for message in consumer:
        # --- NEW: Handle Schema/Payload Wrapper ---
        raw_msg = message.value
        # If the message is wrapped in a schema (which it should be now), unpack it
        if isinstance(raw_msg, dict) and 'payload' in raw_msg:
            data = raw_msg['payload']
        else:
            data = raw_msg # Fallback for old messages

        topic = message.topic
        
        with lock:
            # --- CASE 1: Truck Movement Update ---
            if topic == TOPIC_TRUCK_STATUS:
                t_id = data['truck_id']
                
                # Check ignore list
                if t_id in finished_trucks:
                    continue

                # --- NEW: Use flattened coordinates ---
                x = data['pos_x']
                y = data['pos_y']
                
                o_id = data.get('current_order_id')
                if o_id:
                    order_to_truck_map[o_id] = t_id

                # Update/Create truck
                if t_id not in truck_data:
                    h = hash(t_id)
                    color = ((h & 0xFF) / 255.0, ((h >> 8) & 0xFF) / 255.0, ((h >> 16) & 0xFF) / 255.0)
                    truck_data[t_id] = {'x': x, 'y': y, 'color': color}
                else:
                    truck_data[t_id]['x'] = x
                    truck_data[t_id]['y'] = y

            # --- CASE 2: Order Status Update ---
            elif topic == TOPIC_ORDERS:
                status = data.get('status')
                o_id = data.get('order_id')

                if status == 'shipped' and o_id in order_to_truck_map:
                    t_id = order_to_truck_map[o_id]
                    
                    if t_id in truck_data:
                        del truck_data[t_id]
                    
                    finished_trucks.add(t_id)
                    del order_to_truck_map[o_id]

def update_plot(frame):
    plt.cla() 
    plt.xlim(0, 100)
    plt.ylim(0, 100)
    plt.title("Real-Time Truck Logistics Map (High Speed)")
    plt.xlabel("X")
    plt.ylabel("Y")
    # Reduced alpha for grid to make fast moving dots clearer
    plt.grid(True, linestyle=':', alpha=0.3) 

    with lock:
        if not truck_data:
            plt.text(50, 50, "Waiting for traffic...", ha='center', color='gray')
            return

        x_vals = []
        y_vals = []
        colors = []
        
        # We might skip labels if there are too many trucks to improve performance
        for t_id, info in truck_data.items():
            x_vals.append(info['x'])
            y_vals.append(info['y'])
            colors.append(info['color'])

        plt.scatter(x_vals, y_vals, c=colors, s=80, edgecolors='black', linewidth=0.5)

def main():
    t = threading.Thread(target=kafka_listener, daemon=True)
    t.start()

    fig = plt.figure(figsize=(8, 6))
    
    # --- NEW: Interval set to 20ms (50 updates/sec) ---
    ani = FuncAnimation(fig, update_plot, interval=20) 
    plt.show()

if __name__ == "__main__":
    main()