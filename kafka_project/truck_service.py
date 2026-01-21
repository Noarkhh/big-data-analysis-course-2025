import json
import time
import threading
import random
from kafka import KafkaConsumer, KafkaProducer

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
TOPIC_ORDERS = 'orders'
TOPIC_TRUCK_STATUS = 'truck_status'

# Map settings
MAP_SIZE = 100
DISPATCH_CENTERS = [
    {"x": 10, "y": 10},  # Center 1 (Bottom Left)
    {"x": 10, "y": 90},  # Center 2 (Top Left)
    {"x": 90, "y": 10},  # Center 3 (Bottom Right)
    {"x": 90, "y": 90},  # Center 4 (Top Right)
    {"x": 50, "y": 50}   # Center 5 (Middle)
]

# --- Schemas for JDBC Sink Connector ---

# Schema for Truck Status (Flattened position)
TRUCK_SCHEMA = {
    "type": "struct",
    "fields": [
        {"type": "string", "optional": False, "field": "truck_id"},
        {"type": "int32", "optional": False, "field": "pos_x"},
        {"type": "int32", "optional": False, "field": "pos_y"},
        {"type": "string", "optional": False, "field": "status"},
        {"type": "string", "optional": True, "field": "current_order_id"},
        {"type": "int64", "optional": False, "name": "org.apache.kafka.connect.data.Timestamp", "field": "timestamp"}
    ],
    "optional": False,
    "name": "truck_status"
}

# Schema for Orders (Must match what Generator sends)
ORDER_SCHEMA = {
    "type": "struct",
    "fields": [
        {"type": "string", "optional": False, "field": "order_id"},
        {"type": "string", "optional": False, "field": "client_id"},
        {"type": "float", "optional": False, "field": "price"},
        {"type": "string", "optional": False, "field": "status"},
        {"type": "int64", "optional": False, "name": "org.apache.kafka.connect.data.Timestamp", "field": "timestamp"}
    ],
    "optional": False,
    "name": "orders"
}

# --- Kafka Producer ---
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def send_with_schema(topic, schema, data):
    """Helper to wrap data in schema and send."""
    payload = {"schema": schema, "payload": data}
    producer.send(topic, payload)

def simulate_truck_delivery(truck_id, order_data):
    """Simulates a truck traveling from a hub to a random destination."""
    
    # 1. Assign Start (Dispatch Center) and End (Random Point)
    start_pos = random.choice(DISPATCH_CENTERS).copy()
    destination = {
        "x": random.randint(0, MAP_SIZE),
        "y": random.randint(0, MAP_SIZE)
    }
    
    current_pos = start_pos.copy()
    
    # 2. Update Order -> In Progress
    # We copy the order data to preserve ID/Price/Client, just update Status/Time
    order_update = order_data.copy()
    order_update['status'] = 'in_progress'
    order_update['timestamp'] = int(time.time() * 1000)
    
    send_with_schema(TOPIC_ORDERS, ORDER_SCHEMA, order_update)
    
    # 3. Movement Loop (Manhattan / Grid movement)
    while current_pos["x"] != destination["x"] or current_pos["y"] != destination["y"]:
        
        # Calculate distance remaining
        dx = destination["x"] - current_pos["x"]
        dy = destination["y"] - current_pos["y"]
        
        # Determine Move Distance (Speed)
        step_size = random.randint(2, 5)

        # Logic: Move along grid lines only
        move_x = False
        if abs(dx) > abs(dy):
            move_x = True
        elif abs(dy) > abs(dx):
            move_x = False
        else:
            move_x = random.choice([True, False])

        # Execute Move
        if move_x and dx != 0:
            actual_step = min(step_size, abs(dx)) 
            direction = 1 if dx > 0 else -1
            current_pos["x"] += actual_step * direction
        elif dy != 0:
            actual_step = min(step_size, abs(dy))
            direction = 1 if dy > 0 else -1
            current_pos["y"] += actual_step * direction

        # Send Status Update (FLATTENED for JDBC)
        truck_status = {
            "truck_id": truck_id,
            "pos_x": int(current_pos["x"]),
            "pos_y": int(current_pos["y"]),
            "status": "shipping",
            "current_order_id": order_data['order_id'],
            "timestamp": int(time.time() * 1000)
        }
        send_with_schema(TOPIC_TRUCK_STATUS, TRUCK_SCHEMA, truck_status)
        
        # Simulation Tick
        time.sleep(0.5) 

    # 4. Delivery Complete
    # Send final "delivered" status to truck topic
    final_status = {
        "truck_id": truck_id,
        "pos_x": int(current_pos["x"]),
        "pos_y": int(current_pos["y"]),
        "status": "delivered",
        "current_order_id": order_data['order_id'],
        "timestamp": int(time.time() * 1000)
    }
    send_with_schema(TOPIC_TRUCK_STATUS, TRUCK_SCHEMA, final_status)

    # Update Order -> Shipped (Triggers Visualizer Cleanup & DB Final State)
    order_final = order_data.copy()
    order_final['status'] = 'shipped'
    order_final['timestamp'] = int(time.time() * 1000)
    
    send_with_schema(TOPIC_ORDERS, ORDER_SCHEMA, order_final)

def main():
    consumer = KafkaConsumer(
        TOPIC_ORDERS,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='truck_service_group_jdbc',
        auto_offset_reset='latest'
    )

    print("🚛 Truck Service Listening (Grid Mode + JDBC Support)...")

    for message in consumer:
        try:
            raw_msg = message.value
            
            # Unpack schema/payload wrapper if present
            if isinstance(raw_msg, dict) and 'payload' in raw_msg:
                order_data = raw_msg['payload']
            else:
                order_data = raw_msg

            # Process only new orders
            if order_data.get('status') == 'received':
                truck_id = f"truck_{random.randint(1000, 9999)}"
                
                # Start simulation in a separate thread
                t = threading.Thread(target=simulate_truck_delivery, args=(truck_id, order_data))
                t.start()
                
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    main()