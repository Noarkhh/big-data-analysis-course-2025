import requests
import json
import time

CONNECT_URL = "http://localhost:8083/connectors"

def create_connector():
    config = {
        "name": "postgres-sink-connector",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
            "tasks.max": "1",
            # Topics to consume
            "topics": "orders,truck_status",
            # Database Connection
            "connection.url": "jdbc:postgresql://postgres:5432/logistics_db",
            "connection.user": "user",
            "connection.password": "password",
            # Auto-create tables if they don't exist
            "auto.create": "true",
            # Add columns if schema changes
            "auto.evolve": "true",
            "insert.mode": "insert",
            "pk.mode": "none"
        }
    }

    headers = {'Content-Type': 'application/json'}
    
    print("Waiting for Kafka Connect to start...")
    while True:
        try:
            response = requests.get("http://localhost:8083/")
            if response.status_code == 200:
                break
        except:
            pass
        time.sleep(2)

    print("Deploying Connector...")
    response = requests.post(CONNECT_URL, headers=headers, data=json.dumps(config))
    
    if response.status_code in [201, 409]: # 409 means already exists
        print("✅ Connector created successfully!")
    else:
        print(f"❌ Failed: {response.text}")

if __name__ == "__main__":
    create_connector()