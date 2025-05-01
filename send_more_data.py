
#!/usr/bin/env python3
"""
Send More Data to Kafka

This script sends a series of messages to Kafka with timestamps spanning multiple hours
to trigger segment creation in Druid.
"""

import json
import subprocess
import time
from datetime import datetime, timedelta

def main():
    # Base timestamp
    base_time = datetime(2025, 4, 18, 20, 0, 0)
    
    # Send messages with timestamps spanning multiple hours
    for hour in range(6):
        for minute in range(0, 60, 10):  # Every 10 minutes
            # Create timestamp
            timestamp = base_time + timedelta(hours=hour, minutes=minute)
            timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Create record
            record = {
                "timestamp": timestamp_str,
                "product_id": f"product-{hour}-{minute}",
                "product_name": f"Product {hour}-{minute}",
                "category": "Test",
                "quantity": 100 + (hour * 10) + minute,
                "price": 99.99 + (hour * 10) + minute,
                "warehouse_id": f"WH00{hour+1}"
            }
            
            # Convert record to JSON
            record_json = json.dumps(record)
            
            # Create command to publish to Kafka
            cmd = f'echo \'{record_json}\' | docker exec -i kafka kafka-console-producer --topic inventory_new --bootstrap-server kafka:9092'
            
            # Execute command
            subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            print(f"Published record for {timestamp_str}")
            
            # Small delay
            time.sleep(0.1)
    
    print("All records published successfully!")

if __name__ == "__main__":
    main()
