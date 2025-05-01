#!/usr/bin/env python3
"""
Measure Ingestion Time

This script sends messages to Kafka and measures the total time until the data is available in Druid.
"""

import json
import subprocess
import time
import datetime
import requests
from datetime import datetime, timedelta

def main():
    # Record start time
    start_time = time.time()
    print(f"Starting data ingestion at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Generate a unique batch ID for this run
    batch_id = f"batch-{int(start_time)}"
    print(f"Batch ID: {batch_id}")
    
    # Base timestamp
    base_time = datetime.now()
    
    # Track statistics
    total_records = 0
    
    # Send messages with timestamps spanning multiple hours
    for hour in range(2):  # Reduced to 2 hours for faster testing
        for minute in range(0, 60, 10):  # Every 10 minutes
            # Create timestamp
            timestamp = base_time + timedelta(hours=hour, minutes=minute)
            timestamp_str = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            # Create record with batch ID to track this specific run
            record = {
                "timestamp": timestamp_str,
                "product_id": f"product-{hour}-{minute}",
                "product_name": f"Product {hour}-{minute}",
                "category": "Test",
                "quantity": 100 + (hour * 10) + minute,
                "price": 99.99 + (hour * 10) + minute,
                "warehouse_id": f"WH00{hour+1}",
                "batch_id": batch_id  # Add batch ID to track this run
            }
            
            total_records += 1
            
            # Convert record to JSON
            record_json = json.dumps(record)
            
            # Create command to publish to Kafka
            cmd = f'echo \'{record_json}\' | docker exec -i kafka kafka-console-producer --topic inventory_new --bootstrap-server kafka:9092'
            
            # Execute command
            subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            print(f"Published record for {record['product_id']}")
            
            # Small delay
            time.sleep(0.1)
    
    publish_end_time = time.time()
    publish_duration = publish_end_time - start_time
    print(f"\nAll {total_records} records published to Kafka in {publish_duration:.2f} seconds")
    
    # Now poll Druid until we can find records with our batch ID
    print(f"Waiting for data to be available in Druid...")
    
    query = {
        "query": f"SELECT COUNT(*) AS record_count FROM inventory_new WHERE batch_id = '{batch_id}'"
    }
    
    max_wait_time = 300  # Maximum wait time in seconds
    poll_interval = 5    # Check every 5 seconds
    start_poll_time = time.time()
    
    while time.time() - start_poll_time < max_wait_time:
        try:
            response = requests.post(
                "http://localhost:8082/druid/v2/sql",
                headers={"Content-Type": "application/json"},
                json=query
            )
            
            if response.status_code == 200:
                result = response.json()
                if result and result[0]["record_count"] > 0:
                    record_count = result[0]["record_count"]
                    if record_count == total_records:
                        end_time = time.time()
                        total_duration = end_time - start_time
                        print(f"\nAll {record_count} records found in Druid!")
                        print(f"Total time from start to data availability: {total_duration:.2f} seconds")
                        print(f"  - Kafka publishing time: {publish_duration:.2f} seconds")
                        print(f"  - Druid ingestion time: {total_duration - publish_duration:.2f} seconds")
                        return
                    else:
                        print(f"Found {record_count}/{total_records} records in Druid...")
            
        except Exception as e:
            print(f"Error querying Druid: {e}")
        
        time.sleep(poll_interval)
    
    print(f"\nTimeout reached after {max_wait_time} seconds")
    print(f"Data may still be ingesting. Check Druid manually.")

if __name__ == "__main__":
    main()
