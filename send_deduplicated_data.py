#!/usr/bin/env python3
"""
Send Deduplicated Data to Kafka

This script sends messages to Kafka, but first checks if the record already exists in Redis.
If the hash of the record is found in Redis, the record is skipped.
"""

import json
import subprocess
import time
import hashlib
import redis
from datetime import datetime, timedelta

def main():
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)
    
    # Set expiration time for Redis keys (e.g., 1 day)
    expiration_time = 86400  # 24 hours in seconds
    
    # Base timestamp
    base_time = datetime(2025, 4, 18, 20, 0, 0)
    
    # Track statistics
    total_records = 0
    duplicate_records = 0
    new_records = 0
    
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
            
            total_records += 1
            
            # Create a hash of the record (excluding timestamp to focus on data content)
            record_for_hash = record.copy()
            del record_for_hash["timestamp"]  # Remove timestamp for deduplication
            record_hash = hashlib.md5(json.dumps(record_for_hash, sort_keys=True).encode()).hexdigest()
            
            # Check if this record hash exists in Redis
            if r.exists(f"inventory:{record_hash}"):
                print(f"Skipping duplicate record for {record['product_id']} (hash: {record_hash})")
                duplicate_records += 1
                continue
            
            # Store the hash in Redis with expiration
            r.setex(f"inventory:{record_hash}", expiration_time, "1")
            
            # Convert record to JSON
            record_json = json.dumps(record)
            
            # Create command to publish to Kafka
            cmd = f'echo \'{record_json}\' | docker exec -i kafka kafka-console-producer --topic inventory_new --bootstrap-server kafka:9092'
            
            # Execute command
            subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            print(f"Published new record for {record['product_id']} (hash: {record_hash})")
            new_records += 1
            
            # Small delay
            time.sleep(0.1)
    
    print(f"\nSummary:")
    print(f"Total records processed: {total_records}")
    print(f"Duplicate records skipped: {duplicate_records}")
    print(f"New records published: {new_records}")

if __name__ == "__main__":
    main()
