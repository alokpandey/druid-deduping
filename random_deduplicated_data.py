#!/usr/bin/env python3
"""
Send Random Deduplicated Data to Kafka

This script generates random inventory records, some of which may be duplicates.
It checks if each record already exists in Redis based on its hash value.
If the hash is found in Redis, the record is skipped.
It also measures the total time taken for the entire process.
"""

import json
import subprocess
import time
import hashlib
import redis
import random
from datetime import datetime, timedelta

def main():
    # Record start time
    start_time = time.time()
    # Connect to Redis
    r = redis.Redis(host='localhost', port=6379, db=0)

    # Set expiration time for Redis keys (e.g., 1 day)
    expiration_time = 86400  # 24 hours in seconds

    # Base timestamp
    base_time = datetime.now()

    # Track statistics
    total_records = 0
    duplicate_records = 0
    new_records = 0

    # Define product categories
    categories = ["Electronics", "Furniture", "Clothing", "Books", "Toys"]

    # Define warehouses
    warehouses = ["WH001", "WH002", "WH003", "WH004", "WH005"]

    # Generate a set of records
    num_records = 36  # Total records to process
    duplicate_percentage = 30  # Percentage of records that should be duplicates

    # Calculate how many unique records to generate
    num_unique = int(num_records * (1 - duplicate_percentage / 100))

    # Generate unique records
    unique_records = []
    for i in range(num_unique):
        # Create a random record
        product_id = f"product-{random.randint(1000, 9999)}"
        category = random.choice(categories)
        warehouse = random.choice(warehouses)
        quantity = random.randint(10, 1000)
        price = round(random.uniform(1.99, 999.99), 2)

        record = {
            "timestamp": (base_time + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "product_id": product_id,
            "product_name": f"Random Product {i+1}",
            "category": category,
            "quantity": quantity,
            "price": price,
            "warehouse_id": warehouse
        }

        unique_records.append(record)

    # Create a list of all records to process, including duplicates
    all_records = unique_records.copy()

    # Add duplicates (with different timestamps)
    num_duplicates = num_records - num_unique
    for i in range(num_duplicates):
        # Pick a random record to duplicate
        original = random.choice(unique_records)
        duplicate = original.copy()

        # Update the timestamp to make it different
        duplicate["timestamp"] = (base_time + timedelta(minutes=num_unique+i)).strftime("%Y-%m-%dT%H:%M:%SZ")

        all_records.append(duplicate)

    # Shuffle the records to mix duplicates with unique records
    random.shuffle(all_records)

    # Process each record
    for record in all_records:
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

    # Calculate total time taken
    end_time = time.time()
    total_time = end_time - start_time

    print(f"\nSummary:")
    print(f"Total records processed: {total_records}")
    print(f"Duplicate records skipped: {duplicate_records}")
    print(f"New records published: {new_records}")
    print(f"Duplicate percentage: {duplicate_records/total_records*100:.1f}%")
    print(f"Total time taken: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
