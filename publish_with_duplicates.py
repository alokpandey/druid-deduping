#!/usr/bin/env python3
"""
Publish Records with Duplicates

This script sends messages to Kafka, including 30% duplicates, and measures the time taken.
"""

import json
import subprocess
import time
import random
from datetime import datetime, timedelta

def main():
    # Record start time
    start_time = time.time()
    print(f"Starting data publishing at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Generate a unique batch ID for this run
    batch_id = f"batch-{int(start_time)}"
    print(f"Batch ID: {batch_id}")
    
    # Base timestamp
    base_time = datetime.now()
    
    # Track statistics
    total_records = 0
    duplicate_records = 0
    
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
            "warehouse_id": warehouse,
            "batch_id": batch_id  # Add batch ID to track this run
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
        duplicate_records += 1
    
    # Shuffle the records to mix duplicates with unique records
    random.shuffle(all_records)
    
    # Process each record
    for record in all_records:
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
    print(f"Unique records: {num_unique}, Duplicate records: {duplicate_records}")
    print(f"Duplicate percentage: {duplicate_records/total_records*100:.1f}%")

if __name__ == "__main__":
    main()
