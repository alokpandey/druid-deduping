#!/usr/bin/env python3
"""
Publish Clean Records to Kafka

This script reads inventory records from a JSON file and publishes them to a Kafka topic
with proper JSON formatting.
"""

import json
import subprocess
import time

def main():
    # Read the inventory records from the JSON file
    with open('inventory_small.json', 'r') as f:
        records = json.load(f)
    
    print(f"Publishing {len(records)} records to Kafka...")
    
    # First, delete the topic if it exists
    subprocess.run(["docker", "exec", "kafka", "kafka-topics", "--delete", 
                   "--topic", "inventory_new", "--bootstrap-server", "kafka:9092"],
                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Wait for the topic to be deleted
    time.sleep(2)
    
    # Create a new topic
    subprocess.run(["docker", "exec", "kafka", "kafka-topics", "--create", 
                   "--topic", "inventory_new", "--bootstrap-server", "kafka:9092",
                   "--partitions", "3", "--replication-factor", "1"],
                  stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Wait for the topic to be created
    time.sleep(2)
    
    # Publish each record to Kafka
    for record in records:
        # Convert the record to a properly formatted JSON string
        record_json = json.dumps(record)
        
        # Use the product_id as the key
        key = record.get('product_id', 'default-key')
        
        # Create the command to publish to Kafka
        cmd = f'echo "{key}:{record_json}" | docker exec -i kafka kafka-console-producer --topic inventory_new --bootstrap-server kafka:9092 --property "parse.key=true" --property "key.separator=:"'
        
        # Execute the command
        subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        print(f"Published record for {key}")
        
        # Small delay to avoid overwhelming the broker
        time.sleep(0.1)
    
    print("All records published successfully!")

if __name__ == "__main__":
    main()
