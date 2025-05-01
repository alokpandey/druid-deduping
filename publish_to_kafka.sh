#!/bin/bash

# Extract the records from the JSON file
records=$(cat inventory_records.json | jq -c '.records[]')

# Loop through each record and publish to Kafka
for record in $records; do
  # Generate a random key based on product_id
  product_id=$(echo $record | jq -r '.product_id')
  
  # Publish to Kafka
  echo "$product_id:$record" | docker exec -i kafka kafka-console-producer --topic inventory --bootstrap-server kafka:9092 --property "parse.key=true" --property "key.separator=:"
  
  # Small delay to avoid overwhelming the system
  sleep 0.1
done

echo "Published all records to Kafka"
