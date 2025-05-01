#!/bin/bash

# Read the JSON file
json_file="inventory_small.json"
records=$(cat $json_file)

# Remove the opening and closing brackets and any whitespace
records=$(echo "$records" | sed 's/^\s*\[\s*//' | sed 's/\s*\]\s*$//')

# Split the records by commas followed by newlines or just newlines
IFS=$',\n' read -d '' -ra record_array <<< "$records"

echo "Publishing ${#record_array[@]} records to Kafka..."

# Loop through each record and publish to Kafka
for record in "${record_array[@]}"; do
  # Clean up the record (remove leading/trailing whitespace and commas)
  record=$(echo "$record" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | sed 's/,$//')
  
  # Extract product_id for use as the key
  product_id=$(echo "$record" | grep -o '"product_id":"[^"]*"' | cut -d'"' -f4)
  
  # Skip empty records
  if [ -z "$record" ]; then
    continue
  fi
  
  # Publish to Kafka
  echo "$product_id:$record" | docker exec -i kafka kafka-console-producer --topic inventory --bootstrap-server kafka:9092 --property "parse.key=true" --property "key.separator=:" > /dev/null
  
  echo "Published record for $product_id"
  
  # Small delay
  sleep 0.1
done

echo "All records published successfully!"
