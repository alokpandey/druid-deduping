#!/bin/bash

# Publish the first 70 records (non-duplicates)
for i in {1..70}
do
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  product_id="product$i"
  
  # Set product details based on category
  if [ $i -le 20 ]; then
    category="Electronics"
    warehouse_id="WH001"
    if [ $i -le 5 ]; then
      price=999.99
      quantity=50
      product_name="Laptop"
    elif [ $i -le 10 ]; then
      price=699.99
      quantity=100
      product_name="Smartphone"
    elif [ $i -le 15 ]; then
      price=149.99
      quantity=200
      product_name="Headphones"
    else
      price=349.99
      quantity=75
      product_name="Tablet"
    fi
  elif [ $i -le 35 ]; then
    category="Furniture"
    warehouse_id="WH002"
    price=299.99
    quantity=20
    product_name="Desk"
  elif [ $i -le 50 ]; then
    category="Home Decor"
    warehouse_id="WH003"
    price=39.99
    quantity=50
    product_name="Lamp"
  else
    category="Office Supplies"
    warehouse_id="WH004"
    price=4.99
    quantity=200
    product_name="Notebook"
  fi
  
  # Create JSON record
  record="{\"timestamp\":\"$timestamp\", \"product_id\":\"$product_id\", \"product_name\":\"$product_name\", \"category\":\"$category\", \"quantity\":$quantity, \"price\":$price, \"warehouse_id\":\"$warehouse_id\"}"
  
  # Publish to Kafka
  echo "$product_id:$record" | docker exec -i kafka kafka-console-producer --topic inventory --bootstrap-server kafka:9092 --property "parse.key=true" --property "key.separator=:" > /dev/null
  
  echo "Published record for $product_id"
  
  # Small delay
  sleep 0.1
done

# Publish 30 duplicate records (products 1-30)
for i in {1..30}
do
  timestamp=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  product_id="product$i"
  
  # Set product details based on category
  if [ $i -le 20 ]; then
    category="Electronics"
    warehouse_id="WH001"
    if [ $i -le 5 ]; then
      price=999.99
      quantity=50
      product_name="Laptop"
    elif [ $i -le 10 ]; then
      price=699.99
      quantity=100
      product_name="Smartphone"
    elif [ $i -le 15 ]; then
      price=149.99
      quantity=200
      product_name="Headphones"
    else
      price=349.99
      quantity=75
      product_name="Tablet"
    fi
  elif [ $i -le 30 ]; then
    category="Furniture"
    warehouse_id="WH002"
    price=299.99
    quantity=20
    product_name="Desk"
  fi
  
  # Create JSON record
  record="{\"timestamp\":\"$timestamp\", \"product_id\":\"$product_id\", \"product_name\":\"$product_name\", \"category\":\"$category\", \"quantity\":$quantity, \"price\":$price, \"warehouse_id\":\"$warehouse_id\"}"
  
  # Publish to Kafka
  echo "$product_id:$record" | docker exec -i kafka kafka-console-producer --topic inventory --bootstrap-server kafka:9092 --property "parse.key=true" --property "key.separator=:" > /dev/null
  
  echo "Published duplicate record for $product_id"
  
  # Small delay
  sleep 0.1
done

echo "Published all 100 records to Kafka"
