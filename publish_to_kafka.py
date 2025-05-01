#!/usr/bin/env python3
"""
Kafka Inventory Publisher

This script reads inventory records from a JSON file and publishes them to a Kafka topic.

Usage:
    python publish_to_kafka.py --input-file inventory_data.json --topic inventory --bootstrap-servers kafka:9092
"""

import argparse
import json
import logging
import sys
import time
from confluent_kafka import Producer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def delivery_report(err, msg):
    """Callback function for message delivery reports"""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def publish_records(args):
    """Read records from JSON file and publish to Kafka"""
    # Configure Kafka producer
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': 'inventory-publisher'
    }

    producer = Producer(conf)

    try:
        # Read inventory records from JSON file
        with open(args.input_file, 'r') as f:
            records = json.load(f)

        logger.info(f"Loaded {len(records)} records from {args.input_file}")

        # Publish each record to Kafka
        for i, record in enumerate(records):
            # Convert record to JSON string
            value = json.dumps(record).encode('utf-8')

            # Use product_id as the key if available, otherwise use a sequential number
            key = record.get('product_id', f'key-{i}').encode('utf-8')

            # Publish to Kafka
            producer.produce(
                args.topic,
                key=key,
                value=value,
                callback=delivery_report
            )

            # Flush every 10 records to ensure timely delivery
            if i % 10 == 0:
                producer.flush()

            # Add a small delay to avoid overwhelming the broker
            time.sleep(args.delay)

            # Log progress
            if (i + 1) % 10 == 0 or (i + 1) == len(records):
                logger.info(f"Published {i + 1}/{len(records)} records")

        # Final flush to ensure all messages are delivered
        producer.flush()

        logger.info(f"Successfully published {len(records)} records to topic {args.topic}")

    except FileNotFoundError:
        logger.error(f"Input file not found: {args.input_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error publishing records: {e}")
        sys.exit(1)

def parse_args():
    parser = argparse.ArgumentParser(description='Kafka Inventory Publisher')
    parser.add_argument('--input-file', default='inventory_data.json', help='Path to JSON file containing inventory records')
    parser.add_argument('--topic', default='inventory', help='Kafka topic to publish to')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka bootstrap servers')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between publishing records (seconds)')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    publish_records(args)
