#!/usr/bin/env python3
"""
Reset Kafka Topic

This script deletes and recreates a Kafka topic to effectively clear all messages.

Usage:
    python reset_kafka_topic.py --topic inventory --bootstrap-servers kafka:9092
"""

import argparse
import logging
import sys
import time
from confluent_kafka.admin import AdminClient, NewTopic

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def reset_topic(args):
    """Delete and recreate a Kafka topic"""
    # Configure Kafka admin client
    conf = {
        'bootstrap.servers': args.bootstrap_servers
    }
    
    admin = AdminClient(conf)
    
    # Delete the topic if it exists
    logger.info(f"Deleting topic: {args.topic}")
    try:
        futures = admin.delete_topics([args.topic])
        for topic, future in futures.items():
            try:
                future.result()  # Wait for operation to complete
                logger.info(f"Topic {topic} deleted successfully")
            except Exception as e:
                logger.warning(f"Failed to delete topic {topic}: {e}")
    except Exception as e:
        logger.warning(f"Error during topic deletion: {e}")
    
    # Wait for the deletion to complete
    logger.info("Waiting for topic deletion to complete...")
    time.sleep(5)
    
    # Create the topic
    logger.info(f"Creating topic: {args.topic}")
    topic_list = [
        NewTopic(
            args.topic,
            num_partitions=args.partitions,
            replication_factor=args.replication_factor
        )
    ]
    
    try:
        futures = admin.create_topics(topic_list)
        for topic, future in futures.items():
            try:
                future.result()  # Wait for operation to complete
                logger.info(f"Topic {topic} created successfully")
            except Exception as e:
                logger.error(f"Failed to create topic {topic}: {e}")
                sys.exit(1)
    except Exception as e:
        logger.error(f"Error during topic creation: {e}")
        sys.exit(1)
    
    logger.info(f"Topic {args.topic} has been reset successfully")

def parse_args():
    parser = argparse.ArgumentParser(description='Reset Kafka Topic')
    parser.add_argument('--topic', required=True, help='Kafka topic to reset')
    parser.add_argument('--bootstrap-servers', default='kafka:9092', help='Kafka bootstrap servers')
    parser.add_argument('--partitions', type=int, default=3, help='Number of partitions for the topic')
    parser.add_argument('--replication-factor', type=int, default=1, help='Replication factor for the topic')
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    reset_topic(args)
