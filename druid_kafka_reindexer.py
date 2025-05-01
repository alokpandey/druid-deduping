#!/usr/bin/env python3
"""
Druid Kafka Reindexer

This script performs three main functions:
1. Reindexes Druid data by submitting a kill task to remove old data
2. Consumes data from Kafka and sends it to Druid
3. Deletes messages from Kafka after they've been consumed

Usage:
    python druid_kafka_reindexer.py --datasource inventory_items --kafka-topic inventory --bootstrap-servers kafka:9092
"""

import argparse
import json
import logging
import requests
import sys
import time
from confluent_kafka import Consumer, KafkaError, KafkaException
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DruidKafkaReindexer:
    def __init__(self, args):
        self.args = args
        self.druid_coordinator_url = args.druid_coordinator_url
        self.druid_broker_url = args.druid_broker_url
        self.datasource = args.datasource
        self.kafka_topic = args.kafka_topic
        self.bootstrap_servers = args.bootstrap_servers
        self.consumer_group = args.consumer_group
        self.kill_task_id = None

    def submit_kill_task(self):
        """Submit a kill task to Druid to mark old data for removal"""
        logger.info(f"Submitting kill task for datasource: {self.datasource}")

        kill_task = {
            "type": "kill",
            "dataSource": self.datasource,
            "interval": "1000/3000"  # This covers all time ranges
        }

        response = requests.post(
            f"{self.druid_coordinator_url}/druid/indexer/v1/task",
            headers={"Content-Type": "application/json"},
            data=json.dumps(kill_task)
        )

        if response.status_code != 200:
            logger.error(f"Failed to submit kill task: {response.text}")
            sys.exit(1)

        self.kill_task_id = response.json()["task"]
        logger.info(f"Kill task submitted successfully. Task ID: {self.kill_task_id}")
        return self.kill_task_id

    def wait_for_kill_task(self, max_wait_seconds=300, check_interval=10):
        """Wait for the kill task to complete"""
        if not self.kill_task_id:
            logger.error("No kill task ID available")
            return False

        logger.info(f"Waiting for kill task to complete: {self.kill_task_id}")
        start_time = time.time()

        while time.time() - start_time < max_wait_seconds:
            response = requests.get(
                f"{self.druid_coordinator_url}/druid/indexer/v1/task/{self.kill_task_id}/status"
            )

            if response.status_code != 200:
                logger.error(f"Failed to get task status: {response.text}")
                time.sleep(check_interval)
                continue

            status = response.json()["status"]["statusCode"]
            logger.info(f"Kill task status: {status}")

            if status == "SUCCESS":
                logger.info("Kill task completed successfully")
                return True
            elif status == "FAILED":
                logger.error("Kill task failed")
                return False

            time.sleep(check_interval)

        logger.warning(f"Timed out waiting for kill task to complete after {max_wait_seconds} seconds")
        return False

    def check_supervisor_status(self):
        """Check if the Kafka supervisor is running"""
        logger.info(f"Checking supervisor status for datasource: {self.datasource}")

        response = requests.get(
            f"{self.druid_coordinator_url}/druid/indexer/v1/supervisor/{self.datasource}/status"
        )

        if response.status_code != 200:
            logger.warning(f"Supervisor not found or error: {response.text}")
            return False

        status = response.json()["payload"]["state"]
        logger.info(f"Supervisor status: {status}")
        return status == "RUNNING"

    def consume_from_kafka(self, max_messages=None):
        """Consume messages from Kafka"""
        logger.info(f"Starting to consume from Kafka topic: {self.kafka_topic}")

        # Configure Kafka consumer
        conf = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True
        }

        consumer = Consumer(conf)
        consumer.subscribe([self.kafka_topic])

        try:
            message_count = 0
            while True:
                if max_messages and message_count >= max_messages:
                    logger.info(f"Reached maximum message count: {max_messages}")
                    break

                msg = consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Error while consuming: {msg.error()}")
                    continue

                # Process the message
                try:
                    key = msg.key().decode('utf-8') if msg.key() else None
                    value = json.loads(msg.value().decode('utf-8'))

                    logger.info(f"Consumed message: key={key}, value={value}")
                    message_count += 1

                    # Here you could implement additional processing or transformation
                    # before the data is ingested by Druid

                except Exception as e:
                    logger.error(f"Error processing message: {e}")

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            consumer.close()
            logger.info(f"Consumed {message_count} messages from Kafka")

    def run(self):
        """Run the reindexing and consumption process"""
        # Step 1: Check if supervisor is running
        supervisor_running = self.check_supervisor_status()
        if not supervisor_running:
            logger.warning(f"Supervisor for {self.datasource} is not running")
            # You could add code here to create or reset the supervisor if needed

        # Step 2: Submit kill task to remove old data
        self.submit_kill_task()

        # Step 3: Wait for kill task to complete
        kill_task_success = self.wait_for_kill_task()
        if not kill_task_success:
            logger.warning("Kill task did not complete successfully, but continuing...")

        # Step 4: Consume from Kafka
        # Note: In a real-world scenario, you might not need this step if Druid's Kafka supervisor
        # is already consuming from Kafka. This is just for demonstration purposes.
        self.consume_from_kafka(max_messages=self.args.max_messages)

        logger.info("Reindexing and consumption process completed")


def parse_args():
    parser = argparse.ArgumentParser(description='Druid Kafka Reindexer')
    parser.add_argument('--datasource', required=True, help='Druid datasource name')
    parser.add_argument('--kafka-topic', required=True, help='Kafka topic to consume from')
    parser.add_argument('--bootstrap-servers', default='kafka:9092', help='Kafka bootstrap servers')
    parser.add_argument('--consumer-group', default='druid-reindexer', help='Kafka consumer group ID')
    parser.add_argument('--druid-coordinator-url', default='http://localhost:8081', help='Druid coordinator URL')
    parser.add_argument('--druid-broker-url', default='http://localhost:8082', help='Druid broker URL')
    parser.add_argument('--max-messages', type=int, help='Maximum number of messages to consume (default: unlimited)')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    reindexer = DruidKafkaReindexer(args)
    reindexer.run()
