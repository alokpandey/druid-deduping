# Druid Deduplication Project

A collection of tools and scripts for Apache Druid and Kafka integration with data deduplication capabilities.

## Overview

This project provides a set of utilities for working with Apache Druid and Kafka, with a focus on data deduplication using Redis. It includes scripts for:

- Setting up Kafka and Druid integration
- Publishing data to Kafka topics
- Consuming data from Kafka
- Deduplicating data using Redis
- Automating Druid maintenance

## Components

### Kafka Integration

- `docker-compose-kafka.yml`: Docker Compose file for setting up Kafka
- `kafka_supervisor_spec.json`: Druid supervisor spec for Kafka ingestion
- `inventory_new_spec.json`: Druid ingestion spec for inventory data

### Data Publishing

- `publish_to_kafka.py`: Script to publish data from JSON files to Kafka
- `publish_records.sh`: Shell script to publish records to Kafka
- `publish_records_to_kafka.sh`: Alternative shell script for publishing to Kafka
- `publish_clean_records.py`: Script to publish properly formatted records to Kafka
- `publish_with_duplicates.py`: Script to publish records with intentional duplicates

### Data Deduplication

- `random_deduplicated_data.py`: Script that generates random data and deduplicates using Redis
- `send_deduplicated_data.py`: Script that sends data to Kafka with Redis-based deduplication
- `send_more_data.py`: Script to send additional data to Kafka

### Druid Management

- `druid_kafka_reindexer.py`: Script to reindex Druid data from Kafka
- `kill_task.json`: Druid kill task specification for removing old data
- `restart_druid.sh`: Script to restart Druid services
- `restart_druid_daily.sh`: Script to restart Druid services on a daily schedule

### Measurement and Testing

- `measure_ingestion_time.py`: Script to measure Druid ingestion time
- `measure_ingestion_time_with_duplicates.py`: Script to measure ingestion time with duplicates
- `reset_kafka_topic.py`: Script to reset a Kafka topic

## Sample Data

- `inventory_data.json`: Sample inventory data (100 records)
- `inventory_small.json`: Smaller sample inventory data (10 records)
- `inventory_records.json`: Another set of sample inventory records

## Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.6+
- Redis
- Apache Druid

### Installation

1. Clone this repository:
   ```
   git clone https://github.com/alokpandey/druid-deduping.git
   cd druid-deduping
   ```

2. Create a Python virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Start Kafka:
   ```
   docker-compose -f docker-compose-kafka.yml up -d
   ```

4. Configure Druid for Kafka ingestion:
   ```
   curl -X POST -H "Content-Type: application/json" -d @kafka_supervisor_spec.json http://localhost:8081/druid/indexer/v1/supervisor
   ```

## Usage

### Publishing Data to Kafka

```bash
python publish_to_kafka.py --input-file inventory_data.json --topic inventory --bootstrap-servers kafka:9092
```

### Deduplicating Data

```bash
python random_deduplicated_data.py
```

### Reindexing Druid Data

```bash
python druid_kafka_reindexer.py --datasource inventory_items --kafka-topic inventory --bootstrap-servers kafka:9092
```

### Automating Druid Restarts

Set up a cron job to restart Druid daily:

```bash
(crontab -l 2>/dev/null; echo "30 5 * * * /path/to/restart_druid.sh") | crontab -
```

## License

MIT

## Author

Alok Pandey
