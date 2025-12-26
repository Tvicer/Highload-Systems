import json
import csv
import time
import os
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def wait_for_kafka(bootstrap_servers, max_retries=30):
    for i in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=10000
            )
            topics = admin_client.list_topics()
            admin_client.close()
            logger.info(f"Kafka is ready! Topics: {topics}")
            return True
        except Exception as e:
            logger.warning(f"Kafka attempt {i+1}/{max_retries}: {e}")
            time.sleep(5)
    return False

def send_csv_to_kafka(csv_file_path, topic_name, bootstrap_servers, delay=0.1):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=5,
        max_block_ms=30000
    )
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            count = 0
            
            for row in reader:
                message = {k: (v if v and v.strip() else None) for k, v in row.items()}
                
                future = producer.send(topic_name, value=message)
                count += 1
                
                if count % 100 == 0:
                    logger.info(f"Sent {count} messages to topic '{topic_name}'")
                    producer.flush()
                
                # time.sleep(delay)
            
            producer.flush()
            logger.info(f"Total messages sent from {csv_file_path.name}: {count}")
            
    except Exception as e:
        logger.error(f"Error sending data from {csv_file_path}: {e}")
        raise
    finally:
        producer.close()

if __name__ == "__main__":
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    TOPIC = "sales_data"
    DATA_DIR = Path("/app/data")
    
    if not wait_for_kafka(bootstrap_servers):
        logger.error("Kafka is not available!")
        exit(1)
    
    csv_files = sorted(DATA_DIR.glob("*.csv"))
    
    for csv_file in csv_files:
        logger.info(f"Processing file: {csv_file.name}")
        send_csv_to_kafka(csv_file, TOPIC, bootstrap_servers, delay=0.05)

    time.sleep(120)
