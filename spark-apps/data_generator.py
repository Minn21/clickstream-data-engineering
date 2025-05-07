#!/usr/bin/env python3
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = 'kafka:29092'
KAFKA_TOPIC = 'clickstream'

# Sample data for generation
SAMPLE_PAGES = [
    '/home',
    '/products',
    '/products/electronics',
    '/products/clothing',
    '/products/books',
    '/cart',
    '/checkout',
    '/about',
    '/contact',
    '/blog',
    '/blog/tech-news',
    '/blog/company-updates',
    '/user/profile',
    '/user/orders',
    '/search'
]

def create_producer():
    """
    Create and return a Kafka producer
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        return producer
    except Exception as e:
        logger.error(f"Error creating Kafka producer: {e}")
        raise

def generate_clickstream_data(num_records=100):
    """
    Generate mock clickstream data
    
    Args:
        num_records: Number of records to generate
        
    Returns:
        List of clickstream data records
    """
    records = []
    
    # Generate data for the past 24 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    
    for _ in range(num_records):
        # Generate a random user ID (UUID)
        user_id = str(uuid.uuid4())
        
        # Select a random page
        page_url = random.choice(SAMPLE_PAGES)
        
        # Generate a random timestamp within the past 24 hours
        random_seconds = random.randint(0, int((end_time - start_time).total_seconds()))
        timestamp = start_time + timedelta(seconds=random_seconds)
        
        # Generate a random duration between 5 and 300 seconds (5 minutes)
        # Small chance (1%) of negative duration to test data validation
        if random.random() < 0.01:  # 1% chance
            duration_sec = -random.randint(1, 10)
        else:
            duration_sec = random.randint(5, 300)
        
        # Create the record
        record = {
            'user_id': user_id,
            'page_url': page_url,
            'timestamp': timestamp.isoformat(),
            'duration_sec': duration_sec
        }
        
        records.append(record)
    
    return records

def send_to_kafka(producer, records):
    """
    Send records to Kafka topic
    
    Args:
        producer: Kafka producer
        records: List of records to send
    """
    for record in records:
        try:
            producer.send(KAFKA_TOPIC, value=record)
            logger.debug(f"Sent record: {record}")
        except Exception as e:
            logger.error(f"Error sending record to Kafka: {e}")
    
    # Flush to ensure all messages are sent
    producer.flush()

def main():
    """
    Main function to generate and send clickstream data
    """
    try:
        logger.info("Starting clickstream data generator")
        
        # Create Kafka producer
        producer = create_producer()
        
        # Generate and send a batch of records
        num_records = 1000  # Generate 1000 records
        logger.info(f"Generating {num_records} clickstream records")
        
        records = generate_clickstream_data(num_records)
        logger.info(f"Sending {len(records)} records to Kafka topic '{KAFKA_TOPIC}'")
        
        send_to_kafka(producer, records)
        
        logger.info("Clickstream data generation completed successfully")
        
    except Exception as e:
        logger.error(f"Error in clickstream data generator: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())