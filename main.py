import os
import json
import time
import random
import logging
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC")
KAFKA_USERNAME = os.getenv("KAFKA_USERNAME")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
DATA_INTERVAL = float(os.getenv("DATA_INTERVAL", 0.5))
VEHICLE_ID = os.getenv("VEHICLE_ID", "F1-RED-BULL-01")

def on_send_success(record_metadata):
    """ Called on successful message delivery. """
    logger.debug(f"Message delivered to {record_metadata.topic} [{record_metadata.partition}] at offset {record_metadata.offset}")

def on_send_error(excp, payload):
    """ Called on message delivery failure. """
    logger.error(f"Message delivery failed: {excp}")
    if KAFKA_DLQ_TOPIC:
        produce_to_dlq(payload, str(excp))

def produce_to_dlq(value, error_msg):
    """ Attempts to produce failed messages to a Dead Letter Topic. """
    try:
        # value is expected to be bytes
        original_payload = json.loads(value.decode('utf-8'))
        dlq_data = {
            "original_payload": original_payload,
            "error": error_msg,
            "timestamp": datetime.now().isoformat()
        }
        producer.send(KAFKA_DLQ_TOPIC, value=json.dumps(dlq_data).encode('utf-8'))
        logger.info(f"Sent failed message to DLQ: {KAFKA_DLQ_TOPIC}")
    except Exception as e:
        logger.error(f"Failed to send to DLQ: {e}")

# Initialize Producer
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(',') if KAFKA_BOOTSTRAP_SERVERS else [],
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        retries=5,
        retry_backoff_ms=100,
        acks='all'
    )
except Exception as e:
    logger.critical(f"Failed to create producer: {e}")
    exit(1)

def generate_telemetry():
    """ Simulates F1 live telematics data. """
    return {
        "vehicle_id": VEHICLE_ID,
        "speed": round(random.uniform(100, 350), 2),
        "lat": round(random.uniform(-90, 90), 6),
        "lng": round(random.uniform(-180, 180), 6),
        "g_force": round(random.uniform(0, 5), 2),
        "make": "Oracle Red Bull Racing",
        "rpm": random.randint(8000, 15000),
        "current_gear": random.randint(1, 8),
        "brake_temp": round(random.uniform(200, 1000), 1),
        "tire_temp": round(random.uniform(80, 120), 1),
        "tire_psi": round(random.uniform(18, 25), 1),
        "timestamp": datetime.now().isoformat()
    }

def main():
    logger.info(f"Starting F1 Telematics Simulator for vehicle {VEHICLE_ID}...")
    try:
        while True:
            telemetry = generate_telemetry()
            payload = json.dumps(telemetry).encode('utf-8')
            
            # Produce to main topic
            try:
                producer.send(
                    KAFKA_TOPIC, 
                    value=payload
                ).add_callback(on_send_success).add_errback(on_send_error, payload=payload)
            except Exception as e:
                logger.error(f"Unexpected error producing message: {e}")
            
            time.sleep(DATA_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("Simulator stopped by user.")
    finally:
        logger.info("Flushing remaining messages...")
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
