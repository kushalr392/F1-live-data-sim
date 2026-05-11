# F1 Telematics Simulator

This Python script simulates live F1 telematics data and produces it to a Kafka topic using the `kafka-python` library.

## Features
- Continuous data simulation (speed, RPM, G-force, tire temp, etc.)
- Kafka production with SASL_PLAINTEXT and SCRAM-SHA-512.
- Exponential backoff and retries.
- Dead Letter Topic (DLQ) support for failed production attempts.
- Configurable data generation interval.

## Configuration
Create a `.env` file based on `.env.example`:
```env
KAFKA_BOOTSTRAP_SERVERS=your_kafka_server:9092
KAFKA_TOPIC=f1-telemetry
KAFKA_DLQ_TOPIC=f1-telemetry-dlq
KAFKA_USERNAME=your_username
KAFKA_PASSWORD=your_password
DATA_INTERVAL=0.5
VEHICLE_ID=F1-RED-BULL-01
```

## How to Run

### Locally
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set up your `.env` file.
3. Run the script:
   ```bash
   python main.py
   ```

### Using Docker
1. Build the image:
   ```bash
   docker build -t f1-telemetry-simulator .
   ```
2. Run the container:
   ```bash
   docker run --env-file .env f1-telemetry-simulator
   ```

## Environment Metadata
The `env_config.json` file contains metadata about the required environment variables for integration with Vapr platform tools.
