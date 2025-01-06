# Climate Data Engineering Project

This project demonstrates a data engineering pipeline for processing climate data from NOAA (National Oceanic and Atmospheric Administration) using Apache Kafka for real-time data streaming.

## Project Architecture

```
NOAA API → Kafka Producer → Kafka Topic → Kafka Consumer → (Future: Processing & Storage)
```

## Prerequisites

- Python 3.x
- Java 8 or newer
- Apache Kafka 3.9.0
- NOAA API key

## Installation & Setup

### 1. Kafka Setup

1. Download Apache Kafka 3.9.0 binary release
2. Extract to a convenient location (e.g., `C:\kafka` to avoid Windows path length issues)
3. Generate Kafka cluster ID:
```bash
.\bin\windows\kafka-storage.bat random-uuid
```

4. Format storage directory (replace YOUR_UUID with generated UUID):
```bash
.\bin\windows\kafka-storage.bat format -t YOUR_UUID -c config\kraft\server.properties
```

5. Start Kafka server:
```bash
.\bin\windows\kafka-server-start.bat config\kraft\server.properties
```

### 2. Python Environment Setup

Install required Python packages:
```bash
pip install kafka-python requests
```

### 3. NOAA API Key
1. Request an API key from [NOAA's website](https://www.ncdc.noaa.gov/cdo-web/webservices/v2#gettingStarted)
2. Store the key securely (you'll need it for the producer)

## Project Components

### 1. Test Setup
- `test_producer.py`: Basic producer that sends test messages
- `test_consumer.py`: Basic consumer that receives and displays test messages

### 2. NOAA Data Pipeline
- `noaa_producer.py`: Fetches temperature data from NOAA API and produces to Kafka
- `noaa_consumer.py`: Consumes temperature data and displays it in a formatted way

### Topics
1. Test topic:
```bash
.\bin\windows\kafka-topics.bat --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

2. NOAA data topic:
```bash
.\bin\windows\kafka-topics.bat --create --topic noaa-temperature-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Running the Project

1. Start Kafka server (see Kafka Setup section)

2. Run the NOAA data pipeline:
   - Start consumer:
     ```bash
     python noaa_consumer.py
     ```
   - In a separate terminal, start producer:
     ```bash
     python noaa_producer.py
     ```

## Current Features

- Real-time data streaming from NOAA API
- Temperature data collection from multiple weather stations
- Last 30 days of historical data
- Temperature conversion (Celsius to Fahrenheit)
- Structured message format with geographical data
- Basic error handling and rate limiting

## Next Steps

1. Add data transformation using Apache Spark
2. Implement data storage solution
3. Add data quality checks
4. Create visualization dashboard
5. Implement proper error handling and logging
6. Add more NOAA data types (precipitation, wind, etc.)

## Notes

- The project uses Kafka KRaft instead of ZooKeeper for metadata management
- Current implementation focuses on temperature data (TMAX, TMIN)
- Rate limiting is implemented to respect NOAA API limits

## Architecture Decisions

1. **Kafka vs RabbitMQ**: Chose Kafka for:
   - Message persistence
   - Replay capability
   - High throughput
   - Better suited for data pipeline use cases

2. **KRaft vs ZooKeeper**: Chose KRaft because:
   - Simpler architecture
   - Future-proof (ZooKeeper being phased out)
   - No external dependencies
