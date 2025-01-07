# Climate Analysis

Data pipeline for processing climate data from NOAA using Apache Kafka for real-time data streaming and Apache Spark for stream processing.

## Project Architecture

```
NOAA API → Kafka Producer → Kafka Topic → Spark Streaming → Console Output (Future: Delta Lake)
                                      ↳ Kafka Consumer (monitoring)
```

## Prerequisites

- Python 3.x
- Java 8 or newer
- Apache Kafka 3.9.0
- Apache Spark 3.5.0
- NOAA API key

## Installation & Setup

### 1. Kafka Setup

1. Download Apache Kafka 3.9.0 binary release
2. Extract to a convenient location (e.g., `C:\kafka` to avoid Windows path length issues)
3. Generate Kafka cluster ID:
```bash
.\bin\windows\kafka-storage.bat random-uuid
```

4. Format storage directory:
```bash
.\bin\windows\kafka-storage.bat format -t YOUR_UUID -c config\kraft\server.properties
```

5. Start Kafka server:
```bash
.\bin\windows\kafka-server-start.bat config\kraft\server.properties
```

### 2. Spark Setup (Windows)

1. Set up Hadoop environment:
   - Create directory `C:\hadoop\bin`
   - Download winutils.exe and hadoop.dll from https://github.com/cdarlint/winutils
   - Place these files in `C:\hadoop\bin`
   - Set environment variable: `HADOOP_HOME=C:\hadoop`

2. Create Spark temp directory:
   ```bash
   mkdir C:\tmp\spark-temp
   ```

### 3. Python Environment Setup

Install required Python packages:
```bash
pip install kafka-python requests pyspark findspark
```

### 4. NOAA API Key
1. Request an API key from [NOAA's website](https://www.ncdc.noaa.gov/cdo-web/webservices/v2#gettingStarted)
2. Store the key securely

## Project Components

### 1. Data Ingestion
- `noaa_producer.py`: Fetches temperature data from NOAA API and produces to Kafka
- `noaa_prod_synthetic.py`: Generates synthetic weather data for testing
- `noaa_consumer.py`: Monitors raw temperature data from Kafka

### 2. Stream Processing
- `noaa_spark_processor.py`: Spark Streaming job that processes weather data in real-time
  - Performs hourly temperature aggregations by station
  - Calculates regional temperature averages using lat/long grids
  - (Future) Writes processed data to Delta Lake

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

2. Run the data pipeline:
   ```bash
   # Terminal 1: Start the producer (real or synthetic)
   python src/noaa/noaa_prod_synthetic.py
   
   # Terminal 2: Start Spark processor (optional: start consumer for monitoring)
   python src/spark/noaa_spark_processor.py
   ```

## Current Features

- Real-time data streaming from NOAA API
- Synthetic data generation for testing
- Temperature data collection from multiple weather stations
- Real-time stream processing with Spark:
  - Hourly temperature statistics (avg, max, min)
  - Geographic temperature clustering
- Structured message format with geographical data
- Basic error handling and rate limiting

## Next Steps

1. Implement Delta Lake storage for processed data
2. Add data quality checks, error handling etc
3. Dask or Spark for some kind of analysis
5. Viz. Dashboard or ML route? TBD

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

3. **Spark Streaming vs Plain Kafka Consumer**: Chose Spark for:
   - Parallel processing capability
   - Built-in windowing and aggregation functions
   - Scalability for large data volumes
   - Integration with Delta Lake storage

## Notes

- The project uses Kafka KRaft instead of ZooKeeper for metadata management
- Current implementation focuses on temperature data (TMAX)
- Spark processing uses 5-minute windows for aggregations
- Rate limiting is implemented to respect NOAA API limits