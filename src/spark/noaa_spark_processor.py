'''
Real-time analysis:

Hourly temperature aggregations by station (avg, max, min temperatures)
Regional analysis by grouping stations into 5-degree lat/long grids
'''
import sys
import os

# Set up Hadoop environment before any Spark imports
os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PATH'] = f"{os.environ['HADOOP_HOME']}/bin;" + os.environ['PATH']
os.environ['SPARK_LOCAL_DIRS'] = 'C:/tmp/spark-temp'
os.environ['HADOOP_HOME_DIR'] = os.environ['HADOOP_HOME']
os.environ['JAVA_HOME'] = "C:/Program Files/Java/jdk-22"

# Create temp directory if it doesn't exist
if not os.path.exists('C:/tmp/spark-temp'):
    os.makedirs('C:/tmp/spark-temp')

import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, max, min, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

class NOAASparkProcessor:
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        # Initialize Spark Session with necessary configurations
        self.spark = SparkSession.builder \
            .appName("NOAA Weather Processing") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
            .config("spark.driver.maxResultSize", "0") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("ERROR")
        
        # Define schema for NOAA data
        self.schema = StructType([
            StructField("station_id", StringType()),
            StructField("station_name", StringType()),
            StructField("station_location", StructType([
                StructField("latitude", DoubleType()),
                StructField("longitude", DoubleType()),
                StructField("elevation", DoubleType())
            ])),
            StructField("date", StringType()),
            StructField("type", StringType()),
            StructField("value", DoubleType()),
            StructField("timestamp", IntegerType())
        ])
        
        self.kafka_bootstrap_servers = kafka_bootstrap_servers

    def read_from_kafka(self):
        """Read streaming data from Kafka"""
        return self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
            .option("subscribe", "noaa-temperature-data") \
            .option("startingOffsets", "latest") \
            .load()

    def process_stream(self):
        """Main processing logic for the streaming data"""
        # Read from Kafka
        kafka_df = self.read_from_kafka()
        
        # Parse JSON data
        print("Debugging raw Kafka messages:")
        kafka_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        parsed_df = kafka_df \
            .selectExpr("CAST(value AS STRING)") \
            .select(from_json("value", self.schema).alias("data")) \
            .select("data.*")
        
        # Debug parsed data
        print("Debugging parsed messages:")
        parsed_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .start()
        
        # Convert string timestamp to actual timestamp
        weather_df = parsed_df \
            .withColumn("event_time", col("date").cast(TimestampType()))
        
        # Create different analysis streams
        
        # 1. Hourly temperature aggregations by station
        hourly_aggs = weather_df \
            .withWatermark("event_time", "1 minute") \
            .groupBy(
                window("event_time", "5 minutes"),
                "station_id",
                "station_name"
            ) \
            .agg(
                avg("value").alias("avg_temp"),
                max("value").alias("max_temp"),
                min("value").alias("min_temp"),
                count("*").alias("reading_count")
            )
        
        # 2. Regional temperature analysis (using lat/long grid)
        regional_temps = weather_df \
            .withColumn("lat_grid", (col("station_location.latitude") / 5).cast("int") * 5) \
            .withColumn("long_grid", (col("station_location.longitude") / 5).cast("int") * 5) \
            .withWatermark("event_time", "1 minute") \
            .groupBy(
                window("event_time", "5 minutes"),
                "lat_grid",
                "long_grid"
            ) \
            .agg(
                avg("value").alias("region_avg_temp"),
                count("*").alias("station_count")
            )
        
        # Write streams to console for testing
        query1 = hourly_aggs \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        query2 = regional_temps \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        # Wait for termination
        query1.awaitTermination()
        query2.awaitTermination()

if __name__ == "__main__":
    processor = NOAASparkProcessor()
    processor.process_stream()