from kafka import KafkaConsumer
import json

# Create consumer
consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Starting consumer... Waiting for messages...")

# Consume messages
try:
    for message in consumer:
        print(f"Received message: {message.value}")
        print(f"Partition: {message.partition}, Offset: {message.offset}\n")
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()