from kafka import KafkaProducer
import json
import time

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Create producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=json_serializer
)

# Send some test messages
for i in range(5):
    data = {
        'id': i,
        'timestamp': time.time(),
        'message': f'Test message {i}'
    }
    
    # Send message
    future = producer.send('test-topic', value=data)
    
    # Wait for message to be delivered
    try:
        record_metadata = future.get(timeout=10)
        print(f"Message {i} sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
    except Exception as e:
        print(f"Error sending message: {e}")

# Flush and close
producer.flush()
producer.close()