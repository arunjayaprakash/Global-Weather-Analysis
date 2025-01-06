from kafka import KafkaConsumer
import json
from datetime import datetime

def celsius_to_fahrenheit(celsius):
    """Convert temperature from Celsius to Fahrenheit"""
    return (celsius * 9/5) + 32

class NOAADataConsumer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.consumer = KafkaConsumer(
            'noaa-temperature-data',
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def process_messages(self):
        print("Starting consumer... Waiting for messages...")
        try:
            for message in self.consumer:
                data = message.value
                
                # Convert temperature (NOAA sends temperature in tenths of Celsius)
                temp_celsius = data['value'] / 10
                temp_fahrenheit = celsius_to_fahrenheit(temp_celsius)
                
                print("\nReceived weather data:")
                print(f"Station: {data['station_name']}")
                print(f"Location: {data['station_location']['latitude']}, {data['station_location']['longitude']}")
                print(f"Date: {data['date']}")
                print(f"Temperature: {temp_celsius:.1f}°C / {temp_fahrenheit:.1f}°F")
                print(f"Measurement Type: {data['type']}")
                print(f"Partition: {message.partition}, Offset: {message.offset}")
                print("-" * 50)
                
        except KeyboardInterrupt:
            print("\nStopping consumer...")
        finally:
            self.consumer.close()

if __name__ == "__main__":
    consumer = NOAADataConsumer()
    consumer.process_messages()