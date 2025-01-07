import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
import math

class TestNOAAProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        # Sample stations
        self.stations = [
            {
                'id': 'STATION1',
                'name': 'New York Central Park',
                'latitude': 40.7829,
                'longitude': -73.9654
            },
            {
                'id': 'STATION2',
                'name': 'Los Angeles Downtown',
                'latitude': 34.0522,
                'longitude': -118.2437
            },
            {
                'id': 'STATION3',
                'name': 'Chicago O\'Hare',
                'latitude': 41.9786,
                'longitude': -87.9048
            }
        ]

    def generate_temperature(self, base_temp, hour):
        """Generate realistic temperature with daily variation"""
        # Add daily cycle variation (coldest at 4am, warmest at 4pm)
        hour_variation = math.sin((hour - 4) * math.pi / 12) * 5
        # Add some random noise
        noise = random.uniform(-1, 1)
        return base_temp + hour_variation + noise

    def produce_messages(self, topic='noaa-temperature-data'):
        """Produce synthetic weather data continuously"""
        try:
            while True:
                current_time = datetime.now()
                
                for station in self.stations:
                    # Base temperature varies by latitude (rough approximation)
                    base_temp = 25 - abs(station['latitude']) / 3
                    
                    # Generate temperature data
                    temp = self.generate_temperature(base_temp, current_time.hour)
                    
                    message = {
                        'station_id': station['id'],
                        'station_name': station['name'],
                        'station_location': {
                            'latitude': station['latitude'],
                            'longitude': station['longitude'],
                            'elevation': 0
                        },
                        'date': current_time.isoformat(),
                        'type': 'TMAX',
                        'value': temp * 10,  # Convert to tenths of Celsius as per NOAA format
                        'timestamp': int(time.time())
                    }
                    
                    # Send to Kafka
                    future = self.producer.send(topic, value=message)
                    record_metadata = future.get(timeout=10)
                    print(f"Sent data for {station['name']}: {temp:.1f}°C")
                
                # Wait for a minute before sending next batch
                time.sleep(60)
                
        except KeyboardInterrupt:
            print("\nStopping producer...")
        finally:
            self.producer.close()

if __name__ == "__main__":
    producer = TestNOAAProducer()
    producer.produce_messages()