import json
import time
from datetime import datetime, timedelta
import requests
from kafka import KafkaProducer
import os

from dotenv import load_dotenv
load_dotenv()

NOAA_API_KEY = os.getenv("NOAA_KEY")

class NOAADataProducer:
    def __init__(self, api_key, bootstrap_servers=['localhost:9092']):
        self.base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2'
        self.headers = {'token': api_key}
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def fetch_stations(self, limit=25):
        """Fetch weather stations data"""
        url = f"{self.base_url}/stations"
        params = {
            'limit': limit,
            'datatypeid': 'TMAX',  # Maximum temperature
            'datacategoryid': 'TEMP'  # Temperature data
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 200:
            return response.json()['results']
        else:
            print(f"Error fetching stations: {response.status_code}")
            return []

    def fetch_temperature_data(self, station_id, start_date, end_date):
        """Fetch temperature data for a specific station"""
        url = f"{self.base_url}/data"
        params = {
            'datasetid': 'GHCND',  # Global Historical Climatology Network Daily
            'stationid': station_id,
            'startdate': start_date,
            'enddate': end_date,
            'limit': 1000,
            'datatypeid': ['TMAX', 'TMIN']  # Maximum and minimum temperature
        }
        
        response = requests.get(url, headers=self.headers, params=params)
        if response.status_code == 200:
            return response.json().get('results', [])
        else:
            print(f"Error fetching data: {response.status_code}")
            return []

    def produce_messages(self, topic='noaa-temperature-data'):
        """Main method to fetch and produce messages to Kafka"""
        # Get stations first
        stations = self.fetch_stations(limit=5)  # Start with 5 stations
        
        # Calculate date range for last 30 days
        end_date = datetime.now()
        start_date = end_date - timedelta(days=30)
        
        for station in stations:
            station_id = station['id']
            print(f"Fetching data for station: {station['name']}")
            
            # Fetch temperature data
            temp_data = self.fetch_temperature_data(
                station_id,
                start_date.strftime('%Y-%m-%d'),
                end_date.strftime('%Y-%m-%d')
            )
            
            # Produce messages to Kafka
            for data_point in temp_data:
                message = {
                    'station_id': station_id,
                    'station_name': station['name'],
                    'station_location': {
                        'latitude': station.get('latitude'),
                        'longitude': station.get('longitude'),
                        'elevation': station.get('elevation')
                    },
                    'date': data_point['date'],
                    'type': data_point['datatype'],
                    'value': data_point['value'],
                    'timestamp': int(time.time())
                }
                
                # Send to Kafka
                future = self.producer.send(topic, value=message)
                try:
                    record_metadata = future.get(timeout=10)
                    print(f"Message sent to partition {record_metadata.partition} at offset {record_metadata.offset}")
                except Exception as e:
                    print(f"Error sending message: {e}")
            
            # Sleep between stations to respect API rate limits
            time.sleep(1)

if __name__ == "__main__":
    # Replace 'YOUR_API_KEY' with your actual NOAA API key
    print(NOAA_API_KEY)
    producer = NOAADataProducer(api_key=NOAA_API_KEY)
    
    # Create topic first if it doesn't exist (you can also do this via command line)
    producer.produce_messages()