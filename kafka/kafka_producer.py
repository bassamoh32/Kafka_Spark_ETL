from kafka import KafkaProducer
import json
import os
import requests
import time
import random

def get_weather_data():
    api_key = os.getenv('WEATHER_API_KEY','your_api_key_here')
    city = os.getenv('CITY', 'London')
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def create_kafka_producer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka_local:9092')
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def main():
    producer = create_kafka_producer()
    topic = os.getenv('KAFKA_TOPIC', 'weather_data')

    while True:
        weather_data = get_weather_data()
        if weather_data:
            producer.send(topic, value=weather_data)
            print(f"Sent data to topic {topic}: {weather_data}")
        else:
            print("No data to send.")
        
        time.sleep(random.randint(5, 15))  

if __name__ == "__main__":
    main()