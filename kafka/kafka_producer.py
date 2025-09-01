from kafka import KafkaProducer
import json
import os
import requests
import time
import random

def get_data():
    URL = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 50,
        "page": 1,
        "sparkline": "false"
    }
    
    try:
        response = requests.get(URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

def create_kafka_producer():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def main():
    producer = create_kafka_producer()
    topic = os.getenv('KAFKA_TOPIC', 'crypto_data')

    while True:
        data = get_data()  

        if data is None or len(data) == 0:
            print("No data received from API, retrying...")
        else:
            for coin in data:
                filtered_coin = {
                    "id": coin.get("id"),
                    "symbol": coin.get("symbol"),
                    "price": coin.get("current_price"),
                    "market_cap": coin.get("market_cap"),
                    "price_change_24h": coin.get("price_change_percentage_24h")
                }
                # Only send if filtered_coin has valid data
                if all(value is not None for value in filtered_coin.values()):
                    producer.send(topic, value=filtered_coin)
                    print(f"Sent data to topic {topic}: {filtered_coin}")
                else:
                    print("Skipping incomplete data:", filtered_coin)

        time.sleep(random.randint(5, 15))  

if __name__ == "__main__":
    main()