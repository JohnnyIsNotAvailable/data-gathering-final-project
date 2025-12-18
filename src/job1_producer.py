import json
import time
import os
import requests
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(',')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'raw_events')
API_URL = os.environ.get('API_URL', 'https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest')
CMC_API_KEY = os.environ.get('CMC_API_KEY', '')
FETCH_INTERVAL_SECONDS = int(os.environ.get('FETCH_INTERVAL_SECONDS', '30'))


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )


def fetch_data_from_api():
    headers = {
        'X-CMC_PRO_API_KEY': CMC_API_KEY,
        'Accept': 'application/json'
    }
    try:
        response = requests.get(API_URL, headers=headers, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None


def send_to_kafka(producer, data):
    if data is None:
        return False

    try:
        producer.send(KAFKA_TOPIC, value=data)
        producer.flush()
        print(f"Data sent to Kafka topic '{KAFKA_TOPIC}'")
        return True
    except Exception as e:
        print(f"Error sending data to Kafka: {e}")
        return False


def run_producer(duration_seconds=None):
    producer = create_producer()
    start_time = time.time()

    try:
        while True:
            if duration_seconds and (time.time() - start_time) >= duration_seconds:
                break

            data = fetch_data_from_api()
            send_to_kafka(producer, data)
            time.sleep(FETCH_INTERVAL_SECONDS)
    finally:
        producer.close()


if __name__ == '__main__':
    run_producer()
