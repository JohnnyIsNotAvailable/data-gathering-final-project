import json
import os
import pandas as pd
from kafka import KafkaConsumer
from db_utils import get_db_connection, create_tables


KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092').split(',')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'raw_events')
CONSUMER_GROUP = os.environ.get('CONSUMER_GROUP', 'cleaner_group')


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )


def run_cleaner():
    create_tables()
    consumer = create_consumer()
    raw_data_list = []

    try:
        for message in consumer:
            raw_data_list.append(message.value)
            print(f"Consumed message from partition {message.partition}")
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        consumer.close()

    if raw_data_list:
        cleaned_df = clean_data(raw_data_list)
        save_to_database(cleaned_df)
    else:
        print("No new messages to process")


if __name__ == '__main__':
    run_cleaner()
