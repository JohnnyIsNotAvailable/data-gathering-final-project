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

def parse_global_metrics(raw_response):
    if not raw_response or 'data' not in raw_response:
        return None

    data = raw_response['data']
    quote = data.get('quote', {})
    quote_data = quote.get('USD', {}) if 'USD' in quote else (list(quote.values())[0] if quote else {})

    return {
        'timestamp': data.get('last_updated'),
        'btc_dominance': data.get('btc_dominance'),
        'eth_dominance': data.get('eth_dominance'),
        'active_cryptocurrencies': data.get('active_cryptocurrencies'),
        'active_market_pairs': data.get('active_market_pairs'),
        'active_exchanges': data.get('active_exchanges'),
        'total_market_cap': quote_data.get('total_market_cap'),
        'total_volume_24h': quote_data.get('total_volume_24h'),
        'altcoin_market_cap': quote_data.get('altcoin_market_cap'),
        'altcoin_volume_24h': quote_data.get('altcoin_volume_24h'),
        'defi_market_cap': quote_data.get('defi_market_cap'),
        'defi_volume_24h': quote_data.get('defi_volume_24h'),
        'defi_24h_percentage_change': quote_data.get('defi_24h_percentage_change'),
        'stablecoin_market_cap': quote_data.get('stablecoin_market_cap'),
        'stablecoin_volume_24h': quote_data.get('stablecoin_volume_24h'),
        'stablecoin_24h_percentage_change': quote_data.get('stablecoin_24h_percentage_change'),
        'derivatives_volume_24h': quote_data.get('derivatives_volume_24h'),
        'derivatives_24h_percentage_change': quote_data.get('derivatives_24h_percentage_change'),
    }



def save_to_database(df):
    if df.empty:
        print("No data to save")
        return

    conn = get_db_connection()
    try:
        df.to_sql('events', conn, if_exists='append', index=False)
        print(f"Saved {len(df)} records to database")
    finally:
        conn.close()

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
