import sqlite3
import os

DB_PATH = os.environ.get('DB_PATH', os.path.join(os.path.dirname(os.path.dirname(__file__)), 'data', 'app.db'))


def get_db_connection():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return sqlite3.connect(DB_PATH)


def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME,
            btc_dominance REAL,
            eth_dominance REAL,
            active_cryptocurrencies INTEGER,
            active_market_pairs INTEGER,
            active_exchanges INTEGER,
            total_market_cap REAL,
            total_volume_24h REAL,
            altcoin_market_cap REAL,
            altcoin_volume_24h REAL,
            defi_market_cap REAL,
            defi_volume_24h REAL,
            defi_24h_percentage_change REAL,
            stablecoin_market_cap REAL,
            stablecoin_volume_24h REAL,
            stablecoin_24h_percentage_change REAL,
            derivatives_volume_24h REAL,
            derivatives_24h_percentage_change REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE UNIQUE,
            total_records INTEGER,
            avg_total_market_cap REAL,
            min_total_market_cap REAL,
            max_total_market_cap REAL,
            avg_total_volume_24h REAL,
            avg_btc_dominance REAL,
            min_btc_dominance REAL,
            max_btc_dominance REAL,
            avg_eth_dominance REAL,
            btc_dominance_change REAL,
            avg_defi_market_cap REAL,
            avg_defi_volume_24h REAL,
            avg_stablecoin_market_cap REAL,
            avg_stablecoin_volume_24h REAL,
            avg_derivatives_volume_24h REAL,
            avg_defi_24h_change REAL,
            avg_stablecoin_24h_change REAL,
            avg_derivatives_24h_change REAL,
            avg_active_cryptocurrencies REAL,
            avg_active_exchanges REAL,
            std_total_market_cap REAL,
            std_btc_dominance REAL,
            p25_btc_dominance REAL,
            p75_btc_dominance REAL,
            median_total_market_cap REAL,
            high_volatility_count INTEGER,
            records_per_hour REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    conn.commit()
    conn.close()


def get_table_info(table_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    info = cursor.fetchall()
    conn.close()
    return info


if __name__ == '__main__':
    create_tables()
    print("Database tables created successfully")
    print("\nEvents table schema:")
    for col in get_table_info('events'):
        print(f"  {col}")
    print("\nDaily summary table schema:")
    for col in get_table_info('daily_summary'):
        print(f"  {col}")
