import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from db_utils import get_db_connection, create_tables


def load_events_for_date(target_date=None):
    if target_date is None:
        target_date = datetime.now().date() - timedelta(days=1)

    conn = get_db_connection()
    try:
        query = """
            SELECT * FROM events
            WHERE date(timestamp) = ?
        """
        df = pd.read_sql_query(query, conn, params=(str(target_date),))
        return df
    finally:
        conn.close()


def compute_analytics(df, target_date=None):
    if df.empty:
        return None

    if target_date is None:
        target_date = datetime.now().date() - timedelta(days=1)

    df_sorted = df.sort_values('timestamp')

    btc_dominance_change = None
    if 'btc_dominance' in df_sorted.columns and len(df_sorted) >= 2:
        first_btc = df_sorted['btc_dominance'].iloc[0]
        last_btc = df_sorted['btc_dominance'].iloc[-1]
        if pd.notna(first_btc) and pd.notna(last_btc):
            btc_dominance_change = last_btc - first_btc

    analytics = {
        'date': str(target_date),
        'total_records': len(df),
        'avg_total_market_cap': df['total_market_cap'].mean(),
        'min_total_market_cap': df['total_market_cap'].min(),
        'max_total_market_cap': df['total_market_cap'].max(),
        'avg_total_volume_24h': df['total_volume_24h'].mean(),
        'avg_btc_dominance': df['btc_dominance'].mean(),
        'min_btc_dominance': df['btc_dominance'].min(),
        'max_btc_dominance': df['btc_dominance'].max(),
        'avg_eth_dominance': df['eth_dominance'].mean(),
        'btc_dominance_change': btc_dominance_change,
        'avg_defi_market_cap': df['defi_market_cap'].mean(),
        'avg_defi_volume_24h': df['defi_volume_24h'].mean(),
        'avg_stablecoin_market_cap': df['stablecoin_market_cap'].mean(),
        'avg_stablecoin_volume_24h': df['stablecoin_volume_24h'].mean(),
        'avg_derivatives_volume_24h': df['derivatives_volume_24h'].mean(),
        'avg_defi_24h_change': df['defi_24h_percentage_change'].mean(),
        'avg_stablecoin_24h_change': df['stablecoin_24h_percentage_change'].mean(),
        'avg_derivatives_24h_change': df['derivatives_24h_percentage_change'].mean(),
        'avg_active_cryptocurrencies': df['active_cryptocurrencies'].mean(),
        'avg_active_exchanges': df['active_exchanges'].mean(),
        'std_total_market_cap': df['total_market_cap'].std(),
        'std_btc_dominance': df['btc_dominance'].std(),
        'p25_btc_dominance': df['btc_dominance'].quantile(0.25),
        'p75_btc_dominance': df['btc_dominance'].quantile(0.75),
        'median_total_market_cap': df['total_market_cap'].median(),
        'high_volatility_count': int((df['defi_24h_percentage_change'].abs() > 5).sum()),
        'records_per_hour': len(df) / 24 if len(df) > 0 else 0,
    }

    return analytics


def save_summary(analytics):
    if analytics is None:
        print("No analytics to save")
        return

    conn = get_db_connection()
    try:
        summary_df = pd.DataFrame([analytics])
        summary_df.to_sql('daily_summary', conn, if_exists='append', index=False)
        print(f"Saved daily summary for {analytics['date']}")
    finally:
        conn.close()


def generate_charts(df, target_date):
    if df.empty:
        return

    charts_dir = os.path.join(os.path.dirname(os.environ.get('DB_PATH', 'data/app.db')), 'charts')
    os.makedirs(charts_dir, exist_ok=True)

    df_sorted = df.sort_values('timestamp')

    plt.figure(figsize=(10, 5))
    plt.plot(df_sorted['timestamp'], df_sorted['total_market_cap'] / 1e12)
    plt.title(f'Total Market Cap - {target_date}')
    plt.xlabel('Time')
    plt.ylabel('Market Cap (Trillions USD)')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(charts_dir, f'market_cap_{target_date}.png'))
    plt.close()

    plt.figure(figsize=(10, 5))
    plt.plot(df_sorted['timestamp'], df_sorted['btc_dominance'], label='BTC')
    plt.plot(df_sorted['timestamp'], df_sorted['eth_dominance'], label='ETH')
    plt.title(f'BTC vs ETH Dominance - {target_date}')
    plt.xlabel('Time')
    plt.ylabel('Dominance (%)')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(charts_dir, f'dominance_{target_date}.png'))
    plt.close()

    segments = {
        'DeFi': df['defi_market_cap'].mean(),
        'Stablecoins': df['stablecoin_market_cap'].mean(),
        'Altcoins': df['altcoin_market_cap'].mean()
    }
    plt.figure(figsize=(8, 8))
    plt.pie(segments.values(), labels=segments.keys(), autopct='%1.1f%%')
    plt.title(f'Market Segments - {target_date}')
    plt.savefig(os.path.join(charts_dir, f'segments_{target_date}.png'))
    plt.close()

    print(f"Charts saved to {charts_dir}")


def run_analytics(target_date=None):
    create_tables()

    if target_date is None:
        target_date = datetime.now().date() - timedelta(days=1)

    events_df = load_events_for_date(target_date)
    print(f"Loaded {len(events_df)} events for {target_date}")

    analytics = compute_analytics(events_df, target_date)
    save_summary(analytics)
    generate_charts(events_df, target_date)


if __name__ == '__main__':
    run_analytics()