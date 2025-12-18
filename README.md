# Cryptocurrency Market Data Pipeline

A streaming + batch data pipeline that collects cryptocurrency market metrics from CoinMarketCap API, processes them through Kafka, and stores analytics in SQLite.

## Team Members

- Jakupov Dias
- Shim Daniil
- Kabiyev Zhanbek

## Jobs Description

### DAG 1 - Continuous Ingestion (`job1_ingestion`)

Fetches global cryptocurrency market metrics from CoinMarketCap API and publishes to Kafka.

| Configuration | Value |
|---------------|-------|
| Schedule | Every 5 minutes |
| Fetch Interval | 30 seconds |
| Duration | 240 seconds per run |
| Kafka Topic | `raw_events` |
| API | CoinMarketCap Global Metrics |

### DAG 2 - Hourly Cleaning + Storage (`job2_clean_store`)

Consumes raw data from Kafka, cleans it using Pandas, and stores in SQLite.

| Configuration | Value |
|---------------|-------|
| Schedule | `@hourly` |
| Input | Kafka topic `raw_events` |
| Output | SQLite `events` table |
| Consumer Group | `cleaner_group` |

**Cleaning operations:**
- Timestamp conversion to datetime
- Drop records with missing timestamp or market cap
- Remove duplicates by timestamp
- Clip negative values in numeric columns
- Forward-fill missing dominance values

### DAG 3 - Daily Analytics (`job3_daily_summary`)

Reads cleaned events from SQLite, computes daily aggregations, and stores summary.

| Configuration | Value |
|---------------|-------|
| Schedule | `@daily` |
| Input | SQLite `events` table |
| Output | SQLite `daily_summary` table |

**Computed metrics:**
- Min/Max/Avg total market cap and volume
- BTC/ETH dominance statistics (avg, min, max, std, percentiles)
- DeFi, stablecoin, derivatives averages
- High volatility event count
- Records per hour

## Setup

```bash
pip install -r requirements.txt
```

Requires Kafka and Airflow to be running. Set `CMC_API_KEY` environment variable with your CoinMarketCap API key.
