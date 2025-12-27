# DataProvider

**High-Performance Cryptocurrency Trading Data Infrastructure**

A production-grade data provider system that collects, stores, and serves real-time and historical trade data from Binance Futures. Designed for algorithmic trading, quantitative analysis, and financial research applications.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Architecture](#architecture)
- [System Requirements](#system-requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Core Components](#core-components)
- [API Reference](#api-reference)
- [Data Integrity](#data-integrity)
- [Performance](#performance)
- [Monitoring](#monitoring)
- [Development](#development)
- [Troubleshooting](#troubleshooting)

---

## Overview

DataProvider is a comprehensive cryptocurrency market data infrastructure that processes real-time aggregated trades from Binance Futures for 700+ trading symbols. The system maintains complete historical data with zero-tolerance for trade loss, supporting both real-time streaming and historical data access through REST and WebSocket APIs.

### Use Cases

- **Algorithmic Trading**: Sub-second access to trade data for trading bots
- **Quantitative Research**: Historical data analysis with custom timeframes
- **Technical Analysis**: Real-time OHLCV candles including Fibonacci-based intervals (173m, 2850m)
- **Market Surveillance**: Complete trade history for compliance and auditing
- **Backtesting**: Tick-level historical data for strategy validation

### Scale

| Metric | Value |
|--------|-------|
| Trading Symbols | 700+ |
| Total Trade Records | 36+ billion |
| Daily Ingestion | ~50M trades/day |
| Data Retention | Complete history since 2023 |
| Query Response | Sub-second for most operations |

---

## Key Features

### Data Collection
- **Real-time WebSocket streaming** from Binance Futures
- **Automatic gap detection and recovery** via REST API
- **Archive recovery** from Binance public data archives
- **Zero data loss** architecture with integrity verification

### Storage
- **ClickHouse** for high-performance analytical queries
- **Optimized projections** for 120x faster OHLCV aggregations
- **Efficient compression** using DoubleDelta + ZSTD codecs
- **Partitioning** by month and symbol hash for query optimization

### API
- **REST API** for historical data queries
- **WebSocket API** for real-time OHLCV streaming
- **Custom timeframes** support (1m to 30 days, any minute interval)
- **Fibonacci timeframes** for advanced technical analysis (173m, 2850m, etc.)

### Reliability
- **Automatic integrity checks** at startup and periodic intervals
- **Gap recovery** from Binance API (recent data)
- **Archive recovery** from Binance public archives (historical data)
- **Rate limiting coordination** to respect Binance API limits

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA PROVIDER SYSTEM                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │   Binance    │    │   Binance    │    │   Binance    │                   │
│  │  WebSocket   │    │   REST API   │    │   Archives   │                   │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                   │
│         │                   │                   │                            │
│         ▼                   ▼                   ▼                            │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │  WebSocket   │    │  Historical  │    │   Archive    │                   │
│  │   Updater    │    │  Downloader  │    │   Recovery   │                   │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                   │
│         │                   │                   │                            │
│         └─────────┬─────────┴─────────┬─────────┘                            │
│                   ▼                   ▼                                      │
│            ┌─────────────┐     ┌─────────────┐                               │
│            │   Ticker    │     │  Integrity  │                               │
│            │  Processor  │◄───►│   Manager   │                               │
│            └──────┬──────┘     └─────────────┘                               │
│                   │                                                          │
│                   ▼                                                          │
│            ┌─────────────────────┐                                           │
│            │  GlobalTradesUpdater │  (Centralized Batch Processing)          │
│            └──────────┬──────────┘                                           │
│                       │                                                      │
│                       ▼                                                      │
│            ┌─────────────────────┐                                           │
│            │  ClickHouseManager  │                                           │
│            └──────────┬──────────┘                                           │
│                       │                                                      │
│                       ▼                                                      │
│  ┌───────────────────────────────────────────────────────────────┐          │
│  │                        ClickHouse                              │          │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │          │
│  │  │trades_local │  │ Projections │  │  Partitions (YYYYMM) │   │          │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘   │          │
│  └───────────────────────────────────────────────────────────────┘          │
│                       │                                                      │
│         ┌─────────────┴─────────────┐                                        │
│         ▼                           ▼                                        │
│  ┌─────────────┐             ┌─────────────┐                                 │
│  │  REST API   │             │  WebSocket  │                                 │
│  │  (FastAPI)  │             │   Manager   │                                 │
│  └─────────────┘             └─────────────┘                                 │
│         │                           │                                        │
│         └─────────────┬─────────────┘                                        │
│                       ▼                                                      │
│                ┌─────────────┐                                               │
│                │   Clients   │                                               │
│                └─────────────┘                                               │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Component Overview

| Component | Responsibility |
|-----------|----------------|
| **WebSocketUpdater** | Real-time trade stream from Binance per symbol |
| **HistoricalDownloader** | Gap recovery via Binance REST API |
| **ArchiveRecovery** | Historical data from Binance public archives |
| **TickerProcessor** | Per-symbol orchestration of data collection |
| **GlobalTradesUpdater** | Centralized batch processing for all symbols |
| **IntegrityManager** | Data integrity verification and recovery coordination |
| **WeightCoordinator** | Binance API rate limit management |
| **ClickHouseManager** | Database operations and query optimization |
| **WebSocketManager** | Client WebSocket connections and OHLCV broadcasting |

---

## System Requirements

### Hardware (Production)

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU | 4 cores | 8+ cores |
| RAM | 16 GB | 32+ GB |
| Storage | 500 GB SSD | 1+ TB NVMe |
| Network | 100 Mbps | 1 Gbps |

### Software

- **Python** 3.10+
- **ClickHouse** 23.8+
- **Docker** (optional, for containerized deployment)

### Python Dependencies

```
aiochclient>=2.2.0      # Async ClickHouse client
aiohttp>=3.8.0          # Async HTTP client
binance-futures-connector>=3.3.0  # Binance API
fastapi>=0.100.0        # REST API framework
uvicorn>=0.23.0         # ASGI server
websockets>=11.0        # WebSocket support
pydantic>=2.0.0         # Data validation
clickhouse-driver>=0.2.6  # Native ClickHouse protocol
```

---

## Installation

### 1. Clone Repository

```bash
git clone https://github.com/abzatzz/binance-trades-collector.git
cd binance-trades-collector
```

### 2. Create Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# or
.venv\Scripts\activate     # Windows
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Setup ClickHouse

```bash
# Using Docker
docker run -d \
  --name clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  -v clickhouse-data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:latest
```

### 5. Create Database and Tables

```sql
CREATE DATABASE IF NOT EXISTS trades;

CREATE TABLE trades.trades_local
(
    symbol LowCardinality(String),
    aggregate_id UInt64,
    price Float64 CODEC(DoubleDelta, ZSTD(1)),
    quantity Float64 CODEC(DoubleDelta, ZSTD(1)),
    first_trade_id UInt64,
    last_trade_id UInt64,
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    is_buyer_maker UInt8,
    insert_time DateTime DEFAULT now() CODEC(ZSTD(1)),
    
    -- Projection for fast max aggregate_id lookups
    PROJECTION agg_id_projection (
        SELECT symbol, max(aggregate_id) AS max_agg_id
        GROUP BY symbol
    ),
    
    -- Projection for 1-minute OHLCV aggregation
    PROJECTION ohlcv_1m_projection (
        SELECT
            symbol,
            toStartOfMinute(timestamp) AS candle_time,
            argMin(price, timestamp) AS open,
            max(price) AS high,
            min(price) AS low,
            argMax(price, timestamp) AS close,
            sum(quantity) AS volume,
            count() AS trades_count
        GROUP BY symbol, candle_time
    )
)
ENGINE = MergeTree
PARTITION BY (toYYYYMM(timestamp), cityHash64(symbol) % 16)
ORDER BY (symbol, timestamp, aggregate_id)
SETTINGS index_granularity = 8192;
```

### 6. Configure Environment

```bash
cp .env.example .env
# Edit .env with your settings
```

### 7. Run the System

```bash
python main.py
```

---

## Configuration

### Environment Variables

Create a `.env` file in the project root:

```ini
# ===== ENVIRONMENT =====
ENVIRONMENT=production

# ===== CLICKHOUSE =====
CLICKHOUSE_HOST=localhost
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_NATIVE_PORT=9000
CLICKHOUSE_DATABASE=trades
CLICKHOUSE_USERNAME=default
CLICKHOUSE_PASSWORD=your_password

# ===== API SERVER =====
API_HOST=0.0.0.0
API_PORT=8080

# ===== FEATURES =====
ENABLE_REST_API=true
ENABLE_WEBSOCKET_API=true
ENABLE_INTEGRITY_CHECKS=true
ENABLE_DEBUG_LOGGING=false

# ===== PROCESSING =====
# Leave empty to load all symbols, or specify comma-separated list
LOAD_ONLY_TICKERS=
# Example: LOAD_ONLY_TICKERS=BTCUSDT,ETHUSDT,SOLUSDT

# ===== TELEGRAM NOTIFICATIONS =====
TELEGRAM_ENABLED=false
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# ===== SECURITY =====
# Comma-separated list of allowed IPs
ALLOWED_IPS=127.0.0.1,192.168.1.0/24

# ===== MONITORING =====
SYSTEM_STATS_INTERVAL_SECONDS=300
LOGS_DIR=logs
```

### Feature Flags

| Flag | Description | Default |
|------|-------------|---------|
| `ENABLE_REST_API` | Enable REST API endpoints | `true` |
| `ENABLE_WEBSOCKET_API` | Enable WebSocket streaming | `true` |
| `ENABLE_INTEGRITY_CHECKS` | Enable startup integrity verification | `true` |
| `ENABLE_DEBUG_LOGGING` | Verbose logging output | `false` |

---

## Database Schema

### Main Table: `trades_local`

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | LowCardinality(String) | Trading pair (e.g., BTCUSDT) |
| `aggregate_id` | UInt64 | Binance aggregate trade ID |
| `price` | Float64 | Trade price |
| `quantity` | Float64 | Trade quantity |
| `first_trade_id` | UInt64 | First trade ID in aggregate |
| `last_trade_id` | UInt64 | Last trade ID in aggregate |
| `timestamp` | DateTime64(3, 'UTC') | Trade timestamp (milliseconds) |
| `is_buyer_maker` | UInt8 | 1 if buyer is maker |
| `insert_time` | DateTime | Record insertion time |

### Projections

**agg_id_projection**: Fast lookups for last aggregate_id per symbol
```sql
SELECT symbol, max(aggregate_id) AS max_agg_id
GROUP BY symbol
```

**ohlcv_1m_projection**: Pre-aggregated 1-minute OHLCV data
```sql
SELECT
    symbol,
    toStartOfMinute(timestamp) AS candle_time,
    argMin(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, timestamp) AS close,
    sum(quantity) AS volume,
    count() AS trades_count
GROUP BY symbol, candle_time
```

### Partitioning Strategy

- **Primary**: Monthly (`toYYYYMM(timestamp)`)
- **Secondary**: Symbol hash (`cityHash64(symbol) % 16`)

This enables efficient pruning for time-range and symbol-specific queries.

---

## Core Components

### TickerProcessor

Orchestrates data collection for a single trading symbol:

```python
# Lifecycle phases
1. Integrity Check    → Verify existing data
2. Archive Recovery   → Download missing historical data
3. Historical Load    → Fill gaps via REST API
4. WebSocket Stream   → Real-time data collection
5. Gap Recovery       → Handle disconnections
```

### GlobalTradesUpdater

Centralized batch processing for all symbols:

```python
# Features
- Thread-safe trade buffering (add_trade)
- Periodic batch INSERT to ClickHouse
- Automatic buffer overflow handling
- Statistics and monitoring
```

### IntegrityChecker

Hierarchical data integrity verification:

```python
# Algorithm
1. Monthly analysis    → Find first problematic month
2. Daily analysis      → Find trust point within month
3. Gap detection       → Identify missing aggregate_ids
4. Duplicate detection → Find overlapping data
```

### WeightCoordinator

Binance API rate limit management:

```python
# Features
- Weight tracking per endpoint
- Automatic waiting when limits approached
- Shared coordination across components
- Real-time weight monitoring
```

---

## API Reference

### REST API

Base URL: `http://localhost:8080/api/v1`

#### Get Trades

```http
GET /data/trades/{symbol}?start_time=1640995200000&limit=1000
```

Response:
```json
{
  "success": true,
  "data": [
    {
      "aggregate_id": 26129,
      "price": 50000.50,
      "quantity": 0.01,
      "timestamp": 1640995200153,
      "is_buyer_maker": true
    }
  ],
  "message": "Retrieved 1000 trades"
}
```

#### Get OHLCV Candles

```http
GET /data/klines/{symbol}?timeframe=15&limit=100
```

#### Health Check

```http
GET /monitoring/health
```

### WebSocket API

Endpoint: `ws://localhost:8080/ws/stream`

#### Subscribe to OHLCV

```json
{
  "action": "subscribe",
  "subscriptions": [
    {
      "symbol": "BTCUSDT",
      "timeframes": [
        {"timeframe": 15, "initial_candles": 100},
        {"timeframe": 173, "initial_candles": 20}
      ]
    }
  ]
}
```

#### Real-time Update

```json
{
  "type": "ohlcv_update",
  "symbol": "BTCUSDT",
  "timeframe": "15m",
  "data": {
    "candle_time": 1735691100000,
    "open": 43255.30,
    "high": 43268.90,
    "low": 43251.20,
    "close": 43262.40,
    "volume": 12.45332,
    "trades_count": 142
  },
  "data_status": {
    "historical_loading": false,
    "gap_recovery": false,
    "data_quality": "complete"
  }
}
```

See [API_README.md](API_README.md) for complete API documentation.

---

## Data Integrity

### Trust Point Algorithm

The system uses a hierarchical approach to find the "trust point" - the last known good data position:

```
1. Monthly Scan
   ├── For each month: check gaps_count
   ├── gaps_count > 0: missing trades
   ├── gaps_count < 0: duplicate trades
   └── Find first problematic month

2. Daily Scan (within problem month)
   ├── Check day-to-day continuity
   ├── Verify aggregate_id sequences
   └── Find exact trust point

3. Recovery Decision
   ├── Recent data (< 7 days): Use REST API
   └── Historical data: Use Binance archives
```

### Integrity Check Query

```sql
SELECT 
    toYYYYMM(timestamp) as month,
    count() as total_count,
    min(aggregate_id) as min_id,
    max(aggregate_id) as max_id,
    max(aggregate_id) - min(aggregate_id) + 1 - count() as gaps_count
FROM trades_local
WHERE symbol = 'BTCUSDT'
GROUP BY month
ORDER BY month
```

### Recovery Methods

| Method | Data Age | Source | Speed |
|--------|----------|--------|-------|
| REST API | < 7 days | Binance API | ~10K trades/sec |
| Archive | Any | Binance Public | ~40K trades/sec |

---

## Performance

### Benchmarks

| Operation | Performance |
|-----------|-------------|
| INSERT (batch 50K) | 125,000 rows/sec |
| OHLCV query (1 month) | 150ms |
| Real-time latency | < 100ms |
| Archive download | 2.5M trades/min |

### Optimization Tips

1. **Use projections** for OHLCV queries:
```sql
-- Fast: uses ohlcv_1m_projection
SELECT * FROM trades_local
WHERE symbol = 'BTCUSDT'
  AND toStartOfMinute(timestamp) >= '2024-01-01'
```

2. **Partition pruning** for time ranges:
```sql
-- Efficient: scans only relevant partitions
SELECT * FROM trades_local
WHERE symbol = 'BTCUSDT'
  AND timestamp >= '2024-01-01'
  AND timestamp < '2024-02-01'
```

3. **Batch writes** for historical data:
```python
# Good: single batch insert
await clickhouse.write_trades(symbol, trades_batch)

# Bad: individual inserts
for trade in trades:
    await clickhouse.write_trade(symbol, trade)
```

---

## Monitoring

### Health Endpoint

```http
GET /monitoring/health
```

```json
{
  "status": "healthy",
  "uptime_seconds": 86400,
  "active_tickers": 652,
  "trades_processed_total": 1234567890,
  "clickhouse_status": "connected",
  "websocket_connections": 5,
  "disk_free_percent": 45.2
}
```

### System Statistics

```http
GET /monitoring/system-stats
```

### Ticker Status

```http
GET /admin/tickers/status
```

### Telegram Notifications

Configure alerts for:
- System startup/shutdown
- Archive recovery progress
- Critical errors
- Disk space warnings

---

## Development

### Project Structure

```
dataprovider/
├── main.py                 # Entry point
├── .env                    # Configuration
├── requirements.txt        # Dependencies
├── src/
│   ├── core/
│   │   ├── shared.py                 # Central resource manager
│   │   ├── clickhouse_manager.py     # Database operations
│   │   ├── global_trades_updater.py  # Batch processing
│   │   ├── ticker_processor.py       # Per-symbol orchestration
│   │   ├── ticker_synchronizer.py    # Ticker lifecycle
│   │   ├── historical_downloader.py  # REST API downloads
│   │   ├── websocket_updater.py      # Real-time streams
│   │   ├── weight_coordinator.py     # Rate limiting
│   │   └── integrity/
│   │       ├── integrity_checker.py  # Data verification
│   │       ├── integrity_manager.py  # Recovery coordination
│   │       └── archive_recovery.py   # Archive downloads
│   ├── api/
│   │   ├── rest_api.py              # REST endpoints
│   │   └── websocket_api/
│   │       ├── websocket_manager.py # Client connections
│   │       └── websocket_models.py  # Message schemas
│   └── models/
│       ├── config.py                # Configuration model
│       └── aggtrade.py              # Trade data model
└── tests/
    ├── unit/
    ├── integration/
    └── fixtures/
```

### Running Tests

```bash
# Unit tests
pytest tests/unit -v

# Integration tests (requires ClickHouse)
pytest tests/integration -v

# All tests with coverage
pytest --cov=src --cov-report=html
```

### Code Style

```bash
# Format code
black src/ tests/

# Check types
mypy src/

# Lint
flake8 src/ tests/
```

---

## Troubleshooting

### Common Issues

#### ClickHouse Connection Failed

```
Error: Connection refused to localhost:8123
```

**Solution**: Ensure ClickHouse is running:
```bash
docker ps | grep clickhouse
docker start clickhouse
```

#### High Memory Usage

**Cause**: Large buffers in GlobalTradesUpdater

**Solution**: Adjust batch size and flush interval:
```python
# In configuration
BATCH_SIZE = 10000  # Reduce if memory constrained
FLUSH_INTERVAL = 0.5  # More frequent flushes
```

#### Slow INSERT Performance

**Cause**: Too many Materialized Views

**Solution**: Check and remove unused MVs:
```sql
SELECT database, name FROM system.tables 
WHERE engine = 'MaterializedView';
```

#### Data Gaps After Restart

**Cause**: Integrity check not enabled

**Solution**: Enable integrity checks:
```ini
ENABLE_INTEGRITY_CHECKS=true
```

### Useful Queries

```sql
-- Check for duplicates
SELECT symbol, count() - uniq(aggregate_id) as dups
FROM trades_local
GROUP BY symbol
HAVING dups > 0;

-- Check partition sizes
SELECT partition, formatReadableSize(sum(bytes_on_disk))
FROM system.parts
WHERE table = 'trades_local' AND active
GROUP BY partition
ORDER BY partition;

-- Check active mutations
SELECT * FROM system.mutations WHERE is_done = 0;
```


---

## Support

For technical support or questions:
- Check system health: `GET /monitoring/health`
- Review logs: `logs/` directory
- Contact development team for assistance

## Contact

**Vadim Dolghin** — Python Backend Developer

- GitHub: [@abzatzz](https://github.com/abzatzz)
- LinkedIn: [vadim-dolghin](https://linkedin.com/in/vadim-dolghin)
- Telegram: [@Abzatzz](https://t.me/Abzatzz)
- Email: vadim.dolghin.dev@gmail.com

Open for freelance projects and contract work.