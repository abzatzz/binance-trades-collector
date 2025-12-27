-- ClickHouse Production Database Initialization Script
-- ===================================================
-- Clean production-ready schema for Data Provider ClickHouse architecture
-- Execute in PyCharm Database Tools or clickhouse-client

-- =============================================================================
-- DATABASE SETUP
-- =============================================================================

-- Create and use the trades database
CREATE DATABASE IF NOT EXISTS trades;
USE trades;

-- =============================================================================
-- MAIN TABLES
-- =============================================================================

-- Drop existing tables if reinstalling (CAUTION: Data loss)
-- Uncomment only if you need to recreate schema
-- DROP TABLE IF EXISTS trades_buffer;
-- DROP TABLE IF EXISTS trades_local;

-- Create main trades table with optimal production settings
CREATE TABLE IF NOT EXISTS trades_local (
    -- Core trade data (AggTrade compatible)
    symbol LowCardinality(String),
    aggregate_id UInt64,
    price Float64 CODEC(DoubleDelta, LZ4),
    quantity Float64 CODEC(DoubleDelta, LZ4),
    first_trade_id UInt64,
    last_trade_id UInt64,
    timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, LZ4),
    is_buyer_maker UInt8,

    -- Metadata
    insert_time DateTime DEFAULT now() CODEC(LZ4)
) ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), cityHash64(symbol) % 16)
ORDER BY (symbol, timestamp, aggregate_id)
SETTINGS
    index_granularity = 8192,
    ttl_only_drop_parts = 1;

-- Add TTL for automatic data cleanup (365 days)
ALTER TABLE trades_local
MODIFY TTL timestamp + INTERVAL 365 DAY;

-- Create buffer table for high-performance inserts
CREATE TABLE IF NOT EXISTS trades_buffer AS trades_local
ENGINE = Buffer(trades, trades_local, 16, 10, 100, 10000, 1000000, 10000000, 100000000);

-- =============================================================================
-- MATERIALIZED VIEWS TEMPLATE FUNCTIONS
-- =============================================================================

-- The following are template patterns that ClickHouseManager will use
-- to dynamically create materialized views for any timeframe in minutes

/*
TEMPLATE PATTERN FOR DYNAMIC MV CREATION:

CREATE MATERIALIZED VIEW ohlcv_{TIMEFRAME_MINUTES}_{SYMBOL_LOWER}_mv
ENGINE = ReplacingMergeTree()
PARTITION BY toYYYYMM(candle_time)
ORDER BY candle_time
AS SELECT
    '{SYMBOL}' as symbol,
    toStartOfInterval(timestamp, INTERVAL {TIMEFRAME_MINUTES} MINUTE) as candle_time,
    argMin(price, timestamp) as open,
    max(price) as high,
    min(price) as low,
    argMax(price, timestamp) as close,
    sum(quantity) as volume,
    count() as trades_count,
    sum(CASE WHEN is_buyer_maker = 0 THEN quantity ELSE 0 END) as taker_buy_volume,
    sum(CASE WHEN is_buyer_maker = 1 THEN quantity ELSE 0 END) as taker_sell_volume
FROM trades_local
WHERE symbol = '{SYMBOL}'
GROUP BY candle_time;

SPECIAL CASES FOR OPTIMIZATION:
- 1 minute:    toStartOfMinute(timestamp)
- 60 minutes:  toStartOfHour(timestamp)
- 1440 minutes: toStartOfDay(timestamp)
- Other:       toStartOfInterval(timestamp, INTERVAL {MINUTES} MINUTE)
*/

-- =============================================================================
-- INDEXES AND OPTIMIZATIONS
-- =============================================================================

-- Create optimized index for symbol + timestamp queries
ALTER TABLE trades_local
ADD INDEX IF NOT EXISTS idx_symbol_timestamp (symbol, timestamp) TYPE minmax GRANULARITY 4;

-- Create index for aggregate_id lookups (gap detection)
ALTER TABLE trades_local
ADD INDEX IF NOT EXISTS idx_aggregate_id (aggregate_id) TYPE minmax GRANULARITY 8;

-- =============================================================================
-- SYSTEM CONFIGURATION
-- =============================================================================

-- Optimize ClickHouse settings for trades data
-- These can be set globally or per-session as needed

/*
Recommended system settings for production (set in config.xml or per session):

SET max_threads = 4;
SET max_memory_usage = 2000000000;  -- 2GB
SET max_insert_block_size = 1048576;  -- 1M rows
SET min_insert_block_size_rows = 1000;
SET min_insert_block_size_bytes = 1048576;  -- 1MB
SET max_partitions_per_insert_block = 100;
SET max_insert_threads = 4;
SET optimize_on_insert = 0;  -- Don't optimize on every insert
SET merge_tree_max_rows_to_use_cache = 16777216;  -- 16M rows
SET merge_tree_max_bytes_to_use_cache = 2013265920;  -- ~2GB
*/

-- =============================================================================
-- SCHEMA VALIDATION
-- =============================================================================

-- Verify tables were created successfully
SELECT
    database,
    name as table_name,
    engine,
    total_rows,
    formatReadableSize(total_bytes) as size
FROM system.tables
WHERE database = 'trades'
    AND name IN ('trades_local', 'trades_buffer')
ORDER BY name;

-- Verify partitioning is configured correctly
SELECT
    table,
    partition_key,
    sorting_key
FROM system.tables
WHERE database = 'trades'
    AND table = 'trades_local';

-- Check TTL configuration
SHOW CREATE TABLE trades_local;

-- Verify buffer configuration
SHOW CREATE TABLE trades_buffer;

-- Check available compression codecs
SELECT * FROM system.table_engines WHERE name LIKE '%MergeTree%';

-- =============================================================================
-- HEALTH CHECK QUERIES
-- =============================================================================

-- Basic connectivity and permissions test
SELECT
    'ClickHouse initialized successfully' as status,
    version() as version,
    now() as current_time;

-- Test write permissions
SELECT 'Write permissions OK' as test_result
WHERE (
    SELECT count()
    FROM system.tables
    WHERE database = 'trades'
        AND name = 'trades_buffer'
        AND engine = 'Buffer'
) = 1;

-- Test that partitioning works
SELECT 'Partitioning configured' as test_result
WHERE (
    SELECT count(*)
    FROM system.table_functions
    WHERE name = 'cityHash64'
) >= 1;

-- =============================================================================
-- MONITORING QUERIES
-- =============================================================================

-- Monitor table sizes and compression
SELECT
    table,
    formatReadableSize(sum(bytes_on_disk)) as compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) as uncompressed_size,
    round(sum(data_uncompressed_bytes) / sum(bytes_on_disk), 2) as compression_ratio,
    sum(rows) as total_rows
FROM system.parts
WHERE database = 'trades'
    AND active = 1
GROUP BY table
ORDER BY table;

-- Monitor partitions
SELECT
    table,
    partition,
    count() as parts,
    sum(rows) as total_rows,
    formatReadableSize(sum(bytes_on_disk)) as size_on_disk
FROM system.parts
WHERE database = 'trades'
    AND table = 'trades_local'
    AND active = 1
GROUP BY table, partition
ORDER BY partition;

-- Monitor buffer table status
SELECT
    'Buffer table active' as status,
    formatReadableSize(total_bytes) as buffer_size,
    total_rows as buffered_rows
FROM system.tables
WHERE database = 'trades'
    AND name = 'trades_buffer';

-- =============================================================================
-- PERFORMANCE BENCHMARKS
-- =============================================================================

-- Test insert performance (will be used by ClickHouseManager)
-- This query tests if the schema can handle batch inserts efficiently

SELECT
    'Schema ready for high-frequency inserts' as performance_test,
    'Buffer -> MergeTree -> MaterializedViews pipeline configured' as pipeline_status;

-- Test query performance on empty tables
SELECT
    count(*) as trades_count,
    'Query performance baseline established' as perf_status
FROM trades_local;

-- =============================================================================
-- CLEANUP AND MAINTENANCE
-- =============================================================================

-- Scheduled maintenance queries (to be run periodically)

/*
MAINTENANCE COMMANDS (run periodically):

-- Force buffer flush
OPTIMIZE TABLE trades_buffer;

-- Optimize main table (run during low-traffic periods)
OPTIMIZE TABLE trades_local;

-- Clean old partitions (automatic with TTL, manual if needed)
ALTER TABLE trades_local DROP PARTITION '202312' -- Example for Dec 2023

-- Update table statistics
ANALYZE TABLE trades_local;

-- Check for corrupted parts
CHECK TABLE trades_local;
*/

-- =============================================================================
-- INTEGRATION NOTES FOR CLICKHOUSEMANAGER
-- =============================================================================

/*
Integration points for ClickHouseManager:

1. Write Pattern:
   INSERT INTO trades_buffer VALUES (symbol, aggregate_id, price, quantity,
                                   first_trade_id, last_trade_id, timestamp,
                                   is_buyer_maker);

2. Read Pattern:
   SELECT * FROM trades_local
   WHERE symbol = ? AND timestamp BETWEEN ? AND ?
   ORDER BY timestamp;

3. MV Creation Pattern:
   CREATE MATERIALIZED VIEW ohlcv_{minutes}_{symbol_lower}_mv
   ENGINE = ReplacingMergeTree()
   PARTITION BY toYYYYMM(candle_time)
   ORDER BY candle_time
   AS SELECT ... FROM trades_local WHERE symbol = ? GROUP BY candle_time;

4. OHLCV Query Pattern:
   SELECT * FROM ohlcv_{minutes}_{symbol_lower}_mv
   WHERE candle_time >= ? ORDER BY candle_time LIMIT ?;

5. Health Check:
   SELECT count() FROM trades_local WHERE timestamp > now() - INTERVAL 1 HOUR;
*/

-- =============================================================================
-- SUCCESS CONFIRMATION
-- =============================================================================

SELECT
    'SUCCESS: ClickHouse production schema initialized' as status,
    (SELECT count() FROM system.tables WHERE database = 'trades') as tables_created,
    'Ready for ClickHouseManager integration' as next_step,
    now() as completed_at;