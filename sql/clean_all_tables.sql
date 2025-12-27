-- ClickHouse Database Cleanup Script
-- ==================================
-- CAUTION: This script will permanently delete all data and tables
-- Use only for development/testing or when you need to start fresh

-- =============================================================================
-- SAFETY WARNING
-- =============================================================================

SELECT 'WARNING: This script will delete ALL trades data' as warning_message;
SELECT 'Continue only if you want to completely reset the database' as confirmation;

-- Uncomment the following line to proceed (safety mechanism)
-- SELECT 'PROCEEDING WITH CLEANUP' as status;

-- =============================================================================
-- DROP ALL MATERIALIZED VIEWS
-- =============================================================================

-- Drop all OHLCV materialized views (pattern: ohlcv_*_*_mv)
-- This query will show all materialized views to be dropped
SELECT
    'DROP VIEW IF EXISTS ' || name || ';' as cleanup_commands
FROM system.tables
WHERE database = 'trades'
    AND name LIKE 'ohlcv_%_mv'
    AND engine = 'MaterializedView';

-- Execute the drops (run the commands from above query results)
-- Example drops (replace with actual MV names from your system):

-- BTCUSDT materialized views
DROP VIEW IF EXISTS ohlcv_1_btcusdt_mv;
DROP VIEW IF EXISTS ohlcv_5_btcusdt_mv;
DROP VIEW IF EXISTS ohlcv_15_btcusdt_mv;
DROP VIEW IF EXISTS ohlcv_60_btcusdt_mv;
DROP VIEW IF EXISTS ohlcv_240_btcusdt_mv;
DROP VIEW IF EXISTS ohlcv_1440_btcusdt_mv;
DROP VIEW IF EXISTS ohlcv_183_btcusdt_mv;

-- Other ticker materialized views (add as needed)
-- DROP VIEW IF EXISTS ohlcv_1_ethusdt_mv;
-- DROP VIEW IF EXISTS ohlcv_5_ethusdt_mv;

-- Drop any utility views
DROP VIEW IF EXISTS latest_candles;

-- =============================================================================
-- DROP MAIN TABLES
-- =============================================================================

-- Drop buffer table first (depends on trades_local)
DROP TABLE IF EXISTS trades_buffer;

-- Drop main trades table
DROP TABLE IF EXISTS trades_local;

-- Drop any other tables that might exist
DROP TABLE IF EXISTS trades_test;
DROP TABLE IF EXISTS ohlcv_template;

-- =============================================================================
-- CLEANUP VERIFICATION
-- =============================================================================

-- Verify all tables are dropped
SELECT
    'Remaining tables in trades database:' as status,
    count(*) as table_count
FROM system.tables
WHERE database = 'trades';

-- List any remaining tables
SELECT
    name as remaining_table,
    engine
FROM system.tables
WHERE database = 'trades'
ORDER BY name;

-- =============================================================================
-- OPTIONAL: DROP ENTIRE DATABASE
-- =============================================================================

-- Uncomment to completely remove the trades database
-- WARNING: This will delete everything including the database itself

-- DROP DATABASE IF EXISTS trades;

-- Verify database deletion (should return 0 if database was dropped)
-- SELECT count(*) as database_exists FROM system.databases WHERE name = 'trades';

-- =============================================================================
-- CLEANUP COMPLETE
-- =============================================================================

SELECT
    'Database cleanup completed' as status,
    'Ready to run production initialization script' as next_step,
    now() as completed_at;

-- =============================================================================
-- NEXT STEPS
-- =============================================================================

/*
After running this cleanup script:

1. Verify all tables are dropped by checking the "Remaining tables" query above
2. Run the production initialization script to create clean schema
3. Test ClickHouseManager integration with fresh database

Production script will create:
- trades_local (main table)
- trades_buffer (performance optimization)
- Proper indexes and TTL
- Ready for MaterializedViews creation by ClickHouseManager
*/