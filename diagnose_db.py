"""
Diagnostic script for ClickHouse connection and BTCUSDT data
"""

import asyncio
import os
import sys
from pathlib import Path

# Add project root
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Load .env manually
def load_env():
    env_path = project_root / '.env'
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    if '#' in value:
                        value = value.split('#')[0]
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")


load_env()


async def check_clickhouse_connection():
    """Check ClickHouse connection and queries"""
    print("=" * 60)
    print("CLICKHOUSE DIAGNOSTICS")
    print("=" * 60)

    # Get config from env
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = os.getenv('CLICKHOUSE_HTTP_PORT', '8123')
    database = os.getenv('CLICKHOUSE_DATABASE', 'trades')
    user = os.getenv('CLICKHOUSE_USERNAME', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD', '')

    print(f"\nConnection settings:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    print(f"  Password: {'*' * len(password) if password else '(empty)'}")

    try:
        from aiochclient import ChClient
        from aiohttp import ClientSession

        url = f"http://{host}:{port}"
        print(f"\nConnecting to: {url}")

        async with ClientSession() as session:
            client = ChClient(
                session,
                url=url,
                user=user,
                password=password,
                database=database
            )

            # Test 1: Basic connection
            print("\n--- Test 1: Basic connection ---")
            result = await client.fetchval("SELECT 1")
            print(f"  SELECT 1 = {result} ✓")

            # Test 2: Check database exists
            print("\n--- Test 2: Database check ---")
            result = await client.fetch(f"SHOW DATABASES LIKE '{database}'")
            print(f"  Database '{database}' exists: {len(result) > 0} ✓")

            # Test 3: Check tables
            print("\n--- Test 3: Tables in database ---")
            result = await client.fetch(f"SHOW TABLES FROM {database}")
            tables = [r[0] for r in result] if result else []
            print(f"  Tables: {tables}")

            # Test 4: Check trades_local table
            print("\n--- Test 4: trades_local structure ---")
            if 'trades_local' in tables:
                result = await client.fetch(f"DESCRIBE TABLE {database}.trades_local")
                print(f"  Columns: {[r[0] for r in result]}")
            else:
                print("  trades_local table NOT FOUND!")

            # Test 5: Count all trades
            print("\n--- Test 5: Total trades count ---")
            result = await client.fetchval(f"SELECT count() FROM {database}.trades_local")
            print(f"  Total trades: {result:,}")

            # Test 6: BTCUSDT specific
            print("\n--- Test 6: BTCUSDT data ---")
            result = await client.fetchval(
                f"SELECT count() FROM {database}.trades_local WHERE symbol = 'BTCUSDT'"
            )
            print(f"  BTCUSDT trades: {result:,}")

            if result > 0:
                # Get sample trades instead of complex query
                result = await client.fetch(
                    f"""SELECT symbol, aggregate_id, price, quantity, timestamp 
                        FROM {database}.trades_local 
                        WHERE symbol = 'BTCUSDT' 
                        ORDER BY aggregate_id DESC 
                        LIMIT 5"""
                )
                print(f"\n  Latest 5 trades:")
                for row in result:
                    print(f"    id={row[1]}, price={row[2]}, qty={row[3]}")

            # Test 7: Symbols with data
            print("\n--- Test 7: Symbols with data ---")
            result = await client.fetch(
                f"""SELECT symbol, count() as cnt 
                    FROM {database}.trades_local 
                    GROUP BY symbol 
                    ORDER BY cnt DESC 
                    LIMIT 10"""
            )
            print(f"  Top 10 symbols by trade count:")
            for row in result:
                print(f"    {row[0]}: {row[1]:,}")

            # Test 8: IntegrityChecker query simulation
            print("\n--- Test 8: Timestamp analysis ---")
            # First, let's see what timestamp looks like
            ts_query = f"""
                SELECT 
                    timestamp,
                    toTypeName(timestamp) as ts_type
                FROM {database}.trades_local
                WHERE symbol = 'BTCUSDT'
                LIMIT 3
            """
            result = await client.fetch(ts_query)
            print(f"  Sample timestamps:")
            for row in result:
                print(f"    value={row[0]}, type={row[1]}")

            # Test different conversion approaches
            print("\n--- Test 9: Timestamp conversion tests ---")

            # Try 1: Direct division by 1000 (if milliseconds)
            try:
                q1 = f"""
                    SELECT toYYYYMM(toDate(toInt64(timestamp) / 1000)) as month
                    FROM {database}.trades_local WHERE symbol = 'BTCUSDT' LIMIT 1
                """
                r1 = await client.fetch(q1)
                print(f"  toInt64(ts)/1000: month={r1[0][0]}")
            except Exception as e:
                print(f"  toInt64(ts)/1000: FAILED - {e}")

            # Try 2: Direct (if already seconds)
            try:
                q2 = f"""
                    SELECT toYYYYMM(toDate(toInt64(timestamp))) as month
                    FROM {database}.trades_local WHERE symbol = 'BTCUSDT' LIMIT 1
                """
                r2 = await client.fetch(q2)
                print(f"  toInt64(ts) direct: month={r2[0][0]}")
            except Exception as e:
                print(f"  toInt64(ts) direct: FAILED - {e}")

            # Try 3: As Decimal
            try:
                q3 = f"""
                    SELECT 
                        timestamp,
                        toDateTime(toInt64(timestamp / 1000)) as dt1,
                        toDateTime(toInt64(timestamp)) as dt2
                    FROM {database}.trades_local WHERE symbol = 'BTCUSDT' LIMIT 1
                """
                r3 = await client.fetch(q3)
                print(f"  Raw: {r3[0][0]}, /1000={r3[0][1]}, direct={r3[0][2]}")
            except Exception as e:
                print(f"  Decimal conversion: FAILED - {e}")

            # Test 10: Original query - timestamp is already DateTime64!
            print("\n--- Test 10: Original IntegrityChecker query ---")
            original_query = f"""
                SELECT 
                    toYYYYMM(timestamp) as month,
                    count() as total_count,
                    min(aggregate_id) as min_id,
                    max(aggregate_id) as max_id
                FROM {database}.trades_local
                WHERE symbol = 'BTCUSDT'
                GROUP BY month
                ORDER BY month
                LIMIT 10
            """
            try:
                result = await client.fetch(original_query)
                print(f"  Monthly stats (timestamp is DateTime64 - use directly!):")
                for row in result:
                    print(f"    Month {row[0]}: {row[1]:,} trades, IDs {row[2]} - {row[3]}")
            except Exception as e:
                print(f"  ❌ Query failed: {e}")

            print("\n✅ ClickHouse connection OK")

    except Exception as e:
        print(f"\n❌ ClickHouse error: {e}")
        import traceback
        traceback.print_exc()


async def check_binance_api():
    """Check Binance API connection"""
    print("\n" + "=" * 60)
    print("BINANCE API DIAGNOSTICS")
    print("=" * 60)

    api_key = os.getenv('BINANCE_API_KEY', '')
    secret_key = os.getenv('BINANCE_SECRET_KEY', '')

    print(f"\nAPI Key: {api_key[:20]}..." if api_key else "API Key: NOT SET")
    print(f"Secret Key: {'*' * 20}..." if secret_key else "Secret Key: NOT SET")

    try:
        from binance import AsyncClient

        print("\nConnecting to Binance...")
        client = await AsyncClient.create(api_key, secret_key)

        # Test 1: Server time
        print("\n--- Test 1: Server time ---")
        server_time = await client.get_server_time()
        from datetime import datetime
        ts = server_time['serverTime']
        print(f"  Server time: {datetime.fromtimestamp(ts/1000)}")

        # Test 2: Get BTCUSDT info
        print("\n--- Test 2: BTCUSDT exchange info ---")
        info = await client.get_symbol_info('BTCUSDT')
        print(f"  Symbol: {info['symbol']}")
        print(f"  Status: {info['status']}")

        # Test 3: Get latest aggregate trades
        print("\n--- Test 3: Latest BTCUSDT aggTrades ---")
        trades = await client.get_aggregate_trades(symbol='BTCUSDT', limit=5)
        print(f"  Got {len(trades)} trades")
        for t in trades:
            print(f"    id={t['a']}, price={t['p']}, qty={t['q']}")

        # Test 4: Get historical aggregate trades (test archive endpoint)
        print("\n--- Test 4: Historical aggTrades (from specific ID) ---")
        # Get trades from a specific ID
        first_id = trades[0]['a'] - 1000  # Go back 1000 trades
        historical = await client.get_aggregate_trades(
            symbol='BTCUSDT',
            fromId=first_id,
            limit=5
        )
        print(f"  Got {len(historical)} trades starting from id={first_id}")
        for t in historical:
            print(f"    id={t['a']}, price={t['p']}, qty={t['q']}")

        await client.close_connection()
        print("\n✅ Binance API connection OK")

    except Exception as e:
        print(f"\n❌ Binance API error: {e}")
        import traceback
        traceback.print_exc()


async def check_historical_download():
    """Check downloading a single historical archive"""
    print("\n" + "=" * 60)
    print("HISTORICAL ARCHIVE DOWNLOAD TEST")
    print("=" * 60)

    import aiohttp
    from datetime import datetime, timedelta

    # Try to download yesterday's archive
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime('%Y-%m-%d')

    url = f"https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-{date_str}.zip"

    print(f"\nTrying to download: {url}")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.head(url) as resp:
                print(f"  Status: {resp.status}")
                if resp.status == 200:
                    content_length = resp.headers.get('Content-Length', 'unknown')
                    print(f"  File size: {content_length} bytes")
                    print(f"  Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
                    print("\n✅ Archive available for download")
                elif resp.status == 404:
                    print("  Archive not found (might not be available yet)")
                    # Try older date
                    older_date = (datetime.now() - timedelta(days=3)).strftime('%Y-%m-%d')
                    older_url = f"https://data.binance.vision/data/futures/um/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-{older_date}.zip"
                    print(f"\nTrying older date: {older_url}")
                    async with session.head(older_url) as resp2:
                        print(f"  Status: {resp2.status}")
                        if resp2.status == 200:
                            print(f"  File size: {resp2.headers.get('Content-Length', 'unknown')} bytes")
                            print("\n✅ Older archive available")
                else:
                    print(f"  Unexpected status: {resp.status}")

    except Exception as e:
        print(f"\n❌ Download error: {e}")
        import traceback
        traceback.print_exc()


async def main():
    print("\n" + "=" * 60)
    print("DATA PROVIDER DIAGNOSTIC TOOL")
    print("=" * 60)

    await check_clickhouse_connection()
    await check_binance_api()
    await check_historical_download()

    print("\n" + "=" * 60)
    print("DIAGNOSTICS COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())