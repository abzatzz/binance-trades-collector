"""
ClickHouse Materialized Views Real-time Update Test
==================================================

Test script to verify MV update frequency and behavior with buffer tables.
Tests candle creation on timestamp boundaries and real-time updates.

Usage:
    python test_clickhouse_mv.py
"""

import asyncio
import time
import random
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

import aiochclient
from aiohttp import ClientSession, ClientTimeout


class ClickHouseMVTester:
    """Test ClickHouse MV real-time behavior"""

    def __init__(self, host: str = "localhost", port: int = 8123):
        self.host = host
        self.port = port
        self.database = "test_mv_db"
        self.session = None
        self.client = None

        # Test configuration
        self.test_symbol = "TESTUSDT"
        self.timeframes = [1, 5]  # 1m and 5m timeframes

        print(f"Initialized ClickHouse MV Tester for {host}:{port}")

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.cleanup()

    async def connect(self):
        """Connect to ClickHouse"""
        try:
            timeout = ClientTimeout(total=30)
            self.session = ClientSession(timeout=timeout)

            # First connect without database to create it
            self.client = aiochclient.ChClient(
                session=self.session,
                url=f"http://{self.host}:{self.port}",
                user="default",
                password="123"
            )

            # Test connection
            await self.client.execute("SELECT 1")
            print("✅ Connected to ClickHouse")

        except Exception as e:
            print(f"❌ Failed to connect to ClickHouse: {e}")
            raise

    async def cleanup(self):
        """Cleanup connections and test data"""
        try:
            if self.client:
                # Drop test tables and views
                await self.client.execute(f"DROP DATABASE IF EXISTS {self.database}")
                print("🧹 Cleaned up test database")

        except Exception as e:
            print(f"⚠️  Cleanup error: {e}")

        if self.session and not self.session.closed:
            await self.session.close()

    async def setup_test_environment(self):
        """Create test database, tables, and materialized views"""
        print("\n🔧 Setting up test environment...")

        # Clean up any existing test database first
        try:
            await self.client.execute(f"DROP DATABASE IF EXISTS {self.database}")
            print("🧹 Cleaned up any existing test database")
        except:
            pass

        # Create test database
        await self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        await self.client.execute(f"USE {self.database}")

        # Create main trades table (identical to trades_local)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS test_trades_local (
            symbol LowCardinality(String),
            aggregate_id UInt64,
            price Float64,
            quantity Float64,
            first_trade_id UInt64,
            last_trade_id UInt64,
            timestamp DateTime64(3),
            is_buyer_maker UInt8,
            insert_time DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (symbol, timestamp, aggregate_id)
        SETTINGS index_granularity = 8192
        """
        await self.client.execute(create_table_query)
        print("✅ Created test_trades_local table (NO BUFFER)")

        # Create materialized views for different timeframes
        for timeframe in self.timeframes:
            await self.create_mv(timeframe)

        print("🎯 Test environment ready - Direct inserts only")

    async def create_mv(self, timeframe_minutes: int):
        """Create materialized view for specific timeframe"""
        mv_name = f"test_ohlcv_{timeframe_minutes}m_mv"
        interval_func = f"toStartOfInterval(timestamp, INTERVAL {timeframe_minutes} MINUTE)"

        create_mv_query = f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}
        ENGINE = ReplacingMergeTree()
        PARTITION BY toYYYYMM(candle_time)
        ORDER BY candle_time
        SETTINGS index_granularity = 8192
        AS SELECT
          '{self.test_symbol}' as symbol,
          {interval_func} as candle_time,
          argMin(price, timestamp) as open,
          max(price) as high,
          min(price) as low,
          argMax(price, timestamp) as close,
          sum(quantity) as volume,
          count() as trades_count
        FROM test_trades_local
        WHERE symbol = '{self.test_symbol}'
        GROUP BY candle_time
        """

        await self.client.execute(create_mv_query)
        print(f"✅ Created MV: {mv_name} ({timeframe_minutes}m)")

    def generate_test_trade(self, trade_id: int, base_price: float = 100.0) -> Dict:
        """Generate realistic test trade data"""
        current_time = datetime.now(timezone.utc)

        # Add some price variation
        price_variation = random.uniform(-0.05, 0.05)
        price = base_price + (base_price * price_variation)

        return {
            'symbol': self.test_symbol,
            'aggregate_id': trade_id,
            'price': round(price, 2),
            'quantity': round(random.uniform(0.1, 10.0), 4),
            'first_trade_id': trade_id * 100,
            'last_trade_id': trade_id * 100 + random.randint(1, 10),
            'timestamp': current_time,
            'is_buyer_maker': random.choice([0, 1])
        }

    async def insert_trade(self, trade_data: Dict):
        """Insert single trade directly into main ClickHouse table"""
        query = f"""
        INSERT INTO test_trades_local
        (symbol, aggregate_id, price, quantity, first_trade_id, last_trade_id, timestamp, is_buyer_maker)
        VALUES
        """

        values = [(
            trade_data['symbol'],
            trade_data['aggregate_id'],
            trade_data['price'],
            trade_data['quantity'],
            trade_data['first_trade_id'],
            trade_data['last_trade_id'],
            trade_data['timestamp'],
            trade_data['is_buyer_maker']
        )]

        await self.client.execute(query, *values)

    async def get_mv_data(self, timeframe_minutes: int) -> List[Dict]:
        """Get current data from materialized view"""
        mv_name = f"test_ohlcv_{timeframe_minutes}m_mv"

        query = f"""
        SELECT 
            toUnixTimestamp(candle_time) * 1000 as timestamp,
            open, high, low, close, volume, trades_count,
            formatDateTime(candle_time, '%Y-%m-%d %H:%M:%S') as candle_time_str
        FROM {mv_name}
        ORDER BY candle_time DESC
        LIMIT 10
        """

        rows = await self.client.fetch(query)

        return [
            {
                'timestamp': int(row[0]),
                'open': float(row[1]),
                'high': float(row[2]),
                'low': float(row[3]),
                'close': float(row[4]),
                'volume': float(row[5]),
                'trades_count': int(row[6]),
                'candle_time_str': row[7]
            }
            for row in rows
        ]

    async def show_mv_status(self):
        """Display current MV status"""
        print("\n📊 Current MV Status:")
        print("=" * 70)

        # Check data in main table
        main_count = await self.client.fetchval("SELECT COUNT(*) FROM test_trades_local")
        print(f"📈 Main table: {main_count} rows")

        for timeframe in self.timeframes:
            mv_data = await self.get_mv_data(timeframe)
            print(f"\n🕐 {timeframe}m Timeframe:")

            if mv_data:
                for i, candle in enumerate(mv_data):
                    status_icon = "🟢" if i == 0 else "⚪"
                    print(f"  {status_icon} {candle['candle_time_str']} | "
                          f"O:{candle['open']:.2f} H:{candle['high']:.2f} "
                          f"L:{candle['low']:.2f} C:{candle['close']:.2f} | "
                          f"Vol:{candle['volume']:.2f} | Trades:{candle['trades_count']}")
            else:
                print("  📭 No data in MV yet")

    async def test_production_populate_scenario(self):
        """Test production scenario: populate historical data, then real-time updates"""
        print("\n🏭 Testing Production Scenario: Historical Populate + Real-time")
        print("=" * 70)

        # Step 1: Insert historical trades (simulate existing data)
        print("\n1️⃣ Inserting historical trades (simulating existing data)...")
        historical_base_time = datetime.now(timezone.utc) - timedelta(hours=2)

        # Create 3 historical candles with multiple trades each
        historical_trades = []
        for candle_offset in range(3):  # 3 historical candles
            candle_base_time = historical_base_time + timedelta(minutes=candle_offset)

            # Multiple trades per candle
            for trade_offset in range(5):
                trade_time = candle_base_time + timedelta(seconds=trade_offset * 10)
                trade_id = 1000 + (candle_offset * 10) + trade_offset

                trade = {
                    'symbol': self.test_symbol,
                    'aggregate_id': trade_id,
                    'price': 100.0 + candle_offset + (trade_offset * 0.5),
                    'quantity': 1.0 + (trade_offset * 0.2),
                    'first_trade_id': trade_id * 100,
                    'last_trade_id': trade_id * 100 + 10,
                    'timestamp': trade_time,
                    'is_buyer_maker': 0
                }

                historical_trades.append(trade)
                await self.insert_trade(trade)

        print(f"   Inserted {len(historical_trades)} historical trades across 3 candles")

        # Step 2: Check current MV state (should show separate records due to ReplacingMergeTree issue)
        print("\n2️⃣ Current MV state after historical inserts:")
        await self.show_mv_raw_data()

        # Step 3: Create proper AggregatingMergeTree MV and populate it
        print("\n3️⃣ Creating AggregatingMergeTree MV and populating with historical data...")

        # Drop existing problematic MV
        await self.client.execute("DROP VIEW IF EXISTS test_ohlcv_1m_mv")

        # Create proper AggregatingMergeTree MV
        await self.create_aggregating_mv()

        # Populate with historical data (production scenario)
        await self.populate_historical_data()

        # Check populated state
        print("\n4️⃣ MV state after historical populate:")
        await self.show_aggregating_mv_data()

        # Step 4: Add real-time trades and verify proper aggregation
        print("\n5️⃣ Adding real-time trades to test live aggregation...")

        current_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        realtime_trades = [
            {'id': 2001, 'price': 105.0, 'quantity': 2.0, 'time_offset': 10},
            {'id': 2002, 'price': 95.0, 'quantity': 1.5, 'time_offset': 20},
            {'id': 2003, 'price': 110.0, 'quantity': 0.8, 'time_offset': 30},
            {'id': 2004, 'price': 98.0, 'quantity': 3.0, 'time_offset': 40},
        ]

        for trade_data in realtime_trades:
            trade_time = current_time + timedelta(seconds=trade_data['time_offset'])

            trade = {
                'symbol': self.test_symbol,
                'aggregate_id': trade_data['id'],
                'price': trade_data['price'],
                'quantity': trade_data['quantity'],
                'first_trade_id': trade_data['id'] * 100,
                'last_trade_id': trade_data['id'] * 100 + 10,
                'timestamp': trade_time,
                'is_buyer_maker': 0
            }

            await self.insert_trade(trade)
            print(f"   Real-time trade: ${trade_data['price']:.1f} x {trade_data['quantity']:.1f} @ {trade_time.strftime('%H:%M:%S')}")

        # Step 5: Verify final aggregation includes both historical and real-time data
        print("\n6️⃣ Final MV state (historical + real-time):")
        await self.show_aggregating_mv_data()

        # Step 6: Compare with direct GROUP BY for validation
        print("\n7️⃣ Validation with direct GROUP BY:")
        await self.show_direct_groupby_comparison()

    async def create_aggregating_mv(self):
        """Create proper AggregatingMergeTree MV like production"""
        create_mv_query = f"""
        CREATE MATERIALIZED VIEW test_ohlcv_1m_aggregating_mv
        ENGINE = AggregatingMergeTree()
        ORDER BY (symbol, candle_time)
        AS SELECT
          '{self.test_symbol}' as symbol,
          toStartOfInterval(timestamp, INTERVAL 1 MINUTE) as candle_time,
          argMinState(price, timestamp) as open_state,
          maxState(price) as high_state,
          minState(price) as low_state,
          argMaxState(price, timestamp) as close_state,
          sumState(quantity) as volume_state,
          countState() as trades_state
        FROM test_trades_local
        WHERE symbol = '{self.test_symbol}'
        GROUP BY symbol, candle_time
        """

        await self.client.execute(create_mv_query)
        print("   ✅ Created AggregatingMergeTree MV")

    async def populate_historical_data(self):
        """Populate MV with historical data (production scenario)"""
        # Get historical cutoff (simulate existing data before MV creation)
        historical_cutoff = datetime.now(timezone.utc) - timedelta(minutes=30)

        populate_query = f"""
        INSERT INTO test_ohlcv_1m_aggregating_mv
        SELECT
          '{self.test_symbol}' as symbol,
          toStartOfInterval(timestamp, INTERVAL 1 MINUTE) as candle_time,
          argMinState(price, timestamp) as open_state,
          maxState(price) as high_state,
          minState(price) as low_state,
          argMaxState(price, timestamp) as close_state,
          sumState(quantity) as volume_state,
          countState() as trades_state
        FROM test_trades_local
        WHERE symbol = '{self.test_symbol}' 
          AND timestamp < '{historical_cutoff.strftime('%Y-%m-%d %H:%M:%S')}'
        GROUP BY symbol, candle_time
        """

        await self.client.execute(populate_query)

        # Check how many records were populated
        populated_count = await self.client.fetchval("SELECT COUNT(*) FROM test_ohlcv_1m_aggregating_mv")
        print(f"   ✅ Populated {populated_count} historical candles")

    async def show_aggregating_mv_data(self):
        """Show data from AggregatingMergeTree MV using proper merge functions"""
        query = f"""
        SELECT
            formatDateTime(candle_time, '%H:%M:%S') as candle_str,
            argMinMerge(open_state) as open,
            maxMerge(high_state) as high,
            minMerge(low_state) as low,
            argMaxMerge(close_state) as close,
            sumMerge(volume_state) as volume,
            countMerge(trades_state) as trades_count
        FROM test_ohlcv_1m_aggregating_mv
        GROUP BY candle_time
        ORDER BY candle_time DESC
        LIMIT 10
        """

        rows = await self.client.fetch(query)

        if rows:
            for row in rows:
                print(f"   📊 {row[0]} | O:{row[1]:.1f} H:{row[2]:.1f} L:{row[3]:.1f} C:{row[4]:.1f} | Vol:{row[5]:.1f} | Trades:{row[6]}")
        else:
            print("   📭 No aggregated data in MV")

    async def show_mv_raw_data(self):
        """Show raw MV data to illustrate ReplacingMergeTree problems"""
        mv_data = await self.client.fetch(f"""
            SELECT 
                formatDateTime(candle_time, '%H:%M:%S') as candle_str,
                open, high, low, close, volume, trades_count
            FROM test_ohlcv_1m_mv
            ORDER BY candle_time DESC
            LIMIT 5
        """)

        if mv_data:
            print("   Current ReplacingMergeTree MV (problematic):")
            for row in mv_data:
                print(f"   📊 {row[0]} | O:{row[1]:.1f} H:{row[2]:.1f} L:{row[3]:.1f} C:{row[4]:.1f} | Vol:{row[5]:.1f} | Trades:{row[6]}")
        else:
            print("   📭 No data in current MV")

    async def show_direct_groupby_comparison(self):
        """Show direct GROUP BY for comparison"""
        direct_result = await self.client.fetch(f"""
            SELECT
                formatDateTime(toStartOfInterval(timestamp, INTERVAL 1 MINUTE), '%H:%M:%S') as candle_str,
                argMin(price, timestamp) as open,
                max(price) as high,
                min(price) as low,
                argMax(price, timestamp) as close,
                sum(quantity) as volume,
                count() as trades_count
            FROM test_trades_local
            WHERE symbol = '{self.test_symbol}'
            GROUP BY toStartOfInterval(timestamp, INTERVAL 1 MINUTE)
            ORDER BY toStartOfInterval(timestamp, INTERVAL 1 MINUTE) DESC
            LIMIT 10
        """)

        if direct_result:
            print("   Direct GROUP BY (ground truth):")
            for row in direct_result:
                print(f"   🎯 {row[0]} | O:{row[1]:.1f} H:{row[2]:.1f} L:{row[3]:.1f} C:{row[4]:.1f} | Vol:{row[5]:.1f} | Trades:{row[6]}")
        else:
            print("   📭 No data for direct GROUP BY")

    async def test_different_mv_engines(self):
        """Test different MV engines to find the correct approach"""
        print(f"\n🧪 Testing different MV engines:")

        # Test 1: SummingMergeTree
        print(f"\n1️⃣ Testing SummingMergeTree MV:")
        await self.client.execute("DROP VIEW IF EXISTS test_summing_mv")

        summing_mv_query = f"""
        CREATE MATERIALIZED VIEW test_summing_mv
        ENGINE = SummingMergeTree()
        ORDER BY candle_time
        AS SELECT
          toStartOfInterval(timestamp, INTERVAL 1 MINUTE) as candle_time,
          argMin(price, timestamp) as open,
          max(price) as high,
          min(price) as low,
          argMax(price, timestamp) as close,
          sum(quantity) as volume,
          count() as trades_count
        FROM test_trades_local
        WHERE symbol = '{self.test_symbol}'
        GROUP BY candle_time
        """

        await self.client.execute(summing_mv_query)

        # Insert new test data
        await self.insert_test_batch("SUMMING_TEST", 2000)

        summing_result = await self.client.fetch("SELECT * FROM test_summing_mv ORDER BY candle_time DESC LIMIT 1")
        if summing_result:
            row = summing_result[0]
            print(f"  📊 SummingMergeTree: O:{row[1]:.1f} H:{row[2]:.1f} L:{row[3]:.1f} C:{row[4]:.1f} | Vol:{row[5]:.1f} | Trades:{row[6]}")

        # Test 2: AggregatingMergeTree
        print(f"\n2️⃣ Testing AggregatingMergeTree MV:")
        await self.client.execute("DROP VIEW IF EXISTS test_aggregating_mv")

        aggregating_mv_query = f"""
        CREATE MATERIALIZED VIEW test_aggregating_mv
        ENGINE = AggregatingMergeTree()
        ORDER BY candle_time
        AS SELECT
          toStartOfInterval(timestamp, INTERVAL 1 MINUTE) as candle_time,
          argMinState(price, timestamp) as open_state,
          maxState(price) as high_state,
          minState(price) as low_state,
          argMaxState(price, timestamp) as close_state,
          sumState(quantity) as volume_state,
          countState() as trades_state
        FROM test_trades_local
        WHERE symbol = '{self.test_symbol}'
        GROUP BY candle_time
        """

        await self.client.execute(aggregating_mv_query)

        # Insert new test data
        await self.insert_test_batch("AGGREGATING_TEST", 3000)

        # Query with merge functions
        aggregating_result = await self.client.fetch("""
            SELECT 
                candle_time,
                argMinMerge(open_state) as open,
                maxMerge(high_state) as high,
                minMerge(low_state) as low,
                argMaxMerge(close_state) as close,
                sumMerge(volume_state) as volume,
                countMerge(trades_state) as trades_count
            FROM test_aggregating_mv 
            GROUP BY candle_time
            ORDER BY candle_time DESC 
            LIMIT 1
        """)

        if aggregating_result:
            row = aggregating_result[0]
            print(f"  📊 AggregatingMergeTree: O:{row[1]:.1f} H:{row[2]:.1f} L:{row[3]:.1f} C:{row[4]:.1f} | Vol:{row[5]:.1f} | Trades:{row[6]}")

        # Test 3: Direct table with manual GROUP BY
        print(f"\n3️⃣ Testing direct GROUP BY query:")
        direct_result = await self.client.fetch(f"""
            SELECT
                toStartOfInterval(timestamp, INTERVAL 1 MINUTE) as candle_time,
                argMin(price, timestamp) as open,
                max(price) as high,
                min(price) as low,
                argMax(price, timestamp) as close,
                sum(quantity) as volume,
                count() as trades_count
            FROM test_trades_local
            WHERE symbol = '{self.test_symbol}'
            GROUP BY candle_time
            ORDER BY candle_time DESC
            LIMIT 1
        """)

        if direct_result:
            row = direct_result[0]
            print(f"  📊 Direct GROUP BY: O:{row[1]:.1f} H:{row[2]:.1f} L:{row[3]:.1f} C:{row[4]:.1f} | Vol:{row[5]:.1f} | Trades:{row[6]}")

    async def insert_test_batch(self, test_prefix: str, start_id: int):
        """Insert a batch of test trades for engine comparison"""
        base_time = datetime.now(timezone.utc).replace(second=0, microsecond=0)

        for i in range(3):
            trade_time = base_time + timedelta(seconds=i * 10)
            trade = {
                'symbol': self.test_symbol,
                'aggregate_id': start_id + i,
                'price': 100.0 + i * 2,
                'quantity': 1.0 + i * 0.5,
                'first_trade_id': (start_id + i) * 100,
                'last_trade_id': (start_id + i) * 100 + 10,
                'timestamp': trade_time,
                'is_buyer_maker': 0
            }
            await self.insert_trade(trade)

    async def test_candle_boundary_crossing(self):
        """Specific test for candle boundary crossing"""
        print("\n🎯 Testing Candle Boundary Crossing")
        print("=" * 40)

        # Wait until we're close to minute boundary
        while True:
            current_time = datetime.now(timezone.utc)
            seconds_to_boundary = 60 - current_time.second

            if seconds_to_boundary <= 10:
                break

            print(f"⏳ Waiting {seconds_to_boundary} seconds until minute boundary...")
            await asyncio.sleep(min(5, seconds_to_boundary - 5))

        print("🎯 Starting boundary crossing test...")

        # Insert trades before, during, and after minute boundary
        for i in range(15):  # 30 seconds of trades (2 sec intervals)
            current_time = datetime.now(timezone.utc)
            trade = self.generate_test_trade(1000 + i, 105.0)
            await self.insert_trade(trade)

            if current_time.second in [58, 59, 0, 1, 2]:
                print(f"🔔 Boundary trade at {current_time.strftime('%H:%M:%S')}")
                await self.show_mv_status()

            await asyncio.sleep(2)

        print("✅ Boundary crossing test completed")

    async def run_full_test(self):
        """Run complete MV testing suite with production scenario"""
        print("🧪 ClickHouse Materialized Views Real-time Test")
        print("=" * 60)

        try:
            await self.setup_test_environment()
            await self.test_production_populate_scenario()

            # Final comparison
            print("\n📋 Final Production Scenario Results:")
            print("=" * 50)
            print("✅ Test completed - compare AggregatingMergeTree MV vs Direct GROUP BY")
            print("🎯 If results match, AggregatingMergeTree is ready for production")

        except Exception as e:
            print(f"❌ Test failed: {e}")
            raise


async def main():
    """Main test execution"""
    # Modify these connection parameters as needed
    HOST = "localhost"
    PORT = 8123

    print(f"🔗 Connecting to ClickHouse at {HOST}:{PORT}")
    print("⚠️  This test will create and drop a test database")

    # Confirm before running
    response = input("Continue? (y/N): ").strip().lower()
    if response != 'y':
        print("Test cancelled")
        return

    async with ClickHouseMVTester(HOST, PORT) as tester:
        await tester.run_full_test()

    print("\n🎉 All tests completed successfully!")


if __name__ == "__main__":
    asyncio.run(main())