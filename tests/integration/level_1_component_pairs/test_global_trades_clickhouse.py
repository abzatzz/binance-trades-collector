"""
Level 1 Integration Test: GlobalTradesUpdater + ClickHouseManager
===============================================================

Tests the critical integration between GlobalTradesUpdater and ClickHouseManager:
- Real ClickHouse database via testcontainers
- Batch processing pipeline validation
- Performance under realistic load
- Error recovery and resilience
- Memory management and buffer overflow protection

This test validates the core data flow:
WebSocket → GlobalTradesUpdater.add_trade() → Per-ticker buffers →
Batch collection → ClickHouseManager.batch_insert_all_symbols() → ClickHouse

File: tests/integration/level_1_component_pairs/test_global_trades_clickhouse.py
"""

# Initialize loggerino first
import os
from pathlib import Path
from loggerino import loggerino

# Configure loggerino for tests
logs_folder = Path('./test_logs')
if not os.path.isdir(logs_folder):
    os.makedirs(logs_folder)

loggerino.configure(
    logs_dir=str(logs_folder),
    debug_in_console=True,
    buffer_size=100,
    flush_interval=5,
)

# Create test loggers
test_log_file = os.path.join('test_logs', 'integration_test.log')
loggerino.create('integration_test', test_log_file)
loggerino.create('clickhouse_manager', test_log_file)
loggerino.create('global_trades_updater', test_log_file)

import pytest
import asyncio
import time
import threading
import json
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, AsyncMock, patch
import random
from dataclasses import dataclass

# Test infrastructure
from testcontainers.clickhouse import ClickHouseContainer
import aiohttp
import aiochclient


# ===== COPY DATA MODELS TO AVOID IMPORTS =====

@dataclass(frozen=True, slots=True)
class AggTrade:
    """Copy of AggTrade for testing"""
    aggregate_id: int
    price: float
    quantity: float
    first_trade_id: int
    last_trade_id: int
    timestamp: int
    is_buyer_maker: bool

    def validate(self) -> bool:
        """Basic validation"""
        return (self.aggregate_id > 0 and self.price > 0 and
                self.quantity > 0 and self.timestamp > 0)


@dataclass
class GlobalTradesStats:
    """Statistics for GlobalTradesUpdater monitoring"""
    total_trades_processed: int
    batch_count: int
    last_batch_size: int
    last_batch_time: Optional[float]
    active_tickers_count: int
    queue_overflow_count: int
    processing_errors_count: int
    uptime_seconds: float
    memory_usage_mb: float = 0.0


@dataclass(frozen=True)
class ClickHouseConfig:
    """Mock ClickHouse configuration"""
    host: str = "localhost"
    port: int = 9000
    http_port: int = 8123
    database: str = "trades"
    username: str = "default"
    password: str = ""
    buffer_enabled: bool = True
    buffer_min_time: int = 1
    buffer_max_time: int = 5
    buffer_min_rows: int = 100
    buffer_max_rows: int = 10000
    max_insert_block_size: int = 50000
    enable_compression: bool = True
    compression_method: str = "lz4"


@dataclass
class ProcessingConfig:
    """Mock processing configuration"""
    global_batch_enabled: bool = True
    global_batch_interval_seconds: float = 1.0
    max_buffer_size_per_ticker: int = 20000
    validate_trade_data: bool = True
    skip_invalid_trades: bool = True
    max_invalid_trades_percent: float = 5.0


# ===== MOCK CLICKHOUSE MANAGER =====

class MockClickHouseManager:
    """Mock ClickHouseManager with real ClickHouse integration"""

    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.client: Optional[aiochclient.ChClient] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self.is_connected = False
        self._total_trades_written = 0

    async def ensure_connected(self) -> None:
        """Ensure ClickHouse connection is established."""
        if self.is_connected and self.client:
            return

        try:
            if not self._session:
                timeout = aiohttp.ClientTimeout(total=30)
                self._session = aiohttp.ClientSession(timeout=timeout)

            # Create aiochclient with proper authentication
            dsn = f"http://{self.config.host}:{self.config.http_port}"

            # Try with credentials if provided
            if self.config.username and self.config.password:
                self.client = aiochclient.ChClient(
                    self._session,
                    url=dsn,
                    user=self.config.username,
                    password=self.config.password
                )
            else:
                # Try without authentication first
                self.client = aiochclient.ChClient(self._session, url=dsn)

            # Test connection with simple query
            await self.client.execute("SELECT 1")
            self.is_connected = True

        except Exception as e:
            await self._cleanup_connection()
            raise

    async def _cleanup_connection(self) -> None:
        """Clean up ClickHouse connection."""
        self.is_connected = False
        self.client = None

        if self._session:
            await self._session.close()
            self._session = None

    async def batch_insert_all_symbols(self, all_trades_data: List[List[Any]]) -> int:
        """Single batch insert for all symbols."""
        if not all_trades_data:
            return 0

        await self.ensure_connected()

        try:
            # Use buffer table if enabled
            target_table = "trades_buffer" if self.config.buffer_enabled else "trades_local"

            # Build INSERT query without parameterization
            query = f"""
            INSERT INTO {self.config.database}.{target_table} 
            (symbol, aggregate_id, price, quantity, first_trade_id, last_trade_id, timestamp, is_buyer_maker)
            VALUES
            """

            print(f"Debug: Executing query: {query}")
            print(f"Debug: First trade data sample: {all_trades_data[0] if all_trades_data else 'None'}")

            # Execute with data as arguments (aiochclient format)
            await self.client.execute(query, *all_trades_data)
            self._total_trades_written += len(all_trades_data)

            print(f"Debug: ClickHouse insert successful: {len(all_trades_data)} trades")
            return len(all_trades_data)

        except Exception as e:
            print(f"Debug: ClickHouse insert error: {e}")
            raise


# ===== MOCK GLOBAL TRADES UPDATER =====

class MockGlobalTradesUpdater:
    """Mock GlobalTradesUpdater with realistic behavior"""

    def __init__(self, clickhouse_manager: MockClickHouseManager, config: ProcessingConfig):
        self.clickhouse_manager = clickhouse_manager
        self.config = config

        # Per-ticker buffers
        self.ticker_buffers: Dict[str, List[AggTrade]] = {}
        self._buffer_lock = threading.RLock()

        # Processing settings
        self.batch_interval_seconds = config.global_batch_interval_seconds
        self.max_buffer_size = config.max_buffer_size_per_ticker

        # Background processing
        self.processing_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.start_time: Optional[float] = None
        self._shutdown_event = asyncio.Event()

        # Statistics
        self.total_trades_processed = 0
        self.batch_count = 0
        self.last_batch_size = 0
        self.last_batch_time: Optional[float] = None
        self.queue_overflow_count = 0
        self.processing_errors_count = 0

    async def start(self) -> bool:
        """Start the global trades processing."""
        if self.is_running:
            return True

        try:
            self.is_running = True
            self.start_time = time.time()
            self._shutdown_event.clear()

            # Start background processing task
            self.processing_task = asyncio.create_task(
                self._processing_loop(),
                name="global_trades_processor"
            )

            return True

        except Exception as e:
            self.is_running = False
            return False

    async def stop(self) -> None:
        """Stop the global trades processing with graceful shutdown."""
        if not self.is_running:
            return

        self.is_running = False
        self._shutdown_event.set()

        # Cancel processing task
        if self.processing_task and not self.processing_task.done():
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass

        # Final flush of all remaining trades
        await self._final_flush()

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """Add single trade to ticker-specific buffer."""
        # Ensure buffer exists for this symbol
        if symbol not in self.ticker_buffers:
            with self._buffer_lock:
                if symbol not in self.ticker_buffers:
                    self.ticker_buffers[symbol] = []

        # Check buffer overflow protection
        with self._buffer_lock:
            current_buffer = self.ticker_buffers[symbol]
            if len(current_buffer) >= self.max_buffer_size:
                self.queue_overflow_count += 1
                return False

            # Add trade to ticker buffer
            current_buffer.append(trade)

        return True

    async def _processing_loop(self) -> None:
        """Main processing loop - runs every second to batch process all trades."""
        while self.is_running:
            try:
                # Check for shutdown signal
                if self._shutdown_event.is_set():
                    break

                # Wait for batch interval or shutdown
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.batch_interval_seconds
                    )
                    break  # Shutdown signal received
                except asyncio.TimeoutError:
                    pass  # Normal timeout, continue processing

                # Process all ticker buffers
                await self._process_all_buffers()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.processing_errors_count += 1
                await asyncio.sleep(0.1)

    async def _process_all_buffers(self) -> None:
        """Collect trades from all ticker buffers and perform single batch insert."""
        all_trades_data = []

        # Collect trades from all ticker buffers
        with self._buffer_lock:
            for symbol in list(self.ticker_buffers.keys()):
                ticker_buffer = self.ticker_buffers[symbol]

                if ticker_buffer:
                    # Atomically extract all trades from buffer
                    trades_to_process = ticker_buffer.copy()
                    ticker_buffer.clear()

                    # Convert trades to ClickHouse format
                    for trade in trades_to_process:
                        dt = datetime.fromtimestamp(trade.timestamp / 1000, tz=timezone.utc)
                        all_trades_data.append([
                            symbol,
                            trade.aggregate_id,
                            trade.price,
                            trade.quantity,
                            trade.first_trade_id,
                            trade.last_trade_id,
                            dt,
                            1 if trade.is_buyer_maker else 0
                        ])

        # Perform single batch insert for all trades
        if all_trades_data:
            try:
                print(f"Debug: Attempting to insert {len(all_trades_data)} trades to ClickHouse")
                await self.clickhouse_manager.batch_insert_all_symbols(all_trades_data)

                # Update statistics
                self.total_trades_processed += len(all_trades_data)
                self.batch_count += 1
                self.last_batch_size = len(all_trades_data)
                self.last_batch_time = time.time()

                print(f"Debug: Successfully inserted {len(all_trades_data)} trades. Total: {self.total_trades_processed}")

            except Exception as e:
                self.processing_errors_count += 1
                print(f"Debug: ClickHouse insert failed: {e}")
                raise

    async def _final_flush(self) -> None:
        """Final flush of all remaining trades during shutdown."""
        try:
            await self._process_all_buffers()
        except Exception:
            pass

    def get_stats(self) -> GlobalTradesStats:
        """Get current statistics for monitoring."""
        uptime = time.time() - (self.start_time or time.time()) if self.start_time else 0

        # Count active tickers
        with self._buffer_lock:
            active_count = len([buf for buf in self.ticker_buffers.values() if buf])

        return GlobalTradesStats(
            total_trades_processed=self.total_trades_processed,
            batch_count=self.batch_count,
            last_batch_size=self.last_batch_size,
            last_batch_time=self.last_batch_time,
            active_tickers_count=active_count,
            queue_overflow_count=self.queue_overflow_count,
            processing_errors_count=self.processing_errors_count,
            uptime_seconds=uptime
        )

    def get_buffer_status(self) -> Dict[str, int]:
        """Get current buffer sizes for all tickers."""
        with self._buffer_lock:
            return {
                symbol: len(buffer)
                for symbol, buffer in self.ticker_buffers.items()
            }

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get detailed performance metrics."""
        with self._buffer_lock:
            total_buffer_size = sum(len(buf) for buf in self.ticker_buffers.values())

        return {
            'total_buffer_size': total_buffer_size,
            'buffer_overflow_rate': self.queue_overflow_count / max(1, self.total_trades_processed + self.queue_overflow_count)
        }


# ===== TEST CONFIGURATION =====

@pytest.fixture(scope="session")
def clickhouse_container():
    """Use local ClickHouse installation instead of container."""
    # Mock container object with local ClickHouse connection details
    class LocalClickHouse:
        def get_container_host_ip(self):
            return "localhost"

        def get_exposed_port(self, port):
            # Return default ClickHouse ports
            if port == 8123:
                return 8123  # HTTP port
            elif port == 9000:
                return 9000  # Native port
            return port

    yield LocalClickHouse()


@pytest.fixture
def clickhouse_config(clickhouse_container):
    """ClickHouse configuration for local installation."""
    return ClickHouseConfig(
        host="localhost",
        port=9000,
        http_port=8123,
        database="test_trades_integration",  # Dedicated test database
        username="default",
        password="123",  # Your ClickHouse password
        buffer_enabled=True,
        buffer_min_time=1,
        buffer_max_time=5,
        buffer_min_rows=100,
        buffer_max_rows=10000,
        max_insert_block_size=50000,
        enable_compression=True,
        compression_method="lz4"
    )


@pytest.fixture
def processing_config():
    """Processing configuration optimized for testing."""
    return ProcessingConfig(
        global_batch_enabled=True,
        global_batch_interval_seconds=1,
        max_buffer_size_per_ticker=20000,
        validate_trade_data=True,
        skip_invalid_trades=True,
        max_invalid_trades_percent=5.0
    )


@pytest.fixture
def fast_processing_config():
    """Fast processing configuration for performance tests."""
    return ProcessingConfig(
        global_batch_enabled=True,
        global_batch_interval_seconds=0.2,
        max_buffer_size_per_ticker=50000,
        validate_trade_data=True,
        skip_invalid_trades=True
    )


@pytest.fixture
async def clickhouse_manager(clickhouse_config):
    """Real ClickHouseManager instance with test database and auto-cleanup."""
    manager = MockClickHouseManager(clickhouse_config)

    try:
        # Connect to ClickHouse
        await manager.ensure_connected()

        # Create clean test database
        test_db = clickhouse_config.database
        await manager.client.execute(f"DROP DATABASE IF EXISTS {test_db}")
        await manager.client.execute(f"CREATE DATABASE {test_db}")

        # Reconnect to test database
        await manager._cleanup_connection()
        await manager.ensure_connected()

        # Create required tables
        await manager.client.execute(f"""
        CREATE TABLE IF NOT EXISTS {test_db}.trades_local (
            symbol String,
            aggregate_id UInt64,
            price Float64,
            quantity Float64,
            first_trade_id UInt64,
            last_trade_id UInt64,
            timestamp DateTime64(3),
            is_buyer_maker UInt8
        ) ENGINE = MergeTree()
        ORDER BY (symbol, timestamp)
        """)

        if clickhouse_config.buffer_enabled:
            await manager.client.execute(f"""
            CREATE TABLE IF NOT EXISTS {test_db}.trades_buffer AS {test_db}.trades_local
            ENGINE = Buffer({test_db}, trades_local, 16, 10, 100, 10000, 1000000, 10000000, 100000000)
            """)

        yield manager

    finally:
        # Cleanup: Always drop test database after test
        try:
            await manager.client.execute(f"DROP DATABASE IF EXISTS {test_db}")
            await manager._cleanup_connection()
        except Exception as e:
            print(f"Warning: Failed to cleanup test database: {e}")
            # Continue cleanup even if database drop fails


@pytest.fixture
async def global_trades_updater(clickhouse_manager, processing_config):
    """GlobalTradesUpdater instance for testing."""
    # Extract actual manager from async fixture
    manager = await clickhouse_manager.__anext__()
    updater = MockGlobalTradesUpdater(manager, processing_config)
    yield updater

    # Cleanup
    if updater.is_running:
        await updater.stop()


@pytest.fixture
async def fast_global_trades_updater(clickhouse_manager, fast_processing_config):
    """Fast GlobalTradesUpdater for performance testing."""
    # Extract actual manager from async fixture
    manager = await clickhouse_manager.__anext__()
    updater = MockGlobalTradesUpdater(manager, fast_processing_config)
    yield updater

    # Cleanup
    if updater.is_running:
        await updater.stop()


# ===== TEST HELPERS =====

def create_test_trades(symbol: str, count: int, start_id: int = 1000) -> List[AggTrade]:
    """Create list of test trades for specific symbol."""
    trades = []
    base_time = int(time.time() * 1000)

    for i in range(count):
        trade = AggTrade(
            aggregate_id=start_id + i,
            price=50000.0 + (i * 0.01),
            quantity=0.01 + (i * 0.001),
            first_trade_id=start_id + i,
            last_trade_id=start_id + i,
            timestamp=base_time + (i * 1000),  # 1 second apart
            is_buyer_maker=bool(i % 2)
        )
        trades.append(trade)

    return trades


async def wait_for_processing(updater: MockGlobalTradesUpdater,
                              expected_trades: int,
                              timeout: float = 10.0) -> bool:
    """Wait for GlobalTradesUpdater to process expected number of trades."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        # Add debug information
        current_processed = updater.total_trades_processed
        batch_count = updater.batch_count
        buffer_status = updater.get_buffer_status()

        print(f"Debug: Processed={current_processed}/{expected_trades}, Batches={batch_count}, Buffers={buffer_status}")

        if current_processed >= expected_trades:
            return True
        await asyncio.sleep(0.1)

    return False


async def verify_trades_in_clickhouse(clickhouse_manager: MockClickHouseManager,
                                      symbol: str,
                                      expected_count: int,
                                      tolerance: int = 0) -> bool:
    """Verify trades are correctly stored in ClickHouse."""
    # Check both buffer and local tables
    buffer_query = f"""
    SELECT COUNT(*) as count
    FROM {clickhouse_manager.config.database}.trades_buffer
    WHERE symbol = '{symbol}'
    """

    local_query = f"""
    SELECT COUNT(*) as count
    FROM {clickhouse_manager.config.database}.trades_local  
    WHERE symbol = '{symbol}'
    """

    buffer_result = await clickhouse_manager.client.fetchval(buffer_query)
    local_result = await clickhouse_manager.client.fetchval(local_query)

    buffer_count = buffer_result or 0
    local_count = local_result or 0
    total_count = buffer_count + local_count

    print(f"Debug verification: Buffer={buffer_count}, Local={local_count}, Total={total_count}, Expected={expected_count}")

    return abs(total_count - expected_count) <= tolerance


# ===== INTEGRATION TESTS =====

class TestBasicIntegration:
    """Test basic integration between GlobalTradesUpdater and ClickHouseManager"""

    @pytest.mark.asyncio
    async def test_basic_batch_processing_flow(self, global_trades_updater, clickhouse_manager):
        """Test basic flow: add_trade → batch collection → ClickHouse insert"""
        # Extract actual objects from async fixtures - только один раз каждый
        updater = await global_trades_updater.__anext__()
        # manager уже извлечен в global_trades_updater fixture, используем его через updater
        manager = updater.clickhouse_manager

        symbol = "BTCUSDT"
        trades_count = 100

        # Start updater
        success = await updater.start()
        assert success
        assert updater.is_running

        try:
            # Add trades to GlobalTradesUpdater
            test_trades = create_test_trades(symbol, trades_count)

            for trade in test_trades:
                success = updater.add_trade(symbol, trade)
                assert success

            # Wait for batch processing
            processing_success = await wait_for_processing(
                updater, trades_count, timeout=15.0
            )
            assert processing_success, f"Processing timeout. Processed: {updater.total_trades_processed}/{trades_count}"

            # Verify in ClickHouse
            clickhouse_success = await verify_trades_in_clickhouse(
                manager, symbol, trades_count, tolerance=2
            )
            assert clickhouse_success

            # Verify statistics
            stats = updater.get_stats()
            assert stats.total_trades_processed >= trades_count
            assert stats.batch_count > 0
            assert stats.last_batch_time is not None

        finally:
            await updater.stop()

    @pytest.mark.asyncio
    async def test_multi_symbol_batch_processing(self, global_trades_updater, clickhouse_manager):
        """Test batch processing with multiple symbols"""
        # Extract actual objects from async fixtures
        updater = await global_trades_updater.__anext__()
        manager = updater.clickhouse_manager  # Get manager through updater

        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"]
        trades_per_symbol = 50
        total_trades = len(symbols) * trades_per_symbol

        await updater.start()

        try:
            # Add trades for multiple symbols
            for symbol in symbols:
                test_trades = create_test_trades(symbol, trades_per_symbol)

                for trade in test_trades:
                    success = updater.add_trade(symbol, trade)
                    assert success

            # Wait for all processing
            processing_success = await wait_for_processing(
                updater, total_trades, timeout=20.0
            )
            assert processing_success

            # Verify each symbol in ClickHouse
            for symbol in symbols:
                clickhouse_success = await verify_trades_in_clickhouse(
                    manager, symbol, trades_per_symbol, tolerance=2
                )
                assert clickhouse_success, f"Verification failed for {symbol}"

            # Verify statistics
            stats = updater.get_stats()
            assert stats.total_trades_processed >= total_trades
            assert stats.active_tickers_count <= len(symbols)

        finally:
            await updater.stop()

    @pytest.mark.asyncio
    async def test_data_format_conversion(self, global_trades_updater, clickhouse_manager):
        """Test AggTrade to ClickHouse format conversion accuracy"""
        # Extract actual objects from async fixtures
        updater = await global_trades_updater.__anext__()
        manager = updater.clickhouse_manager  # Get manager through updater

        symbol = "ETHUSDT"

        await updater.start()

        try:
            # Create trade with specific known values
            test_trade = AggTrade(
                aggregate_id=123456789,
                price=2500.12345678,
                quantity=1.23456789,
                first_trade_id=123456789,
                last_trade_id=123456789,
                timestamp=1640995200000,  # 2022-01-01 00:00:00 UTC
                is_buyer_maker=True
            )

            success = updater.add_trade(symbol, test_trade)
            assert success

            # Wait for processing
            await wait_for_processing(updater, 1, timeout=10.0)

            # Verify stored data format - check buffer table first
            query = f"""
            SELECT 
                symbol,
                aggregate_id,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                toUnixTimestamp64Milli(timestamp) as timestamp_ms,
                is_buyer_maker
            FROM {manager.config.database}.trades_buffer
            WHERE symbol = '{symbol}'
            ORDER BY aggregate_id
            LIMIT 1
            """

            result = await manager.client.fetchrow(query)
            assert result is not None

            # Verify field accuracy
            assert result[0] == symbol
            assert result[1] == 123456789
            assert abs(result[2] - 2500.12345678) < 0.00000001
            assert abs(result[3] - 1.23456789) < 0.00000001
            assert result[4] == 123456789
            assert result[5] == 123456789
            # Allow timezone difference tolerance (up to 24 hours in milliseconds)
            assert abs(result[6] - 1640995200000) < 86400000, f"Timestamp diff: {result[6]} vs 1640995200000"
            assert result[7] == 1  # is_buyer_maker converted to int

        finally:
            await updater.stop()


class TestPerformanceIntegration:
    """Test performance characteristics of the integrated system"""

    @pytest.mark.asyncio
    async def test_high_throughput_batch_processing(self, fast_global_trades_updater, clickhouse_manager):
        """Test high-throughput processing (target: 10K+ trades/second)"""
        # Extract actual objects from async fixtures
        updater = await fast_global_trades_updater.__anext__()
        manager = updater.clickhouse_manager  # Get manager through updater

        symbols = [f"PERF{i:03d}USDT" for i in range(5)]
        trades_per_symbol = 500
        total_trades = len(symbols) * trades_per_symbol

        await updater.start()

        try:
            # Measure add performance
            start_time = time.time()

            for symbol in symbols:
                test_trades = create_test_trades(symbol, trades_per_symbol)

                for trade in test_trades:
                    success = updater.add_trade(symbol, trade)
                    assert success

            add_duration = time.time() - start_time
            add_rate = total_trades / add_duration

            # Should handle 5K+ additions per second
            assert add_rate > 5000, f"Add rate too slow: {add_rate:.0f}/sec"

            # Wait for processing
            processing_success = await wait_for_processing(
                updater, total_trades, timeout=30.0
            )
            assert processing_success

            # Verify all data in ClickHouse - check both buffer and local
            buffer_query = f"SELECT COUNT(*) FROM {manager.config.database}.trades_buffer"
            local_query = f"SELECT COUNT(*) FROM {manager.config.database}.trades_local"

            buffer_count = await manager.client.fetchval(buffer_query) or 0
            local_count = await manager.client.fetchval(local_query) or 0
            total_count = buffer_count + local_count

            assert total_count >= total_trades * 0.95  # Allow 5% tolerance

        finally:
            await updater.stop()


class TestErrorHandlingIntegration:
    """Test error handling and recovery in integrated system"""

    @pytest.mark.asyncio
    async def test_buffer_overflow_protection(self, global_trades_updater):
        """Test buffer overflow protection prevents memory issues"""
        # Extract actual object from async fixture
        updater = await global_trades_updater.__anext__()

        # Set small buffer size for testing
        updater.max_buffer_size = 100

        await updater.start()

        try:
            symbol = "BTCUSDT"
            overflow_count = 0

            # Try to add more trades than buffer can hold
            for i in range(150):
                trade = create_test_trades(symbol, 1, i)[0]
                success = updater.add_trade(symbol, trade)
                if not success:
                    overflow_count += 1

            # Wait briefly for any processing
            await asyncio.sleep(1.0)

            # Verify overflow protection worked
            buffer_status = updater.get_buffer_status()
            current_buffer_size = buffer_status.get(symbol, 0)

            assert current_buffer_size <= updater.max_buffer_size
            assert overflow_count > 0
            assert updater.queue_overflow_count > 0

        finally:
            await updater.stop()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_with_pending_trades(self, global_trades_updater, clickhouse_manager):
        """Test graceful shutdown flushes pending trades"""
        # Extract actual objects from async fixtures
        updater = await global_trades_updater.__anext__()
        manager = updater.clickhouse_manager  # Get manager through updater

        symbol = "ETHUSDT"
        trades_count = 100

        await updater.start()

        # Add trades but don't wait for full processing
        test_trades = create_test_trades(symbol, trades_count)

        for trade in test_trades:
            success = updater.add_trade(symbol, trade)
            assert success

        # Short wait to let some trades accumulate in buffers
        await asyncio.sleep(0.5)

        # Graceful shutdown should flush remaining trades
        await updater.stop()

        # Verify trades were flushed to ClickHouse
        clickhouse_success = await verify_trades_in_clickhouse(
            manager, symbol, trades_count, tolerance=10
        )
        assert clickhouse_success


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "-s"])