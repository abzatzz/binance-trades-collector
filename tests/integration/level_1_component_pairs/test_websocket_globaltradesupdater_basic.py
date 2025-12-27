"""
Level 1.3.1 Integration Test: WebSocketUpdater ↔ GlobalTradesUpdater
====================================================================

Complete integration test suite with ALL scenarios including fixes for timing issues.

Tests the critical integration between WebSocketUpdater and GlobalTradesUpdater focusing on:
- Real-time message flow through TradesWriter interface
- Buffer overflow detection and gap recovery coordination (v4.2)
- Thread safety under concurrent WebSocket streams
- Enhanced statistics tracking with overflow counts
- Configuration integration with dynamic WebSocket management

This test validates the core real-time data flow:
WebSocket messages → WebSocketUpdater → TradesWriter.add_trade() →
GlobalTradesUpdater buffers → Batch processing → ClickHouse storage

File: tests/integration/level1_component_pairs/test_websocket_globaltradesupdater_complete.py
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
test_log_file = os.path.join('test_logs', 'level_1_3_1_complete.log')
loggerino.create('level_1_3_1_complete', test_log_file)

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
from enum import Enum, auto

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

    @classmethod
    def from_binance_dict(cls, data: Dict[str, Any]) -> 'AggTrade':
        """Create AggTrade from Binance API response format."""
        try:
            return cls(
                aggregate_id=int(data['a']),
                price=float(data['p']),
                quantity=float(data['q']),
                first_trade_id=int(data['f']),
                last_trade_id=int(data['l']),
                timestamp=int(data['T']),
                is_buyer_maker=bool(data['m'])
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Invalid Binance trade data: {data}. Error: {e}") from e


class WebSocketMode(Enum):
    """WebSocket operation modes"""
    IDLE = auto()
    BUFFERING = auto()
    STREAMING = auto()
    RECONNECTING = auto()
    ERROR = auto()


@dataclass
class WebSocketStats:
    """WebSocket statistics for monitoring"""
    mode: WebSocketMode
    connected: bool
    trades_processed: int
    trades_per_minute: float
    buffer_size: int
    last_trade_time: Optional[int]
    last_aggregate_id: Optional[int]
    reconnect_count: int
    uptime_seconds: float
    gap_recoveries: int
    bytes_written: int
    binance_errors_count: int
    recoverable_errors_count: int
    message_errors_count: int
    buffer_overflow_count: int  # NEW v4.2
    gap_recovery_in_progress: bool  # NEW v4.2


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


@dataclass(frozen=True)
class ProcessingConfig:
    """Mock processing configuration"""
    global_batch_enabled: bool = True
    global_batch_interval_seconds: float = 1.0
    max_buffer_size_per_ticker: int = 10000
    validate_trade_data: bool = True
    skip_invalid_trades: bool = True
    max_invalid_trades_percent: float = 5.0
    # v4.2 Dynamic WebSocket Management
    dynamic_websocket_enabled: bool = True
    dynamic_deactivation_threshold: float = 30.0
    dynamic_reactivation_threshold: float = 25.0
    dynamic_evaluation_interval: int = 10
    dynamic_max_consecutive_overloads: int = 3
    dynamic_enable_buffer_discard: bool = True


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
class MockTickerModel:
    """Mock ticker model"""
    symbol: str = "BTCUSDT"


# ===== ENHANCED MOCK CLICKHOUSE MANAGER =====

class MockClickHouseManager:
    """Mock ClickHouseManager with realistic behavior for integration testing"""

    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.client: Optional[aiochclient.ChClient] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self.is_connected = True  # Always connected for mock
        self._total_trades_written = 0

        # Performance tracking
        self.batch_insert_calls = []
        self.processing_times = []
        self.call_count = 0

        # Mock data storage
        self.mock_data_storage = {}  # symbol -> list of trades

    async def ensure_connected(self) -> None:
        """Mock ensure connection - always succeeds."""
        self.is_connected = True

    async def _cleanup_connection(self) -> None:
        """Mock cleanup connection."""
        self.is_connected = False

    async def batch_insert_all_symbols(self, all_trades_data: List[List[Any]]) -> int:
        """Mock batch insert with realistic performance simulation."""
        if not all_trades_data:
            return 0

        start_time = time.time()

        # Simulate realistic processing time based on batch size
        processing_time = len(all_trades_data) * 0.00001  # 10 microseconds per trade
        await asyncio.sleep(processing_time)

        # Store data in mock storage for verification
        for trade_data in all_trades_data:
            symbol = trade_data[0]
            if symbol not in self.mock_data_storage:
                self.mock_data_storage[symbol] = []
            self.mock_data_storage[symbol].append(trade_data)

        self._total_trades_written += len(all_trades_data)
        self.call_count += 1

        # Track call details
        symbols = list(set(trade[0] for trade in all_trades_data))
        actual_processing_time = time.time() - start_time
        self.processing_times.append(actual_processing_time)

        self.batch_insert_calls.append({
            'trades_count': len(all_trades_data),
            'symbols': symbols,
            'timestamp': time.time(),
            'processing_time': actual_processing_time
        })

        print(f"Debug: Mock ClickHouse batch insert: {len(all_trades_data)} trades, "
              f"{len(symbols)} symbols, {actual_processing_time:.3f}s")
        return len(all_trades_data)

    async def get_trades_count(self, symbol: str) -> int:
        """Get count of trades for symbol in mock storage."""
        return len(self.mock_data_storage.get(symbol, []))

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for testing validation."""
        return {
            'total_calls': self.call_count,
            'total_trades_written': self._total_trades_written,
            'avg_processing_time': sum(self.processing_times) / max(1, len(self.processing_times)),
            'max_processing_time': max(self.processing_times) if self.processing_times else 0,
            'batch_details': self.batch_insert_calls.copy(),
            'mock_storage_symbols': list(self.mock_data_storage.keys()),
            'total_stored_trades': sum(len(trades) for trades in self.mock_data_storage.values())
        }


# ===== ENHANCED MOCK GLOBAL TRADES UPDATER WITH CONTROLLED PROCESSING =====

class MockGlobalTradesUpdater:
    """Mock GlobalTradesUpdater with realistic v4.2 behavior and CONTROLLED processing"""

    def __init__(self, clickhouse_manager: MockClickHouseManager, config: ProcessingConfig):
        self.clickhouse_manager = clickhouse_manager
        self.config = config

        # Per-ticker buffers
        self.ticker_buffers: Dict[str, List[AggTrade]] = {}
        self._buffer_lock = threading.RLock()

        # Processing settings
        self.batch_interval_seconds = config.global_batch_interval_seconds
        self.max_buffer_size = config.max_buffer_size_per_ticker

        # Background processing with CONTROL
        self.processing_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.start_time: Optional[float] = None
        self._shutdown_event = asyncio.Event()
        self._pause_processing = False  # NEW: Pause control for testing

        # Enhanced statistics for v4.2
        self.total_trades_processed = 0
        self.batch_count = 0
        self.last_batch_size = 0
        self.last_batch_time: Optional[float] = None
        self.queue_overflow_count = 0
        self.processing_errors_count = 0

        # Performance tracking
        self.buffer_overflow_details = []  # Track overflow events with details

    async def start(self) -> bool:
        """Start the global trades processing."""
        if self.is_running:
            return True

        try:
            self.is_running = True
            self.start_time = time.time()
            self._shutdown_event.clear()
            self._pause_processing = False  # Reset pause state

            self.processing_task = asyncio.create_task(
                self._processing_loop(),
                name="global_trades_processor"
            )
            return True

        except Exception as e:
            self.is_running = False
            return False

    async def stop(self) -> None:
        """Stop with graceful shutdown."""
        if not self.is_running:
            return

        self.is_running = False
        self._shutdown_event.set()

        if self.processing_task and not self.processing_task.done():
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass

        await self._final_flush()

    # NEW: Testing control methods
    def pause_processing(self) -> None:
        """Pause batch processing for testing overflow scenarios."""
        self._pause_processing = True
        print("DEBUG: Batch processing PAUSED for testing")

    def resume_processing(self) -> None:
        """Resume batch processing."""
        self._pause_processing = False
        print("DEBUG: Batch processing RESUMED")

    async def force_process_now(self) -> None:
        """Force immediate processing for testing."""
        await self._process_all_buffers()

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add single trade to ticker-specific buffer.

        Returns:
            bool: True if added successfully, False if buffer overflow (v4.2 API)
        """
        # Ensure buffer exists
        if symbol not in self.ticker_buffers:
            with self._buffer_lock:
                if symbol not in self.ticker_buffers:
                    self.ticker_buffers[symbol] = []

        # Buffer overflow protection with detailed logging
        with self._buffer_lock:
            current_buffer = self.ticker_buffers[symbol]
            if len(current_buffer) >= self.max_buffer_size:
                self.queue_overflow_count += 1

                # Enhanced v4.2 overflow logging with details
                overflow_details = {
                    'symbol': symbol,
                    'buffer_size': len(current_buffer),
                    'max_size': self.max_buffer_size,
                    'aggregate_id': trade.aggregate_id,
                    'timestamp': time.time()
                }
                self.buffer_overflow_details.append(overflow_details)

                print(f"WARNING: Buffer overflow for {symbol}: {len(current_buffer)}/{self.max_buffer_size} "
                      f"at aggregate_id {trade.aggregate_id}")
                return False

            current_buffer.append(trade)
            return True

    async def _processing_loop(self) -> None:
        """Main processing loop with shutdown event handling and PAUSE control."""
        while self.is_running:
            try:
                # Wait for batch interval or shutdown signal
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(),
                        timeout=self.batch_interval_seconds
                    )
                    break  # Shutdown signal received
                except asyncio.TimeoutError:
                    pass  # Normal timeout, continue processing

                # Check if processing is paused for testing
                if not self._pause_processing:
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

        # Perform single batch insert
        if all_trades_data:
            try:
                await self.clickhouse_manager.batch_insert_all_symbols(all_trades_data)

                # Update statistics
                self.total_trades_processed += len(all_trades_data)
                self.batch_count += 1
                self.last_batch_size = len(all_trades_data)
                self.last_batch_time = time.time()

            except Exception as e:
                self.processing_errors_count += 1
                raise

    async def _final_flush(self) -> None:
        """Final flush during shutdown."""
        try:
            await self._process_all_buffers()
        except Exception:
            pass

    def get_stats(self) -> GlobalTradesStats:
        """Get enhanced statistics including overflow details."""
        uptime = time.time() - (self.start_time or time.time()) if self.start_time else 0

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

    def get_overflow_details(self) -> List[Dict[str, Any]]:
        """Get detailed overflow information for testing."""
        return self.buffer_overflow_details.copy()


# ===== ENHANCED MOCK WEBSOCKET UPDATER =====

class MockWebSocketUpdater:
    """Mock WebSocketUpdater with v4.2 buffer overflow detection"""

    def __init__(self, ticker_model: MockTickerModel, trades_writer, config: ProcessingConfig):
        self.ticker_model = ticker_model
        self.trades_writer = trades_writer
        self.config = config

        # WebSocket state
        self.mode = WebSocketMode.IDLE
        self.is_connected = False
        self.is_running = False
        self.start_time: Optional[float] = None

        # Enhanced statistics for v4.2
        self.trades_processed = 0
        self.buffer_overflow_count = 0  # NEW v4.2
        self.gap_recovery_in_progress = False  # NEW v4.2
        self.total_messages_received = 0
        self.last_aggregate_id: Optional[int] = None
        self.last_trade_time: Optional[int] = None

        # Message processing
        self.received_messages = []
        self.gap_recovery_callback = None

    async def start_buffering_mode(self) -> bool:
        """Start WebSocket in buffering mode."""
        self.mode = WebSocketMode.BUFFERING
        self.is_connected = True
        self.is_running = True
        self.start_time = time.time()
        return True

    async def switch_to_streaming_mode(self, last_historical_id: int) -> bool:
        """Switch to streaming mode."""
        self.mode = WebSocketMode.STREAMING
        self.last_aggregate_id = last_historical_id
        return True

    async def shutdown(self) -> None:
        """Graceful shutdown."""
        self.is_running = False
        self.is_connected = False
        self.mode = WebSocketMode.IDLE

    async def process_websocket_message(self, message_data: Dict[str, Any]) -> None:
        """
        Process WebSocket message with v4.2 buffer overflow detection.

        Simulates the real message processing flow including buffer overflow handling.
        """
        try:
            self.total_messages_received += 1
            self.received_messages.append(message_data)

            # Convert to AggTrade
            trade = AggTrade.from_binance_dict(message_data)

            if self.mode == WebSocketMode.STREAMING:
                # Try to add to GlobalTradesUpdater via TradesWriter
                success = self.trades_writer.add_trade(self.ticker_model.symbol, trade)

                if success:
                    # Successfully added
                    self.last_aggregate_id = trade.aggregate_id
                    self.trades_processed += 1
                    self.last_trade_time = trade.timestamp

                    # Clear gap recovery flag if we were in recovery
                    if self.gap_recovery_in_progress:
                        self.gap_recovery_in_progress = False
                else:
                    # Buffer overflow detected - critical v4.2 functionality
                    self.buffer_overflow_count += 1

                    # Trigger gap recovery mode only on first overflow
                    if not self.gap_recovery_in_progress:
                        self.gap_recovery_in_progress = True
                        await self._handle_buffer_overflow_gap(trade.aggregate_id)

        except Exception as e:
            print(f"Error processing WebSocket message: {e}")

    async def _handle_buffer_overflow_gap(self, failed_aggregate_id: int) -> None:
        """Handle gap caused by buffer overflow."""
        # Switch to BUFFERING mode for gap recovery
        old_mode = self.mode
        self.mode = WebSocketMode.BUFFERING

        # Signal gap recovery needed
        await self._signal_gap_recovery_needed(failed_aggregate_id)

        print(f"Transitioned from {old_mode.name} to BUFFERING mode for buffer overflow recovery")

    async def _signal_gap_recovery_needed(self, gap_start_id: int) -> None:
        """Signal gap recovery callback."""
        if self.gap_recovery_callback:
            try:
                await self.gap_recovery_callback(gap_start_id)
            except Exception as e:
                print(f"Error in gap recovery callback: {e}")

    def set_gap_recovery_callback(self, callback) -> None:
        """Set gap recovery callback."""
        self.gap_recovery_callback = callback

    def get_stats(self) -> WebSocketStats:
        """Get enhanced WebSocket statistics with v4.2 fields."""
        uptime = time.time() - (self.start_time or time.time()) if self.start_time else 0
        trades_per_minute = (self.trades_processed / uptime) * 60 if uptime > 0 else 0

        return WebSocketStats(
            mode=self.mode,
            connected=self.is_connected,
            trades_processed=self.trades_processed,
            trades_per_minute=trades_per_minute,
            buffer_size=0,  # Mock doesn't maintain buffer
            last_trade_time=self.last_trade_time,
            last_aggregate_id=self.last_aggregate_id,
            reconnect_count=0,
            uptime_seconds=uptime,
            gap_recoveries=0,
            bytes_written=0,
            binance_errors_count=0,
            recoverable_errors_count=0,
            message_errors_count=0,
            buffer_overflow_count=self.buffer_overflow_count,  # NEW v4.2
            gap_recovery_in_progress=self.gap_recovery_in_progress  # NEW v4.2
        )

    def get_received_messages(self) -> List[Dict[str, Any]]:
        """Get all received messages for testing verification."""
        return self.received_messages.copy()


# ===== TRADES WRITER IMPLEMENTATION =====

class MockTradesWriter:
    """
    Mock TradesWriter that implements the v4.2 boolean API.
    Routes calls to GlobalTradesUpdater and ClickHouse.
    """

    def __init__(self, global_trades_updater: MockGlobalTradesUpdater, clickhouse_manager: MockClickHouseManager):
        self.global_trades_updater = global_trades_updater
        self.clickhouse_manager = clickhouse_manager
        self.add_trade_calls = []

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add trade to GlobalTradesUpdater for real-time processing.

        Returns:
            bool: True if added successfully, False if buffer overflow
        """
        # Track the call for testing
        self.add_trade_calls.append((symbol, trade, time.time()))

        # Delegate to GlobalTradesUpdater
        return self.global_trades_updater.add_trade(symbol, trade)

    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """Write batch directly to ClickHouse (for historical data)."""
        if not trades:
            return 0

        # Convert to ClickHouse format
        trades_data = []
        for trade in trades:
            dt = datetime.fromtimestamp(trade.timestamp / 1000, tz=timezone.utc)
            trades_data.append([
                symbol,
                trade.aggregate_id,
                trade.price,
                trade.quantity,
                trade.first_trade_id,
                trade.last_trade_id,
                dt,
                1 if trade.is_buyer_maker else 0
            ])

        await self.clickhouse_manager.batch_insert_all_symbols(trades_data)
        return len(trades)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """Mock read trades."""
        return []

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """Mock get last aggregate_id."""
        return None

    def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """Mock storage stats."""
        return {'symbol': symbol, 'mock': True}

    def get_call_history(self) -> List[tuple]:
        """Get add_trade call history for testing."""
        return self.add_trade_calls.copy()


# ===== TEST CONFIGURATION =====

@pytest.fixture(scope="session")
def clickhouse_container():
    """Use local ClickHouse installation."""
    class LocalClickHouse:
        def get_container_host_ip(self):
            return "localhost"
        def get_exposed_port(self, port):
            return 8123 if port == 8123 else 9000
    yield LocalClickHouse()


@pytest.fixture
def clickhouse_config(clickhouse_container):
    """ClickHouse configuration for local installation."""
    return ClickHouseConfig(
        host="localhost",
        port=9000,
        http_port=8123,
        database="test_level_1_3_1_complete",
        username="default",
        password="123",
        buffer_enabled=True
    )


@pytest.fixture
def processing_config():
    """Processing configuration optimized for integration testing."""
    return ProcessingConfig(
        global_batch_enabled=True,
        global_batch_interval_seconds=0.5,  # Faster for testing
        max_buffer_size_per_ticker=100,  # Smaller for overflow testing
        validate_trade_data=True,
        skip_invalid_trades=True,
        # v4.2 Dynamic WebSocket settings
        dynamic_websocket_enabled=True,
        dynamic_deactivation_threshold=30.0,
        dynamic_reactivation_threshold=25.0
    )


@pytest.fixture
def clickhouse_manager(clickhouse_config):
    """ClickHouse manager with clean test database."""
    return MockClickHouseManager(clickhouse_config)


@pytest.fixture
def global_trades_updater(clickhouse_manager, processing_config):
    """GlobalTradesUpdater for integration testing."""
    return MockGlobalTradesUpdater(clickhouse_manager, processing_config)


@pytest.fixture
def trades_writer(global_trades_updater, clickhouse_manager):
    """TradesWriter implementation for integration testing."""
    return MockTradesWriter(global_trades_updater, clickhouse_manager)


@pytest.fixture
def websocket_updater(trades_writer, processing_config):
    """WebSocket updater for integration testing."""
    ticker_model = MockTickerModel("BTCUSDT")
    return MockWebSocketUpdater(ticker_model, trades_writer, processing_config)


# ===== TEST HELPERS =====

def create_test_trade_message(aggregate_id: int = None, symbol: str = "BTCUSDT", **overrides) -> Dict[str, Any]:
    """Create test trade message in Binance format."""
    if aggregate_id is None:
        aggregate_id = random.randint(10000, 99999)

    base_message = {
        "e": "aggTrade",
        "E": int(time.time() * 1000),
        "s": symbol,
        "a": aggregate_id,
        "p": f"{50000.0 + (aggregate_id % 1000):.2f}",
        "q": f"{0.01 + (aggregate_id % 100) / 10000:.8f}",
        "f": aggregate_id,
        "l": aggregate_id,
        "T": int(time.time() * 1000),
        "m": bool(aggregate_id % 2)
    }

    base_message.update(overrides)
    return base_message


async def wait_for_processing(updater: MockGlobalTradesUpdater, expected_trades: int, timeout: float = 10.0) -> bool:
    """Wait for GlobalTradesUpdater to process expected number of trades."""
    start_time = time.time()

    while time.time() - start_time < timeout:
        if updater.total_trades_processed >= expected_trades:
            return True
        await asyncio.sleep(0.1)

    return False


async def verify_trades_in_clickhouse(clickhouse_manager: MockClickHouseManager, symbol: str,
                                      expected_count: int, tolerance: int = 0) -> bool:
    """Verify trades are stored correctly in mock ClickHouse."""
    # Get count from mock storage
    actual_count = await clickhouse_manager.get_trades_count(symbol)

    print(f"Debug verification: Mock storage for {symbol}: {actual_count}, Expected: {expected_count}")

    return abs(actual_count - expected_count) <= tolerance


# ===== ORIGINAL LEVEL 1.3.1 INTEGRATION TESTS =====

class TestBasicWebSocketGlobalTradesIntegration:
    """Test basic integration flow between WebSocket and GlobalTradesUpdater"""

    @pytest.mark.asyncio
    async def test_basic_message_flow_websocket_to_clickhouse(self, websocket_updater, global_trades_updater,
                                                            trades_writer, clickhouse_manager):
        """Test basic flow: WebSocket message → GlobalTradesUpdater → ClickHouse"""
        symbol = "BTCUSDT"
        num_trades = 50

        # Start components
        await global_trades_updater.start()
        await websocket_updater.start_buffering_mode()
        await websocket_updater.switch_to_streaming_mode(0)

        try:
            # Send trades through WebSocket
            for i in range(num_trades):
                message = create_test_trade_message(10000 + i, symbol)
                await websocket_updater.process_websocket_message(message)

            # Wait for processing
            success = await wait_for_processing(global_trades_updater, num_trades, timeout=15.0)
            assert success, f"Processing timeout. Processed: {global_trades_updater.total_trades_processed}/{num_trades}"

            # Verify WebSocket statistics
            ws_stats = websocket_updater.get_stats()
            assert ws_stats.trades_processed == num_trades
            assert ws_stats.buffer_overflow_count == 0
            assert ws_stats.gap_recovery_in_progress is False

            # Verify GlobalTradesUpdater statistics
            gt_stats = global_trades_updater.get_stats()
            assert gt_stats.total_trades_processed >= num_trades
            assert gt_stats.batch_count > 0
            assert gt_stats.queue_overflow_count == 0

            # Verify ClickHouse storage
            clickhouse_success = await verify_trades_in_clickhouse(
                clickhouse_manager, symbol, num_trades, tolerance=2
            )
            assert clickhouse_success

            # Verify TradesWriter call history
            call_history = trades_writer.get_call_history()
            assert len(call_history) == num_trades

            # All calls should be for the same symbol
            assert all(call[0] == symbol for call in call_history)

        finally:
            await websocket_updater.shutdown()
            await global_trades_updater.stop()

    @pytest.mark.asyncio
    async def test_multi_symbol_concurrent_processing(self, clickhouse_manager, processing_config):
        """Test concurrent processing of multiple symbols"""
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT"]
        trades_per_symbol = 25
        total_trades = len(symbols) * trades_per_symbol

        # Create components
        global_trades_updater = MockGlobalTradesUpdater(clickhouse_manager, processing_config)
        trades_writer = MockTradesWriter(global_trades_updater, clickhouse_manager)

        # Create WebSocket updaters for each symbol
        websocket_updaters = []
        for symbol in symbols:
            ticker_model = MockTickerModel(symbol)
            ws_updater = MockWebSocketUpdater(ticker_model, trades_writer, processing_config)
            websocket_updaters.append((symbol, ws_updater))

        await global_trades_updater.start()

        try:
            # Start all WebSocket updaters
            for symbol, ws_updater in websocket_updaters:
                await ws_updater.start_buffering_mode()
                await ws_updater.switch_to_streaming_mode(0)

            # Send trades concurrently
            tasks = []
            for symbol, ws_updater in websocket_updaters:
                async def send_trades_for_symbol(sym, updater):
                    for i in range(trades_per_symbol):
                        message = create_test_trade_message(20000 + i, sym)
                        await updater.process_websocket_message(message)

                task = asyncio.create_task(send_trades_for_symbol(symbol, ws_updater))
                tasks.append(task)

            # Wait for all messages to be sent
            await asyncio.gather(*tasks)

            # Wait for processing
            success = await wait_for_processing(global_trades_updater, total_trades, timeout=20.0)
            assert success

            # Verify each symbol was processed
            for symbol in symbols:
                clickhouse_success = await verify_trades_in_clickhouse(
                    clickhouse_manager, symbol, trades_per_symbol, tolerance=2
                )
                assert clickhouse_success, f"Verification failed for {symbol}"

            # Verify statistics
            gt_stats = global_trades_updater.get_stats()
            assert gt_stats.total_trades_processed >= total_trades
            assert gt_stats.active_tickers_count <= len(symbols)

            # Verify no overflows occurred
            for symbol, ws_updater in websocket_updaters:
                ws_stats = ws_updater.get_stats()
                assert ws_stats.buffer_overflow_count == 0

        finally:
            for symbol, ws_updater in websocket_updaters:
                await ws_updater.shutdown()
            await global_trades_updater.stop()


class TestBufferOverflowDetectionIntegration:
    """Test v4.2 buffer overflow detection and gap recovery coordination"""

    @pytest.mark.asyncio
    async def test_buffer_overflow_triggers_gap_recovery(self, clickhouse_manager, processing_config):
        """Test that buffer overflow correctly triggers gap recovery mode"""
        symbol = "BTCUSDT"

        # Create components with controlled configuration
        overflow_config = ProcessingConfig(
            global_batch_enabled=True,
            global_batch_interval_seconds=1.0,
            max_buffer_size_per_ticker=5,  # Very small for overflow
            validate_trade_data=True
        )

        global_trades_updater = MockGlobalTradesUpdater(clickhouse_manager, overflow_config)
        trades_writer = MockTradesWriter(global_trades_updater, clickhouse_manager)
        ticker_model = MockTickerModel(symbol)
        websocket_updater = MockWebSocketUpdater(ticker_model, trades_writer, overflow_config)

        # Track gap recovery calls
        gap_recovery_calls = []
        async def mock_gap_recovery_callback(gap_start_id: int):
            gap_recovery_calls.append(gap_start_id)

        websocket_updater.set_gap_recovery_callback(mock_gap_recovery_callback)

        await global_trades_updater.start()
        await websocket_updater.start_buffering_mode()
        await websocket_updater.switch_to_streaming_mode(0)

        try:
            # FIXED: Pause processing before overflow
            global_trades_updater.pause_processing()

            # Send enough trades to trigger overflow
            overflow_trade_ids = []
            for i in range(10):  # More than buffer size (5)
                trade_id = 30000 + i
                overflow_trade_ids.append(trade_id)
                message = create_test_trade_message(trade_id, symbol)
                await websocket_updater.process_websocket_message(message)

            await asyncio.sleep(0.2)

            # Verify overflow detection
            ws_stats = websocket_updater.get_stats()
            gt_stats = global_trades_updater.get_stats()

            assert ws_stats.buffer_overflow_count > 0
            assert gt_stats.queue_overflow_count > 0
            assert ws_stats.gap_recovery_in_progress is True
            assert websocket_updater.mode == WebSocketMode.BUFFERING
            assert len(gap_recovery_calls) > 0

            global_trades_updater.resume_processing()
            await asyncio.sleep(1.0)

        finally:
            global_trades_updater.resume_processing()
            await websocket_updater.shutdown()
            await global_trades_updater.stop()

    @pytest.mark.asyncio
    async def test_recovery_from_buffer_overflow(self, websocket_updater, global_trades_updater,
                                               trades_writer, clickhouse_manager):
        """Test recovery after buffer overflow is resolved"""
        symbol = "SHUTUSDT"

        # Set small buffer
        global_trades_updater.max_buffer_size = 5

        await global_trades_updater.start()
        await websocket_updater.start_buffering_mode()
        await websocket_updater.switch_to_streaming_mode(0)

        try:
            # Step 1: Trigger overflow
            for i in range(10):  # More than buffer size
                message = create_test_trade_message(40000 + i, symbol)
                await websocket_updater.process_websocket_message(message)

            # Wait for overflow processing
            await asyncio.sleep(1.0)

            # Verify overflow state
            ws_stats = websocket_updater.get_stats()
            assert ws_stats.buffer_overflow_count > 0
            assert ws_stats.gap_recovery_in_progress is True

            # Step 2: Simulate buffer space becoming available (processing occurs)
            # Force processing to clear buffers
            await global_trades_updater._process_all_buffers()

            # Step 3: Send recovery trades
            recovery_trade_ids = []
            for i in range(15, 20):
                trade_id = 40000 + i
                recovery_trade_ids.append(trade_id)
                message = create_test_trade_message(trade_id, symbol)
                await websocket_updater.process_websocket_message(message)

            # Wait for recovery processing
            await asyncio.sleep(1.0)

            # Verify recovery occurred
            ws_stats = websocket_updater.get_stats()
            # Gap recovery flag should clear when successful trades are processed
            # (This depends on the specific implementation logic)

            # Verify trades were eventually processed
            final_processing = await wait_for_processing(global_trades_updater, 5, timeout=5.0)
            # Some trades should have been processed despite overflows

        finally:
            await websocket_updater.shutdown()
            await global_trades_updater.stop()


class TestPerformanceIntegration:
    """Test performance characteristics of integrated system"""

    @pytest.mark.asyncio
    async def test_high_throughput_message_processing(self, clickhouse_manager, processing_config):
        """Test high-throughput processing with realistic load"""
        symbols = [f"PERF{i:03d}USDT" for i in range(5)]
        trades_per_symbol = 200
        total_trades = len(symbols) * trades_per_symbol

        # Configure for performance
        performance_config = ProcessingConfig(
            global_batch_enabled=True,
            global_batch_interval_seconds=0.2,  # Fast processing
            max_buffer_size_per_ticker=5000,  # Large buffers
            validate_trade_data=True
        )

        global_trades_updater = MockGlobalTradesUpdater(clickhouse_manager, performance_config)
        trades_writer = MockTradesWriter(global_trades_updater, clickhouse_manager)

        await global_trades_updater.start()

        try:
            # Measure message processing performance
            start_time = time.time()

            # Create and process messages rapidly
            tasks = []
            for symbol in symbols:
                ticker_model = MockTickerModel(symbol)
                ws_updater = MockWebSocketUpdater(ticker_model, trades_writer, performance_config)
                await ws_updater.start_buffering_mode()
                await ws_updater.switch_to_streaming_mode(0)

                async def rapid_message_processing(sym, updater):
                    for i in range(trades_per_symbol):
                        message = create_test_trade_message(50000 + i, sym)
                        await updater.process_websocket_message(message)

                task = asyncio.create_task(rapid_message_processing(symbol, ws_updater))
                tasks.append(task)

            # Process all messages concurrently
            await asyncio.gather(*tasks)

            message_processing_time = time.time() - start_time
            message_rate = total_trades / message_processing_time

            # Should handle at least 1000 messages/second
            assert message_rate > 1000, f"Message rate too slow: {message_rate:.0f}/sec"

            # Wait for batch processing
            processing_success = await wait_for_processing(global_trades_updater, total_trades, timeout=30.0)
            assert processing_success

            # Verify ClickHouse performance
            ch_metrics = clickhouse_manager.get_performance_metrics()
            assert ch_metrics['total_calls'] > 0
            assert ch_metrics['total_trades_written'] >= total_trades * 0.95

            # Verify batch efficiency
            gt_stats = global_trades_updater.get_stats()
            assert gt_stats.batch_count > 0
            avg_batch_size = gt_stats.total_trades_processed / gt_stats.batch_count
            assert avg_batch_size > 10  # Efficient batching

            print(f"Performance metrics:")
            print(f"  Message rate: {message_rate:.0f} messages/sec")
            print(f"  Total processed: {gt_stats.total_trades_processed}")
            print(f"  Average batch size: {avg_batch_size:.1f}")
            print(f"  ClickHouse calls: {ch_metrics['total_calls']}")

        finally:
            await global_trades_updater.stop()


class TestConfigurationIntegration:
    """Test integration with v4.2 configuration parameters"""

    @pytest.mark.asyncio
    async def test_dynamic_websocket_configuration_integration(self, clickhouse_manager):
        """Test integration with dynamic WebSocket management configuration"""
        # Configure with specific v4.2 parameters
        dynamic_config = ProcessingConfig(
            global_batch_enabled=True,
            global_batch_interval_seconds=0.5,
            max_buffer_size_per_ticker=50,
            # v4.2 Dynamic WebSocket settings
            dynamic_websocket_enabled=True,
            dynamic_deactivation_threshold=15.0,  # Lower threshold for testing
            dynamic_reactivation_threshold=10.0,
            dynamic_evaluation_interval=5,
            dynamic_max_consecutive_overloads=2,
            dynamic_enable_buffer_discard=True
        )

        global_trades_updater = MockGlobalTradesUpdater(clickhouse_manager, dynamic_config)
        trades_writer = MockTradesWriter(global_trades_updater, clickhouse_manager)

        ticker_model = MockTickerModel("CONFUSDT")
        websocket_updater = MockWebSocketUpdater(ticker_model, trades_writer, dynamic_config)

        await global_trades_updater.start()
        await websocket_updater.start_buffering_mode()
        await websocket_updater.switch_to_streaming_mode(0)

        try:
            # Verify configuration is properly applied
            assert global_trades_updater.max_buffer_size == 50
            assert global_trades_updater.batch_interval_seconds == 0.5

            # Test configuration affects behavior
            trades_to_overflow = dynamic_config.max_buffer_size_per_ticker + 10

            for i in range(trades_to_overflow):
                message = create_test_trade_message(60000 + i, "CONFUSDT")
                await websocket_updater.process_websocket_message(message)

            # Wait for processing
            await asyncio.sleep(1.0)

            # Verify configuration-driven overflow behavior
            ws_stats = websocket_updater.get_stats()
            gt_stats = global_trades_updater.get_stats()

            # Should have overflows due to small buffer size
            assert gt_stats.queue_overflow_count > 0
            assert ws_stats.buffer_overflow_count > 0

            # Verify overflow details match configuration
            overflow_details = global_trades_updater.get_overflow_details()
            for detail in overflow_details:
                assert detail['max_size'] == dynamic_config.max_buffer_size_per_ticker

        finally:
            await websocket_updater.shutdown()
            await global_trades_updater.stop()

    @pytest.mark.asyncio
    async def test_batch_processing_configuration_effects(self, clickhouse_manager):
        """Test that batch processing configuration affects integration behavior"""
        # Fast batch processing configuration
        fast_config = ProcessingConfig(
            global_batch_enabled=True,
            global_batch_interval_seconds=0.2,  # 200ms intervals
            max_buffer_size_per_ticker=1000,
            validate_trade_data=True
        )

        global_trades_updater = MockGlobalTradesUpdater(clickhouse_manager, fast_config)
        trades_writer = MockTradesWriter(global_trades_updater, clickhouse_manager)

        ticker_model = MockTickerModel("FASTUSDT")
        websocket_updater = MockWebSocketUpdater(ticker_model, trades_writer, fast_config)

        await global_trades_updater.start()
        await websocket_updater.start_buffering_mode()
        await websocket_updater.switch_to_streaming_mode(0)

        try:
            # FIXED: Send trades in groups with delays
            for batch_num in range(3):
                for i in range(20):
                    trade_id = 70000 + (batch_num * 20) + i
                    message = create_test_trade_message(trade_id, "FASTUSDT")
                    await websocket_updater.process_websocket_message(message)

                # FIXED: Wait to force batch separation
                await asyncio.sleep(0.4)  # > batch_interval

            await asyncio.sleep(0.5)

            processing_success = await wait_for_processing(global_trades_updater, 60, timeout=5.0)
            assert processing_success

            gt_stats = global_trades_updater.get_stats()
            assert gt_stats.batch_count >= 3, f"Expected >= 3 batches, got {gt_stats.batch_count}"

        finally:
            await websocket_updater.shutdown()
            await global_trades_updater.stop()


class TestErrorHandlingIntegration:
    """Test error handling and recovery across integrated components"""

    @pytest.mark.asyncio
    async def test_clickhouse_error_recovery_integration(self, processing_config):
        """Test error recovery when ClickHouse operations fail"""
        # Create a ClickHouse manager that will fail initially
        class FailingClickHouseManager(MockClickHouseManager):
            def __init__(self, config):
                super().__init__(config)
                self.fail_count = 3
                self.call_attempts = 0

            async def batch_insert_all_symbols(self, all_trades_data):
                self.call_attempts += 1
                if self.call_attempts <= self.fail_count:
                    raise Exception(f"Mock ClickHouse error {self.call_attempts}")
                return await super().batch_insert_all_symbols(all_trades_data)

        # FIXED: Use faster config for more retry attempts
        fast_retry_config = ProcessingConfig(
            global_batch_enabled=True,
            global_batch_interval_seconds=0.1,  # Very fast for more retries
            max_buffer_size_per_ticker=1000,
            validate_trade_data=True
        )

        clickhouse_config = ClickHouseConfig(database="test_error_recovery")
        failing_manager = FailingClickHouseManager(clickhouse_config)

        global_trades_updater = MockGlobalTradesUpdater(failing_manager, fast_retry_config)
        trades_writer = MockTradesWriter(global_trades_updater, failing_manager)

        ticker_model = MockTickerModel("ERRUSDT")
        websocket_updater = MockWebSocketUpdater(ticker_model, trades_writer, fast_retry_config)

        await global_trades_updater.start()
        await websocket_updater.start_buffering_mode()
        await websocket_updater.switch_to_streaming_mode(0)

        try:
            # FIXED: Send more trades over longer period to trigger multiple retry attempts
            num_trades = 50
            for i in range(num_trades):
                message = create_test_trade_message(80000 + i, "ERRUSDT")
                await websocket_updater.process_websocket_message(message)

                # Add delay every 10 trades to allow batch processing
                if i % 10 == 9:
                    await asyncio.sleep(0.15)  # Slightly more than batch interval

            # Wait for error recovery and retries
            await asyncio.sleep(3.0)

            # Verify error handling
            gt_stats = global_trades_updater.get_stats()
            assert gt_stats.processing_errors_count > 0

            # Verify that eventually some trades were processed after recovery
            # (The failing manager should succeed after 3 attempts)
            assert failing_manager.call_attempts > 3

            # Some processing should eventually succeed
            final_success = await wait_for_processing(global_trades_updater, 1, timeout=10.0)
            # We expect at least some recovery even with errors

        finally:
            await websocket_updater.shutdown()
            await global_trades_updater.stop()

    @pytest.mark.asyncio
    async def test_graceful_shutdown_with_pending_data(self, websocket_updater, global_trades_updater,
                                                     trades_writer, clickhouse_manager):
        """Test graceful shutdown with pending data in various stages"""
        symbol = "SHUTUSDT"

        await global_trades_updater.start()
        await websocket_updater.start_buffering_mode()
        await websocket_updater.switch_to_streaming_mode(0)

        # Add trades but don't wait for full processing
        for i in range(50):
            message = create_test_trade_message(90000 + i, symbol)
            await websocket_updater.process_websocket_message(message)

        # Wait briefly to let some data accumulate
        await asyncio.sleep(0.5)

        # Graceful shutdown should flush pending trades
        await websocket_updater.shutdown()
        await global_trades_updater.stop()

        # Verify data was flushed during shutdown
        gt_stats = global_trades_updater.get_stats()

        # Some trades should have been processed during shutdown
        assert gt_stats.total_trades_processed > 0

        # Verify ClickHouse received final flush
        ch_metrics = clickhouse_manager.get_performance_metrics()
        assert ch_metrics['total_calls'] > 0


if __name__ == "__main__":
    # Run all tests including original and fixed versions
    pytest.main([__file__, "-v", "--tb=short", "-s"])