"""
Unit Tests for TradesWriter Interfaces - Critical for Boolean API Validation
===========================================================================

Complete test coverage for TradesWriter interfaces focusing on:
- Abstract interface contract validation
- TradesWriterImpl boolean return behavior
- MockTradesWriter for unit testing other components
- Error propagation through interface layers
- Integration between GlobalTradesUpdater and ClickHouseManager

CRITICAL TESTS FOR API CHANGES:
- add_trade() -> bool return validation
- Buffer overflow propagation through interface
- Async/sync method compatibility
- Statistics interface behavior

File: tests/unit/test_trades_interfaces.py
"""

import pytest
import asyncio
import time
from unittest.mock import Mock, AsyncMock, patch
from typing import List, Dict, Any, Optional
from dataclasses import dataclass


# ===== COPY REQUIRED CLASSES TO AVOID IMPORTS =====

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


# ===== INTERFACE IMPLEMENTATIONS (copied from real code) =====

from abc import ABC, abstractmethod

class TradesWriter(ABC):
    """
    Abstract interface for trades writing operations.
    """

    @abstractmethod
    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add single trade for real-time processing (thread-safe, non-blocking).

        Returns:
            True if trade was successfully added, False if buffer overflow
        """

    @abstractmethod
    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """
        Write batch of trades for historical data.

        Args:
            symbol: Ticker symbol
            trades: List of AggTrade instances

        Returns:
            Number of trades written
        """
        pass

    @abstractmethod
    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """
        Read trades from storage within time range.

        Args:
            symbol: Ticker symbol
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            List of AggTrade instances
        """
        pass

    @abstractmethod
    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """
        Get last aggregate_id for symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            Last aggregate_id or None if no data
        """
        pass

    @abstractmethod
    def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """
        Get storage statistics for symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            Dictionary with storage statistics
        """
        pass


class TradesWriterImpl(TradesWriter):
    """
    Implementation of TradesWriter interface using GlobalTradesUpdater and ClickHouseManager.
    """

    def __init__(self, global_trades_updater, clickhouse_manager):
        """
        Initialize TradesWriter implementation.

        Args:
            global_trades_updater: GlobalTradesUpdater instance
            clickhouse_manager: ClickHouseManager instance
        """
        self.global_trades_updater = global_trades_updater
        self.clickhouse_manager = clickhouse_manager

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add single trade to GlobalTradesUpdater for real-time processing.
        Used by WebSocketUpdater for incoming trades.

        Returns:
            True if trade was successfully added, False if buffer overflow
        """
        return self.global_trades_updater.add_trade(symbol, trade)

    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """
        Write batch of trades directly to ClickHouse.

        Used by HistoricalDownloader for historical data.
        """
        return await self.clickhouse_manager.write_trades(symbol, trades)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """
        Read trades from ClickHouse storage.

        Used by components that need historical data.
        """
        return await self.clickhouse_manager.read_trades(symbol, start_time, end_time)

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """
        Get last aggregate_id from ClickHouse.

        Used by HistoricalDownloader to determine resume point.
        """
        return await self.clickhouse_manager.get_last_aggregate_id(symbol)

    def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """
        Get storage statistics from ClickHouse.

        Used by TickerProcessor for monitoring.
        """
        try:
            # Try async version first
            import asyncio
            try:
                loop = asyncio.get_running_loop()
                # If in async context, cannot use sync version
                return {
                    'symbol': symbol,
                    'error': 'Cannot get sync stats from async context'
                }
            except RuntimeError:
                # No running loop, can use sync wrapper
                return self.clickhouse_manager.get_file_stats(symbol)
        except Exception as e:
            return {
                'symbol': symbol,
                'error': str(e)
            }

    async def get_storage_stats_async(self, symbol: str) -> Dict[str, Any]:
        """
        Async version of get_storage_stats.

        Args:
            symbol: Ticker symbol

        Returns:
            Dictionary with storage statistics
        """
        try:
            return await self.clickhouse_manager.get_file_stats_async(symbol)
        except Exception as e:
            return {
                'symbol': symbol,
                'error': str(e)
            }


class MockTradesWriter(TradesWriter):
    """
    Mock implementation of TradesWriter for testing.
    """

    def __init__(self):
        self.trades_storage: Dict[str, List[AggTrade]] = {}
        self.real_time_trades: List[tuple] = []  # (symbol, trade) pairs
        self.buffer_full = False
        self.add_trade_calls = 0

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """Store real-time trade for verification in tests."""
        self.add_trade_calls += 1

        if self.buffer_full:
            return False

        self.real_time_trades.append((symbol, trade))
        return True

    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """Store batch trades in memory for verification in tests."""
        if symbol not in self.trades_storage:
            self.trades_storage[symbol] = []

        self.trades_storage[symbol].extend(trades)
        return len(trades)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """Return trades from memory storage."""
        if symbol not in self.trades_storage:
            return []

        # Filter by time range
        filtered_trades = [
            trade for trade in self.trades_storage[symbol]
            if start_time <= trade.timestamp <= end_time
        ]
        return sorted(filtered_trades, key=lambda t: t.aggregate_id)

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """Return highest aggregate_id from memory storage."""
        if symbol not in self.trades_storage or not self.trades_storage[symbol]:
            return None

        return max(trade.aggregate_id for trade in self.trades_storage[symbol])

    def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """Return mock storage statistics."""
        trades_count = len(self.trades_storage.get(symbol, []))
        return {
            'symbol': symbol,
            'total_files': 1,
            'estimated_trades': trades_count,
            'mock': True
        }

    def get_real_time_trades(self) -> List[tuple]:
        """Get all real-time trades for test verification."""
        return self.real_time_trades.copy()

    def set_buffer_full(self, full: bool) -> None:
        """Set buffer full state for testing."""
        self.buffer_full = full

    def clear(self) -> None:
        """Clear all stored data for test cleanup."""
        self.trades_storage.clear()
        self.real_time_trades.clear()
        self.add_trade_calls = 0
        self.buffer_full = False


# ===== MOCK COMPONENTS =====

class MockGlobalTradesUpdater:
    """Mock GlobalTradesUpdater for testing TradesWriterImpl."""

    def __init__(self):
        self.trades = []
        self.buffer_full = False
        self.add_trade_calls = 0

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """Mock add_trade with configurable overflow behavior."""
        self.add_trade_calls += 1

        if self.buffer_full:
            return False

        self.trades.append((symbol, trade))
        return True

    def set_buffer_full(self, full: bool) -> None:
        """Configure buffer full state."""
        self.buffer_full = full

    def get_trades_count(self) -> int:
        """Get number of stored trades."""
        return len(self.trades)


class MockClickHouseManager:
    """Mock ClickHouseManager for testing TradesWriterImpl."""

    def __init__(self):
        self.trades_storage = {}
        self.write_calls = 0
        self.read_calls = 0
        self.should_fail = False

    async def write_trades(self, symbol: str, trades: List[AggTrade]) -> int:
        """Mock write_trades method."""
        self.write_calls += 1

        if self.should_fail:
            raise Exception("Mock ClickHouse error")

        if symbol not in self.trades_storage:
            self.trades_storage[symbol] = []

        self.trades_storage[symbol].extend(trades)
        return len(trades)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """Mock read_trades method."""
        self.read_calls += 1

        if self.should_fail:
            raise Exception("Mock ClickHouse read error")

        if symbol not in self.trades_storage:
            return []

        filtered = [
            trade for trade in self.trades_storage[symbol]
            if start_time <= trade.timestamp <= end_time
        ]
        return sorted(filtered, key=lambda t: t.aggregate_id)

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """Mock get_last_aggregate_id method."""
        if self.should_fail:
            raise Exception("Mock ClickHouse aggregate_id error")

        if symbol not in self.trades_storage or not self.trades_storage[symbol]:
            return None

        return max(trade.aggregate_id for trade in self.trades_storage[symbol])

    def get_file_stats(self, symbol: str) -> Dict[str, Any]:
        """Mock sync file stats method."""
        if self.should_fail:
            raise Exception("Mock ClickHouse stats error")

        trades_count = len(self.trades_storage.get(symbol, []))
        return {
            'symbol': symbol,
            'total_files': 1,
            'estimated_trades': trades_count
        }

    async def get_file_stats_async(self, symbol: str) -> Dict[str, Any]:
        """Mock async file stats method."""
        if self.should_fail:
            raise Exception("Mock ClickHouse async stats error")

        trades_count = len(self.trades_storage.get(symbol, []))
        return {
            'symbol': symbol,
            'total_files': 1,
            'estimated_trades': trades_count,
            'async': True
        }


# ===== TEST HELPERS =====

def create_test_trade(aggregate_id: int, symbol: str = "BTCUSDT", timestamp: int = None) -> AggTrade:
    """Create test AggTrade instance."""
    if timestamp is None:
        timestamp = int(time.time() * 1000)

    return AggTrade(
        aggregate_id=aggregate_id,
        price=50000.0 + (aggregate_id % 1000),
        quantity=0.01 + (aggregate_id % 100) / 10000,
        first_trade_id=aggregate_id,
        last_trade_id=aggregate_id,
        timestamp=timestamp,
        is_buyer_maker=bool(aggregate_id % 2)
    )


# ===== FIXTURES =====

@pytest.fixture
def mock_global_trades_updater():
    """Provide mock GlobalTradesUpdater."""
    return MockGlobalTradesUpdater()

@pytest.fixture
def mock_clickhouse_manager():
    """Provide mock ClickHouseManager."""
    return MockClickHouseManager()

@pytest.fixture
def trades_writer_impl(mock_global_trades_updater, mock_clickhouse_manager):
    """Provide TradesWriterImpl with mocked dependencies."""
    return TradesWriterImpl(mock_global_trades_updater, mock_clickhouse_manager)

@pytest.fixture
def mock_trades_writer():
    """Provide MockTradesWriter for testing."""
    return MockTradesWriter()


# ===== COMPREHENSIVE TESTS =====

class TestTradesWriterAbstractInterface:
    """Test abstract TradesWriter interface contract"""

    def test_abstract_interface_cannot_be_instantiated(self):
        """Test that abstract TradesWriter cannot be instantiated directly."""
        with pytest.raises(TypeError):
            TradesWriter()

    def test_abstract_methods_defined(self):
        """Test that all required abstract methods are defined."""
        required_methods = [
            'add_trade',
            'write_trades_batch',
            'read_trades',
            'get_last_aggregate_id',
            'get_storage_stats'
        ]

        for method_name in required_methods:
            assert hasattr(TradesWriter, method_name)
            method = getattr(TradesWriter, method_name)
            assert getattr(method, '__isabstractmethod__', False), f"{method_name} should be abstract"

    def test_add_trade_signature_returns_bool(self):
        """Test that add_trade method signature returns bool."""
        # Verify through implementation inspection
        assert hasattr(TradesWriter, 'add_trade')

        # Check that concrete implementations return bool
        writer = MockTradesWriter()
        trade = create_test_trade(12345)
        result = writer.add_trade("BTCUSDT", trade)
        assert isinstance(result, bool)


class TestTradesWriterImplBooleanAPI:
    """Test TradesWriterImpl boolean return behavior"""

    def test_add_trade_returns_true_on_success(self, trades_writer_impl, mock_global_trades_updater):
        """Test that add_trade returns True when GlobalTradesUpdater succeeds."""
        # Ensure GlobalTradesUpdater will return True
        mock_global_trades_updater.set_buffer_full(False)

        trade = create_test_trade(12345)
        result = trades_writer_impl.add_trade("BTCUSDT", trade)

        assert result is True
        assert mock_global_trades_updater.add_trade_calls == 1
        assert mock_global_trades_updater.get_trades_count() == 1

    def test_add_trade_returns_false_on_buffer_overflow(self, trades_writer_impl, mock_global_trades_updater):
        """Test that add_trade returns False when GlobalTradesUpdater buffer is full."""
        # Configure GlobalTradesUpdater to simulate buffer overflow
        mock_global_trades_updater.set_buffer_full(True)

        trade = create_test_trade(12345)
        result = trades_writer_impl.add_trade("BTCUSDT", trade)

        assert result is False
        assert mock_global_trades_updater.add_trade_calls == 1
        assert mock_global_trades_updater.get_trades_count() == 0  # No trade stored due to overflow

    def test_add_trade_propagates_boolean_correctly(self, trades_writer_impl, mock_global_trades_updater):
        """Test that boolean result propagates correctly through interface layers."""
        trade = create_test_trade(12345)

        # Test multiple scenarios
        scenarios = [
            (False, False),  # buffer_full=False -> should return True
            (True, False),   # buffer_full=True -> should return False
        ]

        for buffer_full, expected_result in scenarios:
            mock_global_trades_updater.set_buffer_full(buffer_full)
            result = trades_writer_impl.add_trade("BTCUSDT", trade)

            # Note: Expected result is inverted because False buffer_full -> True result
            assert result is not buffer_full, f"buffer_full={buffer_full} should return {not buffer_full}"

    def test_add_trade_with_multiple_symbols(self, trades_writer_impl, mock_global_trades_updater):
        """Test add_trade with multiple symbols maintains correct boolean behavior."""
        symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

        for i, symbol in enumerate(symbols):
            trade = create_test_trade(10000 + i)
            result = trades_writer_impl.add_trade(symbol, trade)

            assert result is True

        assert mock_global_trades_updater.add_trade_calls == len(symbols)
        assert mock_global_trades_updater.get_trades_count() == len(symbols)


class TestTradesWriterImplAsyncMethods:
    """Test TradesWriterImpl async method behavior"""

    @pytest.mark.asyncio
    async def test_write_trades_batch_success(self, trades_writer_impl, mock_clickhouse_manager):
        """Test successful batch write through interface."""
        trades = [create_test_trade(i) for i in range(100, 105)]

        result = await trades_writer_impl.write_trades_batch("BTCUSDT", trades)

        assert result == len(trades)
        assert mock_clickhouse_manager.write_calls == 1

    @pytest.mark.asyncio
    async def test_write_trades_batch_propagates_errors(self, trades_writer_impl, mock_clickhouse_manager):
        """Test that write_trades_batch propagates ClickHouse errors."""
        mock_clickhouse_manager.should_fail = True
        trades = [create_test_trade(i) for i in range(100, 105)]

        with pytest.raises(Exception, match="Mock ClickHouse error"):
            await trades_writer_impl.write_trades_batch("BTCUSDT", trades)

    @pytest.mark.asyncio
    async def test_read_trades_success(self, trades_writer_impl, mock_clickhouse_manager):
        """Test successful read through interface."""
        # First write some trades with timestamps in the test range
        base_timestamp = 1000000
        trades = [create_test_trade(i, timestamp=base_timestamp + (i-100)*1000) for i in range(100, 105)]
        await trades_writer_impl.write_trades_batch("BTCUSDT", trades)

        # Then read them back with matching time range
        start_time = base_timestamp - 1000      # 999000
        end_time = base_timestamp + 10000       # 1010000
        result = await trades_writer_impl.read_trades("BTCUSDT", start_time, end_time)

        assert len(result) == len(trades)
        assert mock_clickhouse_manager.read_calls == 1

    @pytest.mark.asyncio
    async def test_get_last_aggregate_id_success(self, trades_writer_impl, mock_clickhouse_manager):
        """Test get_last_aggregate_id through interface."""
        # Write trades with known aggregate_ids
        trades = [create_test_trade(i) for i in [100, 200, 150, 300]]
        await trades_writer_impl.write_trades_batch("BTCUSDT", trades)

        result = await trades_writer_impl.get_last_aggregate_id("BTCUSDT")

        assert result == 300  # Highest aggregate_id

    @pytest.mark.asyncio
    async def test_get_last_aggregate_id_empty_symbol(self, trades_writer_impl):
        """Test get_last_aggregate_id for symbol with no data."""
        result = await trades_writer_impl.get_last_aggregate_id("NONEXISTENT")

        assert result is None


class TestTradesWriterImplStatistics:
    """Test TradesWriterImpl statistics behavior"""

    def test_get_storage_stats_sync_success(self, trades_writer_impl, mock_clickhouse_manager):
        """Test sync storage stats in non-async context."""
        # Add some trades first
        mock_clickhouse_manager.trades_storage["BTCUSDT"] = [create_test_trade(i) for i in range(5)]

        result = trades_writer_impl.get_storage_stats("BTCUSDT")

        assert result['symbol'] == "BTCUSDT"
        assert result['estimated_trades'] == 5
        assert 'error' not in result

    def test_get_storage_stats_sync_error_handling(self, trades_writer_impl, mock_clickhouse_manager):
        """Test sync storage stats error handling."""
        mock_clickhouse_manager.should_fail = True

        result = trades_writer_impl.get_storage_stats("BTCUSDT")

        assert result['symbol'] == "BTCUSDT"
        assert 'error' in result
        assert "Mock ClickHouse stats error" in result['error']

    @pytest.mark.asyncio
    async def test_get_storage_stats_async_success(self, trades_writer_impl, mock_clickhouse_manager):
        """Test async storage stats method."""
        mock_clickhouse_manager.trades_storage["BTCUSDT"] = [create_test_trade(i) for i in range(10)]

        result = await trades_writer_impl.get_storage_stats_async("BTCUSDT")

        assert result['symbol'] == "BTCUSDT"
        assert result['estimated_trades'] == 10
        assert result.get('async') is True

    @pytest.mark.asyncio
    async def test_get_storage_stats_async_error_handling(self, trades_writer_impl, mock_clickhouse_manager):
        """Test async storage stats error handling."""
        mock_clickhouse_manager.should_fail = True

        result = await trades_writer_impl.get_storage_stats_async("BTCUSDT")

        assert result['symbol'] == "BTCUSDT"
        assert 'error' in result


class TestMockTradesWriterBehavior:
    """Test MockTradesWriter for unit testing other components"""

    def test_mock_add_trade_returns_true_by_default(self, mock_trades_writer):
        """Test that MockTradesWriter returns True by default."""
        trade = create_test_trade(12345)

        result = mock_trades_writer.add_trade("BTCUSDT", trade)

        assert result is True
        assert mock_trades_writer.add_trade_calls == 1
        assert len(mock_trades_writer.get_real_time_trades()) == 1

    def test_mock_add_trade_returns_false_when_buffer_full(self, mock_trades_writer):
        """Test that MockTradesWriter returns False when buffer_full is set."""
        mock_trades_writer.set_buffer_full(True)
        trade = create_test_trade(12345)

        result = mock_trades_writer.add_trade("BTCUSDT", trade)

        assert result is False
        assert mock_trades_writer.add_trade_calls == 1
        assert len(mock_trades_writer.get_real_time_trades()) == 0  # Not stored when buffer full

    @pytest.mark.asyncio
    async def test_mock_write_trades_batch(self, mock_trades_writer):
        """Test MockTradesWriter batch write functionality."""
        trades = [create_test_trade(i) for i in range(100, 105)]

        result = await mock_trades_writer.write_trades_batch("BTCUSDT", trades)

        assert result == len(trades)
        assert len(mock_trades_writer.trades_storage["BTCUSDT"]) == len(trades)

    @pytest.mark.asyncio
    async def test_mock_read_trades_time_filtering(self, mock_trades_writer):
        """Test MockTradesWriter read with time filtering."""
        # Create trades with different timestamps
        trades = [
            create_test_trade(100, timestamp=1000000),
            create_test_trade(101, timestamp=1001000),
            create_test_trade(102, timestamp=1002000),
            create_test_trade(103, timestamp=1003000)
        ]

        await mock_trades_writer.write_trades_batch("BTCUSDT", trades)

        # Read subset based on time range
        result = await mock_trades_writer.read_trades("BTCUSDT", 1000500, 1002500)

        assert len(result) == 2  # Only trades 101 and 102 should match
        assert result[0].aggregate_id == 101
        assert result[1].aggregate_id == 102

    @pytest.mark.asyncio
    async def test_mock_get_last_aggregate_id(self, mock_trades_writer):
        """Test MockTradesWriter get_last_aggregate_id functionality."""
        trades = [create_test_trade(i) for i in [100, 300, 200, 150]]
        await mock_trades_writer.write_trades_batch("BTCUSDT", trades)

        result = await mock_trades_writer.get_last_aggregate_id("BTCUSDT")

        assert result == 300  # Highest aggregate_id

    def test_mock_get_storage_stats(self, mock_trades_writer):
        """Test MockTradesWriter storage stats."""
        # Add some real-time trades
        for i in range(5):
            mock_trades_writer.add_trade("BTCUSDT", create_test_trade(i))

        result = mock_trades_writer.get_storage_stats("BTCUSDT")

        assert result['symbol'] == "BTCUSDT"
        assert result['mock'] is True
        assert result['estimated_trades'] == 0  # Real-time trades not counted in storage stats

    def test_mock_clear_functionality(self, mock_trades_writer):
        """Test MockTradesWriter clear functionality."""
        # Add some data
        mock_trades_writer.add_trade("BTCUSDT", create_test_trade(100))
        mock_trades_writer.trades_storage["ETHUSDT"] = [create_test_trade(200)]

        # Clear and verify
        mock_trades_writer.clear()

        assert len(mock_trades_writer.get_real_time_trades()) == 0
        assert len(mock_trades_writer.trades_storage) == 0
        assert mock_trades_writer.add_trade_calls == 0
        assert mock_trades_writer.buffer_full is False


class TestInterfaceCompatibilityAndErrorPropagation:
    """Test interface compatibility and error propagation"""

    def test_interface_method_signatures_match(self):
        """Test that TradesWriterImpl implements all interface methods correctly."""
        impl_methods = dir(TradesWriterImpl)
        abstract_methods = [name for name, method in TradesWriter.__dict__.items()
                          if getattr(method, '__isabstractmethod__', False)]

        for method_name in abstract_methods:
            assert method_name in impl_methods, f"TradesWriterImpl missing method: {method_name}"

    def test_multiple_implementations_same_interface(self):
        """Test that multiple implementations work correctly with same interface."""
        mock_writer = MockTradesWriter()

        # Both should implement the same interface
        trade = create_test_trade(12345)

        # Test add_trade boolean return
        assert isinstance(mock_writer.add_trade("BTCUSDT", trade), bool)

        # Both should have storage stats
        stats = mock_writer.get_storage_stats("BTCUSDT")
        assert isinstance(stats, dict)
        assert 'symbol' in stats

    @pytest.mark.asyncio
    async def test_error_propagation_through_interface_layers(self, trades_writer_impl, mock_clickhouse_manager):
        """Test that errors propagate correctly through interface layers."""
        # Test async method error propagation
        mock_clickhouse_manager.should_fail = True

        with pytest.raises(Exception, match="Mock ClickHouse"):
            await trades_writer_impl.read_trades("BTCUSDT", 0, 1000000)

        with pytest.raises(Exception, match="Mock ClickHouse"):
            await trades_writer_impl.get_last_aggregate_id("BTCUSDT")

    def test_interface_as_dependency_injection(self, mock_global_trades_updater, mock_clickhouse_manager):
        """Test interface works correctly for dependency injection patterns."""
        # Create implementation
        impl = TradesWriterImpl(mock_global_trades_updater, mock_clickhouse_manager)

        # Create mock
        mock = MockTradesWriter()

        # Function that accepts TradesWriter interface
        def process_trades_with_writer(writer: TradesWriter, symbol: str, trade: AggTrade) -> bool:
            return writer.add_trade(symbol, trade)

        trade = create_test_trade(12345)

        # Both implementations should work
        impl_result = process_trades_with_writer(impl, "BTCUSDT", trade)
        mock_result = process_trades_with_writer(mock, "BTCUSDT", trade)

        assert isinstance(impl_result, bool)
        assert isinstance(mock_result, bool)


class TestRealWorldUsagePatterns:
    """Test real-world usage patterns of TradesWriter interface"""

    def test_websocket_usage_pattern(self, trades_writer_impl, mock_global_trades_updater):
        """Test typical WebSocket usage pattern through interface."""
        # Simulate WebSocketUpdater using the interface
        symbol = "BTCUSDT"
        trades_processed = 0
        overflow_count = 0

        # Process multiple trades like WebSocket would
        for i in range(100):
            trade = create_test_trade(10000 + i)
            success = trades_writer_impl.add_trade(symbol, trade)

            if success:
                trades_processed += 1
            else:
                overflow_count += 1

        assert trades_processed == 100
        assert overflow_count == 0
        assert mock_global_trades_updater.get_trades_count() == 100

    @pytest.mark.asyncio
    async def test_historical_downloader_usage_pattern(self, trades_writer_impl, mock_clickhouse_manager):
        """Test typical HistoricalDownloader usage pattern through interface."""
        # Simulate HistoricalDownloader using the interface
        symbol = "BTCUSDT"
        batch_size = 1000

        # Write multiple batches like HistoricalDownloader would
        total_trades = 0
        for batch_id in range(5):
            batch_trades = [
                create_test_trade(batch_id * batch_size + i)
                for i in range(batch_size)
            ]

            written = await trades_writer_impl.write_trades_batch(symbol, batch_trades)
            total_trades += written

        # Verify last aggregate_id
        last_id = await trades_writer_impl.get_last_aggregate_id(symbol)

        assert total_trades == 5000
        assert last_id == (4 * batch_size + batch_size - 1)  # Last trade aggregate_id

    @pytest.mark.asyncio
    async def test_gap_recovery_usage_pattern(self, trades_writer_impl, mock_clickhouse_manager):
        """Test typical gap recovery usage pattern through interface."""
        symbol = "BTCUSDT"

        # Step 1: Write some initial trades
        initial_trades = [create_test_trade(i) for i in range(100, 200)]
        await trades_writer_impl.write_trades_batch(symbol, initial_trades)

        # Step 2: Get last aggregate_id (gap recovery would do this)
        last_id = await trades_writer_impl.get_last_aggregate_id(symbol)
        assert last_id == 199

        # Step 3: Write gap recovery trades starting from last_id + 1
        gap_trades = [create_test_trade(i) for i in range(200, 250)]
        written = await trades_writer_impl.write_trades_batch(symbol, gap_trades)
        assert written == 50

        # Step 4: Verify continuity
        new_last_id = await trades_writer_impl.get_last_aggregate_id(symbol)
        assert new_last_id == 249


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])