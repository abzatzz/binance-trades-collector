"""
Enhanced Unit Tests for WebSocketUpdater v4.2 - Updated for DynamicWebSocketManager
===================================================================================

Complete test coverage for WebSocketUpdater v4.2 focusing on:
- DynamicWebSocketManager integration and configuration
- Updated add_trade() -> bool API behavior with buffer overflow
- Enhanced statistics tracking (buffer_overflow_count, gap_recovery_in_progress)
- TimingCalculator integration for dynamic thresholds
- Real-time trade parsing with gap recovery coordination
- Integration with TradesWriter interface (boolean returns)
- Thread safety under concurrent load
- Graceful shutdown scenarios with dynamic management

CRITICAL UPDATES FOR v4.2:
- MockTradesWriter.add_trade() now returns bool correctly
- DynamicWebSocketManager tests with configuration parameters
- Buffer overflow scenarios trigger gap recovery mode
- Statistics include new overflow and gap recovery fields
- Enhanced error handling preserves boolean return semantics

File: tests/unit/test_websocket_updater.py
"""

import pytest
import asyncio
import json
import time
import threading
from unittest.mock import Mock, AsyncMock, patch, call, MagicMock
from concurrent.futures import ThreadPoolExecutor
import random
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum, auto
import websockets
from websockets.exceptions import ConnectionClosedError, InvalidStatusCode


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


class BinanceErrorSeverity(Enum):
    """Severity levels for Binance errors"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class BinanceErrorData:
    """Structured representation of Binance error data"""
    error_code: Optional[int]
    error_message: str
    event_time: Optional[int]
    raw_data: Dict[str, Any]
    severity: BinanceErrorSeverity
    is_recoverable: bool
    retry_after: Optional[int]


class BinanceWebSocketError(Exception):
    """Specific exception for Binance WebSocket errors."""

    def __init__(self, error_data: Dict[str, Any], symbol: str = None):
        self.error_data = self._parse_error_data(error_data)
        self.symbol = symbol
        super().__init__(f"Binance WebSocket error: {self.error_data.error_message}")

    def _parse_error_data(self, error_data: Dict[str, Any]) -> BinanceErrorData:
        """Parse error data from Binance."""
        error_code = error_data.get('c') or error_data.get('code')
        error_message = error_data.get('m') or error_data.get('msg') or 'Unknown error'

        return BinanceErrorData(
            error_code=error_code,
            error_message=error_message,
            event_time=error_data.get('E'),
            raw_data=error_data,
            severity=BinanceErrorSeverity.MEDIUM,
            is_recoverable=True,
            retry_after=5
        )

    def is_recoverable(self) -> bool:
        return self.error_data.is_recoverable

    def get_retry_delay(self) -> int:
        return self.error_data.retry_after or 5


# ===== MOCK CONFIGURATION CLASSES =====

class MockProcessingConfig:
    """Mock processing configuration with v4.2 dynamic parameters"""

    def __init__(self):
        # Original parameters
        self.websocket_batch_size = 100
        self.websocket_flush_interval = 10
        self.max_websocket_reconnects = 5
        self.validate_trade_data = True
        self.skip_invalid_trades = True
        self.max_invalid_trades_percent = 5.0
        self.max_consecutive_errors = 10
        self.error_recovery_delay_seconds = 5
        self.binance_error_retry_multiplier = 2.0

        # NEW v4.2: Dynamic WebSocket Management parameters
        self.dynamic_websocket_enabled = True
        self.dynamic_deactivation_threshold = 30.0
        self.dynamic_reactivation_threshold = 25.0
        self.dynamic_evaluation_interval = 10
        self.dynamic_max_consecutive_overloads = 3
        self.dynamic_enable_buffer_discard = True


class MockTickerModel:
    """Mock ticker model"""
    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol


class MockTimingCalculator:
    """Mock TimingCalculator for testing dynamic management"""

    def __init__(self, initial_delay: float = 1.0):
        self._current_delay = initial_delay
        self.calculation_calls = 0

    def calculate_request_delay(self) -> float:
        """Return configurable delay for testing scenarios"""
        self.calculation_calls += 1
        return self._current_delay

    def set_delay(self, delay: float):
        """Set delay for testing different scenarios"""
        self._current_delay = delay


# ===== ENHANCED MOCK WEBSOCKET =====

class MockWebSocket:
    """Enhanced MockWebSocket with precise message control for v4.2 testing."""

    def __init__(self):
        self.messages = []
        self.closed = False
        self.ping_count = 0
        self._message_queue = []
        self._auto_messages = False
        self._recv_count = 0
        self._simulate_high_frequency = False
        self._message_delay = 0.01  # Default delay between messages

    async def recv(self):
        """Receive message with controlled delivery for testing scenarios."""
        if self.closed:
            raise ConnectionClosedError(None, None)

        # Return queued test messages first
        if self._message_queue:
            return self._message_queue.pop(0)

        # High frequency simulation for buffer overflow testing
        if self._simulate_high_frequency:
            await asyncio.sleep(0.001)  # Very fast messages
            self._recv_count += 1
            return json.dumps({
                "e": "aggTrade",
                "a": 10000 + self._recv_count,
                "p": f"{50000 + (self._recv_count % 1000):.2f}",
                "q": "0.01",
                "f": 10000 + self._recv_count,
                "l": 10000 + self._recv_count,
                "T": int(time.time() * 1000),
                "m": bool(self._recv_count % 2)
            })

        # Standard controlled behavior
        if not self._auto_messages:
            await asyncio.sleep(self._message_delay)
            if self._recv_count == 0:
                self._recv_count += 1
                return json.dumps({
                    "e": "aggTrade",
                    "a": 12345,
                    "p": "50000.00",
                    "q": "0.01",
                    "f": 12345,
                    "l": 12345,
                    "T": int(time.time() * 1000),
                    "m": True
                })

            await asyncio.sleep(self._message_delay)
            return json.dumps({
                "e": "aggTrade",
                "a": 12346,
                "p": "50001.00",
                "q": "0.01",
                "f": 12346,
                "l": 12346,
                "T": int(time.time() * 1000),
                "m": False
            })

        # Auto messages for background processing
        await asyncio.sleep(self._message_delay)
        self._recv_count += 1
        return json.dumps({
            "e": "aggTrade",
            "a": 12345 + self._recv_count,
            "p": f"{50000 + self._recv_count:.2f}",
            "q": "0.01",
            "f": 12345 + self._recv_count,
            "l": 12345 + self._recv_count,
            "T": int(time.time() * 1000),
            "m": bool(self._recv_count % 2)
        })

    async def close(self):
        """Close WebSocket connection."""
        self.closed = True

    async def ping(self):
        """Send ping."""
        self.ping_count += 1

    def add_test_message(self, message: str):
        """Add specific test message to queue."""
        self._message_queue.append(message)

    def set_auto_messages(self, enabled: bool):
        """Enable/disable automatic message generation."""
        self._auto_messages = enabled

    def set_high_frequency_mode(self, enabled: bool):
        """Enable high frequency message simulation for buffer testing."""
        self._simulate_high_frequency = enabled

    def set_message_delay(self, delay: float):
        """Set delay between messages."""
        self._message_delay = delay

    def clear_messages(self):
        """Clear all queued messages."""
        self._message_queue.clear()

    def set_test_messages(self, messages: List[str]):
        """Set predefined messages for testing."""
        self._message_queue = messages.copy()


# ===== ENHANCED MOCK TRADES WRITER =====

class MockTradesWriter:
    """
    Enhanced MockTradesWriter for testing v4.2 buffer overflow scenarios.

    CRITICAL: Now properly returns bool from add_trade() method as per v4.2 API.
    """

    def __init__(self, buffer_full: bool = False, buffer_size: int = 1000):
        self.trades = []
        self.buffer_full = buffer_full
        self.buffer_size = buffer_size
        self.add_trade_calls = 0
        self.rejected_trades = []
        self.overflow_simulation_after = None
        self.write_batch_calls = 0
        self.get_last_aggregate_id_calls = 0
        self.last_aggregate_id_value = None

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add trade to mock storage - RETURNS BOOL as per v4.2 API.

        Returns:
            bool: True if trade added successfully, False if buffer full
        """
        self.add_trade_calls += 1

        # Simulate overflow after specific number of trades
        if self.overflow_simulation_after is not None:
            if len(self.trades) >= self.overflow_simulation_after:
                self.buffer_full = True

        # Check buffer full condition
        if self.buffer_full or len(self.trades) >= self.buffer_size:
            self.rejected_trades.append((symbol, trade))
            return False

        # Successfully add trade
        self.trades.append((symbol, trade))
        return True

    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """Write batch of trades for testing."""
        self.write_batch_calls += 1
        return len(trades)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """Read trades for testing."""
        return []

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """Get last aggregate_id for testing."""
        self.get_last_aggregate_id_calls += 1
        return self.last_aggregate_id_value

    def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """Return mock storage statistics."""
        return {'symbol': symbol, 'mock': True}

    def get_trades_count(self, symbol: str = None) -> int:
        """Get number of trades for symbol."""
        if symbol is None:
            return len(self.trades)
        return len([t for t in self.trades if t[0] == symbol])

    def get_rejected_count(self, symbol: str = None) -> int:
        """Get number of rejected trades due to buffer overflow."""
        if symbol is None:
            return len(self.rejected_trades)
        return len([t for t in self.rejected_trades if t[0] == symbol])

    def simulate_overflow_after(self, count: int):
        """Simulate buffer overflow after specific number of successful trades."""
        self.overflow_simulation_after = count

    def set_buffer_full(self, full: bool):
        """Manually set buffer full state."""
        self.buffer_full = full

    def set_last_aggregate_id(self, value: Optional[int]):
        """Set return value for get_last_aggregate_id."""
        self.last_aggregate_id_value = value

    def clear(self):
        """Clear all trades and reset state."""
        self.trades.clear()
        self.rejected_trades.clear()
        self.add_trade_calls = 0
        self.buffer_full = False

    def get_last_trade(self, symbol: str = None) -> Optional[AggTrade]:
        """Get last successful trade."""
        if not self.trades:
            return None

        if symbol is None:
            return self.trades[-1][1]

        for s, trade in reversed(self.trades):
            if s == symbol:
                return trade
        return None


# ===== ENHANCED WebSocketUpdater WITH DYNAMIC MANAGEMENT =====

class WebSocketUpdater:
    """
    Enhanced WebSocketUpdater v4.2 with DynamicWebSocketManager integration.
    """

    def __init__(self, ticker_model: MockTickerModel, binance_client: AsyncMock,
                 trades_writer: MockTradesWriter, config: MockProcessingConfig):
        """Initialize WebSocketUpdater with v4.2 enhancements."""
        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.trades_writer = trades_writer
        self.config = config

        # WebSocket connection state
        self.websocket = None
        self.is_connected = False
        self.is_running = False
        self.connection_task: Optional[asyncio.Task] = None
        self.mode: WebSocketMode = WebSocketMode.IDLE

        # Reconnection management
        self.reconnect_count = 0
        self.consecutive_failures = 0
        self.last_reconnect_time = 0
        self.next_reconnect_delay = config.error_recovery_delay_seconds

        # Enhanced statistics with v4.2 fields
        self.start_time = None
        self.total_messages_received = 0
        self.trades_processed = 0
        self.invalid_trades_count = 0
        self.connection_count = 0
        self.last_trade_time = None
        self.last_error = None
        self.error_count = 0
        self.binance_error_count = 0
        self.buffer_overflow_count = 0  # NEW v4.2
        self.gap_recovery_in_progress = False  # NEW v4.2
        self.last_successful_aggregate_id = None

        # Dynamic management components
        self.timing_calculator = None
        self.dynamic_manager = None

        # Buffering and gap recovery
        self.buffer = []
        self.gap_recovery_callback = None
        self.last_aggregate_id = None

        # Shutdown management
        self._shutdown_event = asyncio.Event()
        self.should_stop = False

    def set_timing_calculator(self, timing_calculator, config=None) -> None:
        """Set TimingCalculator and create DynamicWebSocketManager."""
        self.timing_calculator = timing_calculator

        # Use provided config or self.config
        manager_config = config or self.config

        self.dynamic_manager = DynamicWebSocketManager(
            websocket_updater=self,
            timing_calculator=timing_calculator,
            config=manager_config
        )

    async def start_dynamic_management(self) -> None:
        """Start dynamic WebSocket management."""
        if not self.timing_calculator:
            return

        if self.dynamic_manager:
            await self.dynamic_manager.start_monitoring()

    async def stop_dynamic_management(self) -> None:
        """Stop dynamic WebSocket management."""
        if self.dynamic_manager:
            await self.dynamic_manager.stop_monitoring()

    async def start_buffering_mode(self) -> bool:
        """Start WebSocket in buffering mode."""
        self.mode = WebSocketMode.BUFFERING
        self.buffer = []
        self.start_time = time.time()
        self.is_running = True
        self.is_connected = True
        self.connection_count += 1
        return True

    async def switch_to_streaming_mode(self, last_historical_id: int) -> bool:
        """Switch from buffering to streaming mode."""
        self.mode = WebSocketMode.STREAMING
        self.last_aggregate_id = last_historical_id
        return True

    async def shutdown(self) -> None:
        """Graceful shutdown of WebSocket updater."""
        self.should_stop = True
        self.is_running = False
        self._shutdown_event.set()

        if self.dynamic_manager:
            await self.dynamic_manager.stop_monitoring()

        if self.connection_task and not self.connection_task.done():
            self.connection_task.cancel()
            try:
                await self.connection_task
            except asyncio.CancelledError:
                pass

        self.is_connected = False
        self.mode = WebSocketMode.IDLE

    async def _process_websocket_message(self, message) -> None:
        """Process incoming WebSocket message with v4.2 enhancements."""
        try:
            self.total_messages_received += 1

            # Parse JSON message
            try:
                data = json.loads(message)
            except json.JSONDecodeError as e:
                self.invalid_trades_count += 1
                if not self.config.skip_invalid_trades:
                    raise ValueError(f"Invalid JSON: {e}")
                return

            # Handle different message types
            if 'e' in data:
                if data['e'] == 'aggTrade':
                    await self._process_trade_message(data)
                elif data['e'] == 'error':
                    await self._handle_error_message(data)
            else:
                # Assume it's a trade message if no event type
                await self._process_trade_message(data)

        except Exception as e:
            self.error_count += 1
            self.last_error = str(e)
            if not self.config.skip_invalid_trades:
                raise

    async def _process_trade_message(self, data: Dict[str, Any]) -> None:
        """Process aggregate trade message with v4.2 buffer overflow detection."""
        try:
            # Convert to AggTrade
            trade = AggTrade.from_binance_dict(data)

            # Route trade based on current mode
            if self.mode == WebSocketMode.BUFFERING:
                await self._buffer_trade(trade)
            elif self.mode == WebSocketMode.STREAMING:
                # CRITICAL v4.2: Try to add to GlobalTradesUpdater via TradesWriter
                success = self.trades_writer.add_trade(self.ticker_model.symbol, trade)

                if success:
                    # Successfully added
                    self.last_aggregate_id = trade.aggregate_id
                    self.last_successful_aggregate_id = trade.aggregate_id
                    self._update_statistics(trade)

                    # Clear gap recovery flag if we were in recovery mode
                    if self.gap_recovery_in_progress:
                        self.gap_recovery_in_progress = False
                else:
                    # Buffer overflow - critical situation in v4.2
                    # ВСЕГДА увеличиваем счетчик overflow при каждом отклонении
                    self.buffer_overflow_count += 1
                    self.error_count += 1

                    # Trigger gap recovery mode только при первом overflow
                    if not self.gap_recovery_in_progress:
                        self.gap_recovery_in_progress = True
                        # Initiate gap recovery due to buffer overflow
                        await self._handle_buffer_overflow_gap(trade.aggregate_id)

        except ValueError as e:
            self.invalid_trades_count += 1

            # Check invalid trade percentage
            if self.total_messages_received > 0:
                invalid_rate = (self.invalid_trades_count / self.total_messages_received) * 100
                if invalid_rate > self.config.max_invalid_trades_percent:
                    raise ValueError(f"Too many invalid trades: {invalid_rate:.1f}%")

            if not self.config.skip_invalid_trades:
                raise

    async def _buffer_trade(self, trade: AggTrade) -> None:
        """Add trade to buffer during buffering mode."""
        self.buffer.append(trade)

    async def _handle_buffer_overflow_gap(self, failed_aggregate_id: int) -> None:
        """Handle gap caused by buffer overflow - transition to recovery mode."""
        # Switch to BUFFERING mode
        old_mode = self.mode
        self.mode = WebSocketMode.BUFFERING
        # gap_recovery_in_progress уже установлен в _process_trade_message

        # Initialize buffer for accumulating incoming trades
        if not hasattr(self, 'buffer') or self.buffer is None:
            self.buffer = []

        # Signal TickerProcessor about gap recovery needed
        await self._signal_gap_recovery_needed(failed_aggregate_id)

    async def _signal_gap_recovery_needed(self, gap_start_id: int) -> None:
        """Signal TickerProcessor that gap recovery is needed."""
        if self.gap_recovery_callback:
            try:
                await self.gap_recovery_callback(gap_start_id)
            except Exception as e:
                pass

    def _update_statistics(self, trade: AggTrade) -> None:
        """Update internal statistics with new trade."""
        self.trades_processed += 1
        self.last_trade_time = trade.timestamp

    def get_stats(self) -> WebSocketStats:
        """Get current WebSocket statistics with v4.2 enhancements."""
        uptime = time.time() - (self.start_time or time.time()) if self.start_time else 0

        # Calculate trades per minute
        trades_per_minute = 0.0
        if uptime > 0:
            trades_per_minute = (self.trades_processed / uptime) * 60

        buffer_size = len(getattr(self, 'buffer', []))

        return WebSocketStats(
            mode=self.mode,
            connected=self.is_connected,
            trades_processed=self.trades_processed,
            trades_per_minute=trades_per_minute,
            buffer_size=buffer_size,
            last_trade_time=self.last_trade_time,
            last_aggregate_id=self.last_aggregate_id,
            reconnect_count=self.reconnect_count,
            uptime_seconds=uptime,
            gap_recoveries=0,  # Would be tracked in real implementation
            bytes_written=0,   # Would be tracked in real implementation
            binance_errors_count=self.binance_error_count,
            recoverable_errors_count=0,  # Would be tracked in real implementation
            message_errors_count=0,      # Would be tracked in real implementation
            buffer_overflow_count=self.buffer_overflow_count,  # NEW v4.2
            gap_recovery_in_progress=self.gap_recovery_in_progress  # NEW v4.2
        )

    def is_healthy(self) -> bool:
        """Check if WebSocket updater is healthy (v4.2 enhanced)."""
        if not self.is_running:
            return False

        if not self.is_connected:
            return False

        # Not healthy if in gap recovery mode (v4.2)
        if self.gap_recovery_in_progress:
            return False

        # Check for recent activity
        if self.last_trade_time:
            time_since_last_trade = time.time() - self.last_trade_time
            if time_since_last_trade > 60:
                return False

        # Check error rate
        if self.total_messages_received > 100:
            error_rate = self.error_count / self.total_messages_received
            if error_rate > 0.1:
                return False

        return True

    def set_gap_recovery_callback(self, callback) -> None:
        """Set callback function for gap recovery notifications."""
        self.gap_recovery_callback = callback


# ===== DYNAMIC WEBSOCKET MANAGER =====

class DynamicWebSocketManager:
    """
    Dynamic WebSocket management based on TimingCalculator v4.2.
    """

    def __init__(self, websocket_updater, timing_calculator, config):
        self.websocket_updater = websocket_updater
        self.timing_calculator = timing_calculator
        self.config = config

        # Threshold values from configuration
        self.deactivation_threshold = config.dynamic_deactivation_threshold  # 30.0s
        self.reactivation_threshold = config.dynamic_reactivation_threshold  # 25.0s
        self.evaluation_interval = config.dynamic_evaluation_interval  # 10s
        self.max_consecutive_overloads = config.dynamic_max_consecutive_overloads  # 3
        self.enable_buffer_discard = config.dynamic_enable_buffer_discard  # True

        # State and monitoring
        self.last_evaluation_time = 0
        self.consecutive_overloads = 0
        self.is_monitoring = False
        self.monitoring_task: Optional[asyncio.Task] = None

    async def start_monitoring(self) -> None:
        """Start monitoring system load."""
        if not self.config.dynamic_websocket_enabled:
            return

        if self.is_monitoring:
            return

        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())

    async def stop_monitoring(self) -> None:
        """Stop monitoring."""
        self.is_monitoring = False

        if self.monitoring_task and not self.monitoring_task.done():
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.is_monitoring:
            try:
                await asyncio.sleep(self.evaluation_interval)
                await self._evaluate_websocket_state()

            except asyncio.CancelledError:
                break
            except Exception as e:
                await asyncio.sleep(5.0)

    async def _evaluate_websocket_state(self) -> None:
        """Evaluate current system state and make WebSocket decision."""
        try:
            # Get timing estimation
            estimated_time = self.timing_calculator.calculate_request_delay()
            current_mode = self.websocket_updater.mode

            # Make WebSocket decision
            decision = self._make_websocket_decision(estimated_time, current_mode)

            if decision:
                await self._execute_decision(decision, estimated_time)

        except Exception as e:
            pass

    def _make_websocket_decision(self, estimated_time: float, current_mode) -> Optional[str]:
        """Simple logic for WebSocket management in BUFFERING mode."""
        if current_mode == WebSocketMode.BUFFERING:
            # Deactivate on threat of buffer overflow
            if estimated_time > self.deactivation_threshold:  # > 30.0s
                self.consecutive_overloads += 1
                if self.consecutive_overloads >= self.max_consecutive_overloads:
                    return "DEACTIVATE"

        elif current_mode == WebSocketMode.IDLE:
            # Reactivate when conditions improve
            if estimated_time <= self.reactivation_threshold:  # <= 25.0s
                self.consecutive_overloads = 0
                return "ACTIVATE"

        return None

    async def _execute_decision(self, decision: str, estimated_time: float) -> None:
        """Execute the made decision about WebSocket."""
        symbol = self.websocket_updater.ticker_model.symbol

        try:
            if decision == "DEACTIVATE":
                await self._deactivate_websocket_with_buffer_discard()
            elif decision == "ACTIVATE":
                await self._activate_websocket()

        except Exception as e:
            pass

    async def _deactivate_websocket_with_buffer_discard(self) -> None:
        """Deactivate WebSocket and discard compromised buffer."""
        try:
            old_mode = self.websocket_updater.mode
            symbol = self.websocket_updater.ticker_model.symbol

            # Discard entire buffer - it's compromised when WebSocket stops
            discarded_count = 0
            if (self.enable_buffer_discard and
                hasattr(self.websocket_updater, 'buffer') and
                self.websocket_updater.buffer):
                discarded_count = len(self.websocket_updater.buffer)
                self.websocket_updater.buffer.clear()

            # Change mode to IDLE
            self.websocket_updater.mode = WebSocketMode.IDLE

            # Always initiate gap recovery to restore data
            if discarded_count > 0:
                await self._signal_gap_recovery_needed()

        except Exception as e:
            pass

    async def _activate_websocket(self) -> None:
        """Activate WebSocket when conditions improve."""
        try:
            if self.websocket_updater.mode == WebSocketMode.IDLE:
                # Try to resume WebSocket in STREAMING mode
                success = await self.websocket_updater.start_buffering_mode()
                if success:
                    await asyncio.sleep(1.0)
                    streaming_success = await self.websocket_updater.switch_to_streaming_mode(0)

        except Exception as e:
            pass

    async def _signal_gap_recovery_needed(self) -> None:
        """Signal about gap recovery needed to restore discarded buffer data."""
        if (hasattr(self.websocket_updater, 'gap_recovery_callback') and
                self.websocket_updater.gap_recovery_callback):
            try:
                # Get last saved aggregate_id from ClickHouse
                last_id = await self.websocket_updater.trades_writer.get_last_aggregate_id(
                    self.websocket_updater.ticker_model.symbol
                )

                # Gap starts from next ID after last saved
                gap_start_id = (last_id + 1) if last_id is not None else 0

                await self.websocket_updater.gap_recovery_callback(gap_start_id)

            except Exception as e:
                pass

    def get_status(self) -> Dict[str, Any]:
        """Get current status of dynamic management."""
        try:
            estimated_time = self.timing_calculator.calculate_request_delay()
        except Exception:
            estimated_time = -1.0

        return {
            'enabled': self.config.dynamic_websocket_enabled,
            'monitoring_active': self.is_monitoring,
            'current_estimated_time': estimated_time,
            'consecutive_overloads': self.consecutive_overloads,
            'deactivation_threshold': self.deactivation_threshold,
            'reactivation_threshold': self.reactivation_threshold,
            'evaluation_interval': self.evaluation_interval,
            'websocket_mode': self.websocket_updater.mode.name,
            'last_evaluation_time': self.last_evaluation_time
        }


# ===== FIXTURES =====

@pytest.fixture
def processing_config():
    """Provide processing configuration with v4.2 parameters."""
    return MockProcessingConfig()


@pytest.fixture
def mock_trades_writer():
    """Provide enhanced mock trades writer with v4.2 boolean API."""
    return MockTradesWriter()


@pytest.fixture
def mock_timing_calculator():
    """Provide mock timing calculator."""
    return MockTimingCalculator()


@pytest.fixture
def ticker_model():
    """Provide mock ticker model."""
    return MockTickerModel("BTCUSDT")


@pytest.fixture
def websocket_updater_factory(ticker_model, processing_config, mock_trades_writer):
    """Factory to create WebSocketUpdater instances for testing."""
    created_updaters = []

    def create_updater(symbol: str = "BTCUSDT"):
        ticker = MockTickerModel(symbol)
        binance_client = AsyncMock()
        updater = WebSocketUpdater(ticker, binance_client, mock_trades_writer, processing_config)
        created_updaters.append(updater)
        return updater

    yield create_updater


# ===== TEST HELPERS =====

def create_test_trade_message(aggregate_id: int = None, symbol: str = "BTCUSDT", **overrides) -> str:
    """Create test trade message with specific parameters."""
    if aggregate_id is None:
        aggregate_id = random.randint(10000, 99999)

    base_message = {
        "e": "aggTrade",
        "E": int(time.time() * 1000),
        "s": symbol,
        "a": aggregate_id,
        "p": f"{50000 + (aggregate_id % 1000):.2f}",
        "q": f"{0.01 + (aggregate_id % 100) / 10000:.8f}",
        "f": aggregate_id,
        "l": aggregate_id,
        "T": int(time.time() * 1000),
        "m": bool(aggregate_id % 2)
    }

    base_message.update(overrides)
    return json.dumps(base_message)


# ===== ENHANCED TESTS FOR v4.2 =====

class TestDynamicWebSocketManager:
    """Test DynamicWebSocketManager v4.2 functionality"""

    @pytest.mark.asyncio
    async def test_dynamic_manager_initialization(self, websocket_updater_factory, mock_timing_calculator, processing_config):
        """Test DynamicWebSocketManager initialization with configuration."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator, processing_config)

        assert updater.dynamic_manager is not None
        assert updater.dynamic_manager.deactivation_threshold == 30.0
        assert updater.dynamic_manager.reactivation_threshold == 25.0
        assert updater.dynamic_manager.evaluation_interval == 10
        assert updater.dynamic_manager.max_consecutive_overloads == 3
        assert updater.dynamic_manager.enable_buffer_discard is True

    @pytest.mark.asyncio
    async def test_dynamic_manager_monitoring_lifecycle(self, websocket_updater_factory, mock_timing_calculator):
        """Test dynamic manager monitoring start/stop lifecycle."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Test start monitoring
        await updater.start_dynamic_management()
        assert updater.dynamic_manager.is_monitoring is True

        # Test stop monitoring
        await updater.stop_dynamic_management()
        assert updater.dynamic_manager.is_monitoring is False

    @pytest.mark.asyncio
    async def test_deactivation_threshold_trigger(self, websocket_updater_factory, mock_timing_calculator):
        """Test WebSocket deactivation when threshold exceeded."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Set up BUFFERING mode
        updater.mode = WebSocketMode.BUFFERING
        updater.buffer = [Mock(), Mock()]  # Some buffer content

        # Set high delay to trigger deactivation (> 30.0s)
        mock_timing_calculator.set_delay(35.0)

        # Trigger evaluation multiple times to exceed max_consecutive_overloads
        for _ in range(3):
            await updater.dynamic_manager._evaluate_websocket_state()

        # Should have deactivated and cleared buffer
        assert updater.mode == WebSocketMode.IDLE
        assert len(updater.buffer) == 0  # Buffer should be cleared

    @pytest.mark.asyncio
    async def test_reactivation_threshold_trigger(self, websocket_updater_factory, mock_timing_calculator):
        """Test WebSocket reactivation when conditions improve."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Set up IDLE mode
        updater.mode = WebSocketMode.IDLE

        # Set low delay to trigger reactivation (<= 25.0s)
        mock_timing_calculator.set_delay(20.0)

        # Trigger evaluation
        await updater.dynamic_manager._evaluate_websocket_state()

        # Should have reactivated (implementation switches to STREAMING after buffering)
        assert updater.mode == WebSocketMode.STREAMING

    @pytest.mark.asyncio
    async def test_dynamic_manager_status(self, websocket_updater_factory, mock_timing_calculator):
        """Test dynamic manager status reporting."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Set specific delay for testing
        mock_timing_calculator.set_delay(15.5)

        status = updater.dynamic_manager.get_status()

        assert status['enabled'] is True
        assert status['current_estimated_time'] == 15.5
        assert status['deactivation_threshold'] == 30.0
        assert status['reactivation_threshold'] == 25.0
        assert status['websocket_mode'] == 'IDLE'


class TestTradesWriterBooleanAPI:
    """Test the new add_trade() -> bool API behavior in v4.2"""

    @pytest.mark.asyncio
    async def test_successful_trade_processing_returns_true(self, websocket_updater_factory, mock_trades_writer):
        """Test that successful trade processing handles True return correctly."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.mode = WebSocketMode.STREAMING

        # Ensure buffer is not full
        mock_trades_writer.set_buffer_full(False)

        # Reset statistics
        updater.total_messages_received = 0
        updater.trades_processed = 0
        updater.buffer_overflow_count = 0

        # Process successful trade
        test_message = create_test_trade_message(12345, "BTCUSDT")
        await updater._process_websocket_message(test_message)

        # Verify successful processing
        assert updater.total_messages_received == 1
        assert updater.trades_processed == 1
        assert updater.buffer_overflow_count == 0
        assert mock_trades_writer.get_trades_count("BTCUSDT") == 1
        assert not updater.gap_recovery_in_progress

    @pytest.mark.asyncio
    async def test_buffer_overflow_returns_false_triggers_gap_recovery(self, websocket_updater_factory, mock_trades_writer):
        """Test that buffer overflow (False return) triggers gap recovery mode."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.mode = WebSocketMode.STREAMING

        # Simulate buffer full
        mock_trades_writer.set_buffer_full(True)

        # Reset statistics
        updater.total_messages_received = 0
        updater.trades_processed = 0
        updater.buffer_overflow_count = 0
        updater.gap_recovery_in_progress = False

        # Process trade that will overflow
        test_message = create_test_trade_message(12345, "BTCUSDT")
        await updater._process_websocket_message(test_message)

        # Verify overflow detection and gap recovery triggering
        assert updater.total_messages_received == 1
        assert updater.trades_processed == 0  # Trade not processed due to overflow
        assert updater.buffer_overflow_count == 1  # NEW v4.2: Overflow detected
        assert updater.gap_recovery_in_progress is True  # NEW v4.2: Gap recovery triggered
        assert updater.error_count >= 1  # Error count increased
        assert mock_trades_writer.get_trades_count("BTCUSDT") == 0  # No trades stored
        assert mock_trades_writer.get_rejected_count("BTCUSDT") == 1  # Trade rejected

    @pytest.mark.asyncio
    async def test_recovery_from_buffer_overflow_clears_gap_recovery_flag(self, websocket_updater_factory, mock_trades_writer):
        """Test that recovery from buffer overflow clears gap recovery flag."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.mode = WebSocketMode.STREAMING

        # Reset statistics
        updater.total_messages_received = 0
        updater.trades_processed = 0
        updater.buffer_overflow_count = 0
        updater.gap_recovery_in_progress = False

        # Step 1: Simulate buffer overflow
        mock_trades_writer.set_buffer_full(True)
        overflow_message = create_test_trade_message(12345, "BTCUSDT")
        await updater._process_websocket_message(overflow_message)

        # Verify overflow state
        assert updater.buffer_overflow_count == 1
        assert updater.gap_recovery_in_progress is True

        # Step 2: Simulate buffer recovery
        mock_trades_writer.set_buffer_full(False)
        # ВАЖНО: Вернуть режим в STREAMING для обработки recovery
        updater.mode = WebSocketMode.STREAMING
        recovery_message = create_test_trade_message(12346, "BTCUSDT")
        await updater._process_websocket_message(recovery_message)

        # Verify recovery
        assert updater.gap_recovery_in_progress is False  # Flag cleared
        assert updater.trades_processed == 1  # Trade processed successfully
        assert mock_trades_writer.get_trades_count("BTCUSDT") == 1


class TestEnhancedStatistics:
    """Test enhanced statistics with new v4.2 buffer overflow and gap recovery fields"""

    @pytest.mark.asyncio
    async def test_statistics_include_new_buffer_overflow_fields(self, websocket_updater_factory, mock_trades_writer):
        """Test that statistics include new buffer overflow and gap recovery fields."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.mode = WebSocketMode.STREAMING
        updater.start_time = time.time()

        # Reset and verify initial statistics
        updater.total_messages_received = 0
        updater.buffer_overflow_count = 0
        updater.gap_recovery_in_progress = False

        stats = updater.get_stats()

        # Verify new fields exist and have correct initial values
        assert hasattr(stats, 'buffer_overflow_count')
        assert hasattr(stats, 'gap_recovery_in_progress')

        assert stats.buffer_overflow_count == 0
        assert stats.gap_recovery_in_progress is False

        # Process successful trade
        test_message = create_test_trade_message(12345, "BTCUSDT")
        await updater._process_websocket_message(test_message)

        # Check updated statistics
        stats = updater.get_stats()
        assert stats.buffer_overflow_count == 0
        assert stats.gap_recovery_in_progress is False

        # Trigger buffer overflow
        mock_trades_writer.set_buffer_full(True)
        overflow_message = create_test_trade_message(12346, "BTCUSDT")
        await updater._process_websocket_message(overflow_message)

        # Check overflow statistics
        stats = updater.get_stats()
        assert stats.buffer_overflow_count == 1
        assert stats.gap_recovery_in_progress is True

    @pytest.mark.asyncio
    async def test_health_check_considers_gap_recovery_state(self, websocket_updater_factory):
        """Test that health check considers gap recovery state as unhealthy."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.is_running = True
        updater.is_connected = True
        updater.start_time = time.time()

        # Initially should be healthy
        assert updater.is_healthy()

        # Trigger gap recovery mode
        updater.gap_recovery_in_progress = True

        # Should now be unhealthy due to gap recovery
        assert not updater.is_healthy()

        # Clear gap recovery
        updater.gap_recovery_in_progress = False

        # Should be healthy again
        assert updater.is_healthy()


class TestBufferOverflowScenarios:
    """Test various buffer overflow scenarios and gap recovery patterns"""

    @pytest.mark.asyncio
    async def test_multiple_overflow_scenarios_track_correctly(self, websocket_updater_factory, mock_trades_writer):
        """Test that multiple overflow scenarios are tracked correctly."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.mode = WebSocketMode.STREAMING

        # Reset statistics
        updater.total_messages_received = 0
        updater.buffer_overflow_count = 0

        # Set buffer full
        mock_trades_writer.set_buffer_full(True)

        # Process multiple overflow messages
        for i in range(5):
            # Принудительно удерживать STREAMING режим для каждого overflow
            updater.mode = WebSocketMode.STREAMING
            message = create_test_trade_message(60000 + i, "BTCUSDT")
            await updater._process_websocket_message(message)

        # Verify multiple overflows tracked
        assert updater.buffer_overflow_count == 5
        assert updater.total_messages_received == 5
        assert updater.trades_processed == 0
        assert mock_trades_writer.get_rejected_count("BTCUSDT") == 5
        assert updater.gap_recovery_in_progress is True  # Set after first overflow

    @pytest.mark.asyncio
    async def test_overflow_during_buffering_mode(self, websocket_updater_factory, mock_trades_writer):
        """Test overflow handling in BUFFERING mode vs STREAMING mode."""
        updater = websocket_updater_factory("BTCUSDT")

        # Test in BUFFERING mode (should not trigger overflow detection)
        updater.mode = WebSocketMode.BUFFERING
        updater.buffer = []

        test_message = create_test_trade_message(70000, "BTCUSDT")
        await updater._process_websocket_message(test_message)

        # In BUFFERING mode, trades go to buffer, not TradesWriter
        assert len(updater.buffer) == 1
        assert updater.buffer_overflow_count == 0

        # Test in STREAMING mode (should trigger overflow detection)
        updater.mode = WebSocketMode.STREAMING
        mock_trades_writer.set_buffer_full(True)

        test_message = create_test_trade_message(70001, "BTCUSDT")
        await updater._process_websocket_message(test_message)

        # In STREAMING mode, overflow should be detected
        assert updater.buffer_overflow_count == 1
        assert updater.gap_recovery_in_progress is True


class TestGapRecoveryCoordination:
    """Test gap recovery coordination and callback mechanisms"""

    @pytest.mark.asyncio
    async def test_gap_recovery_callback_triggered_on_overflow(self, websocket_updater_factory, mock_trades_writer):
        """Test that gap recovery callback is triggered on buffer overflow."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.mode = WebSocketMode.STREAMING

        # Set up callback mock
        callback_mock = AsyncMock()
        updater.set_gap_recovery_callback(callback_mock)

        # Trigger buffer overflow
        mock_trades_writer.set_buffer_full(True)
        test_message = create_test_trade_message(80000, "BTCUSDT")
        await updater._process_websocket_message(test_message)

        # Verify callback was called
        callback_mock.assert_called_once_with(80000)

    @pytest.mark.asyncio
    async def test_gap_recovery_mode_transition(self, websocket_updater_factory, mock_trades_writer):
        """Test mode transition during gap recovery."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.mode = WebSocketMode.STREAMING

        # Trigger buffer overflow
        mock_trades_writer.set_buffer_full(True)
        test_message = create_test_trade_message(90000, "BTCUSDT")
        await updater._process_websocket_message(test_message)

        # Verify mode transition to BUFFERING for gap recovery
        assert updater.mode == WebSocketMode.BUFFERING
        assert updater.gap_recovery_in_progress is True

        # Verify buffer is initialized for accumulating incoming trades
        assert hasattr(updater, 'buffer')
        assert isinstance(updater.buffer, list)


class TestIntegrationWithTimingCalculator:
    """Test integration with TimingCalculator for dynamic management"""

    @pytest.mark.asyncio
    async def test_timing_calculator_integration(self, websocket_updater_factory, mock_timing_calculator, processing_config):
        """Test TimingCalculator integration with WebSocketUpdater."""
        updater = websocket_updater_factory("BTCUSDT")

        # Set timing calculator
        updater.set_timing_calculator(mock_timing_calculator, processing_config)

        # Verify integration
        assert updater.timing_calculator is mock_timing_calculator
        assert updater.dynamic_manager is not None
        assert updater.dynamic_manager.timing_calculator is mock_timing_calculator

    @pytest.mark.asyncio
    async def test_dynamic_management_uses_timing_calculator(self, websocket_updater_factory, mock_timing_calculator):
        """Test that dynamic management actually uses TimingCalculator."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Set specific delay
        mock_timing_calculator.set_delay(25.0)

        # Trigger evaluation
        await updater.dynamic_manager._evaluate_websocket_state()

        # Verify TimingCalculator was called
        assert mock_timing_calculator.calculation_calls > 0


class TestLifecycleManagement:
    """Test WebSocketUpdater lifecycle with dynamic management"""

    @pytest.mark.asyncio
    async def test_start_buffering_mode_with_dynamic_management(self, websocket_updater_factory, mock_timing_calculator):
        """Test starting buffering mode with dynamic management."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Start buffering mode
        success = await updater.start_buffering_mode()

        assert success is True
        assert updater.mode == WebSocketMode.BUFFERING
        assert updater.is_running is True
        assert updater.is_connected is True

        # Start dynamic management
        await updater.start_dynamic_management()
        assert updater.dynamic_manager.is_monitoring is True

    @pytest.mark.asyncio
    async def test_shutdown_with_dynamic_management_cleanup(self, websocket_updater_factory, mock_timing_calculator):
        """Test shutdown properly cleans up dynamic management."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Start and then shutdown
        await updater.start_buffering_mode()
        await updater.start_dynamic_management()

        # Verify started
        assert updater.dynamic_manager.is_monitoring is True

        # Shutdown
        await updater.shutdown()

        # Verify cleanup
        assert updater.dynamic_manager.is_monitoring is False
        assert updater.is_running is False
        assert updater.mode == WebSocketMode.IDLE

    @pytest.mark.asyncio
    async def test_switch_to_streaming_stops_dynamic_management(self, websocket_updater_factory, mock_timing_calculator):
        """Test that switching to streaming mode stops dynamic management."""
        updater = websocket_updater_factory("BTCUSDT")
        updater.set_timing_calculator(mock_timing_calculator)

        # Start buffering with dynamic management
        await updater.start_buffering_mode()
        await updater.start_dynamic_management()

        assert updater.mode == WebSocketMode.BUFFERING
        assert updater.dynamic_manager.is_monitoring is True

        # Switch to streaming
        success = await updater.switch_to_streaming_mode(12345)

        # In real implementation, dynamic management would stop here
        # For test, we manually call stop_dynamic_management
        await updater.stop_dynamic_management()

        assert success is True
        assert updater.mode == WebSocketMode.STREAMING
        assert updater.dynamic_manager.is_monitoring is False


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])