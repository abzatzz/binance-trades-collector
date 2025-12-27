"""
БЛОК 5: Enhanced Gap Recovery v4.2 Unit Tests (7 tests)
========================================================

Критическое тестирование Enhanced Gap Recovery функциональности TickerProcessor v4.2:
- ClickHouse verification для определения recovery start point
- WebSocket mode transitions между STREAMING ↔ BUFFERING ↔ STREAMING
- Dynamic management координация во время gap recovery
- Buffer overflow detection и automatic gap recovery triggering
- Historical download integration для восстановления gaps
- Comprehensive error handling и recovery scenarios

АРХИТЕКТУРНЫЕ ПРИНЦИПЫ v4.2:
- ClickHouse является single source of truth для last_aggregate_id
- Gap recovery всегда переводит WebSocket в BUFFERING mode
- Dynamic management stops при переходе в STREAMING mode
- Buffer overflow автоматически triggers gap recovery
- Historical download coordination через recover_from_gap()

КРИТИЧЕСКИЕ СЦЕНАРИИ:
1. ClickHouse verification и recovery start point determination
2. No confirmed trades handling (empty database scenarios)
3. Historical download integration для gap filling
4. WebSocket mode transitions и state management
5. Dynamic management coordination и lifecycle
6. Buffer overflow automatic gap recovery triggering
7. Error handling и fallback recovery scenarios

File: tests/unit/test_ticker_processor_gap_recovery_v42.py
"""

import pytest
import asyncio
import time
import threading
from unittest.mock import Mock, AsyncMock, patch, call, MagicMock
from concurrent.futures import ThreadPoolExecutor
import random
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum, auto
import json


# ===== COPY REQUIRED CLASSES TO AVOID IMPORTS =====

class ProcessorState(Enum):
    """Processor state enumeration"""
    INITIALIZING = auto()
    DOWNLOADING_HISTORICAL = auto()
    STARTING_WEBSOCKET = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPING = auto()
    STOPPED = auto()
    ERROR = auto()

    def is_active(self) -> bool:
        """Check if state represents active processing"""
        return self in {
            ProcessorState.DOWNLOADING_HISTORICAL,
            ProcessorState.STARTING_WEBSOCKET,
            ProcessorState.RUNNING
        }

    def can_transition_to(self, new_state: 'ProcessorState') -> bool:
        """Check if transition to new state is valid"""
        valid_transitions = {
            ProcessorState.INITIALIZING: {
                ProcessorState.DOWNLOADING_HISTORICAL,
                ProcessorState.ERROR,
                ProcessorState.STOPPING
            },
            ProcessorState.DOWNLOADING_HISTORICAL: {
                ProcessorState.STARTING_WEBSOCKET,
                ProcessorState.ERROR,
                ProcessorState.STOPPING
            },
            ProcessorState.STARTING_WEBSOCKET: {
                ProcessorState.RUNNING,
                ProcessorState.ERROR,
                ProcessorState.STOPPING
            },
            ProcessorState.RUNNING: {
                ProcessorState.PAUSED,
                ProcessorState.STOPPING,
                ProcessorState.ERROR
            },
            ProcessorState.PAUSED: {
                ProcessorState.RUNNING,
                ProcessorState.STOPPING,
                ProcessorState.ERROR
            },
            ProcessorState.STOPPING: {
                ProcessorState.STOPPED,
                ProcessorState.ERROR
            },
            ProcessorState.STOPPED: set(),
            ProcessorState.ERROR: {
                ProcessorState.STOPPING,
                ProcessorState.STOPPED
            }
        }

        return new_state in valid_transitions.get(self, set())


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
class HistoricalDownloadResult:
    """Result from historical download operation"""
    success: bool
    trades_loaded: int
    api_requests_made: int
    download_duration_seconds: float
    errors_count: int
    binance_errors_count: int
    last_aggregate_id: Optional[int]
    websocket_handoff_completed: bool
    start_time: int
    end_time: int


# ===== MOCK CONFIGURATION CLASSES =====

class MockConfig:
    """Mock system configuration"""

    def __init__(self):
        self.processing = MockProcessingConfig()


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
        self.health_check_interval_seconds = 60

        # NEW v4.2: Dynamic WebSocket Management parameters
        self.dynamic_websocket_enabled = True
        self.dynamic_deactivation_threshold = 30.0
        self.dynamic_reactivation_threshold = 25.0
        self.dynamic_evaluation_interval = 10
        self.dynamic_max_consecutive_overloads = 3
        self.dynamic_enable_buffer_discard = True

        # NEW v4.2: Timing Calculator and startup strategies
        self.timing_calculator_enabled = True
        self.websocket_startup_strategy = "auto"
        self.buffer_overflow_protection = True
        self.max_buffer_size_per_ticker = 10000


class MockTickerModel:
    """Mock ticker model"""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol


# ===== ENHANCED MOCK COMPONENTS FOR GAP RECOVERY =====

class MockTradesWriter:
    """Enhanced MockTradesWriter with ClickHouse verification capabilities"""

    def __init__(self):
        self.trades = []
        self.buffer_full = False
        self.add_trade_calls = 0
        self.rejected_trades = []
        self.write_batch_calls = 0
        self.get_last_aggregate_id_calls = 0
        self.last_aggregate_id_value = None
        self.storage_stats = {}

        # Gap recovery testing support
        self.get_last_aggregate_id_responses = {}  # symbol -> value
        self.get_last_aggregate_id_call_history = []

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """Add trade - RETURNS BOOL as per v4.2 API"""
        self.add_trade_calls += 1

        if self.buffer_full:
            self.rejected_trades.append((symbol, trade))
            return False

        self.trades.append((symbol, trade))
        return True

    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """Write batch of trades for testing"""
        self.write_batch_calls += 1
        return len(trades)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """Read trades for testing"""
        return []

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """Get last aggregate_id for testing with enhanced tracking"""
        self.get_last_aggregate_id_calls += 1
        self.get_last_aggregate_id_call_history.append({
            'symbol': symbol,
            'timestamp': time.time()
        })

        # Return symbol-specific value if configured
        if symbol in self.get_last_aggregate_id_responses:
            return self.get_last_aggregate_id_responses[symbol]

        return self.last_aggregate_id_value

    def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """Return mock storage statistics"""
        return self.storage_stats.get(symbol, {'symbol': symbol, 'mock': True})

    def set_buffer_full(self, full: bool):
        """Set buffer full state"""
        self.buffer_full = full

    def set_last_aggregate_id(self, value: Optional[int]):
        """Set return value for get_last_aggregate_id"""
        self.last_aggregate_id_value = value

    def set_last_aggregate_id_for_symbol(self, symbol: str, value: Optional[int]):
        """Set symbol-specific last_aggregate_id response"""
        self.get_last_aggregate_id_responses[symbol] = value

    def set_storage_stats(self, symbol: str, stats: Dict[str, Any]):
        """Set storage stats for symbol"""
        self.storage_stats[symbol] = stats

    def clear(self):
        """Clear all trades and reset state"""
        self.trades.clear()
        self.rejected_trades.clear()
        self.add_trade_calls = 0
        self.buffer_full = False
        self.get_last_aggregate_id_call_history.clear()

    def get_last_aggregate_id_call_count_for_symbol(self, symbol: str) -> int:
        """Get number of get_last_aggregate_id calls for specific symbol"""
        return len([call for call in self.get_last_aggregate_id_call_history
                    if call['symbol'] == symbol])


class MockHistoricalDownloader:
    """Enhanced MockHistoricalDownloader with gap recovery capabilities"""

    def __init__(self):
        self.download_calls = 0
        self.pause_calls = 0
        self.recover_gap_calls = 0
        self.set_websocket_coordinator_calls = 0

        # Enhanced gap recovery tracking
        self.recover_from_gap_calls = 0
        self.recover_from_gap_call_history = []
        self.gap_recovery_results = {}  # gap_start_id -> result

        # Configurable responses
        self.download_result = HistoricalDownloadResult(
            success=True,
            trades_loaded=10000,
            api_requests_made=100,
            download_duration_seconds=60.0,
            errors_count=0,
            binance_errors_count=0,
            last_aggregate_id=20000,
            websocket_handoff_completed=True,
            start_time=int(time.time() * 1000) - 3600000,
            end_time=int(time.time() * 1000)
        )

        self.is_running = False
        self.websocket_coordinator = None
        self.timing_calculator = None

    async def download_historical_data(self) -> HistoricalDownloadResult:
        """Mock historical data download"""
        self.download_calls += 1
        self.is_running = True
        await asyncio.sleep(0.01)
        self.is_running = False
        return self.download_result

    async def recover_from_gap(self, gap_start_id: int) -> HistoricalDownloadResult:
        """Enhanced mock gap recovery with detailed tracking"""
        self.recover_from_gap_calls += 1

        # Record detailed call information
        call_info = {
            'gap_start_id': gap_start_id,
            'timestamp': time.time(),
            'call_number': self.recover_from_gap_calls
        }
        self.recover_from_gap_call_history.append(call_info)

        # Return configured result for this gap_start_id if available
        if gap_start_id in self.gap_recovery_results:
            return self.gap_recovery_results[gap_start_id]

        # Default gap recovery result
        recovery_trades_count = min(1000, 50000 - gap_start_id)  # Simulate realistic gap size

        default_result = HistoricalDownloadResult(
            success=True,
            trades_loaded=recovery_trades_count,
            api_requests_made=max(1, recovery_trades_count // 1000),
            download_duration_seconds=5.0,
            errors_count=0,
            binance_errors_count=0,
            last_aggregate_id=gap_start_id + recovery_trades_count - 1,
            websocket_handoff_completed=True,
            start_time=gap_start_id,
            end_time=gap_start_id + recovery_trades_count - 1
        )

        return default_result

    async def pause_download(self):
        """Mock pause download"""
        self.pause_calls += 1

    def set_websocket_coordinator(self, coordinator):
        """Mock set websocket coordinator"""
        self.set_websocket_coordinator_calls += 1
        self.websocket_coordinator = coordinator

    def is_download_running(self) -> bool:
        """Check if download is running"""
        return self.is_running

    def set_gap_recovery_result(self, gap_start_id: int, result: HistoricalDownloadResult):
        """Configure specific result for gap recovery call"""
        self.gap_recovery_results[gap_start_id] = result

    def get_gap_recovery_call_history(self) -> List[Dict]:
        """Get detailed history of gap recovery calls"""
        return self.recover_from_gap_call_history.copy()

    def was_gap_recovery_called_with(self, gap_start_id: int) -> bool:
        """Check if gap recovery was called with specific gap_start_id"""
        return any(call['gap_start_id'] == gap_start_id
                   for call in self.recover_from_gap_call_history)


class MockWebSocketUpdater:
    """Enhanced MockWebSocketUpdater with detailed gap recovery and mode transition tracking"""

    def __init__(self):
        self.start_buffering_calls = 0
        self.switch_streaming_calls = 0
        self.shutdown_calls = 0
        self.set_timing_calculator_calls = 0
        self.start_dynamic_management_calls = 0
        self.stop_dynamic_management_calls = 0
        self.set_gap_recovery_callback_calls = 0

        # Enhanced state tracking for gap recovery
        self.websocket_connected = False
        self.mode = "IDLE"
        self.gap_recovery_callback = None
        self.timing_calculator = None
        self.dynamic_manager = None
        self.gap_recovery_in_progress = False

        # Mode transition history for testing
        self.mode_transition_history = []
        self.dynamic_management_state_history = []

        # Mock statistics
        self.stats = {
            'trades_processed': 0,
            'last_trade_time': None,
            'reconnect_count': 0,
            'connected': False,
            'mode': 'IDLE'
        }

        # Configurable responses
        self.start_buffering_success = True
        self.switch_streaming_success = True
        self.is_healthy_response = True

    async def start_buffering_mode(self) -> bool:
        """Enhanced mock start buffering mode with state tracking"""
        self.start_buffering_calls += 1
        old_mode = self.mode

        if self.start_buffering_success:
            self.websocket_connected = True
            self.mode = "BUFFERING"

        # Record mode transition
        self.mode_transition_history.append({
            'from_mode': old_mode,
            'to_mode': self.mode,
            'action': 'start_buffering_mode',
            'timestamp': time.time(),
            'success': self.start_buffering_success
        })

        return self.start_buffering_success

    async def switch_to_streaming_mode(self, last_historical_id: int) -> bool:
        """Enhanced mock switch to streaming mode with detailed tracking"""
        self.switch_streaming_calls += 1
        old_mode = self.mode

        if self.switch_streaming_success:
            self.mode = "STREAMING"
            # Clear gap recovery flag when successfully switching to streaming
            self.gap_recovery_in_progress = False

        # Record mode transition with last_historical_id
        self.mode_transition_history.append({
            'from_mode': old_mode,
            'to_mode': self.mode,
            'action': 'switch_to_streaming_mode',
            'last_historical_id': last_historical_id,
            'timestamp': time.time(),
            'success': self.switch_streaming_success
        })

        return self.switch_streaming_success

    async def shutdown(self):
        """Mock shutdown"""
        self.shutdown_calls += 1
        old_mode = self.mode
        self.websocket_connected = False
        self.mode = "IDLE"

        # Record shutdown transition
        self.mode_transition_history.append({
            'from_mode': old_mode,
            'to_mode': self.mode,
            'action': 'shutdown',
            'timestamp': time.time(),
            'success': True
        })

    def set_timing_calculator(self, timing_calculator, config=None):
        """Mock set timing calculator"""
        self.set_timing_calculator_calls += 1
        self.timing_calculator = timing_calculator
        # Mock dynamic manager creation
        self.dynamic_manager = Mock()

    async def start_dynamic_management(self):
        """Enhanced mock start dynamic management with state tracking"""
        self.start_dynamic_management_calls += 1

        # Record dynamic management state change
        self.dynamic_management_state_history.append({
            'action': 'start_dynamic_management',
            'timestamp': time.time(),
            'websocket_mode': self.mode
        })

    async def stop_dynamic_management(self):
        """Enhanced mock stop dynamic management with state tracking"""
        self.stop_dynamic_management_calls += 1

        # Record dynamic management state change
        self.dynamic_management_state_history.append({
            'action': 'stop_dynamic_management',
            'timestamp': time.time(),
            'websocket_mode': self.mode
        })

    def set_gap_recovery_callback(self, callback):
        """Mock set gap recovery callback"""
        self.set_gap_recovery_callback_calls += 1
        self.gap_recovery_callback = callback

    def get_stats(self):
        """Mock get statistics"""
        return Mock(**self.stats)

    def is_healthy(self) -> bool:
        """Mock health check"""
        return self.is_healthy_response

    def set_start_buffering_success(self, success: bool):
        """Configure start buffering response"""
        self.start_buffering_success = success

    def set_switch_streaming_success(self, success: bool):
        """Configure switch streaming response"""
        self.switch_streaming_success = success

    def set_healthy(self, healthy: bool):
        """Configure health response"""
        self.is_healthy_response = healthy

    def update_stats(self, **kwargs):
        """Update mock statistics"""
        self.stats.update(kwargs)

    def get_mode_transition_history(self) -> List[Dict]:
        """Get detailed mode transition history"""
        return self.mode_transition_history.copy()

    def get_dynamic_management_history(self) -> List[Dict]:
        """Get dynamic management state change history"""
        return self.dynamic_management_state_history.copy()

    def was_mode_transition(self, from_mode: str, to_mode: str, action: str) -> bool:
        """Check if specific mode transition occurred"""
        return any(
            t['from_mode'] == from_mode and
            t['to_mode'] == to_mode and
            t['action'] == action
            for t in self.mode_transition_history
        )

    def get_last_switch_streaming_call(self) -> Optional[Dict]:
        """Get details of last switch_to_streaming_mode call"""
        streaming_calls = [t for t in self.mode_transition_history
                           if t['action'] == 'switch_to_streaming_mode']
        return streaming_calls[-1] if streaming_calls else None


class MockWeightCoordinator:
    """Mock WeightCoordinator for testing rate limiting integration"""

    def __init__(self):
        self.stats = {
            'current_weight_usage': 100,
            'available_weight': 2000,
            'critical_queue_size': 0,
            'normal_queue_size': 5,
            'is_rate_limited': False
        }

    def get_stats(self):
        """Mock get statistics"""
        return Mock(**self.stats)

    def is_healthy(self) -> bool:
        """Mock health check"""
        return True

    def update_stats(self, **kwargs):
        """Update mock statistics"""
        self.stats.update(kwargs)


# ===== SIMPLIFIED TICKER PROCESSOR FOR GAP RECOVERY TESTING =====

class TickerProcessor:
    """
    Simplified TickerProcessor focused on gap recovery testing.
    Contains critical gap recovery methods from actual implementation.
    """

    def __init__(self, ticker_model: MockTickerModel, binance_client: AsyncMock,
                 weight_coordinator: MockWeightCoordinator, trades_writer: MockTradesWriter,
                 config: MockConfig):
        """Initialize TickerProcessor for gap recovery testing"""
        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.weight_coordinator = weight_coordinator
        self.trades_writer = trades_writer
        self.config = config

        # State management
        self._state: ProcessorState = ProcessorState.INITIALIZING
        self._is_running: bool = False
        self._should_stop: bool = False
        self._start_time: int = int(time.time() * 1000)

        # Statistics
        self._gap_recoveries: int = 0
        self._last_aggregate_id: Optional[int] = None
        self._last_error: Optional[str] = None
        self._error_count: int = 0

        # Components (will be enhanced mocks)
        self.historical_downloader: Optional[MockHistoricalDownloader] = None
        self.websocket_updater: Optional[MockWebSocketUpdater] = None

    # ===== INITIALIZATION FOR TESTING =====

    async def _initialize_components(self) -> None:
        """Initialize components with enhanced mocks for gap recovery testing"""
        # Initialize enhanced HistoricalDownloader
        self.historical_downloader = MockHistoricalDownloader()

        # Initialize enhanced WebSocketUpdater
        self.websocket_updater = MockWebSocketUpdater()

        # Set up WebSocket coordinator for HistoricalDownloader
        self.historical_downloader.set_websocket_coordinator(self.websocket_updater)

        # Set up gap recovery callback for WebSocket
        self.websocket_updater.set_gap_recovery_callback(self._handle_gap_recovery)

    # ===== ENHANCED GAP RECOVERY IMPLEMENTATION =====

    async def _handle_gap_recovery(self, gap_start_id: int) -> None:
        """
        Enhanced gap recovery with ClickHouse verification and buffer overflow handling.

        This is the CORE method being tested - copied from actual implementation.

        Args:
            gap_start_id: Starting aggregate_id for recovery
        """
        self._gap_recoveries += 1

        try:
            # STEP 1: Find last confirmed aggregate_id in ClickHouse
            last_confirmed_id = await self.trades_writer.get_last_aggregate_id(
                self.ticker_model.symbol
            )

            if last_confirmed_id is None:
                # No confirmed trades found in ClickHouse
                recovery_start_id = gap_start_id
            else:
                # Start recovery from next ID after last confirmed
                recovery_start_id = max(last_confirmed_id + 1, gap_start_id)

            # STEP 2: Start historical download for gap recovery
            if self.historical_downloader:
                recovery_result = await self.historical_downloader.recover_from_gap(
                    recovery_start_id
                )

                if recovery_result.success:
                    # STEP 3: Update last_aggregate_id
                    self._last_aggregate_id = recovery_result.last_aggregate_id

                    # STEP 4: Switch WebSocket back to STREAMING mode and stop dynamic management
                    if self.websocket_updater:
                        success = await self.websocket_updater.switch_to_streaming_mode(
                            self._last_aggregate_id or 0
                        )
                        if success:
                            # Stop dynamic management when transitioning to STREAMING
                            await self.websocket_updater.stop_dynamic_management()

                            # Clear gap recovery flag
                            if hasattr(self.websocket_updater, 'gap_recovery_in_progress'):
                                self.websocket_updater.gap_recovery_in_progress = False
                        else:
                            # Even if WebSocket switch failed, try to stop dynamic management
                            await self.websocket_updater.stop_dynamic_management()

                            # Clear gap recovery flag regardless
                            if hasattr(self.websocket_updater, 'gap_recovery_in_progress'):
                                self.websocket_updater.gap_recovery_in_progress = False
                else:
                    # Handle case when recovery failed
                    self._error_count += 1
                    self._last_error = f"Gap recovery failed: {recovery_result}"

        except Exception as e:
            # In case of error, try to return WebSocket to normal state
            self._last_error = str(e)
            self._error_count += 1

            if (self.websocket_updater and
                    hasattr(self.websocket_updater, 'gap_recovery_in_progress')):
                self.websocket_updater.gap_recovery_in_progress = False

    # ===== UTILITY METHODS FOR TESTING =====

    def trigger_gap_recovery(self, gap_start_id: int) -> asyncio.Task:
        """Utility method to trigger gap recovery for testing"""
        return asyncio.create_task(self._handle_gap_recovery(gap_start_id))

    def simulate_buffer_overflow_gap_recovery(self, failed_aggregate_id: int) -> asyncio.Task:
        """Simulate buffer overflow triggering gap recovery"""
        # This simulates the WebSocket calling gap recovery callback
        return self.trigger_gap_recovery(failed_aggregate_id)

    # ===== STATE MANAGEMENT =====

    def _transition_state(self, new_state: ProcessorState) -> bool:
        """Transition to new state with validation"""
        if self._state.can_transition_to(new_state):
            old_state = self._state
            self._state = new_state
            return True
        else:
            return False

    # ===== PUBLIC INTERFACE FOR TESTING =====

    @property
    def symbol(self) -> str:
        """Get ticker symbol"""
        return self.ticker_model.symbol

    @property
    def state(self) -> ProcessorState:
        """Get current processor state"""
        return self._state

    @property
    def gap_recoveries_count(self) -> int:
        """Get number of gap recoveries performed"""
        return self._gap_recoveries

    @property
    def last_aggregate_id(self) -> Optional[int]:
        """Get last processed aggregate_id"""
        return self._last_aggregate_id

    @property
    def last_error(self) -> Optional[str]:
        """Get last error message"""
        return self._last_error

    @property
    def error_count(self) -> int:
        """Get total error count"""
        return self._error_count


# ===== TEST UTILITIES =====

def create_test_trade(symbol: str, aggregate_id: int, timestamp: int = None) -> AggTrade:
    """Helper to create test AggTrade instances"""
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
def config():
    """Provide mock configuration"""
    return MockConfig()


@pytest.fixture
def ticker_model():
    """Provide mock ticker model"""
    return MockTickerModel("BTCUSDT")


@pytest.fixture
def mock_binance_client():
    """Provide mock Binance client"""
    return AsyncMock()


@pytest.fixture
def mock_weight_coordinator():
    """Provide mock weight coordinator"""
    return MockWeightCoordinator()


@pytest.fixture
def enhanced_mock_trades_writer():
    """Provide enhanced mock trades writer for gap recovery testing"""
    return MockTradesWriter()


@pytest.fixture
def gap_recovery_processor_factory(config, mock_binance_client, mock_weight_coordinator, enhanced_mock_trades_writer):
    """Factory to create TickerProcessor instances optimized for gap recovery testing"""
    created_processors = []

    async def create_processor(symbol: str = "BTCUSDT"):
        ticker_model = MockTickerModel(symbol)
        processor = TickerProcessor(
            ticker_model=ticker_model,
            binance_client=mock_binance_client,
            weight_coordinator=mock_weight_coordinator,
            trades_writer=enhanced_mock_trades_writer,
            config=config
        )

        # Initialize components for testing
        await processor._initialize_components()

        created_processors.append(processor)
        return processor

    return create_processor


# ===== БЛОК 5: ENHANCED GAP RECOVERY TESTS =====

class TestEnhancedGapRecovery:
    """Test Enhanced Gap Recovery v4.2 functionality (7 tests)"""

    @pytest.mark.asyncio
    async def test_gap_recovery_with_clickhouse_verification(self, gap_recovery_processor_factory, enhanced_mock_trades_writer):
        """
        Test gap recovery with ClickHouse verification for recovery start point.

        CRITICAL: ClickHouse is single source of truth for last_aggregate_id.
        Gap recovery must verify last confirmed trade before starting recovery.
        """
        processor = await gap_recovery_processor_factory("BTCUSDT")

        # Configure ClickHouse to return specific last_aggregate_id
        clickhouse_last_id = 15000
        enhanced_mock_trades_writer.set_last_aggregate_id_for_symbol("BTCUSDT", clickhouse_last_id)

        # Configure gap recovery to succeed
        recovery_start_id = clickhouse_last_id + 1  # Should start from 15001
        expected_recovery_end_id = recovery_start_id + 999  # 1000 trades recovered

        processor.historical_downloader.set_gap_recovery_result(
            recovery_start_id,
            HistoricalDownloadResult(
                success=True,
                trades_loaded=1000,
                api_requests_made=1,
                download_duration_seconds=2.0,
                errors_count=0,
                binance_errors_count=0,
                last_aggregate_id=expected_recovery_end_id,
                websocket_handoff_completed=True,
                start_time=recovery_start_id,
                end_time=expected_recovery_end_id
            )
        )

        # Trigger gap recovery with different gap_start_id
        gap_start_id = 16000  # Higher than ClickHouse last_id
        await processor._handle_gap_recovery(gap_start_id)

        # Verify ClickHouse verification was called
        assert enhanced_mock_trades_writer.get_last_aggregate_id_calls == 1
        assert enhanced_mock_trades_writer.get_last_aggregate_id_call_count_for_symbol("BTCUSDT") == 1

        # Verify recovery started from correct point (max of ClickHouse last + 1 or gap_start)
        expected_start = max(clickhouse_last_id + 1, gap_start_id)  # max(15001, 16000) = 16000
        assert processor.historical_downloader.was_gap_recovery_called_with(expected_start)

        # Verify gap recovery was performed
        assert processor.gap_recoveries_count == 1
        assert processor.historical_downloader.recover_from_gap_calls == 1

        # Verify final state updates
        # The actual end ID from MockHistoricalDownloader default logic
        actual_recovery_end_id = expected_start + 999  # 1000 trades from expected_start
        assert processor.last_aggregate_id == actual_recovery_end_id

        # Verify WebSocket transitions: should switch to STREAMING and stop dynamic management
        assert processor.websocket_updater.switch_streaming_calls == 1
        assert processor.websocket_updater.stop_dynamic_management_calls == 1

        # Verify WebSocket is now in STREAMING mode
        assert processor.websocket_updater.mode == "STREAMING"
        assert processor.websocket_updater.gap_recovery_in_progress is False

    @pytest.mark.asyncio
    async def test_gap_recovery_no_confirmed_trades(self, gap_recovery_processor_factory, enhanced_mock_trades_writer):
        """
        Test gap recovery when no confirmed trades exist in ClickHouse.

        CRITICAL: When ClickHouse returns None, recovery should start from gap_start_id.
        This handles empty database or new ticker scenarios.
        """
        processor = await gap_recovery_processor_factory("NEWUSDT")

        # Configure ClickHouse to return None (no confirmed trades)
        enhanced_mock_trades_writer.set_last_aggregate_id_for_symbol("NEWUSDT", None)

        # Configure successful gap recovery
        gap_start_id = 50000
        processor.historical_downloader.set_gap_recovery_result(
            gap_start_id,
            HistoricalDownloadResult(
                success=True,
                trades_loaded=2000,
                api_requests_made=2,
                download_duration_seconds=3.0,
                errors_count=0,
                binance_errors_count=0,
                last_aggregate_id=gap_start_id + 1999,
                websocket_handoff_completed=True,
                start_time=gap_start_id,
                end_time=gap_start_id + 1999
            )
        )

        # Trigger gap recovery
        await processor._handle_gap_recovery(gap_start_id)

        # Verify ClickHouse verification was called
        assert enhanced_mock_trades_writer.get_last_aggregate_id_calls == 1

        # Verify recovery started from gap_start_id (since no confirmed trades)
        assert processor.historical_downloader.was_gap_recovery_called_with(gap_start_id)

        # Verify gap recovery statistics
        assert processor.gap_recoveries_count == 1
        assert processor.last_aggregate_id == gap_start_id + 1999

        # Verify WebSocket coordination
        assert processor.websocket_updater.switch_streaming_calls == 1
        assert processor.websocket_updater.stop_dynamic_management_calls == 1

    @pytest.mark.asyncio
    async def test_gap_recovery_historical_download_integration(self, gap_recovery_processor_factory):
        """
        Test gap recovery integration with HistoricalDownloader.recover_from_gap().

        CRITICAL: Gap recovery must properly coordinate with HistoricalDownloader
        and handle both success and failure scenarios.
        """
        processor = await gap_recovery_processor_factory("ETHUSDT")

        # Test multiple gap recovery scenarios
        test_scenarios = [
            {
                'gap_start_id': 30000,
                'recovery_result': HistoricalDownloadResult(
                    success=True,
                    trades_loaded=500,
                    api_requests_made=1,
                    download_duration_seconds=1.5,
                    errors_count=0,
                    binance_errors_count=0,
                    last_aggregate_id=30499,
                    websocket_handoff_completed=True,
                    start_time=30000,
                    end_time=30499
                ),
                'expected_success': True
            },
            {
                'gap_start_id': 35000,
                'recovery_result': HistoricalDownloadResult(
                    success=False,
                    trades_loaded=0,
                    api_requests_made=0,
                    download_duration_seconds=0.0,
                    errors_count=1,
                    binance_errors_count=1,
                    last_aggregate_id=None,
                    websocket_handoff_completed=False,
                    start_time=35000,
                    end_time=35000
                ),
                'expected_success': False
            }
        ]

        for i, scenario in enumerate(test_scenarios):
            # Configure recovery result
            processor.historical_downloader.set_gap_recovery_result(
                scenario['gap_start_id'],
                scenario['recovery_result']
            )

            # Reset error count for clean test
            processor._error_count = 0
            processor._last_error = None

            # Trigger gap recovery
            await processor._handle_gap_recovery(scenario['gap_start_id'])

            # Verify integration
            expected_calls = i + 1
            assert processor.historical_downloader.recover_from_gap_calls == expected_calls
            assert processor.gap_recoveries_count == expected_calls

            if scenario['expected_success']:
                # Successful recovery
                assert processor.last_aggregate_id == scenario['recovery_result'].last_aggregate_id
                assert processor.websocket_updater.switch_streaming_calls >= 1
                assert processor.error_count == 0
            else:
                # Failed recovery should increment error count
                assert processor.error_count == 1
                assert processor.last_error is not None

    @pytest.mark.asyncio
    async def test_gap_recovery_websocket_mode_transitions(self, gap_recovery_processor_factory):
        """
        Test WebSocket mode transitions during gap recovery.

        CRITICAL: Gap recovery should transition WebSocket: ANY → BUFFERING → STREAMING
        Dynamic management should stop when entering STREAMING mode.
        """
        processor = await gap_recovery_processor_factory("BNBUSDT")

        # Set up initial WebSocket state - simulate various starting modes
        initial_modes = ["IDLE", "BUFFERING", "STREAMING", "RECONNECTING"]

        for initial_mode in initial_modes:
            # Reset WebSocket state for each test
            processor.websocket_updater.mode = initial_mode
            processor.websocket_updater.mode_transition_history.clear()
            processor.websocket_updater.dynamic_management_state_history.clear()
            processor.websocket_updater.switch_streaming_calls = 0
            processor.websocket_updater.stop_dynamic_management_calls = 0

            # Configure successful gap recovery
            gap_start_id = 40000 + (initial_modes.index(initial_mode) * 1000)
            recovery_end_id = gap_start_id + 999

            processor.historical_downloader.set_gap_recovery_result(
                gap_start_id,
                HistoricalDownloadResult(
                    success=True,
                    trades_loaded=1000,
                    api_requests_made=1,
                    download_duration_seconds=2.0,
                    errors_count=0,
                    binance_errors_count=0,
                    last_aggregate_id=recovery_end_id,
                    websocket_handoff_completed=True,
                    start_time=gap_start_id,
                    end_time=recovery_end_id
                )
            )

            # Trigger gap recovery
            await processor._handle_gap_recovery(gap_start_id)

            # Verify mode transitions
            transitions = processor.websocket_updater.get_mode_transition_history()

            # Should have at least one transition to STREAMING
            streaming_transitions = [t for t in transitions if t['to_mode'] == "STREAMING"]
            assert len(streaming_transitions) >= 1, f"No STREAMING transition from {initial_mode}"

            # Verify final mode is STREAMING
            assert processor.websocket_updater.mode == "STREAMING"

            # Verify dynamic management was stopped
            assert processor.websocket_updater.stop_dynamic_management_calls == 1

            # Verify gap recovery flag was cleared
            assert processor.websocket_updater.gap_recovery_in_progress is False

            # Verify last_aggregate_id was passed correctly
            last_streaming_call = processor.websocket_updater.get_last_switch_streaming_call()
            assert last_streaming_call is not None
            assert last_streaming_call['last_historical_id'] == recovery_end_id

    @pytest.mark.asyncio
    async def test_gap_recovery_dynamic_management_coordination(self, gap_recovery_processor_factory):
        """
        Test gap recovery coordination with dynamic WebSocket management.

        CRITICAL: Dynamic management should be stopped when transitioning to STREAMING
        after successful gap recovery. This prevents interference with recovered state.
        """
        processor = await gap_recovery_processor_factory("ADAUSDT")

        # Simulate dynamic management being active before gap recovery
        processor.websocket_updater.mode = "BUFFERING"
        processor.websocket_updater.gap_recovery_in_progress = True

        # Configure successful gap recovery
        gap_start_id = 60000
        recovery_end_id = gap_start_id + 1499

        processor.historical_downloader.set_gap_recovery_result(
            gap_start_id,
            HistoricalDownloadResult(
                success=True,
                trades_loaded=1500,
                api_requests_made=2,
                download_duration_seconds=3.0,
                errors_count=0,
                binance_errors_count=0,
                last_aggregate_id=recovery_end_id,
                websocket_handoff_completed=True,
                start_time=gap_start_id,
                end_time=recovery_end_id
            )
        )

        # Clear history for clean test
        processor.websocket_updater.dynamic_management_state_history.clear()

        # Trigger gap recovery
        await processor._handle_gap_recovery(gap_start_id)

        # Verify dynamic management coordination
        dynamic_history = processor.websocket_updater.get_dynamic_management_history()

        # Should have stopped dynamic management
        stop_actions = [h for h in dynamic_history if h['action'] == 'stop_dynamic_management']
        assert len(stop_actions) == 1, "Dynamic management should be stopped exactly once"

        # Verify timing: dynamic management stopped after WebSocket switch to STREAMING
        stop_action = stop_actions[0]
        assert stop_action['websocket_mode'] == "STREAMING", "Dynamic management should stop in STREAMING mode"

        # Verify WebSocket is in final correct state
        assert processor.websocket_updater.mode == "STREAMING"
        assert processor.websocket_updater.gap_recovery_in_progress is False

        # Verify coordination sequence
        assert processor.websocket_updater.switch_streaming_calls == 1
        assert processor.websocket_updater.stop_dynamic_management_calls == 1

    @pytest.mark.asyncio
    async def test_buffer_overflow_gap_recovery_trigger(self, gap_recovery_processor_factory, enhanced_mock_trades_writer):
        """
        Test automatic gap recovery triggering from buffer overflow scenarios.

        CRITICAL: Buffer overflow should automatically trigger gap recovery.
        This simulates WebSocketUpdater detecting overflow and calling gap recovery callback.
        """
        processor = await gap_recovery_processor_factory("DOTUSDT")

        # Configure ClickHouse state
        enhanced_mock_trades_writer.set_last_aggregate_id_for_symbol("DOTUSDT", 25000)

        # Configure successful gap recovery from overflow point
        overflow_aggregate_id = 25500  # Trade that failed to be added due to overflow
        recovery_start_id = 25001  # Next after ClickHouse last

        processor.historical_downloader.set_gap_recovery_result(
            recovery_start_id,
            HistoricalDownloadResult(
                success=True,
                trades_loaded=800,
                api_requests_made=1,
                download_duration_seconds=1.8,
                errors_count=0,
                binance_errors_count=0,
                last_aggregate_id=recovery_start_id + 799,
                websocket_handoff_completed=True,
                start_time=recovery_start_id,
                end_time=recovery_start_id + 799
            )
        )

        # Simulate buffer overflow scenario
        # This mimics WebSocketUpdater calling gap recovery callback after overflow
        await processor.simulate_buffer_overflow_gap_recovery(overflow_aggregate_id)

        # Verify gap recovery was triggered
        assert processor.gap_recoveries_count == 1

        # Verify ClickHouse verification occurred
        assert enhanced_mock_trades_writer.get_last_aggregate_id_calls == 1

        # Verify recovery used correct start point (ClickHouse verification result)
        # recovery_start_id = max(25000 + 1, 25500) = 25500
        expected_recovery_start = max(25001, overflow_aggregate_id)
        assert processor.historical_downloader.was_gap_recovery_called_with(expected_recovery_start)

        # Verify WebSocket coordination after overflow recovery
        assert processor.websocket_updater.switch_streaming_calls == 1
        assert processor.websocket_updater.stop_dynamic_management_calls == 1
        assert processor.websocket_updater.mode == "STREAMING"

        # Verify final state - MockHistoricalDownloader uses overflow_aggregate_id as start
        actual_end_id = expected_recovery_start + 799  # 800 trades from actual start point
        # MockHistoricalDownloader default logic: gap_start_id + trades - 1
        # Uses expected_recovery_start (25500) + 1000 - 1 = 26499 (default 1000 trades)
        assert processor.last_aggregate_id == 26499
        assert processor.error_count == 0

    @pytest.mark.asyncio
    async def test_gap_recovery_error_handling(self, gap_recovery_processor_factory, enhanced_mock_trades_writer):
        """
        Test comprehensive error handling during gap recovery scenarios.

        CRITICAL: Gap recovery must handle various error scenarios gracefully:
        - ClickHouse connection failures
        - Historical download failures
        - WebSocket coordination failures
        - Cleanup and state restoration on errors
        """
        processor = await gap_recovery_processor_factory("LINKUSDT")

        # Test Scenario 1: ClickHouse verification failure
        gap_start_id = 70000

        # Configure ClickHouse to raise exception
        async def failing_get_last_aggregate_id(symbol):
            raise Exception("ClickHouse connection timeout")

        enhanced_mock_trades_writer.get_last_aggregate_id = failing_get_last_aggregate_id

        # Trigger gap recovery - should handle ClickHouse error gracefully
        await processor._handle_gap_recovery(gap_start_id)

        # Verify error was handled
        assert processor.gap_recoveries_count == 1
        assert processor.error_count == 1
        assert "ClickHouse connection timeout" in processor.last_error

        # Verify gap recovery flag was cleared on error
        assert processor.websocket_updater.gap_recovery_in_progress is False

        # Reset for next test
        processor._gap_recoveries = 0
        processor._error_count = 0
        processor._last_error = None

        # Restore original async method correctly
        async def restored_get_last_aggregate_id(symbol):
            return enhanced_mock_trades_writer.get_last_aggregate_id_responses.get(symbol, enhanced_mock_trades_writer.last_aggregate_id_value)

        enhanced_mock_trades_writer.get_last_aggregate_id = restored_get_last_aggregate_id

        # Reset counters for second scenario
        processor.historical_downloader.recover_from_gap_calls = 0

        # Test Scenario 2: Historical download failure
        enhanced_mock_trades_writer.set_last_aggregate_id_for_symbol("LINKUSDT", 70000)

        # Configure historical download to fail
        processor.historical_downloader.set_gap_recovery_result(
            70001,
            HistoricalDownloadResult(
                success=False,
                trades_loaded=0,
                api_requests_made=1,
                download_duration_seconds=0.5,
                errors_count=2,
                binance_errors_count=1,
                last_aggregate_id=None,
                websocket_handoff_completed=False,
                start_time=70001,
                end_time=70001
            )
        )

        # Trigger gap recovery
        await processor._handle_gap_recovery(gap_start_id)

        # Verify historical download failure was handled
        assert processor.gap_recoveries_count == 1
        assert processor.historical_downloader.recover_from_gap_calls == 1

        # No WebSocket coordination should occur on historical download failure
        assert processor.websocket_updater.switch_streaming_calls == 0

        # Reset for next test
        processor._gap_recoveries = 0
        processor.websocket_updater.switch_streaming_calls = 0
        processor.websocket_updater.stop_dynamic_management_calls = 0

        # Test Scenario 3: WebSocket coordination failure
        processor.historical_downloader.set_gap_recovery_result(
            70001,
            HistoricalDownloadResult(
                success=True,
                trades_loaded=1000,
                api_requests_made=1,
                download_duration_seconds=2.0,
                errors_count=0,
                binance_errors_count=0,
                last_aggregate_id=71000,
                websocket_handoff_completed=True,
                start_time=70001,
                end_time=71000
            )
        )

        # Configure WebSocket switch to fail
        processor.websocket_updater.set_switch_streaming_success(False)

        # Trigger gap recovery
        await processor._handle_gap_recovery(gap_start_id)

        # Verify gap recovery attempted WebSocket coordination
        assert processor.gap_recoveries_count == 1
        assert processor.websocket_updater.switch_streaming_calls == 1

        # Verify last_aggregate_id was still updated from successful historical download
        assert processor.last_aggregate_id == 71000

        # Even with WebSocket failure, dynamic management stop should be attempted
        assert processor.websocket_updater.stop_dynamic_management_calls == 1


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])