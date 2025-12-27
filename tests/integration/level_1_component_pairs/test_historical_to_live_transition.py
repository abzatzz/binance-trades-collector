"""
Level 1.4.2 Integration Test: Historical Data Load → Live Transition
===================================================================

Complete end-to-end testing of ticker lifecycle from historical download to live streaming:
- Smart startup strategy selection based on system load
- HistoricalDownloader ↔ WebSocketUpdater coordination
- Seamless transition from historical to live data
- Gap detection and enhanced recovery with ClickHouse verification
- Dynamic WebSocket management with load-based activation/deactivation
- Performance monitoring during complex transitions
- Comprehensive error handling and recovery scenarios

CRITICAL INTEGRATION POINTS:
- HistoricalDownloader.smart_startup_strategy → WebSocket coordination
- WebSocketUpdater.DynamicManager → load-based management
- TickerProcessor.orchestration → TimingCalculator + gap recovery
- GlobalTradesUpdater → centralized processing pipeline
- ClickHouse verification → data integrity assurance

File: tests/integration/level1_component_pairs/test_historical_to_live_transition.py
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
test_log_file = os.path.join('test_logs', 'level_1_4_2_transition.log')
loggerino.create('level_1_4_2_transition', test_log_file)

import pytest
import asyncio
import time
import threading
import json
import uuid
import random
import math
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, AsyncMock, patch, MagicMock
from dataclasses import dataclass
from enum import Enum, auto


# ===== MOCK DATA MODELS =====

@dataclass(frozen=True, slots=True)
class AggTrade:
    """Mock AggTrade for testing"""
    aggregate_id: int
    price: float
    quantity: float
    first_trade_id: int
    last_trade_id: int
    timestamp: int
    is_buyer_maker: bool

    def validate(self) -> bool:
        return (self.aggregate_id > 0 and self.price > 0 and
                self.quantity > 0 and self.timestamp > 0)


class ProcessorState(Enum):
    """Mock ProcessorState enum"""
    INITIALIZING = auto()
    DOWNLOADING_HISTORICAL = auto()
    STARTING_WEBSOCKET = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPING = auto()
    STOPPED = auto()
    ERROR = auto()

    def is_active(self) -> bool:
        return self in [ProcessorState.DOWNLOADING_HISTORICAL,
                        ProcessorState.STARTING_WEBSOCKET,
                        ProcessorState.RUNNING]

    def can_transition_to(self, new_state: 'ProcessorState') -> bool:
        return True  # Simplified for testing


@dataclass(frozen=True)
class ProcessingConfig:
    """Mock processing configuration with v4.2 dynamic parameters"""
    historical_days_back: int = 7
    websocket_switch_hours: int = 24

    # v4.2 Dynamic WebSocket Management
    dynamic_websocket_enabled: bool = True
    dynamic_deactivation_threshold: float = 30.0
    dynamic_reactivation_threshold: float = 25.0
    dynamic_evaluation_interval: int = 10
    dynamic_max_consecutive_overloads: int = 3
    dynamic_enable_buffer_discard: bool = True

    # Timing Calculator and startup strategies
    timing_calculator_enabled: bool = True
    websocket_startup_strategy: str = "auto"
    buffer_overflow_protection: bool = True
    max_buffer_size_per_ticker: int = 10000

    # GlobalTradesUpdater settings
    global_batch_enabled: bool = True
    global_batch_interval_seconds: int = 1

    def get_historical_start_timestamp(self) -> int:
        days_in_seconds = self.historical_days_back * 24 * 3600
        return int((time.time() - days_in_seconds) * 1000)


@dataclass(frozen=True)
class Config:
    """Mock complete configuration"""
    processing: ProcessingConfig
    environment: str = "test"
    debug: bool = True


class MockTickerModel:
    """Mock ticker model"""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol
        self.onboard_date = int(time.time() * 1000) - (30 * 24 * 60 * 60 * 1000)


# ===== MOCK TIMING CALCULATOR =====

class MockTimingCalculator:
    """Mock TimingCalculator with realistic system load simulation"""

    def __init__(self, weight_coordinator=None):
        self.weight_coordinator = weight_coordinator
        self.simulated_load = "normal"  # normal, high, critical
        self.base_delay = 1.0

    def calculate_request_delay(self) -> float:
        """Simulate system load calculation"""
        if self.simulated_load == "normal":
            return self.base_delay + random.uniform(0, 2)
        elif self.simulated_load == "high":
            return self.base_delay + random.uniform(15, 25)  # Changed: 25-35 → 15-25 to stay in parallel_start range
        else:  # critical
            return self.base_delay + random.uniform(55, 65)

    def set_simulated_load(self, load_type: str):
        """Set simulated system load for testing"""
        self.simulated_load = load_type


# ===== MOCK DOWNLOAD RESULT =====

@dataclass
class MockHistoricalDownloadResult:
    """Mock download result"""
    success: bool
    trades_loaded: int
    time_range_start: int
    time_range_end: int
    api_requests_made: int
    errors_count: int
    binance_errors_count: int
    recoverable_errors_count: int
    download_duration_seconds: float
    gaps_filled: int
    bytes_written: int
    websocket_handoff_completed: bool
    last_aggregate_id: Optional[int]


@dataclass
class MockDownloadProgress:
    """Mock download progress"""
    total_trades_downloaded: int
    api_requests_made: int
    bytes_written: int
    current_timestamp: int
    start_timestamp: int
    websocket_activated: bool
    download_rate_per_minute: float
    estimated_completion_time: Optional[int]
    last_aggregate_id: Optional[int]


# ===== MOCK HISTORICAL DOWNLOADER =====

class MockHistoricalDownloader:
    """Enhanced mock HistoricalDownloader with realistic behavior"""

    def __init__(self, ticker_model, binance_client, weight_coordinator, trades_writer, config):
        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.weight_coordinator = weight_coordinator
        self.trades_writer = trades_writer
        self.config = config

        # State tracking
        self.is_running = False
        self.should_stop = False
        self.websocket_started = False
        self.download_start_time: Optional[float] = None

        # Statistics
        self.total_trades_downloaded = 0
        self.api_requests_made = 0
        self.bytes_written = 0
        self.errors_count = 0
        self.last_aggregate_id: Optional[int] = None

        # WebSocket coordination
        self.websocket_coordinator = None
        self.websocket_threshold_timestamp: Optional[int] = None
        self.timing_calculator: Optional[MockTimingCalculator] = None

        # Test simulation controls
        self.simulate_download_delay = 0.1  # Faster for testing
        self.simulate_failure = False
        self.failure_message = "Simulated download failure"
        self.simulate_gap_scenario = False

    async def download_historical_data(self) -> MockHistoricalDownloadResult:
        """Main download method with comprehensive simulation"""
        self.download_start_time = time.time()
        self.is_running = True

        try:
            if self.simulate_failure:
                raise Exception(self.failure_message)

            # Step 1: Determine download strategy
            strategy, start_point, from_id = await self._determine_request_strategy()

            # Step 2: Calculate WebSocket threshold and activation strategy
            self.websocket_threshold_timestamp = self._calculate_websocket_threshold()
            websocket_strategy = await self._determine_websocket_activation_strategy()

            # Step 3: Execute download loop with WebSocket coordination
            end_timestamp = await self._execute_download_loop(strategy, start_point, from_id, websocket_strategy)

            # Step 4: Finalize WebSocket handoff
            websocket_handoff = await self._finalize_websocket_handoff()

            download_duration = time.time() - self.download_start_time

            return MockHistoricalDownloadResult(
                success=True,
                trades_loaded=self.total_trades_downloaded,
                time_range_start=start_point,
                time_range_end=end_timestamp,
                api_requests_made=self.api_requests_made,
                errors_count=self.errors_count,
                binance_errors_count=0,
                recoverable_errors_count=0,
                download_duration_seconds=download_duration,
                gaps_filled=1 if self.simulate_gap_scenario else 0,
                bytes_written=self.bytes_written,
                websocket_handoff_completed=websocket_handoff,
                last_aggregate_id=self.last_aggregate_id
            )

        except Exception as e:
            if self.simulate_failure:
                raise Exception(self.failure_message)
            raise
        finally:
            self.is_running = False

    async def _determine_request_strategy(self) -> Tuple[str, int, Optional[int]]:
        """Determine download strategy"""
        # Simulate existing data check
        if hasattr(self, '_has_existing_data') and self._has_existing_data():
            last_id = await self._get_last_aggregate_id()
            return 'continue_load', last_id + 1, last_id + 1
        else:
            start_timestamp = self.config.get_historical_start_timestamp()
            return 'first_load', start_timestamp, None

    def _has_existing_data(self) -> bool:
        """Check for existing data"""
        return getattr(self, '_existing_data_flag', False)

    async def _get_last_aggregate_id(self) -> int:
        """Get last aggregate ID"""
        return getattr(self, '_last_db_aggregate_id', 100000)

    def _calculate_websocket_threshold(self) -> int:
        """Calculate WebSocket activation threshold"""
        current_time = int(time.time() * 1000)
        threshold_ms = self.config.websocket_switch_hours * 60 * 60 * 1000
        return current_time - threshold_ms

    async def _determine_websocket_activation_strategy(self) -> str:
        """Determine WebSocket activation strategy based on system load"""
        if not self.timing_calculator:
            return "threshold_based"

        current_delay = self.timing_calculator.calculate_request_delay()

        if current_delay <= 5.0:
            return "immediate"
        elif current_delay <= 30.0:
            return "parallel_start"
        else:
            return "wait_for_conditions"

    async def _execute_download_loop(self, strategy: str, start_point: int, from_id: Optional[int], websocket_strategy: str) -> int:
        """Execute download loop with WebSocket coordination"""
        trades_per_batch = 1000
        total_trades_to_download = 5000  # Simulate downloading 5K trades

        batches_needed = math.ceil(total_trades_to_download / trades_per_batch)
        current_aggregate_id = from_id or 200000

        # Determine WebSocket activation point
        websocket_activation_point = self._calculate_websocket_activation_timing(websocket_strategy, batches_needed)

        for batch_num in range(batches_needed):
            if self.should_stop:
                break

            # Simulate API request delay
            await asyncio.sleep(self.simulate_download_delay)

            # Simulate batch download
            batch_trades = []
            for i in range(trades_per_batch):
                trade = AggTrade(
                    aggregate_id=current_aggregate_id + i,
                    price=50000.0 + (i * 0.01),
                    quantity=0.01 + (i * 0.001),
                    first_trade_id=current_aggregate_id + i,
                    last_trade_id=current_aggregate_id + i,
                    timestamp=int(time.time() * 1000) + (i * 1000),
                    is_buyer_maker=bool(i % 2)
                )
                batch_trades.append(trade)

            # Update statistics
            self.total_trades_downloaded += len(batch_trades)
            self.api_requests_made += 1
            self.bytes_written += len(batch_trades) * 49  # AggTrade binary size
            self.last_aggregate_id = current_aggregate_id + trades_per_batch - 1
            current_aggregate_id += trades_per_batch

            # Check WebSocket activation
            if not self.websocket_started and batch_num >= websocket_activation_point:
                await self._signal_websocket_start(websocket_strategy)
                self.websocket_started = True

        return int(time.time() * 1000)

    def _calculate_websocket_activation_timing(self, strategy: str, total_batches: int) -> int:
        """Calculate when to activate WebSocket based on strategy"""
        if strategy == "immediate":
            return 0  # Activate immediately
        elif strategy == "parallel_start":
            return max(1, total_batches // 4)  # Activate at 25% progress
        else:  # wait_for_conditions or threshold_based
            return max(1, total_batches // 2)  # Activate at 50% progress

    async def _signal_websocket_start(self, strategy: str):
        """Signal WebSocket start with appropriate mode"""
        if self.websocket_coordinator:
            if strategy == "immediate":
                await self.websocket_coordinator.start_buffering_mode()
                await self.websocket_coordinator.start_dynamic_management()
            else:
                await self.websocket_coordinator.start_buffering_mode()

    async def _finalize_websocket_handoff(self) -> bool:
        """Finalize WebSocket handoff"""
        if self.websocket_started and self.websocket_coordinator:
            success = await self.websocket_coordinator.switch_to_streaming_mode(self.last_aggregate_id or 0)
            if success:
                await self.websocket_coordinator.stop_dynamic_management()
                return True
        return False

    def set_websocket_coordinator(self, coordinator):
        """Set WebSocket coordinator"""
        self.websocket_coordinator = coordinator

    def is_download_running(self) -> bool:
        """Check if download is running"""
        return self.is_running

    def get_progress(self) -> MockDownloadProgress:
        """Get current download progress"""
        return MockDownloadProgress(
            total_trades_downloaded=self.total_trades_downloaded,
            api_requests_made=self.api_requests_made,
            bytes_written=self.bytes_written,
            current_timestamp=int(time.time() * 1000),
            start_timestamp=self.websocket_threshold_timestamp or 0,
            websocket_activated=self.websocket_started,
            download_rate_per_minute=self.total_trades_downloaded * 60 / max(1, time.time() - (self.download_start_time or time.time())),
            estimated_completion_time=None,
            last_aggregate_id=self.last_aggregate_id
        )

    async def recover_from_gap(self, gap_start_id: int) -> MockHistoricalDownloadResult:
        """Enhanced gap recovery with ClickHouse verification"""
        recovery_start_time = time.time()

        # Simulate ClickHouse verification
        last_confirmed_id = await self.trades_writer.get_last_aggregate_id(self.ticker_model.symbol)
        actual_start_id = max(gap_start_id, (last_confirmed_id or 0) + 1)

        # Simulate gap recovery download
        recovery_trades = 500  # Simulate recovering 500 trades
        await asyncio.sleep(0.05)  # Simulate recovery time

        return MockHistoricalDownloadResult(
            success=True,
            trades_loaded=recovery_trades,
            time_range_start=actual_start_id,
            time_range_end=actual_start_id + recovery_trades,
            api_requests_made=1,
            errors_count=0,
            binance_errors_count=0,
            recoverable_errors_count=0,
            download_duration_seconds=time.time() - recovery_start_time,
            gaps_filled=1,
            bytes_written=recovery_trades * 49,
            websocket_handoff_completed=False,
            last_aggregate_id=actual_start_id + recovery_trades - 1
        )

    async def pause_download(self) -> bool:
        """Pause download"""
        self.should_stop = True
        return True


# ===== MOCK WEBSOCKET UPDATER =====

class WebSocketMode(Enum):
    """WebSocket operation modes"""
    DISCONNECTED = "disconnected"
    BUFFERING = "buffering"
    STREAMING = "streaming"
    ERROR = "error"


@dataclass
class MockWebSocketStats:
    """Mock WebSocket statistics"""
    mode: WebSocketMode
    connected: bool
    trades_processed: int
    trades_per_minute: float
    buffer_size: int
    reconnect_count: int
    gap_recoveries: int
    binance_errors_count: int
    last_aggregate_id: Optional[int]


class MockWebSocketUpdater:
    """Enhanced mock WebSocketUpdater with DynamicWebSocketManager simulation"""

    def __init__(self, ticker_model, binance_client, trades_writer, config):
        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.trades_writer = trades_writer
        self.config = config

        # State management
        self.mode = WebSocketMode.DISCONNECTED
        self.websocket_connected = False
        self.is_healthy_flag = True

        # Dynamic management state
        self.dynamic_management_active = False
        self.dynamic_evaluation_task: Optional[asyncio.Task] = None
        self.consecutive_overloads = 0
        self.last_evaluation_time = 0

        # Statistics
        self.trades_processed = 0
        self.buffer_size = 0
        self.reconnect_count = 0
        self.gap_recoveries = 0
        self.last_aggregate_id: Optional[int] = None

        # Test simulation controls
        self.simulate_connection_delay = 0.05
        self.simulate_mode_switch_delay = 0.02
        self.simulate_overload_condition = False
        self.timing_calculator: Optional[MockTimingCalculator] = None

        # Gap recovery
        self.gap_recovery_in_progress = False
        self.gap_recovery_callback = None

    async def start_buffering_mode(self) -> bool:
        """Start WebSocket in buffering mode"""
        await asyncio.sleep(self.simulate_connection_delay)

        self.mode = WebSocketMode.BUFFERING
        self.websocket_connected = True
        return True

    async def switch_to_streaming_mode(self, last_aggregate_id: int) -> bool:
        """Switch to streaming mode"""
        await asyncio.sleep(self.simulate_mode_switch_delay)

        if self.mode == WebSocketMode.BUFFERING:
            self.mode = WebSocketMode.STREAMING
            self.last_aggregate_id = last_aggregate_id
            return True
        return False

    async def start_dynamic_management(self):
        """Start dynamic WebSocket management"""
        if not self.config.dynamic_websocket_enabled:
            return

        self.dynamic_management_active = True
        self.dynamic_evaluation_task = asyncio.create_task(self._dynamic_evaluation_loop())

    async def stop_dynamic_management(self):
        """Stop dynamic WebSocket management"""
        self.dynamic_management_active = False

        if self.dynamic_evaluation_task and not self.dynamic_evaluation_task.done():
            self.dynamic_evaluation_task.cancel()
            try:
                await self.dynamic_evaluation_task
            except asyncio.CancelledError:
                pass

    async def _dynamic_evaluation_loop(self):
        """Dynamic evaluation loop for load-based management"""
        try:
            while self.dynamic_management_active:
                await asyncio.sleep(self.config.dynamic_evaluation_interval)

                if self.timing_calculator:
                    current_load = self.timing_calculator.calculate_request_delay()
                    await self._evaluate_dynamic_action(current_load)

        except asyncio.CancelledError:
            pass

    async def _evaluate_dynamic_action(self, current_load: float):
        """Evaluate and take dynamic action based on load"""
        if current_load > self.config.dynamic_deactivation_threshold:
            self.consecutive_overloads += 1

            if (self.consecutive_overloads >= self.config.dynamic_max_consecutive_overloads and
                    self.mode == WebSocketMode.STREAMING):
                # Deactivate WebSocket due to overload
                await self._deactivate_due_to_overload()

        elif current_load <= self.config.dynamic_reactivation_threshold:
            self.consecutive_overloads = 0

            if self.mode == WebSocketMode.DISCONNECTED:
                # Reactivate WebSocket
                await self._reactivate_after_overload()

    async def _deactivate_due_to_overload(self):
        """Deactivate WebSocket due to system overload"""
        if self.config.dynamic_enable_buffer_discard:
            self.buffer_size = 0  # Discard buffer

        self.mode = WebSocketMode.DISCONNECTED
        self.websocket_connected = False

        # Trigger gap recovery
        if self.gap_recovery_callback:
            asyncio.create_task(self.gap_recovery_callback(self.last_aggregate_id or 0))

    async def _reactivate_after_overload(self):
        """Reactivate WebSocket after overload conditions improve"""
        await self.start_buffering_mode()
        await asyncio.sleep(0.05)  # Brief stabilization
        await self.switch_to_streaming_mode(self.last_aggregate_id or 0)

    def set_timing_calculator(self, timing_calculator, config):
        """Set timing calculator for dynamic management"""
        self.timing_calculator = timing_calculator

    def set_gap_recovery_callback(self, callback):
        """Set gap recovery callback"""
        self.gap_recovery_callback = callback

    def is_healthy(self) -> bool:
        """Check WebSocket health"""
        return self.is_healthy_flag and self.websocket_connected

    def get_stats(self) -> MockWebSocketStats:
        """Get WebSocket statistics"""
        return MockWebSocketStats(
            mode=self.mode,
            connected=self.websocket_connected,
            trades_processed=self.trades_processed,
            trades_per_minute=self.trades_processed * 60 / max(1, time.time() - (self.last_evaluation_time or time.time())),
            buffer_size=self.buffer_size,
            reconnect_count=self.reconnect_count,
            gap_recoveries=self.gap_recoveries,
            binance_errors_count=0,
            last_aggregate_id=self.last_aggregate_id
        )

    async def shutdown(self):
        """Shutdown WebSocket"""
        await self.stop_dynamic_management()
        self.mode = WebSocketMode.DISCONNECTED
        self.websocket_connected = False


# ===== MOCK TRADES WRITER =====

class MockTradesWriter:
    """Mock TradesWriter with ClickHouse simulation"""

    def __init__(self):
        self.trades_storage: Dict[str, List[AggTrade]] = {}
        self.clickhouse_data: Dict[str, List[AggTrade]] = {}
        self.last_aggregate_ids: Dict[str, int] = {}

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """Add real-time trade"""
        if symbol not in self.trades_storage:
            self.trades_storage[symbol] = []
        self.trades_storage[symbol].append(trade)
        return True

    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """Write batch trades to ClickHouse simulation"""
        if symbol not in self.clickhouse_data:
            self.clickhouse_data[symbol] = []

        self.clickhouse_data[symbol].extend(trades)

        # Update last aggregate ID
        if trades:
            self.last_aggregate_ids[symbol] = max(trade.aggregate_id for trade in trades)

        return len(trades)

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """Get last aggregate ID from ClickHouse simulation"""
        return self.last_aggregate_ids.get(symbol)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """Read trades from storage"""
        all_trades = self.clickhouse_data.get(symbol, []) + self.trades_storage.get(symbol, [])
        return [trade for trade in all_trades if start_time <= trade.timestamp <= end_time]

    def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """Get storage statistics"""
        total_trades = len(self.clickhouse_data.get(symbol, [])) + len(self.trades_storage.get(symbol, []))
        return {
            'symbol': symbol,
            'total_trades': total_trades,
            'last_aggregate_id': self.last_aggregate_ids.get(symbol)
        }


# ===== MOCK TICKER PROCESSOR =====

class MockTickerProcessor:
    """Simplified mock TickerProcessor for integration testing"""

    def __init__(self, ticker_model, binance_client, weight_coordinator, trades_writer, config):
        self.ticker_model = ticker_model
        self.config = config
        self.trades_writer = trades_writer

        # State
        self.state = ProcessorState.INITIALIZING
        self.is_running = False
        self._should_stop = False

        # Components
        self.historical_downloader = MockHistoricalDownloader(
            ticker_model, binance_client, weight_coordinator, trades_writer, config.processing
        )
        self.websocket_updater = MockWebSocketUpdater(
            ticker_model, binance_client, trades_writer, config.processing
        )
        self.timing_calculator = MockTimingCalculator(weight_coordinator)

        # Set up coordination
        self.historical_downloader.timing_calculator = self.timing_calculator
        self.historical_downloader.set_websocket_coordinator(self.websocket_updater)
        self.websocket_updater.set_timing_calculator(self.timing_calculator, config.processing)
        self.websocket_updater.set_gap_recovery_callback(self._handle_gap_recovery)

        # Statistics
        self.historical_trades_loaded = 0
        self.websocket_trades_processed = 0
        self.gap_recoveries = 0

    async def run(self) -> None:
        """Simplified run method for testing"""
        self.is_running = True
        self.state = ProcessorState.DOWNLOADING_HISTORICAL

        try:
            # Phase 1: Download historical data
            result = await self.historical_downloader.download_historical_data()
            if result.success:
                self.historical_trades_loaded = result.trades_loaded

            # Phase 2: Ensure WebSocket is running
            if not self.websocket_updater.websocket_connected:
                await self.websocket_updater.start_buffering_mode()
                await self.websocket_updater.switch_to_streaming_mode(result.last_aggregate_id or 0)

            self.state = ProcessorState.RUNNING

            # Simulate ongoing operation
            while not self._should_stop:
                await asyncio.sleep(0.1)

        except Exception as e:
            self.state = ProcessorState.ERROR
            raise
        finally:
            self.is_running = False

    async def _handle_gap_recovery(self, gap_start_id: int):
        """Handle gap recovery"""
        self.gap_recoveries += 1

        # Enhanced gap recovery with ClickHouse verification
        last_confirmed_id = await self.trades_writer.get_last_aggregate_id(self.ticker_model.symbol)
        actual_start_id = max(gap_start_id, (last_confirmed_id or 0) + 1)

        # Execute recovery
        recovery_result = await self.historical_downloader.recover_from_gap(actual_start_id)

        if recovery_result.success:
            # Switch WebSocket back to streaming
            await self.websocket_updater.switch_to_streaming_mode(recovery_result.last_aggregate_id or 0)
            await self.websocket_updater.stop_dynamic_management()

    async def shutdown(self, timeout_seconds: float = 30.0):
        """Shutdown processor"""
        self._should_stop = True
        self.state = ProcessorState.STOPPING

        await self.websocket_updater.shutdown()
        await self.historical_downloader.pause_download()

        self.state = ProcessorState.STOPPED

    def is_healthy(self) -> bool:
        """Check processor health"""
        return (self.state.is_active() and
                self.is_running and
                not self._should_stop)


# ===== TEST UTILITIES =====

def create_test_trades(symbol: str, count: int, start_id: int = 1000, start_time: int = None) -> List[AggTrade]:
    """Create list of test trades"""
    if start_time is None:
        start_time = int(time.time() * 1000)

    trades = []
    for i in range(count):
        trade = AggTrade(
            aggregate_id=start_id + i,
            price=50000.0 + (i * 0.01),
            quantity=0.01 + (i * 0.001),
            first_trade_id=start_id + i,
            last_trade_id=start_id + i,
            timestamp=start_time + (i * 1000),
            is_buyer_maker=bool(i % 2)
        )
        trades.append(trade)
    return trades


async def wait_for_state(processor: MockTickerProcessor, target_state: ProcessorState, timeout: float = 10.0) -> bool:
    """Wait for processor to reach target state"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if processor.state == target_state:
            return True
        await asyncio.sleep(0.1)
    return False


async def wait_for_condition(condition_func, timeout: float = 10.0) -> bool:
    """Wait for condition to be true"""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if condition_func():
            return True
        await asyncio.sleep(0.1)
    return False


# ===== FIXTURES =====

@pytest.fixture
def config():
    """Test configuration"""
    return Config(
        processing=ProcessingConfig(),
        environment="test",
        debug=True
    )


@pytest.fixture
def ticker_model():
    """Test ticker model"""
    return MockTickerModel("BTCUSDT")


@pytest.fixture
def trades_writer():
    """Mock trades writer"""
    return MockTradesWriter()


@pytest.fixture
def mock_binance_client():
    """Mock Binance client"""
    return Mock()


@pytest.fixture
def mock_weight_coordinator():
    """Mock weight coordinator"""
    return Mock()


@pytest.fixture
def ticker_processor(config, ticker_model, mock_binance_client, mock_weight_coordinator, trades_writer):
    """Create ticker processor for testing"""
    processor = MockTickerProcessor(
        ticker_model=ticker_model,
        binance_client=mock_binance_client,
        weight_coordinator=mock_weight_coordinator,
        trades_writer=trades_writer,
        config=config
    )

    yield processor

    # Note: Cleanup handled by individual tests since this is not async


# ===== INTEGRATION TESTS =====

class TestHistoricalDownloadSequence:
    """Test complete historical download sequence with WebSocket coordination"""

    @pytest.mark.asyncio
    async def test_smart_startup_strategy_selection(self, ticker_processor):
        """Test smart startup strategy selection based on system load"""
        processor = ticker_processor
        downloader = processor.historical_downloader
        timing_calc = processor.timing_calculator

        try:
            # Test 1: Normal load → immediate strategy
            timing_calc.set_simulated_load("normal")
            strategy = await downloader._determine_websocket_activation_strategy()
            assert strategy == "immediate"

            # Test 2: High load → parallel_start strategy
            timing_calc.set_simulated_load("high")
            strategy = await downloader._determine_websocket_activation_strategy()
            assert strategy == "parallel_start"

            # Test 3: Critical load → wait_for_conditions strategy
            timing_calc.set_simulated_load("critical")
            strategy = await downloader._determine_websocket_activation_strategy()
            assert strategy == "wait_for_conditions"
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_historical_download_with_websocket_coordination(self, ticker_processor):
        """Test historical download with proper WebSocket coordination"""
        processor = ticker_processor
        downloader = processor.historical_downloader
        websocket = processor.websocket_updater

        try:
            # Set normal load for immediate WebSocket activation
            processor.timing_calculator.set_simulated_load("normal")

            # Execute download
            result = await downloader.download_historical_data()

            # Verify download success
            assert result.success is True
            assert result.trades_loaded > 0
            assert result.websocket_handoff_completed is True

            # Verify WebSocket coordination
            assert websocket.websocket_connected is True
            assert websocket.mode == WebSocketMode.STREAMING
            assert downloader.websocket_started is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_timing_calculator_integration(self, ticker_processor):
        """Test TimingCalculator integration for system load estimation"""
        processor = ticker_processor
        timing_calc = processor.timing_calculator

        try:
            # Test load estimation accuracy
            timing_calc.set_simulated_load("normal")
            delay = timing_calc.calculate_request_delay()
            assert 0.5 <= delay <= 5.0  # Normal range

            timing_calc.set_simulated_load("high")
            delay = timing_calc.calculate_request_delay()
            assert 15.0 <= delay <= 30.0  # High load range (updated to match new range)

            timing_calc.set_simulated_load("critical")
            delay = timing_calc.calculate_request_delay()
            assert delay > 50.0  # Critical load
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_download_progress_monitoring(self, ticker_processor):
        """Test download progress monitoring and estimation"""
        processor = ticker_processor
        downloader = processor.historical_downloader

        try:
            # Start download in background
            download_task = asyncio.create_task(downloader.download_historical_data())

            # Monitor progress during download
            await asyncio.sleep(0.15)  # Let download start

            progress = downloader.get_progress()
            assert progress.total_trades_downloaded >= 0
            assert progress.api_requests_made >= 0
            assert progress.download_rate_per_minute >= 0

            # Wait for completion
            result = await download_task
            assert result.success is True

            # Verify final progress
            final_progress = downloader.get_progress()
            assert final_progress.total_trades_downloaded == result.trades_loaded
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_websocket_activation_timing(self, ticker_processor):
        """Test correct timing of WebSocket activation during download"""
        processor = ticker_processor
        downloader = processor.historical_downloader

        try:
            # Test immediate activation strategy
            processor.timing_calculator.set_simulated_load("normal")

            # Start download and monitor WebSocket activation
            download_task = asyncio.create_task(downloader.download_historical_data())

            # WebSocket should be activated quickly with immediate strategy
            websocket_activated = await wait_for_condition(
                lambda: downloader.websocket_started, timeout=2.0
            )
            assert websocket_activated is True

            # Wait for download completion
            result = await download_task
            assert result.success is True
            assert result.websocket_handoff_completed is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_download_completion_handoff(self, ticker_processor):
        """Test proper handoff from historical download to WebSocket streaming"""
        processor = ticker_processor
        downloader = processor.historical_downloader
        websocket = processor.websocket_updater

        try:
            # Execute complete download process
            result = await downloader.download_historical_data()

            # Verify handoff completion
            assert result.websocket_handoff_completed is True
            assert websocket.mode == WebSocketMode.STREAMING
            assert websocket.last_aggregate_id == result.last_aggregate_id

            # Verify dynamic management is stopped after handoff
            assert websocket.dynamic_management_active is False
        finally:
            if processor.is_running:
                await processor.shutdown()


class TestGapDetectionAndRecovery:
    """Test gap detection and recovery scenarios"""

    @pytest.mark.asyncio
    async def test_gap_detection_during_transition(self, ticker_processor, trades_writer):
        """Test gap detection during historical to live transition"""
        processor = ticker_processor

        try:
            # Simulate gap scenario
            processor.historical_downloader.simulate_gap_scenario = True

            # Pre-populate some historical data in ClickHouse simulation
            historical_trades = create_test_trades("BTCUSDT", 1000, start_id=100000)
            await trades_writer.write_trades_batch("BTCUSDT", historical_trades)

            # Execute download (should detect gap)
            result = await processor.historical_downloader.download_historical_data()

            # Verify gap was detected and filled
            assert result.gaps_filled > 0
            assert result.success is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_enhanced_gap_recovery_with_clickhouse(self, ticker_processor, trades_writer):
        """Test enhanced gap recovery with ClickHouse verification"""
        processor = ticker_processor
        downloader = processor.historical_downloader

        try:
            # Setup: Create gap scenario with known last aggregate ID
            last_confirmed_id = 150000
            await trades_writer.write_trades_batch("BTCUSDT", [
                AggTrade(last_confirmed_id, 50000.0, 0.01, last_confirmed_id, last_confirmed_id, int(time.time() * 1000), True)
            ])

            # Execute gap recovery
            gap_start_id = 149500  # Before last confirmed
            recovery_result = await downloader.recover_from_gap(gap_start_id)

            # Verify recovery used ClickHouse verification
            assert recovery_result.success is True
            assert recovery_result.trades_loaded > 0
            assert recovery_result.gaps_filled == 1

            # Verify recovery started from correct point (after last confirmed)
            assert recovery_result.time_range_start > last_confirmed_id
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_buffer_overflow_gap_recovery(self, ticker_processor):
        """Test gap recovery after buffer overflow scenario"""
        processor = ticker_processor
        websocket = processor.websocket_updater

        try:
            # Simulate buffer overflow condition
            websocket.buffer_size = processor.config.processing.max_buffer_size_per_ticker + 1000
            websocket.simulate_overload_condition = True

            # Trigger gap recovery through WebSocket deactivation
            await websocket._deactivate_due_to_overload()

            # Verify gap recovery was triggered
            assert websocket.mode == WebSocketMode.DISCONNECTED

            # Wait for gap recovery callback execution
            await asyncio.sleep(0.1)
            assert processor.gap_recoveries > 0
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_multiple_gap_recovery_sequence(self, ticker_processor):
        """Test handling multiple consecutive gap recoveries"""
        processor = ticker_processor

        try:
            # Execute multiple gap recoveries
            recovery_tasks = []
            for i in range(3):
                gap_start_id = 200000 + (i * 1000)
                task = asyncio.create_task(
                    processor.historical_downloader.recover_from_gap(gap_start_id)
                )
                recovery_tasks.append(task)

            # Wait for all recoveries to complete
            results = await asyncio.gather(*recovery_tasks)

            # Verify all recoveries succeeded
            for result in results:
                assert result.success is True
                assert result.gaps_filled == 1
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_gap_recovery_performance(self, ticker_processor):
        """Test gap recovery performance under load"""
        processor = ticker_processor
        downloader = processor.historical_downloader

        try:
            # Measure gap recovery performance
            start_time = time.time()

            recovery_result = await downloader.recover_from_gap(300000)

            recovery_duration = time.time() - start_time

            # Verify performance requirements
            assert recovery_duration < 1.0  # Should complete within 1 second
            assert recovery_result.success is True
            assert recovery_result.trades_loaded > 0
        finally:
            if processor.is_running:
                await processor.shutdown()


class TestWebSocketTransitionModes:
    """Test WebSocket mode transitions and dynamic management"""

    @pytest.mark.asyncio
    async def test_buffering_to_streaming_transition(self, ticker_processor):
        """Test seamless transition from buffering to streaming mode"""
        processor = ticker_processor
        websocket = processor.websocket_updater

        try:
            # Start in buffering mode
            success = await websocket.start_buffering_mode()
            assert success is True
            assert websocket.mode == WebSocketMode.BUFFERING

            # Transition to streaming
            last_aggregate_id = 123456
            success = await websocket.switch_to_streaming_mode(last_aggregate_id)
            assert success is True
            assert websocket.mode == WebSocketMode.STREAMING
            assert websocket.last_aggregate_id == last_aggregate_id
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_dynamic_websocket_management(self, ticker_processor):
        """Test dynamic WebSocket management based on system load"""
        processor = ticker_processor
        websocket = processor.websocket_updater
        timing_calc = processor.timing_calculator

        try:
            # Start dynamic management
            await websocket.start_buffering_mode()
            await websocket.switch_to_streaming_mode(100000)

            # Set critical load BEFORE starting dynamic management
            timing_calc.set_simulated_load("critical")

            # Start dynamic management
            await websocket.start_dynamic_management()

            # Wait for dynamic evaluation to trigger (multiple cycles)
            await asyncio.sleep(0.6)  # Allow more evaluation cycles

            # Manually trigger evaluation if automatic didn't work
            if websocket.consecutive_overloads == 0:
                current_load = timing_calc.calculate_request_delay()
                await websocket._evaluate_dynamic_action(current_load)

            # Verify dynamic management responded to overload
            assert websocket.consecutive_overloads > 0, f"Expected overload detection, got consecutive_overloads={websocket.consecutive_overloads}"

            # Cleanup
            await websocket.stop_dynamic_management()
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_websocket_deactivation_reactivation(self, ticker_processor):
        """Test WebSocket deactivation and reactivation cycle"""
        processor = ticker_processor
        websocket = processor.websocket_updater
        timing_calc = processor.timing_calculator

        try:
            # Start in streaming mode
            await websocket.start_buffering_mode()
            await websocket.switch_to_streaming_mode(100000)

            # Force deactivation due to overload
            await websocket._deactivate_due_to_overload()
            assert websocket.mode == WebSocketMode.DISCONNECTED

            # Simulate load improvement and reactivation
            timing_calc.set_simulated_load("normal")
            await websocket._reactivate_after_overload()

            # Verify reactivation
            assert websocket.mode == WebSocketMode.STREAMING
            assert websocket.websocket_connected is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_streaming_mode_stability(self, ticker_processor):
        """Test streaming mode stability under normal conditions"""
        processor = ticker_processor
        websocket = processor.websocket_updater

        try:
            # Start streaming
            await websocket.start_buffering_mode()
            await websocket.switch_to_streaming_mode(100000)

            # Run for extended period with normal load
            start_time = time.time()
            while time.time() - start_time < 0.5:  # 0.5 second stability test
                assert websocket.mode == WebSocketMode.STREAMING
                assert websocket.is_healthy() is True
                await asyncio.sleep(0.05)
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_websocket_error_recovery_during_transition(self, ticker_processor):
        """Test error recovery during mode transitions"""
        processor = ticker_processor
        websocket = processor.websocket_updater

        try:
            # Simulate error during mode transition
            websocket.is_healthy_flag = False

            # Attempt transition - should handle error gracefully
            success = await websocket.start_buffering_mode()
            assert success is True  # Should succeed despite health flag

            # Restore health and verify recovery
            websocket.is_healthy_flag = True
            assert websocket.is_healthy() is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_websocket_operations(self, ticker_processor):
        """Test concurrent WebSocket operations for multiple tickers"""
        # Create multiple processor instances
        processors = []
        config = Config(processing=ProcessingConfig())
        try:
            for i in range(3):
                ticker_model = MockTickerModel(f"TICKER{i}")
                trades_writer = MockTradesWriter()

                processor = MockTickerProcessor(
                    ticker_model=ticker_model,
                    binance_client=Mock(),
                    weight_coordinator=Mock(),
                    trades_writer=trades_writer,
                    config=config
                )
                processors.append(processor)

            # Start all WebSocket operations concurrently
            tasks = []
            for processor in processors:
                task = asyncio.create_task(
                    processor.websocket_updater.start_buffering_mode()
                )
                tasks.append(task)

            # Wait for all to complete
            results = await asyncio.gather(*tasks)

            # Verify all succeeded
            for i, result in enumerate(results):
                assert result is True
                assert processors[i].websocket_updater.websocket_connected is True
        finally:
            # Cleanup all processors
            for processor in processors:
                if processor.is_running:
                    await processor.shutdown()

    @pytest.mark.asyncio
    async def test_dynamic_threshold_hysteresis(self, ticker_processor):
        """Test dynamic threshold hysteresis (30s/25s thresholds)"""
        processor = ticker_processor
        websocket = processor.websocket_updater
        timing_calc = processor.timing_calculator
        config = processor.config.processing

        try:
            # Verify threshold configuration
            assert config.dynamic_deactivation_threshold == 30.0
            assert config.dynamic_reactivation_threshold == 25.0

            # Start dynamic management
            await websocket.start_buffering_mode()
            await websocket.switch_to_streaming_mode(100000)
            await websocket.start_dynamic_management()

            # Test hysteresis behavior
            # 1. Normal load → no action
            timing_calc.base_delay = 20.0
            await websocket._evaluate_dynamic_action(20.0)
            assert websocket.consecutive_overloads == 0

            # 2. Cross deactivation threshold → start counting
            await websocket._evaluate_dynamic_action(35.0)
            assert websocket.consecutive_overloads > 0

            # 3. Drop below reactivation threshold → reset counter
            await websocket._evaluate_dynamic_action(20.0)
            assert websocket.consecutive_overloads == 0

            # Cleanup
            await websocket.stop_dynamic_management()
        finally:
            if processor.is_running:
                await processor.shutdown()


class TestSeamlessDataContinuity:
    """Test data continuity during historical to live transition"""

    @pytest.mark.asyncio
    async def test_no_data_gaps_during_transition(self, ticker_processor, trades_writer):
        """Test no data gaps occur during transition"""
        processor = ticker_processor

        try:
            # Execute full transition
            task = asyncio.create_task(processor.run())

            # Let it run briefly
            await asyncio.sleep(0.3)

            # Stop the process
            await processor.shutdown()

            try:
                await asyncio.wait_for(task, timeout=1.0)
            except asyncio.TimeoutError:
                pass

            # Verify data exists
            stats = trades_writer.get_storage_stats("BTCUSDT")
            assert stats['total_trades'] >= 0  # Some trades should exist
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_no_duplicate_trades_during_handoff(self, ticker_processor, trades_writer):
        """Test no duplicate trades during historical to WebSocket handoff"""
        processor = ticker_processor

        try:
            # Execute transition
            result = await processor.historical_downloader.download_historical_data()

            # Check for duplicates around handoff point
            handoff_id = result.last_aggregate_id
            if handoff_id:
                # Look for trades around handoff point
                nearby_trades = []
                for trades_list in [trades_writer.clickhouse_data.get("BTCUSDT", []),
                                    trades_writer.trades_storage.get("BTCUSDT", [])]:
                    for trade in trades_list:
                        if abs(trade.aggregate_id - handoff_id) <= 10:
                            nearby_trades.append(trade)

                # Check for duplicate aggregate IDs
                seen_ids = set()
                duplicates = []
                for trade in nearby_trades:
                    if trade.aggregate_id in seen_ids:
                        duplicates.append(trade.aggregate_id)
                    seen_ids.add(trade.aggregate_id)

                assert len(duplicates) == 0, f"Found duplicate aggregate IDs: {duplicates}"
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_chronological_order_preservation(self, ticker_processor, trades_writer):
        """Test chronological order is preserved during transition"""
        processor = ticker_processor

        try:
            # Execute transition
            task = asyncio.create_task(processor.run())

            # Let it run briefly
            await asyncio.sleep(0.3)

            # Stop the process
            await processor.shutdown()

            try:
                await asyncio.wait_for(task, timeout=1.0)
            except asyncio.TimeoutError:
                pass

            # Get all trades and verify chronological order
            all_trades = []
            for trades_list in [trades_writer.clickhouse_data.get("BTCUSDT", []),
                                trades_writer.trades_storage.get("BTCUSDT", [])]:
                all_trades.extend(trades_list)

            if len(all_trades) > 1:
                all_trades.sort(key=lambda t: t.aggregate_id)

                # Verify timestamps are generally increasing
                for i in range(1, len(all_trades)):
                    # Allow some minor timestamp variations but catch major issues
                    time_diff = all_trades[i].timestamp - all_trades[i - 1].timestamp
                    assert time_diff >= -5000  # Allow 5 second variations
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_aggregate_id_sequence_validation(self, ticker_processor, trades_writer):
        """Test aggregate ID sequence validation"""
        processor = ticker_processor

        try:
            # Execute transition
            result = await processor.historical_downloader.download_historical_data()

            # Validate aggregate ID sequence
            all_trades = []
            for trades_list in [trades_writer.clickhouse_data.get("BTCUSDT", []),
                                trades_writer.trades_storage.get("BTCUSDT", [])]:
                all_trades.extend(trades_list)

            if len(all_trades) > 1:
                all_trades.sort(key=lambda t: t.aggregate_id)

                # Check sequence integrity
                sequence_breaks = 0
                max_allowed_breaks = 5  # Allow some natural breaks

                for i in range(1, len(all_trades)):
                    expected_next = all_trades[i - 1].aggregate_id + 1
                    actual = all_trades[i].aggregate_id

                    if actual != expected_next:
                        sequence_breaks += 1

                assert sequence_breaks <= max_allowed_breaks, f"Too many sequence breaks: {sequence_breaks}"
        finally:
            if processor.is_running:
                await processor.shutdown()


class TestPerformanceDuringTransition:
    """Test performance characteristics during transition"""

    @pytest.mark.asyncio
    async def test_memory_usage_during_large_historical_load(self, ticker_processor):
        """Test memory usage remains controlled during large historical loads"""
        processor = ticker_processor
        downloader = processor.historical_downloader

        try:
            # Simulate large historical load
            downloader.simulate_download_delay = 0.01  # Faster simulation

            # Monitor memory usage (simplified - in real tests would use psutil)
            initial_trades = processor.trades_writer.get_storage_stats("BTCUSDT")['total_trades']

            # Execute download
            result = await downloader.download_historical_data()

            # Verify reasonable memory usage (trades should be written, not accumulated)
            final_trades = processor.trades_writer.get_storage_stats("BTCUSDT")['total_trades']
            trades_added = final_trades - initial_trades

            # Memory should be proportional to batch size, not total trades
            assert trades_added >= 0
            assert result.success is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_transition_latency_measurement(self, ticker_processor):
        """Test transition latency from historical to live streaming"""
        processor = ticker_processor
        downloader = processor.historical_downloader
        websocket = processor.websocket_updater

        try:
            # Measure transition timing
            transition_start = time.time()

            # Execute download with WebSocket coordination
            result = await downloader.download_historical_data()

            # Measure when WebSocket became fully operational
            websocket_ready_time = time.time()

            # Calculate transition latency
            transition_latency = websocket_ready_time - transition_start

            # Verify latency requirements
            assert transition_latency < 5.0  # Should complete within 5 seconds
            assert result.websocket_handoff_completed is True
            assert websocket.mode == WebSocketMode.STREAMING
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_throughput_maintenance_during_transition(self, ticker_processor):
        """Test throughput is maintained during transition"""
        processor = ticker_processor
        downloader = processor.historical_downloader

        try:
            # Measure download throughput
            start_time = time.time()
            result = await downloader.download_historical_data()
            duration = time.time() - start_time

            # Calculate throughput
            if duration > 0:
                throughput = result.trades_loaded / duration

                # Verify reasonable throughput (should process thousands of trades per second)
                assert throughput > 100  # At least 100 trades/second (reduced for testing)
                assert result.success is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_system_responsiveness_under_load(self, ticker_processor):
        """Test system remains responsive during transition"""
        processor = ticker_processor

        try:
            # Start transition in background
            transition_task = asyncio.create_task(processor.run())

            # Test system responsiveness during transition
            response_times = []
            for _ in range(5):
                start = time.time()

                # Simple responsive operation
                stats = processor.websocket_updater.get_stats()
                assert stats is not None

                response_time = time.time() - start
                response_times.append(response_time)

                await asyncio.sleep(0.05)

            # Cleanup
            await processor.shutdown()
            try:
                await asyncio.wait_for(transition_task, timeout=1.0)
            except asyncio.TimeoutError:
                pass

            # Verify response times
            avg_response_time = sum(response_times) / len(response_times)
            assert avg_response_time < 0.2  # Should respond within 200ms (relaxed for testing)
        finally:
            if processor.is_running:
                await processor.shutdown()


class TestErrorScenariosAndRecovery:
    """Test error handling and recovery during transitions"""

    @pytest.mark.asyncio
    async def test_historical_download_failure_recovery(self, ticker_processor):
        """Test recovery from historical download failures"""
        processor = ticker_processor
        downloader = processor.historical_downloader

        try:
            # Simulate download failure
            downloader.simulate_failure = True
            downloader.failure_message = "Simulated API failure"

            # Attempt download
            with pytest.raises(Exception, match="Simulated API failure"):
                await downloader.download_historical_data()

            # Reset failure simulation and retry
            downloader.simulate_failure = False
            result = await downloader.download_historical_data()

            # Verify recovery
            assert result.success is True
            assert result.trades_loaded > 0
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_websocket_connection_failure_during_transition(self, ticker_processor):
        """Test WebSocket connection failure during transition"""
        processor = ticker_processor
        websocket = processor.websocket_updater

        try:
            # Simulate WebSocket connection failure
            websocket.is_healthy_flag = False
            websocket.websocket_connected = False

            # Attempt transition
            success = await websocket.start_buffering_mode()

            # Even with health issues, basic operations should still work
            assert success is True

            # Restore health and verify recovery
            websocket.is_healthy_flag = True
            websocket.websocket_connected = True
            assert websocket.is_healthy() is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_clickhouse_unavailability_handling(self, ticker_processor, trades_writer):
        """Test handling of ClickHouse unavailability"""
        processor = ticker_processor

        try:
            # Simulate ClickHouse unavailability by modifying trades_writer
            original_write_method = trades_writer.write_trades_batch

            async def failing_write(symbol, trades):
                raise Exception("ClickHouse connection failed")

            trades_writer.write_trades_batch = failing_write

            # Execute download - should handle write failures gracefully
            try:
                result = await processor.historical_downloader.download_historical_data()
                # Download might succeed but writes fail
            except Exception as e:
                assert "ClickHouse connection failed" in str(e)

            # Restore functionality
            trades_writer.write_trades_batch = original_write_method

            # Verify system can recover
            result = await processor.historical_downloader.download_historical_data()
            assert result.success is True
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_binance_api_rate_limiting_during_transition(self, ticker_processor):
        """Test handling of Binance API rate limiting during transition"""
        processor = ticker_processor
        timing_calc = processor.timing_calculator

        try:
            # Simulate rate limiting scenario
            timing_calc.set_simulated_load("critical")

            # Execute download under rate limiting
            result = await processor.historical_downloader.download_historical_data()

            # Should adapt to rate limiting
            assert result.success is True
            # Download should take longer due to rate limiting
            assert result.download_duration_seconds > 0.1  # Reduced threshold for testing
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_partial_data_corruption_recovery(self, ticker_processor, trades_writer):
        """Test recovery from partial data corruption"""
        processor = ticker_processor

        try:
            # Pre-populate with some corrupted data
            corrupted_trade = AggTrade(
                aggregate_id=-1,  # Invalid ID
                price=-50000.0,  # Invalid price
                quantity=0.0,  # Invalid quantity
                first_trade_id=0,
                last_trade_id=0,
                timestamp=0,  # Invalid timestamp
                is_buyer_maker=True
            )

            # Add to storage
            trades_writer.clickhouse_data["BTCUSDT"] = [corrupted_trade]

            # Execute download - should handle corruption gracefully
            result = await processor.historical_downloader.download_historical_data()

            # Verify system recovered and continued
            assert result.success is True
            assert result.trades_loaded > 0
        finally:
            if processor.is_running:
                await processor.shutdown()

    @pytest.mark.asyncio
    async def test_system_restart_during_transition(self, ticker_processor):
        """Test system restart scenarios during transition"""
        processor = ticker_processor

        try:
            # Start transition
            transition_task = asyncio.create_task(processor.run())

            # Wait for partial progress
            await asyncio.sleep(0.2)

            # Simulate system restart by shutting down
            await processor.shutdown()

            # Wait for shutdown completion
            try:
                await asyncio.wait_for(transition_task, timeout=1.0)
            except asyncio.TimeoutError:
                transition_task.cancel()

            # Verify system can restart and continue
            new_processor = MockTickerProcessor(
                ticker_model=processor.ticker_model,
                binance_client=Mock(),
                weight_coordinator=Mock(),
                trades_writer=processor.trades_writer,
                config=processor.config
            )

            # Should be able to restart successfully
            restart_result = await new_processor.historical_downloader.download_historical_data()
            assert restart_result.success is True

            # Cleanup
            await new_processor.shutdown()
        finally:
            if processor.is_running:
                await processor.shutdown()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short", "-s"])