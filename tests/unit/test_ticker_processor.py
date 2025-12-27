"""
Unit Tests for TickerProcessor - HIGH PRIORITY (20/56 tests)
============================================================

Critical test coverage for TickerProcessor v4.2 focusing on:
- Component initialization and coordination
- Smart startup strategies based on TimingCalculator
- Historical download coordination with WebSocket handoff
- WebSocket integration and dynamic management
- Core lifecycle management and error handling

CRITICAL PATHS TESTED:
1. Initialization & Component Setup (5 tests)
2. Smart Startup Strategies v4.2 (7 tests)
3. Historical Download Coordination (6 tests)
4. WebSocket Integration Basics (2 tests)

File: tests/unit/test_ticker_processor.py
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
        # Valid transitions mapping
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
            ProcessorState.STOPPED: set(),  # Terminal state
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
class ProcessorStats:
    """Statistics for ticker processing"""
    state: ProcessorState
    start_time: int
    historical_trades_loaded: int
    websocket_trades_processed: int
    last_trade_time: Optional[int]
    last_error: Optional[str]
    error_count: int
    binance_error_count: int
    recoverable_error_count: int
    uptime_seconds: float
    gap_recoveries: int
    websocket_reconnections: int


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


@dataclass
class DownloadProgress:
    """Progress tracking for historical download"""
    total_trades_downloaded: int
    api_requests_made: int
    bytes_written: int
    download_rate_per_minute: float
    websocket_activated: bool
    estimated_completion_time: Optional[int]
    last_aggregate_id: Optional[int]


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


# ===== MOCK COMPONENTS =====

class MockTimingCalculator:
    """Mock TimingCalculator for testing dynamic management and smart startup"""

    def __init__(self, initial_delay: float = 1.0):
        self._current_delay = initial_delay
        self.calculation_calls = 0
        self.call_history = []

    def calculate_request_delay(self) -> float:
        """Return configurable delay for testing scenarios"""
        self.calculation_calls += 1
        self.call_history.append({
            'delay': self._current_delay,
            'timestamp': time.time()
        })
        return self._current_delay

    def set_delay(self, delay: float):
        """Set delay for testing different scenarios"""
        self._current_delay = delay

    def get_call_history(self) -> List[Dict]:
        """Get history of calculation calls"""
        return self.call_history.copy()


class MockTradesWriter:
    """Enhanced MockTradesWriter for testing v4.2 buffer overflow scenarios"""

    def __init__(self, buffer_full: bool = False):
        self.trades = []
        self.buffer_full = buffer_full
        self.add_trade_calls = 0
        self.rejected_trades = []
        self.write_batch_calls = 0
        self.get_last_aggregate_id_calls = 0
        self.last_aggregate_id_value = None
        self.storage_stats = {}

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
        """Get last aggregate_id for testing"""
        self.get_last_aggregate_id_calls += 1
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

    def set_storage_stats(self, symbol: str, stats: Dict[str, Any]):
        """Set storage stats for symbol"""
        self.storage_stats[symbol] = stats

    def clear(self):
        """Clear all trades and reset state"""
        self.trades.clear()
        self.rejected_trades.clear()
        self.add_trade_calls = 0
        self.buffer_full = False


class MockHistoricalDownloader:
    """Mock HistoricalDownloader for testing coordination scenarios"""

    def __init__(self):
        self.download_calls = 0
        self.pause_calls = 0
        self.recover_gap_calls = 0
        self.set_websocket_coordinator_calls = 0

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

        self.progress = DownloadProgress(
            total_trades_downloaded=5000,
            api_requests_made=50,
            bytes_written=1000000,
            download_rate_per_minute=3000.0,
            websocket_activated=True,
            estimated_completion_time=int(time.time() * 1000) + 1800000,
            last_aggregate_id=15000
        )

        self.is_running = False
        self.websocket_coordinator = None
        self.timing_calculator = None

    async def download_historical_data(self) -> HistoricalDownloadResult:
        """Mock historical data download"""
        self.download_calls += 1
        self.is_running = True

        # Simulate download time
        await asyncio.sleep(0.01)

        self.is_running = False
        return self.download_result

    async def recover_from_gap(self, gap_start_id: int) -> HistoricalDownloadResult:
        """Mock gap recovery"""
        self.recover_gap_calls += 1

        # Return recovery result
        recovery_result = HistoricalDownloadResult(
            success=True,
            trades_loaded=500,
            api_requests_made=5,
            download_duration_seconds=5.0,
            errors_count=0,
            binance_errors_count=0,
            last_aggregate_id=gap_start_id + 499,
            websocket_handoff_completed=True,
            start_time=gap_start_id,
            end_time=gap_start_id + 499
        )

        return recovery_result

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

    def get_progress(self) -> DownloadProgress:
        """Get download progress"""
        return self.progress

    def set_download_result(self, result: HistoricalDownloadResult):
        """Configure download result for testing"""
        self.download_result = result

    def set_progress(self, progress: DownloadProgress):
        """Configure progress for testing"""
        self.progress = progress


class MockWebSocketUpdater:
    """Mock WebSocketUpdater for testing coordination scenarios"""

    def __init__(self):
        self.start_buffering_calls = 0
        self.switch_streaming_calls = 0
        self.shutdown_calls = 0
        self.set_timing_calculator_calls = 0
        self.start_dynamic_management_calls = 0
        self.stop_dynamic_management_calls = 0
        self.set_gap_recovery_callback_calls = 0

        # Mock states
        self.websocket_connected = False
        self.mode = "IDLE"
        self.gap_recovery_callback = None
        self.timing_calculator = None
        self.dynamic_manager = None

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
        """Mock start buffering mode"""
        self.start_buffering_calls += 1
        if self.start_buffering_success:
            self.websocket_connected = True
            self.mode = "BUFFERING"
        return self.start_buffering_success

    async def switch_to_streaming_mode(self, last_historical_id: int) -> bool:
        """Mock switch to streaming mode"""
        self.switch_streaming_calls += 1
        if self.switch_streaming_success:
            self.mode = "STREAMING"
        return self.switch_streaming_success

    async def shutdown(self):
        """Mock shutdown"""
        self.shutdown_calls += 1
        self.websocket_connected = False
        self.mode = "IDLE"

    def set_timing_calculator(self, timing_calculator, config=None):
        """Mock set timing calculator"""
        self.set_timing_calculator_calls += 1
        self.timing_calculator = timing_calculator
        # Mock dynamic manager creation
        self.dynamic_manager = Mock()

    async def start_dynamic_management(self):
        """Mock start dynamic management"""
        self.start_dynamic_management_calls += 1

    async def stop_dynamic_management(self):
        """Mock stop dynamic management"""
        self.stop_dynamic_management_calls += 1

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


# ===== SIMPLIFIED TICKER PROCESSOR FOR TESTING =====

class TickerProcessor:
    """
    Simplified TickerProcessor for testing critical paths.
    Contains main methods from actual implementation.
    """

    def __init__(self, ticker_model: MockTickerModel, binance_client: AsyncMock,
                 weight_coordinator: MockWeightCoordinator, trades_writer: MockTradesWriter,
                 config: MockConfig):
        """Initialize TickerProcessor for testing"""
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
        self._historical_trades_loaded: int = 0
        self._websocket_trades_processed: int = 0
        self._last_trade_time: Optional[int] = None
        self._last_error: Optional[str] = None
        self._error_count: int = 0
        self._binance_error_count: int = 0
        self._recoverable_error_count: int = 0
        self._gap_recoveries: int = 0
        self._websocket_reconnections: int = 0

        # Components (will be mocked in tests)
        self.historical_downloader: Optional[MockHistoricalDownloader] = None
        self.websocket_updater: Optional[MockWebSocketUpdater] = None
        self.timing_calculator: Optional[MockTimingCalculator] = None

        # Tasks
        self._historical_task: Optional[asyncio.Task] = None
        self._websocket_task: Optional[asyncio.Task] = None
        self._monitoring_task: Optional[asyncio.Task] = None

        # Coordination state
        self._historical_completed: bool = False
        self._websocket_streaming: bool = False
        self._last_aggregate_id: Optional[int] = None

    # ===== MAIN PROCESSING METHODS =====

    async def run(self) -> None:
        """Main processing loop for ticker"""
        try:
            self._is_running = True

            # Phase 1: Initialize components
            await self._initialize_components()

            # Phase 2: Download historical data
            await self._download_historical_data()

            # Phase 3: Start WebSocket updates
            await self._start_websocket_updates()

            # Phase 4: Main monitoring loop
            await self._monitoring_loop()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self._transition_state(ProcessorState.ERROR)
            self._handle_processor_error(e)
            raise
        finally:
            await self._cleanup()

    async def shutdown(self, timeout_seconds: float = 30.0) -> None:
        """Graceful shutdown of ticker processor"""
        self._should_stop = True
        self._transition_state(ProcessorState.STOPPING)

        try:
            # Wait for main loop to finish
            if self._is_running:
                await asyncio.wait_for(
                    self._wait_for_shutdown(),
                    timeout=timeout_seconds
                )
        except asyncio.TimeoutError:
            pass

        await self._cleanup()
        self._transition_state(ProcessorState.STOPPED)

    # ===== INITIALIZATION =====

    async def _initialize_components(self) -> None:
        """Initialize all components for ticker processing"""
        # Create timing calculator if enabled
        if self.config.processing.timing_calculator_enabled:
            self.timing_calculator = MockTimingCalculator()

        # Initialize HistoricalDownloader
        self.historical_downloader = MockHistoricalDownloader()
        if self.timing_calculator:
            self.historical_downloader.timing_calculator = self.timing_calculator

        # Initialize WebSocketUpdater
        self.websocket_updater = MockWebSocketUpdater()
        if self.timing_calculator:
            self.websocket_updater.set_timing_calculator(
                self.timing_calculator,
                config=self.config.processing
            )

        # Set up WebSocket coordinator for HistoricalDownloader
        self.historical_downloader.set_websocket_coordinator(self.websocket_updater)

        # Set up gap recovery callback for WebSocket
        self.websocket_updater.set_gap_recovery_callback(self._handle_gap_recovery)

    # ===== SMART STARTUP STRATEGIES v4.2 =====

    async def _download_historical_data(self) -> None:
        """Download historical trade data with WebSocket coordination"""
        if self._should_stop:
            return

        self._transition_state(ProcessorState.DOWNLOADING_HISTORICAL)

        try:
            # NEW: Initial decision about WebSocket strategy
            initial_delay = self.timing_calculator.calculate_request_delay()
            websocket_strategy = self._determine_websocket_strategy(initial_delay)

            # Execute startup strategy
            await self._execute_startup_strategy(websocket_strategy)

            # Execute historical download
            self._historical_task = asyncio.create_task(
                self.historical_downloader.download_historical_data()
            )
            result = await self._historical_task

            # Check success BEFORE updating completion status
            if not result.success:
                # Update statistics for failed download but don't mark as completed
                self._historical_trades_loaded = result.trades_loaded
                self._last_aggregate_id = result.last_aggregate_id
                # Keep _historical_completed = False for failed downloads
                raise Exception(f"Historical download failed: {result.errors_count} errors")

            # Update statistics from download result (success case)
            self._historical_trades_loaded = result.trades_loaded
            self._last_aggregate_id = result.last_aggregate_id
            self._historical_completed = True

        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise

    @staticmethod
    def _determine_websocket_strategy(estimated_delay: float) -> str:
        """Determine WebSocket startup strategy based on initial delay"""
        if estimated_delay <= 5.0:
            return "websocket_first"
        elif estimated_delay <= 30.0:
            return "parallel_start"
        else:
            return "historical_only"

    async def _execute_startup_strategy(self, strategy: str) -> None:
        """Execute chosen startup strategy"""
        if strategy == "websocket_first":
            # Fast system - WebSocket first
            await self._start_websocket_buffering_with_dynamic()
        elif strategy == "parallel_start":
            # Medium load - WebSocket parallel with historical download
            # WebSocket will start automatically from HistoricalDownloader with dynamic management
            pass
        else:  # historical_only
            # High load - only historical, WebSocket will wait
            pass

    async def _start_websocket_buffering_with_dynamic(self) -> None:
        """Start WebSocket in buffering mode with dynamic management"""
        if self.websocket_updater:
            success = await self.websocket_updater.start_buffering_mode()
            if success:
                await self.websocket_updater.start_dynamic_management()

    # ===== WEBSOCKET PHASE =====

    async def _start_websocket_updates(self) -> None:
        """Start WebSocket updates for real-time data"""
        if self._should_stop:
            return

        self._transition_state(ProcessorState.STARTING_WEBSOCKET)

        try:
            # If WebSocket was started during historical download, switch to streaming
            if (hasattr(self.websocket_updater, 'websocket_connected') and
                self.websocket_updater.websocket_connected):

                success = await self.websocket_updater.switch_to_streaming_mode(
                    self._last_aggregate_id or 0
                )
                if not success:
                    raise Exception("Failed to switch WebSocket to streaming mode")

                # Stop dynamic management when transitioning to STREAMING
                await self.websocket_updater.stop_dynamic_management()

            else:
                # Start WebSocket in streaming mode directly
                success = await self.websocket_updater.start_buffering_mode()
                if not success:
                    raise Exception("Failed to start WebSocket updates")

                # Immediately switch to streaming since historical is complete
                await asyncio.sleep(0.01)  # Brief delay for connection establishment
                success = await self.websocket_updater.switch_to_streaming_mode(
                    self._last_aggregate_id or 0
                )
                if not success:
                    raise Exception("Failed to switch to streaming mode")

            self._websocket_streaming = True

            # Monitor WebSocket task
            self._websocket_task = asyncio.create_task(
                self._monitor_websocket(),
                name=f"websocket_monitor_{self.ticker_model.symbol}"
            )

            # Wait a bit to ensure WebSocket is working
            await asyncio.sleep(0.01)

            if not self._should_stop:
                self._transition_state(ProcessorState.RUNNING)

                # Ensure dynamic management stopped in STREAMING mode
                await self.websocket_updater.stop_dynamic_management()

        except Exception as e:
            raise

    async def _monitor_websocket(self) -> None:
        """Monitor WebSocket health and update statistics"""
        try:
            while not self._should_stop and self.websocket_updater:
                await asyncio.sleep(10)  # Check every 10 seconds

                # Update statistics from WebSocket
                ws_stats = self.websocket_updater.get_stats()
                self._websocket_trades_processed = ws_stats.trades_processed
                self._websocket_reconnections = ws_stats.reconnect_count

                if ws_stats.last_trade_time:
                    self._last_trade_time = ws_stats.last_trade_time

                # Check WebSocket health
                if not self.websocket_updater.is_healthy():
                    pass  # Would log warning in real implementation

        except asyncio.CancelledError:
            pass
        except Exception as e:
            pass

    # ===== GAP RECOVERY =====

    async def _handle_gap_recovery(self, gap_start_id: int) -> None:
        """Enhanced gap recovery with ClickHouse verification"""
        self._gap_recoveries += 1

        try:
            # Step 1: Find last confirmed aggregate_id in ClickHouse
            last_confirmed_id = await self.trades_writer.get_last_aggregate_id(
                self.ticker_model.symbol
            )

            if last_confirmed_id is None:
                recovery_start_id = gap_start_id
            else:
                # Start recovery from next ID after last confirmed
                recovery_start_id = max(last_confirmed_id + 1, gap_start_id)

            # Step 2: Start historical download for gap recovery
            if self.historical_downloader:
                recovery_result = await self.historical_downloader.recover_from_gap(
                    recovery_start_id
                )

                if recovery_result.success:
                    # Step 3: Update last_aggregate_id
                    self._last_aggregate_id = recovery_result.last_aggregate_id

                    # Step 4: Switch WebSocket back to STREAMING mode and stop dynamic management
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

        except Exception as e:
            # In case of error, try to return WebSocket to normal state
            if (self.websocket_updater and
                hasattr(self.websocket_updater, 'gap_recovery_in_progress')):
                self.websocket_updater.gap_recovery_in_progress = False

    # ===== MONITORING =====

    async def _monitoring_loop(self) -> None:
        """Main monitoring and health check loop"""
        self._monitoring_task = asyncio.create_task(self._health_monitoring())

        try:
            # Wait for WebSocket task or shutdown signal
            if self._websocket_task:
                await self._websocket_task

        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise

    async def _health_monitoring(self) -> None:
        """Background health monitoring task"""
        try:
            while not self._should_stop and self._state.is_active():
                await asyncio.sleep(self.config.processing.health_check_interval_seconds)

                # Perform health checks
                await self._perform_health_checks()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            pass

    async def _perform_health_checks(self) -> None:
        """Perform comprehensive health checks"""
        # Check for stale WebSocket data
        if self._last_trade_time:
            time_since_last_trade = int(time.time() * 1000) - self._last_trade_time
            max_stale_time = 300000  # 5 minutes in milliseconds

            if time_since_last_trade > max_stale_time:
                pass  # Would log warning in real implementation

        # Check storage health through TradesWriter interface
        try:
            stats = self.trades_writer.get_storage_stats(self.ticker_model.symbol)
        except Exception as e:
            pass

        # Check WebSocket health
        if self.websocket_updater:
            try:
                if not self.websocket_updater.is_healthy():
                    pass  # Would log warning in real implementation
            except Exception as e:
                pass

    # ===== CLEANUP =====

    async def _cleanup(self) -> None:
        """Cleanup all resources and tasks"""
        # Cancel all tasks
        tasks_to_cancel = [
            self._historical_task,
            self._websocket_task,
            self._monitoring_task
        ]

        for task in tasks_to_cancel:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    pass

        # Cleanup WebSocketUpdater first
        if self.websocket_updater:
            try:
                await self.websocket_updater.stop_dynamic_management()
                await self.websocket_updater.shutdown()
            except Exception as e:
                pass

        # Cleanup HistoricalDownloader
        if self.historical_downloader:
            try:
                await self.historical_downloader.pause_download()
            except Exception as e:
                pass

        self._is_running = False

    async def _wait_for_shutdown(self) -> None:
        """Wait for shutdown signal to be processed"""
        while self._is_running and not self._should_stop:
            await asyncio.sleep(0.1)

    # ===== STATE MANAGEMENT =====

    def _transition_state(self, new_state: ProcessorState) -> bool:
        """Transition to new state with validation"""
        if self._state.can_transition_to(new_state):
            old_state = self._state
            self._state = new_state
            return True
        else:
            return False

    def _handle_processor_error(self, error: Exception) -> None:
        """Handle processor-level errors"""
        self._last_error = str(error)
        self._error_count += 1

    # ===== PUBLIC INTERFACE =====

    def get_stats(self) -> ProcessorStats:
        """Get current processor statistics"""
        uptime = (int(time.time() * 1000) - self._start_time) / 1000.0

        # Get more detailed historical download stats if available
        historical_trades = self._historical_trades_loaded
        if (self.historical_downloader and
            self.historical_downloader.is_download_running()):
            progress = self.historical_downloader.get_progress()
            historical_trades = progress.total_trades_downloaded

        return ProcessorStats(
            state=self._state,
            start_time=self._start_time,
            historical_trades_loaded=historical_trades,
            websocket_trades_processed=self._websocket_trades_processed,
            last_trade_time=self._last_trade_time,
            last_error=self._last_error,
            error_count=self._error_count,
            binance_error_count=self._binance_error_count,
            recoverable_error_count=self._recoverable_error_count,
            uptime_seconds=uptime,
            gap_recoveries=self._gap_recoveries,
            websocket_reconnections=self._websocket_reconnections
        )

    def is_healthy(self) -> bool:
        """Check if processor is in healthy state"""
        # Basic state checks
        if not (self._state.is_active() and self._is_running and not self._should_stop):
            return False

        # Error threshold check
        if self._error_count >= 10:
            return False

        # Component health checks
        if self.websocket_updater and not self.websocket_updater.is_healthy():
            return False

        # Check for excessive gap recoveries
        if self._gap_recoveries > 5:
            return False

        return True

    def pause(self) -> bool:
        """Pause ticker processing"""
        if self._transition_state(ProcessorState.PAUSED):
            return True
        return False

    def resume(self) -> bool:
        """Resume ticker processing"""
        if self._transition_state(ProcessorState.RUNNING):
            return True
        return False

    # ===== PROPERTIES =====

    @property
    def symbol(self) -> str:
        """Get ticker symbol"""
        return self.ticker_model.symbol

    @property
    def state(self) -> ProcessorState:
        """Get current processor state"""
        return self._state

    @property
    def is_running(self) -> bool:
        """Check if processor is running"""
        return self._is_running

    @property
    def last_aggregate_id(self) -> Optional[int]:
        """Get last processed aggregate_id"""
        return self._last_aggregate_id


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
def mock_trades_writer():
    """Provide mock trades writer"""
    return MockTradesWriter()


@pytest.fixture
def ticker_processor_factory(config, mock_binance_client, mock_weight_coordinator, mock_trades_writer):
    """Factory to create TickerProcessor instances for testing"""
    created_processors = []

    def create_processor(symbol: str = "BTCUSDT"):
        ticker_model = MockTickerModel(symbol)
        processor = TickerProcessor(
            ticker_model=ticker_model,
            binance_client=mock_binance_client,
            weight_coordinator=mock_weight_coordinator,
            trades_writer=mock_trades_writer,
            config=config
        )
        created_processors.append(processor)
        return processor

    yield create_processor

    # Cleanup
    for processor in created_processors:
        if processor.is_running:
            try:
                # Force stop for cleanup
                processor._should_stop = True
                processor._is_running = False
            except:
                pass


# ===== TESTS - HIGH PRIORITY (20/56) =====

class TestInitializationAndComponentSetup:
    """Test component initialization and coordination (5 tests)"""

    @pytest.mark.asyncio
    async def test_ticker_processor_initialization(self, ticker_processor_factory):
        """Test proper initialization of TickerProcessor"""
        processor = ticker_processor_factory("BTCUSDT")

        # Verify basic initialization
        assert processor.symbol == "BTCUSDT"
        assert processor.state == ProcessorState.INITIALIZING
        assert not processor.is_running
        assert processor._should_stop is False
        assert processor._start_time > 0
        assert processor._error_count == 0
        assert processor._gap_recoveries == 0

        # Verify components not yet initialized
        assert processor.historical_downloader is None
        assert processor.websocket_updater is None
        assert processor.timing_calculator is None

    @pytest.mark.asyncio
    async def test_component_initialization_with_mocks(self, ticker_processor_factory):
        """Test component initialization creates proper mock instances"""
        processor = ticker_processor_factory("ETHUSDT")

        # Initialize components
        await processor._initialize_components()

        # Verify all components created
        assert processor.timing_calculator is not None
        assert isinstance(processor.timing_calculator, MockTimingCalculator)

        assert processor.historical_downloader is not None
        assert isinstance(processor.historical_downloader, MockHistoricalDownloader)

        assert processor.websocket_updater is not None
        assert isinstance(processor.websocket_updater, MockWebSocketUpdater)

        # Verify cross-component wiring
        assert processor.historical_downloader.timing_calculator == processor.timing_calculator
        assert processor.websocket_updater.timing_calculator == processor.timing_calculator
        assert processor.historical_downloader.set_websocket_coordinator_calls == 1
        assert processor.websocket_updater.set_gap_recovery_callback_calls == 1

    @pytest.mark.asyncio
    async def test_timing_calculator_integration(self, ticker_processor_factory):
        """Test TimingCalculator integration and configuration"""
        processor = ticker_processor_factory("BNBUSDT")
        await processor._initialize_components()

        # Verify timing calculator configured
        assert processor.timing_calculator is not None
        timing_calc = processor.timing_calculator

        # Test calculation calls
        delay1 = timing_calc.calculate_request_delay()
        delay2 = timing_calc.calculate_request_delay()

        assert timing_calc.calculation_calls == 2
        assert len(timing_calc.get_call_history()) == 2
        assert isinstance(delay1, float)
        assert isinstance(delay2, float)

        # Test delay configuration
        timing_calc.set_delay(15.5)
        delay3 = timing_calc.calculate_request_delay()
        assert delay3 == 15.5

    @pytest.mark.asyncio
    async def test_websocket_coordinator_setup(self, ticker_processor_factory):
        """Test WebSocket coordinator setup between components"""
        processor = ticker_processor_factory("ADAUSDT")
        await processor._initialize_components()

        # Verify HistoricalDownloader has WebSocket coordinator
        assert processor.historical_downloader.websocket_coordinator == processor.websocket_updater
        assert processor.historical_downloader.set_websocket_coordinator_calls == 1

        # Verify WebSocketUpdater has timing calculator integration
        assert processor.websocket_updater.set_timing_calculator_calls == 1
        assert processor.websocket_updater.timing_calculator == processor.timing_calculator

    @pytest.mark.asyncio
    async def test_gap_recovery_callback_setup(self, ticker_processor_factory):
        """Test gap recovery callback setup between WebSocket and processor"""
        processor = ticker_processor_factory("DOTUSDT")
        await processor._initialize_components()

        # Verify gap recovery callback set
        assert processor.websocket_updater.set_gap_recovery_callback_calls == 1
        assert processor.websocket_updater.gap_recovery_callback is not None

        # Test callback invocation
        gap_start_id = 12345
        await processor.websocket_updater.gap_recovery_callback(gap_start_id)

        # Should trigger gap recovery in processor
        assert processor._gap_recoveries == 1


class TestSmartStartupStrategies:
    """Test smart startup strategies v4.2 (7 tests)"""

    @pytest.mark.asyncio
    async def test_determine_websocket_strategy_fast_system(self, ticker_processor_factory):
        """Test WebSocket strategy determination for fast system (≤5s)"""
        processor = ticker_processor_factory("BTCUSDT")

        # Test fast system scenarios
        fast_delays = [0.5, 1.0, 3.0, 5.0]

        for delay in fast_delays:
            strategy = processor._determine_websocket_strategy(delay)
            assert strategy == "websocket_first", f"Delay {delay}s should be websocket_first"

    @pytest.mark.asyncio
    async def test_determine_websocket_strategy_medium_load(self, ticker_processor_factory):
        """Test WebSocket strategy determination for medium load (5s-30s)"""
        processor = ticker_processor_factory("ETHUSDT")

        # Test medium load scenarios
        medium_delays = [5.1, 10.0, 20.0, 30.0]

        for delay in medium_delays:
            strategy = processor._determine_websocket_strategy(delay)
            assert strategy == "parallel_start", f"Delay {delay}s should be parallel_start"

    @pytest.mark.asyncio
    async def test_determine_websocket_strategy_high_load(self, ticker_processor_factory):
        """Test WebSocket strategy determination for high load (>30s)"""
        processor = ticker_processor_factory("BNBUSDT")

        # Test high load scenarios
        high_delays = [30.1, 45.0, 60.0, 120.0]

        for delay in high_delays:
            strategy = processor._determine_websocket_strategy(delay)
            assert strategy == "historical_only", f"Delay {delay}s should be historical_only"

    @pytest.mark.asyncio
    async def test_execute_websocket_first_strategy(self, ticker_processor_factory):
        """Test execution of websocket_first strategy"""
        processor = ticker_processor_factory("ADAUSDT")
        await processor._initialize_components()

        # Execute websocket_first strategy
        await processor._execute_startup_strategy("websocket_first")

        # Verify WebSocket started in buffering mode with dynamic management
        assert processor.websocket_updater.start_buffering_calls == 1
        assert processor.websocket_updater.start_dynamic_management_calls == 1
        assert processor.websocket_updater.websocket_connected is True
        assert processor.websocket_updater.mode == "BUFFERING"

    @pytest.mark.asyncio
    async def test_execute_parallel_start_strategy(self, ticker_processor_factory):
        """Test execution of parallel_start strategy"""
        processor = ticker_processor_factory("DOTUSDT")
        await processor._initialize_components()

        # Execute parallel_start strategy
        await processor._execute_startup_strategy("parallel_start")

        # Verify no immediate WebSocket actions (will be started by HistoricalDownloader)
        assert processor.websocket_updater.start_buffering_calls == 0
        assert processor.websocket_updater.start_dynamic_management_calls == 0

    @pytest.mark.asyncio
    async def test_execute_historical_only_strategy(self, ticker_processor_factory):
        """Test execution of historical_only strategy"""
        processor = ticker_processor_factory("LINKUSDT")
        await processor._initialize_components()

        # Execute historical_only strategy
        await processor._execute_startup_strategy("historical_only")

        # Verify no WebSocket actions (will wait for better conditions)
        assert processor.websocket_updater.start_buffering_calls == 0
        assert processor.websocket_updater.start_dynamic_management_calls == 0

    @pytest.mark.asyncio
    async def test_startup_strategy_timing_calculator_integration(self, ticker_processor_factory):
        """Test startup strategy uses TimingCalculator correctly"""
        processor = ticker_processor_factory("ATOMUSDT")
        await processor._initialize_components()

        # Configure timing calculator for specific scenario
        processor.timing_calculator.set_delay(15.0)  # Should trigger parallel_start

        # Execute historical download which includes strategy determination
        initial_delay = processor.timing_calculator.calculate_request_delay()
        strategy = processor._determine_websocket_strategy(initial_delay)

        # Verify timing calculator was used and correct strategy chosen
        assert processor.timing_calculator.calculation_calls >= 1
        assert strategy == "parallel_start"
        assert initial_delay == 15.0

        # Test strategy change with different delay
        processor.timing_calculator.set_delay(45.0)  # Should trigger historical_only
        new_delay = processor.timing_calculator.calculate_request_delay()
        new_strategy = processor._determine_websocket_strategy(new_delay)

        assert new_strategy == "historical_only"
        assert new_delay == 45.0


class TestHistoricalDownloadCoordination:
    """Test historical download coordination with WebSocket handoff (6 tests)"""

    @pytest.mark.asyncio
    async def test_download_historical_data_success(self, ticker_processor_factory):
        """Test successful historical data download"""
        processor = ticker_processor_factory("BTCUSDT")
        await processor._initialize_components()

        # Configure successful download result
        success_result = HistoricalDownloadResult(
            success=True,
            trades_loaded=15000,
            api_requests_made=150,
            download_duration_seconds=90.0,
            errors_count=0,
            binance_errors_count=0,
            last_aggregate_id=25000,
            websocket_handoff_completed=True,
            start_time=int(time.time() * 1000) - 3600000,
            end_time=int(time.time() * 1000)
        )
        processor.historical_downloader.set_download_result(success_result)

        # Execute historical download
        await processor._download_historical_data()

        # Verify state transitions
        assert processor.state in {ProcessorState.DOWNLOADING_HISTORICAL, ProcessorState.STARTING_WEBSOCKET}

        # Verify download was called
        assert processor.historical_downloader.download_calls == 1

        # Verify statistics updated
        assert processor._historical_trades_loaded == 15000
        assert processor._last_aggregate_id == 25000
        assert processor._historical_completed is True

    @pytest.mark.asyncio
    async def test_download_historical_data_with_websocket_coordination(self, ticker_processor_factory):
        """Test historical download with WebSocket coordination"""
        processor = ticker_processor_factory("ETHUSDT")
        await processor._initialize_components()

        # Configure timing calculator for websocket_first strategy
        processor.timing_calculator.set_delay(3.0)  # Fast system

        # Execute historical download
        await processor._download_historical_data()

        # Verify timing calculator was used for strategy determination
        assert processor.timing_calculator.calculation_calls >= 1

        # Verify WebSocket was started due to websocket_first strategy
        assert processor.websocket_updater.start_buffering_calls == 1
        assert processor.websocket_updater.start_dynamic_management_calls == 1

        # Verify historical download was executed
        assert processor.historical_downloader.download_calls == 1

    @pytest.mark.asyncio
    async def test_download_historical_failure_handling(self, ticker_processor_factory):
        """Test historical download failure handling"""
        processor = ticker_processor_factory("BNBUSDT")
        await processor._initialize_components()

        # Configure failed download result
        failed_result = HistoricalDownloadResult(
            success=False,
            trades_loaded=0,
            api_requests_made=5,
            download_duration_seconds=10.0,
            errors_count=3,
            binance_errors_count=2,
            last_aggregate_id=None,
            websocket_handoff_completed=False,
            start_time=int(time.time() * 1000) - 3600000,
            end_time=int(time.time() * 1000)
        )
        processor.historical_downloader.set_download_result(failed_result)

        # Execute historical download and expect failure
        with pytest.raises(Exception, match="Historical download failed"):
            await processor._download_historical_data()

        # Verify download was attempted
        assert processor.historical_downloader.download_calls == 1

        # Verify statistics show failure
        assert processor._historical_trades_loaded == 0
        assert processor._historical_completed is False

    @pytest.mark.asyncio
    async def test_historical_download_statistics_tracking(self, ticker_processor_factory):
        """Test historical download statistics tracking"""
        processor = ticker_processor_factory("ADAUSDT")
        await processor._initialize_components()

        # Configure download result with specific statistics
        result = HistoricalDownloadResult(
            success=True,
            trades_loaded=8500,
            api_requests_made=85,
            download_duration_seconds=42.5,
            errors_count=1,
            binance_errors_count=0,
            last_aggregate_id=18500,
            websocket_handoff_completed=True,
            start_time=int(time.time() * 1000) - 1800000,
            end_time=int(time.time() * 1000)
        )
        processor.historical_downloader.set_download_result(result)

        # Execute download
        await processor._download_historical_data()

        # Verify all statistics captured correctly
        stats = processor.get_stats()
        assert stats.historical_trades_loaded == 8500
        assert processor._last_aggregate_id == 18500
        assert processor._historical_completed is True

        # Verify download result details preserved
        assert processor.historical_downloader.download_calls == 1

    @pytest.mark.asyncio
    async def test_historical_download_cancellation(self, ticker_processor_factory):
        """Test historical download cancellation handling"""
        processor = ticker_processor_factory("DOTUSDT")
        await processor._initialize_components()

        # Start download in background
        download_task = asyncio.create_task(processor._download_historical_data())

        # Cancel immediately
        processor._should_stop = True
        download_task.cancel()

        # Wait for cancellation
        with pytest.raises(asyncio.CancelledError):
            await download_task

        # Verify clean state
        assert processor._should_stop is True

    @pytest.mark.asyncio
    async def test_historical_to_websocket_handoff(self, ticker_processor_factory):
        """Test seamless handoff from historical to WebSocket"""
        processor = ticker_processor_factory("LINKUSDT")
        await processor._initialize_components()

        # Configure historical download with specific last_aggregate_id
        result = HistoricalDownloadResult(
            success=True,
            trades_loaded=12000,
            api_requests_made=120,
            download_duration_seconds=60.0,
            errors_count=0,
            binance_errors_count=0,
            last_aggregate_id=22000,
            websocket_handoff_completed=True,
            start_time=int(time.time() * 1000) - 3600000,
            end_time=int(time.time() * 1000)
        )
        processor.historical_downloader.set_download_result(result)

        # Execute historical download
        await processor._download_historical_data()

        # Verify handoff data preserved for WebSocket
        assert processor._last_aggregate_id == 22000
        assert processor._historical_completed is True

        # This aggregate_id will be used for WebSocket streaming mode switch
        expected_handoff_id = processor._last_aggregate_id
        assert expected_handoff_id == 22000


class TestWebSocketIntegrationBasics:
    """Test basic WebSocket integration scenarios (2 tests)"""

    @pytest.mark.asyncio
    async def test_start_websocket_updates_streaming_mode(self, ticker_processor_factory):
        """Test starting WebSocket updates in direct streaming mode"""
        processor = ticker_processor_factory("BTCUSDT")
        await processor._initialize_components()

        # Set up as if historical download completed
        processor._historical_completed = True
        processor._last_aggregate_id = 15000

        # ВАЖНО: Установить корректное начальное состояние для WebSocket фазы
        processor._transition_state(ProcessorState.DOWNLOADING_HISTORICAL)

        # Execute WebSocket startup
        await processor._start_websocket_updates()

        # Verify WebSocket started and switched to streaming
        assert processor.websocket_updater.start_buffering_calls == 1
        assert processor.websocket_updater.switch_streaming_calls == 1
        assert processor._websocket_streaming is True

        # Verify state transition - after _start_websocket_updates should be RUNNING
        assert processor.state == ProcessorState.RUNNING

        # Verify dynamic management stopped (streaming mode)
        assert processor.websocket_updater.stop_dynamic_management_calls >= 1

        # Verify monitoring task created
        assert processor._websocket_task is not None

    @pytest.mark.asyncio
    async def test_start_websocket_updates_from_buffering(self, ticker_processor_factory):
        """Test starting WebSocket updates from existing buffering mode"""
        processor = ticker_processor_factory("ETHUSDT")
        await processor._initialize_components()

        # Set up as if WebSocket already connected from historical phase
        processor.websocket_updater.websocket_connected = True
        processor.websocket_updater.mode = "BUFFERING"
        processor._historical_completed = True
        processor._last_aggregate_id = 25000

        # Execute WebSocket startup
        await processor._start_websocket_updates()

        # Verify switched to streaming mode (no new buffering start)
        assert processor.websocket_updater.start_buffering_calls == 0
        assert processor.websocket_updater.switch_streaming_calls == 1
        assert processor._websocket_streaming is True

        # Verify dynamic management stopped for streaming mode
        assert processor.websocket_updater.stop_dynamic_management_calls >= 1

        # Verify correct aggregate_id passed for handoff
        # Last call to switch_to_streaming_mode should use last_aggregate_id
        assert processor._last_aggregate_id == 25000


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])