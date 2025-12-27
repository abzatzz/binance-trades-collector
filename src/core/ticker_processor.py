"""
TickerProcessor - Complete Ticker Lifecycle Management
======================================================

Coordinates all processing for a single ticker including historical downloads,
WebSocket updates, binary storage, health monitoring, and comprehensive error handling.

File: src/core/ticker_processor.py
"""

import asyncio
import time
from typing import Optional, Dict, Any
from dataclasses import dataclass

from loggerino import loggerino
from binance import AsyncClient

from ..models.config import Config
from ..models.ticker_model import TickerModel
from ..models.enums import ProcessorState
from ..models.binance_errors import BinanceClientError
from .historical_downloader import HistoricalDownloader
from .websocket_updater import WebSocketUpdater
from .weight_coordinator import WeightCoordinator
from .trades_interfaces import TradesWriter
from .telegram_notifier import get_notifier, AlertLevel
from .integrity import RecoveryState, RecoveryStatus, IntegrityManager


@dataclass
class ProcessorStats:
    """Statistics for ticker processing."""
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


class TickerProcessor:
    """
    Complete ticker processing coordinator with full WebSocket and historical integration.

    Responsibilities:
    - Initialize BinaryFileManager for ticker storage
    - Download historical trade data using HistoricalDownloader
    - Start real-time WebSocket updates using WebSocketUpdater
    - Coordinate seamless handoff between historical and real-time data
    - Handle gap detection and recovery
    - Monitor health and comprehensive error handling
    - Provide graceful shutdown with resource cleanup
    """

    def __init__(
            self,
            ticker_model: TickerModel,
            binance_client: AsyncClient,
            weight_coordinator: WeightCoordinator,
            trades_writer: TradesWriter,
            config: Config,
            logger_name: str = None,
            initial_last_aggregate_id: Optional[int] = None,
            integrity_manager: Optional['IntegrityManager'] = None,
            recovery_state: Optional[RecoveryState] = None,
            central_websocket_manager=None
    ):
        """
        Initialize TickerProcessor for specific ticker.

        Args:
            ticker_model: TickerModel with ticker metadata
            binance_client: Binance AsyncClient instance
            weight_coordinator: WeightCoordinator instance for API rate limiting
            config: System configuration
        """
        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.weight_coordinator = weight_coordinator
        self.trades_writer = trades_writer
        self.config = config

        self.integrity_manager = integrity_manager
        self.recovery_state = recovery_state
        self.central_websocket_manager = central_websocket_manager

        if logger_name:
            try:
                self.logger = loggerino.get(logger_name)
            except Exception as e:
                self.logger = loggerino.get('system')
                self.logger.warning(f"Failed to get ticker logger {logger_name}: {e}")
        else:
            self.logger = loggerino.get('system')

        # State management with validation
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
        self._consecutive_gap_failures: int = 0
        self._websocket_reconnections: int = 0

        # Components
        self.historical_downloader: Optional[HistoricalDownloader] = None
        self.websocket_updater: Optional[WebSocketUpdater] = None

        # Tasks
        self._historical_task: Optional[asyncio.Task] = None
        self._websocket_task: Optional[asyncio.Task] = None
        self._monitoring_task: Optional[asyncio.Task] = None

        # Coordination state
        self._historical_completed: bool = False
        self._websocket_streaming: bool = False
        self._last_aggregate_id: Optional[int] = initial_last_aggregate_id

        # Log cache usage
        if initial_last_aggregate_id is not None:
            self.logger.info(f"Using cached last_aggregate_id: {initial_last_aggregate_id}")
        else:
            self.logger.info("No cached last_aggregate_id, will query on demand")

        self.logger.info(f"TickerProcessor initialized for {self.ticker_model.symbol}")

    # ===== MAIN PROCESSING LOOP =====

    async def run(self) -> None:
        """
        Main processing loop for ticker with complete lifecycle management.

        This method coordinates:
        1. Component initialization
        2. Historical data download
        3. WebSocket stream startup and coordination
        4. Continuous monitoring and health checks
        5. Gap detection and recovery
        6. Error handling and recovery
        """
        self.logger.info(f"Starting TickerProcessor for {self.ticker_model.symbol}")

        try:
            self._is_running = True

            # Phase 1: Initialize components
            await self._initialize_components()

            # Phase 2: Request integrity check and wait
            await self._wait_for_integrity_check()

            # Phase 3: Download historical data
            await self._download_historical_data()

            # Phase 4: Start WebSocket updates
            await self._start_websocket_updates()

            # Phase 5: Main monitoring loop
            await self._monitoring_loop()

        except asyncio.CancelledError:
            self.logger.info(f"TickerProcessor cancelled for {self.ticker_model.symbol}")
            raise
        except Exception as e:
            self._transition_state(ProcessorState.ERROR)
            self._handle_processor_error(e)
            raise
        finally:
            await self._cleanup()

    async def shutdown(self, timeout_seconds: float = 30.0) -> None:
        """
        Graceful shutdown of ticker processor.

        Args:
            timeout_seconds: Maximum time to wait for shutdown
        """
        self.logger.info(f"Shutting down TickerProcessor for {self.ticker_model.symbol}")

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
            self.logger.warning(f"Shutdown timeout for {self.ticker_model.symbol}, forcing stop")

        await self._cleanup()
        self._transition_state(ProcessorState.STOPPED)
        self.logger.info(f"TickerProcessor shutdown complete for {self.ticker_model.symbol}")

    # ===== STATE MANAGEMENT =====

    def _transition_state(self, new_state: ProcessorState) -> bool:
        """
        Transition to new state with validation.

        Args:
            new_state: Target state

        Returns:
            True if transition was successful
        """
        # Skip if already in target state
        if self._state == new_state:
            return True

        if self._state.can_transition_to(new_state):
            old_state = self._state
            self._state = new_state
            self.logger.debug(f"State transition for {self.ticker_model.symbol}: {old_state.name} -> {new_state.name}")
            return True
        else:
            self.logger.warning(f"Invalid state transition for {self.ticker_model.symbol}: {self._state.name} -> {new_state.name}")
            return False

    def _handle_processor_error(self, error: Exception) -> None:
        """Handle processor-level errors with appropriate classification."""
        self._last_error = str(error)
        self._error_count += 1

        if isinstance(error, BinanceClientError):
            self._binance_error_count += 1
            if error.is_recoverable():
                self._recoverable_error_count += 1

        self.logger.error(f"TickerProcessor error for {self.ticker_model.symbol}: {error}", exc_info=True)

    # ===== INITIALIZATION PHASE =====

    async def _initialize_components(self) -> None:
        """Initialize all components for ticker processing."""
        self.logger.info(f"Initializing components for {self.ticker_model.symbol}")

        try:

            # Initialize HistoricalDownloader
            self.historical_downloader = HistoricalDownloader(
                ticker_model=self.ticker_model,
                binance_client=self.binance_client,
                weight_coordinator=self.weight_coordinator,
                trades_writer=self.trades_writer,
                config=self.config.processing,
                logger=self.logger
            )

            # Initialize WebSocketUpdater
            self.websocket_updater = WebSocketUpdater(
                ticker_model=self.ticker_model,
                binance_client=self.binance_client,
                trades_writer=self.trades_writer,
                config=self.config.processing,
                logger=self.logger
            )

            # Set up gap recovery callback for WebSocket
            self.websocket_updater.set_gap_recovery_callback(self._handle_gap_recovery)

            self.logger.info(f"Components initialized for {self.ticker_model.symbol}")

        except Exception as e:
            self.logger.error(f"Component initialization failed for {self.ticker_model.symbol}: {e}")
            raise

    async def _wait_for_integrity_check(self) -> None:
        """
        Request integrity check and wait for completion.

        Sends request to IntegrityManager queue and waits until:
        - Integrity check passes (no issues)
        - Archive recovery completes (if needed)
        - Error occurs (will raise exception)
        """
        if self._should_stop:
            return

        # If no integrity manager, skip this phase
        if not self.integrity_manager or not self.recovery_state:
            self.logger.info(f"No integrity manager configured, skipping integrity check for {self.ticker_model.symbol}")
            return

        self.logger.info(f"Requesting integrity check for {self.ticker_model.symbol}")
        self._transition_state(ProcessorState.WAITING_INTEGRITY_CHECK)

        # Broadcast status
        await self._broadcast_status("waiting_integrity_check")

        # Request check from IntegrityManager
        await self.integrity_manager.request_check(
            symbol=self.ticker_model.symbol,
            priority=self._get_symbol_priority()
        )

        # Wait for result
        while not self._should_stop:
            status = self.recovery_state.get_status(self.ticker_model.symbol)

            if status == RecoveryStatus.READY:
                # Check passed or recovery completed
                info = self.recovery_state.get_info(self.ticker_model.symbol)
                if info.trades_recovered > 0:
                    self.logger.info(
                        f"Integrity recovery completed for {self.ticker_model.symbol}: "
                        f"{info.trades_recovered:,} trades, {info.days_processed} days"
                    )
                else:
                    self.logger.info(f"Integrity check passed for {self.ticker_model.symbol}")
                break

            elif status == RecoveryStatus.ERROR:
                # Recovery failed - cannot proceed
                error = self.recovery_state.get_error(self.ticker_model.symbol)
                self.logger.error(f"Integrity check failed for {self.ticker_model.symbol}: {error}")
                raise Exception(f"Integrity check failed: {error}")

            elif status == RecoveryStatus.ARCHIVE_RECOVERY:
                # Archive recovery in progress
                if self._state != ProcessorState.ARCHIVE_RECOVERY:
                    self._transition_state(ProcessorState.ARCHIVE_RECOVERY)
                    await self._broadcast_status("archive_recovery")
                    self.logger.info(f"Archive recovery in progress for {self.ticker_model.symbol}")

            elif status == RecoveryStatus.CHECKING:
                # Still checking
                if self._state != ProcessorState.WAITING_INTEGRITY_CHECK:
                    self._transition_state(ProcessorState.WAITING_INTEGRITY_CHECK)

            await asyncio.sleep(1)

        if self._should_stop:
            self.logger.info(f"Integrity check interrupted by shutdown for {self.ticker_model.symbol}")
            return

    def _get_symbol_priority(self) -> int:
        """Get priority for this symbol (higher = more important)."""
        top_symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
        symbol = self.ticker_model.symbol

        if symbol in top_symbols:
            return 100 - top_symbols.index(symbol)
        return 0

    async def _broadcast_status(self, status: str, extra: dict = None) -> None:
        """Broadcast status to connected WebSocket clients."""
        if not self.central_websocket_manager:
            return

        try:
            message = {
                "type": "ticker_status",
                "symbol": self.ticker_model.symbol,
                "status": status,
                "timestamp": int(time.time() * 1000)
            }
            if extra:
                message.update(extra)

            await self.central_websocket_manager.broadcast_ticker_status(
                self.ticker_model.symbol,
                message
            )
        except Exception as e:
            self.logger.debug(f"Failed to broadcast status: {e}")

    # ===== HISTORICAL DATA PHASE =====

    async def _download_historical_data(self) -> None:
        """Download historical trade data with WebSocket coordination."""
        if self._should_stop:
            return

        self.logger.info(f"Starting historical data download for {self.ticker_model.symbol}")
        self._transition_state(ProcessorState.DOWNLOADING_HISTORICAL)

        try:
            self.logger.info(f"Starting WebSocket buffering for {self.ticker_model.symbol}")
            success = await self.websocket_updater.start_buffering_mode()
            if success:
                self.logger.info(f"WebSocket buffering started for {self.ticker_model.symbol}")

            # Execute historical download through HistoricalDownloader
            self._historical_task = asyncio.create_task(
                self.historical_downloader.download_historical_data()
            )
            result = await self._historical_task

            if result.success:
                # Update statistics ONLY for successful download
                self._historical_trades_loaded = result.trades_loaded
                self._last_aggregate_id = result.last_aggregate_id
                self._historical_completed = True  # ✅ Только при успехе

                self.logger.info(f"Historical data download completed for {self.ticker_model.symbol}: "
                                 f"{result.trades_loaded} trades, {result.api_requests_made} requests, "
                                 f"{result.download_duration_seconds:.1f}s, websocket_handoff: {result.websocket_handoff_completed}")
            else:
                # Update partial statistics for failed download but DON'T mark as completed
                self._historical_trades_loaded = result.trades_loaded  # Может быть частичная загрузка
                self._last_aggregate_id = result.last_aggregate_id  # Может быть частичная загрузка
                # self._historical_completed остается False

                self.logger.error(f"Historical data download failed for {self.ticker_model.symbol}: "
                                  f"{result.errors_count} errors, {result.binance_errors_count} Binance errors")
                raise Exception(f"Historical download failed: {result.errors_count} total errors")

        except asyncio.CancelledError:
            self.logger.info(f"Historical download cancelled for {self.ticker_model.symbol}")
            raise
        except Exception as e:
            self.logger.error(f"Historical download failed for {self.ticker_model.symbol}: {e}")
            raise

    # ===== WEBSOCKET PHASE =====

    async def _start_websocket_updates(self) -> None:
        """Start WebSocket updates for real-time data with seamless handoff."""
        if self._should_stop:
            return

        self.logger.info(f"Starting WebSocket updates for {self.ticker_model.symbol}")
        self._transition_state(ProcessorState.STARTING_WEBSOCKET)

        try:
            # If WebSocket was started during historical download, switch to streaming
            if hasattr(self.websocket_updater, 'websocket_connected') and self.websocket_updater.websocket_connected:
                self.logger.info(f"WebSocket already active for {self.ticker_model.symbol}, switching to streaming mode")
                success = await self.websocket_updater.switch_to_streaming_mode(self._last_aggregate_id or 0)
                if not success:
                    raise Exception("Failed to switch WebSocket to streaming mode")

            else:
                # Start WebSocket in streaming mode directly
                self.logger.info(f"Starting WebSocket in direct streaming mode for {self.ticker_model.symbol}")
                success = await self.websocket_updater.start_buffering_mode()
                if not success:
                    raise Exception("Failed to start WebSocket updates")

                # Immediately switch to streaming since historical is complete
                await asyncio.sleep(1)  # Brief delay for connection establishment
                success = await self.websocket_updater.switch_to_streaming_mode(self._last_aggregate_id or 0)
                if not success:
                    raise Exception("Failed to switch to streaming mode")

            self._websocket_streaming = True

            # Monitor WebSocket task
            self._websocket_task = asyncio.create_task(
                self._monitor_websocket(),
                name=f"websocket_monitor_{self.ticker_model.symbol}"
            )

            # Wait a bit to ensure WebSocket is working
            await asyncio.sleep(2)

            if not self._should_stop:
                self._transition_state(ProcessorState.RUNNING)

                self.logger.info(f"WebSocket streaming mode started for {self.ticker_model.symbol}")

        except Exception as e:
            self.logger.error(f"WebSocket startup failed for {self.ticker_model.symbol}: {e}")
            raise

    async def _monitor_websocket(self) -> None:
        """Monitor WebSocket health and update statistics."""
        self.logger.debug(f"WebSocket monitoring started for {self.ticker_model.symbol}")

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
                # if not self.websocket_updater.is_healthy():
                #     self.logger.warning(f"WebSocket unhealthy for {self.ticker_model.symbol}")

        except asyncio.CancelledError:
            self.logger.debug(f"WebSocket monitoring cancelled for {self.ticker_model.symbol}")
        except Exception as e:
            self.logger.error(f"WebSocket monitoring error for {self.ticker_model.symbol}: {e}")

    # ===== GAP RECOVERY =====

    async def _handle_gap_recovery(self, gap_start_id: int) -> None:
        """
        Enhanced gap recovery with ClickHouse verification and buffer overflow handling.
        Stops ticker after 5 consecutive failures to prevent infinite loop.

        Args:
            gap_start_id: Starting aggregate_id for recovery
        """
        MAX_CONSECUTIVE_FAILURES = 5

        self._gap_recoveries += 1
        self._consecutive_gap_failures += 1

        self.logger.warning(f"Starting enhanced gap recovery for {self.ticker_model.symbol} from ID {gap_start_id} "
                            f"(attempt {self._consecutive_gap_failures}/{MAX_CONSECUTIVE_FAILURES})")

        # Check if we exceeded max consecutive failures
        if self._consecutive_gap_failures > MAX_CONSECUTIVE_FAILURES:
            self.logger.error(f"Gap recovery limit exceeded for {self.ticker_model.symbol} "
                              f"({self._consecutive_gap_failures} consecutive failures). Stopping ticker.")

            # Send Telegram alert
            notifier = get_notifier()
            if notifier:
                try:
                    await notifier.send_alert(
                        message=f"Gap recovery failed {self._consecutive_gap_failures} times. Ticker stopped.",
                        level=AlertLevel.CRITICAL,
                        symbol=self.ticker_model.symbol
                    )
                except Exception as e:
                    self.logger.error(f"Failed to send Telegram alert: {e}")

            # Stop the ticker
            self._should_stop = True
            return

        try:
            # ШАГ 1: Найти последний подтвержденный aggregate_id в ClickHouse
            last_confirmed_id = await self.trades_writer.get_last_aggregate_id(self.ticker_model.symbol)

            if last_confirmed_id is None:
                self.logger.error(f"No confirmed trades found in ClickHouse for {self.ticker_model.symbol}")
                recovery_start_id = gap_start_id
            else:
                # Начинаем recovery с следующего ID после последнего подтвержденного
                recovery_start_id = max(last_confirmed_id + 1, gap_start_id)
                self.logger.info(f"Last confirmed aggregate_id in ClickHouse: {last_confirmed_id}, "
                                 f"gap_start_id: {gap_start_id}, starting recovery from {recovery_start_id}")

            # ШАГ 2: Запустить historical download для gap recovery
            if self.historical_downloader:
                self.logger.info(f"Starting historical download for gap recovery: {recovery_start_id}")
                recovery_result = await self.historical_downloader.recover_from_gap(recovery_start_id)

                if recovery_result.success:
                    self.logger.info(f"Gap recovery completed for {self.ticker_model.symbol}: "
                                     f"{recovery_result.trades_loaded} trades recovered, "
                                     f"last_aggregate_id: {recovery_result.last_aggregate_id}")

                    # SUCCESS: Reset consecutive failures counter
                    self._consecutive_gap_failures = 0

                    # ШАГ 3: Обновить last_aggregate_id
                    self._last_aggregate_id = recovery_result.last_aggregate_id

                    # Шаг 4: Переключить WebSocket обратно в STREAMING mode
                    if self.websocket_updater:
                        success = await self.websocket_updater.switch_to_streaming_mode(self._last_aggregate_id or 0)
                        if success:
                            if hasattr(self.websocket_updater, 'gap_recovery_in_progress'):
                                self.websocket_updater.gap_recovery_in_progress = False
                            self.logger.info(f"Successfully resumed streaming mode after gap recovery")
                        else:
                            self.logger.error(f"Failed to switch WebSocket back to streaming mode")
                else:
                    self.logger.error(f"Gap recovery failed for {self.ticker_model.symbol}: {recovery_result.errors_count} errors")

        except Exception as e:
            self.logger.error(f"Gap recovery error for {self.ticker_model.symbol}: {e}", exc_info=True)

            # В случае ошибки, попытаться вернуть WebSocket в нормальное состояние
            if self.websocket_updater and hasattr(self.websocket_updater, 'gap_recovery_in_progress'):
                self.websocket_updater.gap_recovery_in_progress = False

    # ===== MONITORING PHASE =====

    async def _monitoring_loop(self) -> None:
        """Main monitoring and health check loop."""
        self.logger.info(f"Starting monitoring loop for {self.ticker_model.symbol}")

        self._monitoring_task = asyncio.create_task(self._health_monitoring())

        try:
            # Wait for WebSocket task or shutdown signal
            if self._websocket_task:
                await self._websocket_task

        except asyncio.CancelledError:
            self.logger.info(f"Monitoring loop cancelled for {self.ticker_model.symbol}")
            raise
        except Exception as e:
            self.logger.error(f"Monitoring loop error for {self.ticker_model.symbol}: {e}")
            raise

    async def _health_monitoring(self) -> None:
        """Background health monitoring task."""
        self.logger.debug(f"Health monitoring started for {self.ticker_model.symbol}")

        try:
            while not self._should_stop and self._state.is_active():
                await asyncio.sleep(self.config.processing.health_check_interval_seconds)

                # Perform health checks
                await self._perform_health_checks()

                # Log periodic status
                stats = self.get_stats()
                self.logger.debug(f"Health check for {self.ticker_model.symbol}: "
                                  f"{stats.historical_trades_loaded} historical trades, "
                                  f"{stats.websocket_trades_processed} websocket trades, "
                                  f"{stats.gap_recoveries} gap recoveries")

        except asyncio.CancelledError:
            self.logger.debug(f"Health monitoring cancelled for {self.ticker_model.symbol}")
            raise
        except Exception as e:
            self.logger.error(f"Health monitoring error for {self.ticker_model.symbol}: {e}")

    async def _perform_health_checks(self) -> None:
        """Perform comprehensive health checks and recovery if needed."""
        # Check for stale WebSocket data
        if self._last_trade_time:
            time_since_last_trade = int(time.time() * 1000) - self._last_trade_time
            max_stale_time = 300000  # 5 minutes in milliseconds

            if time_since_last_trade > max_stale_time:
                # Debug level to avoid spam for low-volume tickers
                self.logger.debug(f"Stale data detected for {self.ticker_model.symbol}, "
                                  f"last trade {time_since_last_trade}ms ago")

        # Check WebSocket health
        if self.websocket_updater:
            try:
                ws_stats = self.websocket_updater.get_stats()
                if not self.websocket_updater.is_healthy():
                    # Only log at debug level to avoid spam for low-volume tickers
                    self.logger.debug(f"WebSocket health issues for {self.ticker_model.symbol}: "
                                      f"mode={ws_stats.mode.name}, connected={ws_stats.connected}")
            except Exception as e:
                self.logger.debug(f"WebSocket health check failed for {self.ticker_model.symbol}: {e}")

        # Check historical downloader health if running
        if (self.historical_downloader and
                self.historical_downloader.is_download_running()):
            try:
                progress = self.historical_downloader.get_progress()
                self.logger.debug(f"Historical download progress for {self.ticker_model.symbol}: "
                                  f"{progress.total_trades_downloaded} trades, "
                                  f"{progress.download_rate_per_minute:.0f} trades/min")
            except Exception as e:
                self.logger.warning(f"Historical download health check failed for {self.ticker_model.symbol}: {e}")

    # ===== CLEANUP =====

    async def _cleanup(self) -> None:
        """Cleanup all resources and tasks."""
        self.logger.info(f"Cleaning up TickerProcessor for {self.ticker_model.symbol}")

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
                    self.logger.error(f"Task cleanup error for {self.ticker_model.symbol}: {e}")

        # Cleanup WebSocketUpdater first
        if self.websocket_updater:
            try:
                await self.websocket_updater.shutdown()
                self.logger.debug(f"WebSocketUpdater stopped for {self.ticker_model.symbol}")
            except Exception as e:
                self.logger.error(f"WebSocketUpdater cleanup error for {self.ticker_model.symbol}: {e}")

        # Cleanup HistoricalDownloader
        if self.historical_downloader:
            try:
                await self.historical_downloader.pause_download()
                self.logger.debug(f"HistoricalDownloader stopped for {self.ticker_model.symbol}")
            except Exception as e:
                self.logger.error(f"HistoricalDownloader cleanup error for {self.ticker_model.symbol}: {e}")

        self._is_running = False
        self.logger.info(f"TickerProcessor cleanup completed for {self.ticker_model.symbol}")

    async def _wait_for_shutdown(self) -> None:
        """Wait for shutdown signal to be processed."""
        while self._is_running and not self._should_stop:
            await asyncio.sleep(0.1)

    # ===== PUBLIC INTERFACE =====

    def get_stats(self) -> ProcessorStats:
        """
        Get current processor statistics with comprehensive metrics.

        Returns:
            ProcessorStats with current state and metrics
        """
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
        """
        Check if processor is in healthy state with comprehensive checks.

        Returns:
            True if processor is running normally
        """
        # Basic state checks
        if not (self._state.is_active() and self._is_running and not self._should_stop):
            return False

        # Error threshold check
        if self._error_count >= 10:  # Increased threshold for better tolerance
            return False

        # Component health checks
        if self.websocket_updater and not self.websocket_updater.is_healthy():
            return False

        # Check for excessive gap recoveries
        if self._gap_recoveries > 5:
            return False

        return True

    def pause(self) -> bool:
        """
        Pause ticker processing (pause WebSocket, keep storage).

        Returns:
            True if pause was successful
        """
        if self._transition_state(ProcessorState.PAUSED):
            # Pause WebSocket updates but don't shut down completely
            if self.websocket_updater:
                # WebSocketUpdater doesn't have pause, so we'd need to implement it
                self.logger.info(f"TickerProcessor paused for {self.ticker_model.symbol}")
            return True
        return False

    def resume(self) -> bool:
        """
        Resume ticker processing (restart WebSocket).

        Returns:
            True if resume was successful
        """
        if self._transition_state(ProcessorState.RUNNING):
            # Resume WebSocket updates
            self.logger.info(f"TickerProcessor resumed for {self.ticker_model.symbol}")
            return True
        return False

    async def get_storage_stats(self) -> Optional[Dict[str, Any]]:
        """Get storage statistics through TradesWriter interface."""
        try:
            return await self.trades_writer.get_storage_stats(self.ticker_model.symbol)
        except Exception as e:
            self.logger.error(f"Failed to get storage stats for {self.ticker_model.symbol}: {e}")
            return None

    def get_websocket_stats(self) -> Optional[Dict[str, Any]]:
        """Get WebSocket statistics."""
        if self.websocket_updater:
            ws_stats = self.websocket_updater.get_stats()
            return {
                'mode': ws_stats.mode.name,
                'connected': ws_stats.connected,
                'trades_processed': ws_stats.trades_processed,
                'trades_per_minute': ws_stats.trades_per_minute,
                'buffer_size': ws_stats.buffer_size,
                'reconnect_count': ws_stats.reconnect_count,
                'gap_recoveries': ws_stats.gap_recoveries,
                'binance_errors': ws_stats.binance_errors_count,
                'last_aggregate_id': ws_stats.last_aggregate_id
            }
        return None

    def pause_historical_download(self) -> bool:
        """Pause historical download if running."""
        if self.historical_downloader and self.historical_downloader.is_download_running():
            asyncio.create_task(self.historical_downloader.pause_download())
            return True
        return False

    def get_download_progress(self) -> Optional[Dict[str, Any]]:
        """Get detailed download progress."""
        if self.historical_downloader:
            progress = self.historical_downloader.get_progress()
            return {
                'total_trades': progress.total_trades_downloaded,
                'api_requests': progress.api_requests_made,
                'bytes_written': progress.bytes_written,
                'download_rate_per_minute': progress.download_rate_per_minute,
                'websocket_activated': progress.websocket_activated,
                'estimated_completion': progress.estimated_completion_time,
                'last_aggregate_id': progress.last_aggregate_id
            }
        return None

    async def get_comprehensive_status(self) -> Dict[str, Any]:
        """Get comprehensive status of all components."""
        storage_stats = await self.get_storage_stats()
        status = {
            'processor': {
                'symbol': self.ticker_model.symbol,
                'state': self._state.name,
                'is_healthy': self.is_healthy(),
                'uptime_seconds': (int(time.time() * 1000) - self._start_time) / 1000.0,
                'historical_completed': self._historical_completed,
                'websocket_streaming': self._websocket_streaming
            },
            'statistics': self.get_stats().__dict__,
            'storage': storage_stats,
            'websocket': self.get_websocket_stats(),
            'historical': self.get_download_progress()
        }
        return status

    @property
    def symbol(self) -> str:
        """Get ticker symbol."""
        return self.ticker_model.symbol

    @property
    def state(self) -> ProcessorState:
        """Get current processor state."""
        return self._state

    @property
    def is_running(self) -> bool:
        """Check if processor is running."""
        return self._is_running

    @property
    def last_aggregate_id(self) -> Optional[int]:
        """Get last processed aggregate_id."""
        return self._last_aggregate_id

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"TickerProcessor({self.ticker_model.symbol}, {self._state.name})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"TickerProcessor(symbol='{self.ticker_model.symbol}', "
            f"state={self._state.name}, running={self._is_running}, "
            f"historical_trades={self._historical_trades_loaded}, "
            f"websocket_trades={self._websocket_trades_processed})"
        )
