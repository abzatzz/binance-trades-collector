"""
WebSocketUpdater - Real-time AggTrade Data via Binance WebSocket
==============================================================

Manages real-time trade data streaming from Binance WebSocket API with comprehensive
error handling and coordination with HistoricalDownloader for seamless data continuity.

File: src/core/websocket_updater.py
"""

import asyncio
import random
import time
from enum import Enum, auto
from dataclasses import dataclass
from typing import List, Optional, Callable
from collections import deque

from loggerino import loggerino
from binance import AsyncClient, BinanceSocketManager

from .trades_interfaces import TradesWriter
from ..models.ticker_model import TickerModel
from ..models.aggtrade import AggTrade
from ..models.config import ProcessingConfig
from ..models.binance_errors import BinanceClientError, BinanceWebSocketError
from .telegram_notifier import get_notifier, AlertLevel

# Default logger, will be overridden by instance logger
_default_logger = loggerino.get('websocket_updater')


class WebSocketMode(Enum):
    """WebSocket operation modes"""
    IDLE = auto()  # Not active
    BUFFERING = auto()  # Accumulating trades in memory (coordination with Historical)
    STREAMING = auto()  # Direct write to files
    RECONNECTING = auto()  # Reconnection in progress
    ERROR = auto()  # Connection error state


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
    # Error statistics
    binance_errors_count: int
    recoverable_errors_count: int
    message_errors_count: int
    buffer_overflow_count: int
    gap_recovery_in_progress: bool


class WebSocketUpdater:
    """
    Real-time WebSocket trade data updater with comprehensive error handling.

    Responsibilities:
    - Establish and maintain WebSocket connection through BinanceSocketManager
    - Process incoming aggTrade messages with proper Binance error handling
    - Coordinate with HistoricalDownloader for seamless data continuity
    - Buffer trades during historical download, stream directly afterward
    - Handle all types of Binance errors with appropriate recovery strategies
    """

    def __init__(
            self,
            ticker_model: TickerModel,
            binance_client: AsyncClient,
            trades_writer: TradesWriter,
            config: ProcessingConfig,
            logger=None
    ):
        """
        Initialize WebSocketUpdater.

        Args:
            ticker_model: TickerModel with ticker metadata
            binance_client: Binance AsyncClient for WebSocket connections
            trades_writer: TradesWriter interface for data operations
            config: ProcessingConfig with WebSocket settings
        """
        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.trades_writer = trades_writer
        self.config = config
        # Use provided logger or fallback to default
        self.logger = logger if logger else _default_logger

        # WebSocket components
        self.socket_manager: Optional[BinanceSocketManager] = None
        self.websocket = None
        self.websocket_task: Optional[asyncio.Task] = None

        # State management
        self.mode: WebSocketMode = WebSocketMode.IDLE
        self.websocket_connected: bool = False
        self.should_stop: bool = False
        self.start_time: Optional[float] = None

        # Data management

        self.buffer = deque(maxlen=config.websocket_buffer_max_trades)
        self.last_aggregate_id: Optional[int] = None

        # Statistics and monitoring
        self.trades_processed: int = 0
        self.bytes_written: int = 0
        self.reconnect_attempts: int = 0
        self.gap_recoveries: int = 0
        self.last_trade_time: Optional[int] = None

        # Error statistics
        self.binance_errors_count: int = 0
        self.recoverable_errors_count: int = 0
        self.message_errors_count: int = 0

        self.buffer_overflow_count: int = 0
        self.gap_recovery_in_progress: bool = False

        # Configuration
        self.max_reconnect_attempts: int = config.max_websocket_reconnects

        # Hourly reconnect tracking for alerts
        self._reconnects_this_hour: int = 0
        self._hour_start_time: float = time.time()
        self._reconnect_alert_threshold: int = 10  # Alert if > 10 reconnects/hour
        self._last_reconnect_alert_time: float = 0

        # Coordination callbacks
        self.gap_recovery_callback: Optional[Callable[[int], None]] = None

        if self.config.verbose_trade_logging:
            self.logger.info(f"WebSocket buffer initialized with maxlen={config.websocket_buffer_max_trades} trades for {ticker_model.symbol}")

    # ===== LIFECYCLE MANAGEMENT =====

    async def start_buffering_mode(self) -> bool:
        """
        Start WebSocket in buffering mode (called by HistoricalDownloader).

        Returns:
            True if started successfully
        """
        if self.config.verbose_trade_logging:
            self.logger.info(f"Starting WebSocket buffering mode for {self.ticker_model.symbol}")

        self.mode = WebSocketMode.BUFFERING
        self.buffer.clear()
        self.start_time = time.time()

        # Establish WebSocket connection
        success = await self.connect_websocket()
        if not success:
            return False

        # Start WebSocket processing loop
        self.websocket_task = asyncio.create_task(
            self._websocket_loop(),
            name=f"websocket_{self.ticker_model.symbol}"
        )

        self.logger.info(f"WebSocket buffering mode started for {self.ticker_model.symbol}")
        return True

    async def switch_to_streaming_mode(self, last_historical_id: int) -> bool:
        """
        Switch from buffering to streaming mode after HistoricalDownloader completion.

        Args:
            last_historical_id: Last aggregate_id from historical data

        Returns:
            True if switch was successful
        """
        if self.config.verbose_trade_logging:
            self.logger.info(f"Switching to streaming mode for {self.ticker_model.symbol}, "
                             f"last_historical_id: {last_historical_id}")

        try:
            # Process buffered trades
            await self._merge_buffer_with_historical(last_historical_id)

            # Switch to streaming mode
            self.mode = WebSocketMode.STREAMING

            self.logger.info(f"Successfully switched to streaming mode for {self.ticker_model.symbol}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to switch to streaming mode for {self.ticker_model.symbol}: {e}")
            return False

    async def shutdown(self) -> None:
        """Graceful shutdown of WebSocket updater."""
        if self.config.verbose_trade_logging:
            self.logger.info(f"Shutting down WebSocketUpdater for {self.ticker_model.symbol}")

        self.should_stop = True

        # Flush remaining data
        if hasattr(self, 'dynamic_manager') and self.dynamic_manager:
            await self.dynamic_manager.stop_monitoring()

        try:
            if self.mode == WebSocketMode.BUFFERING and hasattr(self, 'buffer') and self.buffer:
                if self.config.verbose_trade_logging:
                    self.logger.debug(f"Flushing {len(self.buffer)} buffered trades for {self.ticker_model.symbol}")
                await self.trades_writer.write_trades_batch(self.ticker_model.symbol, list(self.buffer))
                self.buffer.clear()

        except Exception as e:
            self.logger.error(f"Error flushing data during shutdown for {self.ticker_model.symbol}: {e}")

        # Close WebSocket connection
        if self.socket_manager:
            try:
                self.socket_manager = None
                if self.config.verbose_trade_logging:
                    self.logger.debug(f"Socket manager closed for {self.ticker_model.symbol}")
            except Exception as e:
                self.logger.error(f"Error closing socket manager for {self.ticker_model.symbol}: {e}")

        # Cancel WebSocket task
        if self.websocket_task and not self.websocket_task.done():
            self.websocket_task.cancel()
            try:
                await self.websocket_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self.self.logger.error(f"Error cancelling websocket task for {self.ticker_model.symbol}: {e}")

        self.websocket_connected = False
        self.mode = WebSocketMode.IDLE
        if self.config.verbose_trade_logging:
            self.logger.info(f"WebSocketUpdater shutdown complete for {self.ticker_model.symbol}")

    # ===== WEBSOCKET CONNECTION MANAGEMENT =====

    async def connect_websocket(self) -> bool:
        """
        Establish WebSocket connection using BinanceSocketManager.

        Returns:
            True if connected successfully
        """
        try:

            self.logger.info(f"Connecting WebSocket for {self.ticker_model.symbol}")

            # Create BinanceSocketManager using binance client
            self.socket_manager = BinanceSocketManager(self.binance_client)

            # Create aggTrade futures socket for the symbol
            self.websocket = self.socket_manager.aggtrade_futures_socket(
                symbol=self.ticker_model.symbol
            )

            self.websocket_connected = True
            self.reconnect_attempts = 0  # Reset on successful connection

            self.logger.info(f"WebSocket connected for {self.ticker_model.symbol}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to connect WebSocket for {self.ticker_model.symbol}: {e}")
            self.websocket_connected = False
            return False

    async def _websocket_loop(self) -> None:
        """Main WebSocket message processing loop with comprehensive error handling."""
        self.logger.info(f"Starting WebSocket loop for {self.ticker_model.symbol}")

        while not self.should_stop:
            try:
                if not self.websocket_connected:
                    success = await self._attempt_reconnection()
                    if not success:
                        await asyncio.sleep(5)
                        continue

                # Process WebSocket messages with proper structure handling
                async with self.websocket as ws:
                    while not self.should_stop and self.websocket_connected:
                        try:
                            # Receive message from WebSocket
                            message = await ws.recv()
                            await self._process_websocket_message(message)

                        except BinanceClientError as e:
                            self._update_error_statistics(e)
                            self.logger.error(f"Binance client error for {self.ticker_model.symbol}: {e}")

                            # Handle based on error recoverability
                            if e.is_recoverable():
                                self.logger.info(f"Recoverable Binance error, attempting reconnection for {self.ticker_model.symbol}")
                                self.websocket_connected = False

                                # Wait for retry delay if specified
                                retry_delay = e.get_retry_delay()
                                if retry_delay > 0:
                                    self.logger.info(f"Waiting {retry_delay}s before reconnection for {self.ticker_model.symbol}")
                                    await asyncio.sleep(retry_delay)

                                break  # Exit inner loop to reconnect
                            else:
                                self.logger.error(f"Non-recoverable Binance error for {self.ticker_model.symbol}: {e}")
                                self.mode = WebSocketMode.ERROR
                                return

                        except asyncio.CancelledError:
                            self.logger.info(f"WebSocket cancelled for {self.ticker_model.symbol}")
                            break

                        except Exception as e:
                            # Check for benign "closed connection" errors
                            error_str = str(e).lower()

                            # These errors are normal during reconnection/shutdown - ignore them
                            if any(pattern in error_str for pattern in [
                                'read loop has been closed',
                                'read loop closed',
                                'connection closed',
                                'websocket closed'
                            ]):
                                # Just log as debug and trigger reconnection
                                if self.config.verbose_trade_logging:
                                    self.logger.debug(f"WebSocket connection closed for {self.ticker_model.symbol}, will reconnect")
                                self.websocket_connected = False
                                break  # Exit inner loop to reconnect

                            # Real errors - log and update statistics
                            self._update_error_statistics(e)
                            self.logger.error(f"WebSocket message error for {self.ticker_model.symbol}: {e}")
                            continue

            except Exception as e:
                # Same check for outer loop
                error_str = str(e).lower()
                if any(pattern in error_str for pattern in [
                    'read loop has been closed',
                    'read loop closed',
                    'connection closed',
                    'websocket closed'
                ]):
                    if self.config.verbose_trade_logging:
                        self.logger.debug(f"WebSocket outer loop closed for {self.ticker_model.symbol}")
                    self.websocket_connected = False
                else:
                    self.logger.error(f"WebSocket loop error for {self.ticker_model.symbol}: {type(e).__name__}: {e or 'no message'}")
                    await self._handle_websocket_error(e)

        self.logger.info(f"WebSocket loop ended for {self.ticker_model.symbol}")

    async def _process_websocket_message(self, message) -> None:
        """
        Process incoming WebSocket message with gap detection and buffer overflow handling.
        """
        try:
            # Extract actual trade data from Binance WebSocket wrapper
            if isinstance(message, dict) and 'data' in message:
                trade_data = message['data']
            else:
                trade_data = message

            # Check for Binance client errors
            if trade_data.get('e') == 'error':
                raise BinanceWebSocketError(trade_data, symbol=self.ticker_model.symbol)

            # Process only aggTrade events
            if trade_data.get('e') != 'aggTrade':
                self.logger.debug(f"Non-aggTrade message for {self.ticker_model.symbol}: {trade_data.get('e')}")
                return

            # Convert to AggTrade object
            trade = AggTrade.from_binance_dict(trade_data)

            # Check for gap in aggregate_id chain
            gap_start_id = self._check_aggregate_id_continuity(trade)
            if gap_start_id is not None:
                await self._handle_gap_recovery(gap_start_id)
                # Continue processing current trade after gap handling

            # Route trade based on current mode
            if self.mode == WebSocketMode.BUFFERING:
                await self._buffer_trade(trade)
                # Update last_aggregate_id AFTER successful buffer
                self.last_aggregate_id = trade.aggregate_id

            elif self.mode == WebSocketMode.STREAMING:
                # Пытаемся добавить в GlobalTradesUpdater
                success = self.trades_writer.add_trade(self.ticker_model.symbol, trade)

                if success:
                    # Update last_aggregate_id ONLY AFTER successful add
                    self.last_aggregate_id = trade.aggregate_id
                    self._update_statistics(trade)
                else:
                    # Buffer overflow - критическая ситуация
                    self.logger.critical(f"Buffer overflow for {self.ticker_model.symbol} at aggregate_id {trade.aggregate_id}")
                    self.buffer_overflow_count += 1

                    # Инициируем gap recovery из-за buffer overflow
                    await self._handle_buffer_overflow_gap(trade.aggregate_id)

        except BinanceClientError:
            # Re-raise Binance errors to be handled by outer loop
            raise
        except Exception as e:
            self.logger.error(f"Error processing WebSocket message for {self.ticker_model.symbol}: {e}")
            raise

    # ===== GAP DETECTION AND RECOVERY =====

    def _check_aggregate_id_continuity(self, new_trade: AggTrade) -> Optional[int]:
        """
        Check continuity of aggregate_id chain.

        Args:
            new_trade: Newly received trade

        Returns:
            Gap start ID if gap detected, None if continuous
        """
        if self.last_aggregate_id is None:
            return None

        expected_id = self.last_aggregate_id + 1
        actual_id = new_trade.aggregate_id

        if actual_id > expected_id:
            gap_start = expected_id

            self.logger.warning(f"Gap detected for {self.ticker_model.symbol}: "
                                f"expected {expected_id}, got {actual_id} "
                                f"(missing {actual_id - expected_id} trades: {actual_id}-{expected_id})")

            return gap_start

        elif actual_id < expected_id:
            # Дубликат - игнорируем
            return None

        # actual_id == expected_id: нормально
        return None

    async def _handle_gap_recovery(self, gap_start_id: int) -> None:
        """
        Handle gap recovery by switching to buffering and signaling recovery needed.

        Args:
            gap_start_id: Starting aggregate_id for recovery
        """
        self.gap_recoveries += 1

        self.logger.info(f"Initiating gap recovery for {self.ticker_model.symbol} "
                         f"from aggregate_id {gap_start_id}")

        # Switch to buffering mode if not already
        if self.mode != WebSocketMode.BUFFERING:
            old_mode = self.mode
            self.mode = WebSocketMode.BUFFERING

            self.logger.info(f"Switched from {old_mode.name} to BUFFERING mode for gap recovery")

        # Signal gap recovery needed to TickerProcessor
        await self._signal_gap_recovery_needed(gap_start_id)

    async def _handle_buffer_overflow_gap(self, failed_aggregate_id: int) -> None:
        """
        Handle gap caused by buffer overflow - transition to recovery mode.

        Args:
            failed_aggregate_id: aggregate_id of trade that failed to be added
        """
        self.logger.warning(f"Buffer overflow detected for {self.ticker_model.symbol}, "
                            f"initiating gap recovery from aggregate_id {failed_aggregate_id}")

        # Переключаемся в BUFFERING mode
        old_mode = self.mode
        self.mode = WebSocketMode.BUFFERING
        self.gap_recovery_in_progress = True

        # Сигнализируем TickerProcessor о необходимости gap recovery
        await self._signal_gap_recovery_needed(failed_aggregate_id)

        self.gap_recoveries += 1
        self.logger.info(f"Transitioned from {old_mode.name} to BUFFERING mode for buffer overflow recovery")

    async def _signal_gap_recovery_needed(self, gap_start_id: int) -> None:
        """
        Signal TickerProcessor that gap recovery is needed.

        Args:
            gap_start_id: Starting aggregate_id for recovery
        """
        if self.gap_recovery_callback:
            try:
                await self.gap_recovery_callback(gap_start_id)
            except Exception as e:
                self.logger.error(f"Error in gap recovery callback for {self.ticker_model.symbol}: {e}")
        else:
            self.logger.warning(f"No gap recovery callback set for {self.ticker_model.symbol}")

    # ===== BUFFERING MODE OPERATIONS =====

    async def _buffer_trade(self, trade: AggTrade) -> None:
        """
        Add trade to buffer during buffering mode.

        Args:
            trade: AggTrade to buffer
        """
        self.buffer.append(trade)

    async def _merge_buffer_with_historical(self, last_historical_id: int) -> None:
        """
        Merge buffered trades with historical data by removing duplicates.

        CRITICAL: Buffer MUST connect seamlessly with historical data.
        Any gap means data corruption - buffer is discarded.

        Args:
            last_historical_id: Last aggregate_id from historical download

        Raises:
            ValueError: If there is ANY gap between historical and buffer
        """
        from .telegram_notifier import get_notifier

        if not self.buffer:
            self.logger.info(f"No buffered trades to merge for {self.ticker_model.symbol}")
            return

        original_size = len(self.buffer)

        # Filter out trades that are already in historical data
        filtered_trades = [trade for trade in list(self.buffer) if trade.aggregate_id > last_historical_id]

        if not filtered_trades:
            self.logger.info(f"All buffered trades already covered by historical data for {self.ticker_model.symbol}")
            self.buffer.clear()
            return

        # Sort by aggregate_id for consistency
        filtered_trades.sort(key=lambda t: t.aggregate_id)

        # CRITICAL: Buffer must start exactly where historical ended
        min_buffer_id = filtered_trades[0].aggregate_id
        expected_next_id = last_historical_id + 1

        if min_buffer_id != expected_next_id:
            gap_size = min_buffer_id - expected_next_id

            error_msg = (
                f"Gap detected ({gap_size:,} trades). "
                f"Expected buffer to start at {expected_next_id}, got {min_buffer_id}."
            )

            self.logger.error(
                f"CRITICAL: Gap detected for {self.ticker_model.symbol}! "
                f"Historical ended at ID {last_historical_id}, "
                f"buffer starts at ID {min_buffer_id}, "
                f"gap = {gap_size:,} trades. "
                f"Buffer DISCARDED to prevent data corruption!"
            )

            # Send Telegram alert
            notifier = get_notifier()
            if notifier:
                try:
                    await notifier.send_gap_alert(
                        symbol=self.ticker_model.symbol,
                        expected_id=expected_next_id,
                        received_id=min_buffer_id,
                        gap_size=gap_size
                    )
                except Exception as e:
                    self.logger.error(f"Failed to send Telegram alert: {e}")

            self.buffer.clear()
            raise ValueError(error_msg)

        # No gap - proceed with merge
        await self.trades_writer.write_trades_batch(self.ticker_model.symbol, filtered_trades)

        duplicates_removed = original_size - len(filtered_trades)

        self.logger.info(f"Buffer merge completed for {self.ticker_model.symbol}: "
                         f"{original_size} buffered, {duplicates_removed} duplicates, "
                         f"{len(filtered_trades)} saved")

        self.buffer.clear()

    # ===== DATA MANAGEMENT =====

    def _validate_trades(self, trades: List[AggTrade]) -> List[AggTrade]:
        """
        Validate trade data and filter out invalid trades.

        Args:
            trades: List of trades to validate

        Returns:
            List of valid trades
        """
        valid_trades = []
        invalid_count = 0

        for trade in trades:
            if trade.validate():
                valid_trades.append(trade)
            else:
                invalid_count += 1
                if not self.config.skip_invalid_trades:
                    raise ValueError(f"Invalid trade data: {trade}")

        # Check invalid trades threshold
        if trades and invalid_count > 0:
            invalid_percent = (invalid_count / len(trades)) * 100
            if invalid_percent > self.config.max_invalid_trades_percent:
                raise ValueError(f"Too many invalid trades: {invalid_percent:.1f}% > {self.config.max_invalid_trades_percent}%")

            if invalid_count > 0:
                self.logger.warning(f"Filtered {invalid_count} invalid trades for {self.ticker_model.symbol} "
                                    f"({invalid_percent:.1f}%)")

        return valid_trades

    # ===== ERROR HANDLING AND RECONNECTION =====

    def _update_error_statistics(self, error: Exception) -> None:
        """Update error statistics based on error type."""
        if isinstance(error, BinanceClientError):
            self.binance_errors_count += 1
            if error.is_recoverable():
                self.recoverable_errors_count += 1
        else:
            self.message_errors_count += 1

    async def _handle_websocket_error(self, error: Exception) -> None:
        """
        Handle WebSocket errors with appropriate recovery strategy.

        Args:
            error: Exception that occurred
        """
        self.websocket_connected = False
        self._update_error_statistics(error)

        if isinstance(error, BinanceClientError):
            self.logger.error(f"Binance WebSocket error for {self.ticker_model.symbol}: {type(error).__name__}: {error or 'no message'}")

            # Log error context for debugging
            error_context = error.get_error_context()
            self.logger.error(f"Error context for {self.ticker_model.symbol}: {error_context}")

            if not error.is_recoverable():
                self.logger.error(f"Non-recoverable Binance error for {self.ticker_model.symbol}, stopping")
                self.mode = WebSocketMode.ERROR
                return
        else:
            self.logger.error(f"WebSocket error for {self.ticker_model.symbol}: {type(error).__name__}: {error or 'no message'}")

        # Don't attempt reconnection if shutting down
        if self.should_stop:
            return

        await self._attempt_reconnection()

    async def _attempt_reconnection(self) -> bool:
        """
        Attempt to reconnect WebSocket with exponential backoff.

        Returns:
            True if reconnection was successful
        """
        if self.reconnect_attempts >= self.max_reconnect_attempts:
            self.logger.error(f"Max reconnection attempts ({self.max_reconnect_attempts}) "
                              f"exceeded for {self.ticker_model.symbol}")
            self.mode = WebSocketMode.ERROR
            return False

        self.mode = WebSocketMode.RECONNECTING
        self.reconnect_attempts += 1

        # Exponential backoff with jitter
        base_delay = min(60, 5 * (2 ** (self.reconnect_attempts - 1)))
        jitter = random.uniform(0, 5)  # 0-15 seconds random
        delay = base_delay + jitter

        self.logger.info(f"Attempting WebSocket reconnection for {self.ticker_model.symbol} "
                         f"in {delay:.1f}s (attempt {self.reconnect_attempts})")

        await asyncio.sleep(delay)

        success = await self.connect_websocket()
        if success:
            # Reset reconnection counter and return to appropriate mode
            self.reconnect_attempts = 0
            self.mode = WebSocketMode.STREAMING if not self.buffer else WebSocketMode.BUFFERING
            self.logger.info(f"WebSocket reconnection successful for {self.ticker_model.symbol}")

            # Track reconnects per hour
            current_time = time.time()
            if current_time - self._hour_start_time > 3600:  # Reset hourly counter
                self._reconnects_this_hour = 0
                self._hour_start_time = current_time

            self._reconnects_this_hour += 1

            # Alert if too many reconnects
            if self._reconnects_this_hour > self._reconnect_alert_threshold:
                if current_time - self._last_reconnect_alert_time > 3600:  # Max 1 alert per hour
                    notifier = get_notifier()
                    if notifier:
                        try:
                            await notifier.send_alert(
                                message=f"Frequent WebSocket reconnects: {self._reconnects_this_hour}/hour",
                                level=AlertLevel.WARNING,
                                symbol=self.ticker_model.symbol
                            )
                            self._last_reconnect_alert_time = current_time
                        except Exception as e:
                            self.logger.error(f"Failed to send reconnect alert: {e}")

        return success

    # ===== STATISTICS AND MONITORING =====

    def _update_statistics(self, trade: AggTrade) -> None:
        """
        Update internal statistics with new trade.

        Args:
            trade: Processed trade
        """
        self.trades_processed += 1
        self.last_trade_time = trade.timestamp

    def get_stats(self) -> WebSocketStats:
        """
        Get current WebSocket statistics.

        Returns:
            WebSocketStats with current metrics
        """
        uptime = time.time() - (self.start_time or time.time()) if self.start_time else 0

        # Calculate trades per minute
        trades_per_minute = 0.0
        if uptime > 0:
            trades_per_minute = (self.trades_processed / uptime) * 60

        buffer_size = len(getattr(self, 'buffer', [])) + len(getattr(self, 'pending_trades', []))

        return WebSocketStats(
            mode=self.mode,
            connected=self.websocket_connected,
            trades_processed=self.trades_processed,
            trades_per_minute=trades_per_minute,
            buffer_size=buffer_size,
            last_trade_time=self.last_trade_time,
            last_aggregate_id=self.last_aggregate_id,
            reconnect_count=self.reconnect_attempts,
            uptime_seconds=uptime,
            gap_recoveries=self.gap_recoveries,
            bytes_written=self.bytes_written,
            binance_errors_count=self.binance_errors_count,
            recoverable_errors_count=self.recoverable_errors_count,
            message_errors_count=self.message_errors_count,
            buffer_overflow_count=self.buffer_overflow_count,
            gap_recovery_in_progress=self.gap_recovery_in_progress
        )

    def is_healthy(self) -> bool:
        """
        Check if WebSocket is operating normally.

        Returns:
            True if WebSocket is healthy
        """
        # Check connection status
        if not self.websocket_connected:
            return False

        # Check if we're not in error state
        if self.mode == WebSocketMode.ERROR:
            return False

        # In STREAMING mode with active connection - consider healthy
        # Low-volume tickers may not have trades for extended periods
        if self.mode == WebSocketMode.STREAMING:
            return True

        return True

    # ===== COORDINATION INTERFACE =====

    def set_gap_recovery_callback(self, callback: Callable[[int], None]) -> None:
        """
        Set callback function for gap recovery notifications.

        Args:
            callback: Async function to call when gap recovery is needed
        """
        self.gap_recovery_callback = callback

    def get_current_mode(self) -> WebSocketMode:
        """Get current WebSocket mode."""
        return self.mode

    def get_buffer_size(self) -> int:
        """Get current buffer size."""
        return len(getattr(self, 'buffer', []))

    def get_last_aggregate_id(self) -> Optional[int]:
        """Get last processed aggregate_id."""
        return self.last_aggregate_id

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"WebSocketUpdater({self.ticker_model.symbol}, {self.mode.name})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (f"WebSocketUpdater(symbol='{self.ticker_model.symbol}', "
                f"mode={self.mode.name}, connected={self.websocket_connected}, "
                f"trades_processed={self.trades_processed})")
