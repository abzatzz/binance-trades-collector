"""
GlobalTradesUpdater - Optimized for High-Throughput WebSocket Processing
=========================================================================

Centralized trades processor optimized for 600+ concurrent WebSocket streams.

Key optimizations:
- Per-ticker lock (no global contention)
- Atomic buffer swap instead of copy+clear
- Restore on failure (no data loss)
- Minimal lock duration

File: src/core/global_trades_updater.py
"""

import asyncio
import time
import threading
from collections import deque
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from loggerino import loggerino
from ..models.aggtrade import AggTrade
from ..models.config import ProcessingConfig
from .telegram_notifier import get_notifier, AlertLevel

logger = loggerino.get('global_trades_updater')


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


class TickerBuffer:
    """
    Thread-safe buffer for a single ticker.

    Uses per-ticker lock (not global!) for thread safety.
    Different tickers don't block each other.

    Lock duration is minimal:
    - append(): ~1 microsecond
    - swap_and_get(): ~1 microsecond
    - restore_trades(): ~10 microseconds
    """

    __slots__ = ('_buffer', '_max_size', '_overflow_count', '_lock')

    def __init__(self, max_size: int = 10000):
        self._buffer: deque = deque()
        self._max_size = max_size
        self._overflow_count = 0
        self._lock = threading.Lock()

    def append(self, trade: AggTrade) -> bool:
        """
        Add trade to buffer. Thread-safe with minimal lock.

        Args:
            trade: AggTrade to add

        Returns:
            True if added, False if buffer full (overflow)
        """
        with self._lock:
            if len(self._buffer) >= self._max_size:
                self._overflow_count += 1
                return False
            self._buffer.append(trade)
            return True

    def swap_and_get(self) -> List[AggTrade]:
        """
        Atomically swap buffer with empty one and return old contents.

        This is O(1) under lock - just swaps references.
        Conversion to list happens OUTSIDE lock.

        Returns:
            List of trades (may be empty)
        """
        with self._lock:
            old_buffer = self._buffer
            self._buffer = deque()

        # Convert outside lock
        return list(old_buffer)

    def restore_trades(self, trades: List[AggTrade]) -> None:
        """
        Restore trades to the FRONT of buffer after failed INSERT.

        This preserves order: restored (old) trades come before
        any new trades that arrived during INSERT attempt.

        Args:
            trades: Trades to restore (from failed INSERT)
        """
        if not trades:
            return

        with self._lock:
            # Create new deque: old trades + current trades
            new_buffer = deque(trades)
            new_buffer.extend(self._buffer)
            self._buffer = new_buffer

    def __len__(self) -> int:
        return len(self._buffer)

    @property
    def overflow_count(self) -> int:
        return self._overflow_count

    def reset_overflow_count(self) -> None:
        self._overflow_count = 0


class GlobalTradesUpdater:
    """
    Centralized trades processor for all WebSocket updates.

    Architecture:
    - Per-ticker TickerBuffer with independent locks
    - Single background task processes all buffers every second
    - One batch INSERT for all symbols
    - Swap + Restore pattern ensures no data loss

    Flow:
    1. WebSocket calls add_trade() - per-ticker lock, ~1μs
    2. Every second, _process_all_buffers():
       a. swap_and_get() each buffer - per-ticker lock, ~1μs each
       b. Convert to ClickHouse format - no locks
       c. Batch INSERT to ClickHouse - no locks
       d. On failure: restore_trades() - per-ticker lock, ~10μs each

    Data safety:
    - SUCCESS: data in ClickHouse, buffers empty
    - FAILURE: data restored to buffers, retry next cycle
    """

    def __init__(self, clickhouse_manager, config: ProcessingConfig):
        """
        Initialize GlobalTradesUpdater.

        Args:
            clickhouse_manager: ClickHouseManager instance for batch writes
            config: ProcessingConfig with batch settings
        """
        self.clickhouse_manager = clickhouse_manager
        self.config = config

        # Per-ticker buffers
        self.ticker_buffers: Dict[str, TickerBuffer] = {}
        self._buffer_creation_lock = threading.Lock()

        # Processing settings
        self.batch_interval_seconds = getattr(config, 'global_batch_interval_seconds', 1)
        self.max_buffer_size = getattr(config, 'max_buffer_size_per_ticker', 10000)

        # Background processing
        self.processing_task: Optional[asyncio.Task] = None
        self.is_running = False
        self.start_time: Optional[float] = None

        # Statistics
        self.total_trades_processed = 0
        self.batch_count = 0
        self.last_batch_size = 0
        self.last_batch_time: Optional[float] = None
        self.queue_overflow_count = 0
        self.processing_errors_count = 0
        self.restore_count = 0  # How many times we had to restore

        self.consecutive_errors = 0
        self._shutdown_event = asyncio.Event()

        # Performance monitoring
        self.batch_processing_times: deque = deque(maxlen=100)
        self.peak_buffer_sizes: Dict[str, int] = {}

        # Anti-spam for Telegram alerts
        self._overflow_alert_sent: bool = False
        self._last_overflow_alert_time: float = 0
        self._overflow_alert_cooldown: float = 600  # 10 minutes

        logger.info("GlobalTradesUpdater initialized (optimized: per-ticker locks, swap+restore)")

    async def start(self) -> bool:
        """
        Start the global trades processing.

        Returns:
            True if started successfully
        """
        if self.is_running:
            logger.warning("GlobalTradesUpdater already running")
            return True

        try:
            self.is_running = True
            self.start_time = time.time()

            # Start background processing task
            self.processing_task = asyncio.create_task(
                self._processing_loop(),
                name="global_trades_processor"
            )

            logger.info(f"GlobalTradesUpdater started with {self.batch_interval_seconds}s intervals")
            return True

        except Exception as e:
            logger.error(f"Failed to start GlobalTradesUpdater: {e}")
            self.is_running = False
            return False

    async def stop(self) -> None:
        """Stop the global trades processing with graceful shutdown."""
        if not self.is_running:
            return

        logger.info("Stopping GlobalTradesUpdater...")
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

        logger.info("GlobalTradesUpdater stopped")

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add trade to buffer. Thread-safe with minimal lock time.

        This method is called from 600+ WebSocket handlers concurrently.
        Per-ticker lock ensures no contention between different tickers.

        Args:
            symbol: Trading symbol
            trade: AggTrade to add

        Returns:
            True if added, False if buffer full (overflow)
        """
        # Get or create buffer for symbol
        buffer = self.ticker_buffers.get(symbol)

        if buffer is None:
            # Only lock for buffer creation (rare operation)
            with self._buffer_creation_lock:
                # Double-check after acquiring lock
                buffer = self.ticker_buffers.get(symbol)
                if buffer is None:
                    buffer = TickerBuffer(max_size=self.max_buffer_size)
                    self.ticker_buffers[symbol] = buffer

        # Per-ticker lock inside append()
        success = buffer.append(trade)

        if not success:
            self.queue_overflow_count += 1
            logger.warning(
                f"Buffer overflow for {symbol}: {len(buffer)}/{self.max_buffer_size} "
                f"at aggregate_id {trade.aggregate_id}"
            )

        return success

    async def _processing_loop(self) -> None:
        """
        Main processing loop - runs every second to batch process all trades.
        """
        logger.info("Global trades processing loop started")

        while self.is_running:
            try:
                # Wait for batch interval
                await asyncio.sleep(self.batch_interval_seconds)

                # Process all ticker buffers
                await self._process_all_buffers()

            except asyncio.CancelledError:
                logger.info("Global trades processing loop cancelled")
                break

            except Exception as e:
                self.processing_errors_count += 1
                self.consecutive_errors += 1
                logger.error(f"Processing loop error: {e}", exc_info=True)

                # Exponential backoff on consecutive errors
                backoff_delay = min(0.1 * (2 ** self.consecutive_errors), 5.0)
                await asyncio.sleep(backoff_delay)

        logger.info("Global trades processing loop ended")

    async def _process_all_buffers(self) -> None:
        """
        Collect trades from all ticker buffers and perform single batch insert.

        Flow:
        1. Swap each buffer (atomic, per-ticker lock)
        2. Convert to ClickHouse format (no locks)
        3. Batch INSERT to ClickHouse
        4. On failure: restore trades to buffers

        This ensures NO DATA LOSS even if ClickHouse fails.
        """
        batch_start_time = time.time()

        # Phase 1: Atomic swap for each buffer
        # Each swap has its own lock, different tickers don't block each other
        swapped_data: Dict[str, List[AggTrade]] = {}

        for symbol, buffer in list(self.ticker_buffers.items()):
            trades = buffer.swap_and_get()
            if trades:
                swapped_data[symbol] = trades

                # Track peak buffer sizes
                if len(trades) > self.peak_buffer_sizes.get(symbol, 0):
                    self.peak_buffer_sizes[symbol] = len(trades)

        if not swapped_data:
            # No trades to process
            if self.consecutive_errors > 0:
                self.consecutive_errors = 0
            return

        # Phase 2: Convert to ClickHouse format (NO LOCKS)
        clickhouse_data = []
        total_trades = 0

        for symbol, trades in swapped_data.items():
            for trade in trades:
                clickhouse_data.append([
                    symbol,
                    trade.aggregate_id,
                    trade.price,
                    trade.quantity,
                    trade.first_trade_id,
                    trade.last_trade_id,
                    trade.timestamp,
                    1 if trade.is_buyer_maker else 0
                ])
            total_trades += len(trades)

        # Phase 3: Batch INSERT to ClickHouse
        try:
            await self._batch_insert_all(clickhouse_data)

            # SUCCESS - update statistics
            self.total_trades_processed += total_trades
            self.batch_count += 1
            self.last_batch_size = total_trades
            self.last_batch_time = time.time()
            self.consecutive_errors = 0
            self._overflow_alert_sent = False

            batch_duration = time.time() - batch_start_time
            self.batch_processing_times.append(batch_duration)

            if self.config.verbose_trade_logging:
                logger.debug(
                    f"Batch processed: {total_trades} trades from "
                    f"{len(swapped_data)} tickers in {batch_duration:.3f}s"
                )

        except Exception as e:
            # FAILURE - restore trades to buffers
            self.processing_errors_count += 1
            self.consecutive_errors += 1
            self.restore_count += 1

            logger.error(
                f"ClickHouse INSERT failed for {total_trades} trades from "
                f"{len(swapped_data)} tickers: {e}. Restoring to buffers..."
            )

            # Restore trades to their buffers
            # Use 'is not None' because empty buffer has __len__=0 which is falsy
            restored_count = 0
            for symbol, trades in swapped_data.items():
                buffer = self.ticker_buffers.get(symbol)
                if buffer is not None:
                    buffer.restore_trades(trades)
                    restored_count += len(trades)

            logger.info(f"Restored {restored_count} trades to buffers, will retry next cycle")

            # Check if we need to send alert
            await self._check_and_send_alert(total_trades)

    async def _batch_insert_all(self, all_trades_data: List[List[Any]]) -> None:
        """
        Perform single batch insert for all trades.

        Uses ClickHouseManager's batch insert with retry mechanism.
        """
        if not all_trades_data:
            return

        await self.clickhouse_manager.batch_insert_all_symbols(all_trades_data)

    async def _final_flush(self) -> None:
        """Flush all remaining trades on shutdown."""
        logger.info("Performing final flush...")

        # Count remaining trades
        remaining = sum(len(buf) for buf in self.ticker_buffers.values())
        if remaining > 0:
            logger.info(f"Final flush: {remaining} trades remaining")

        try:
            await self._process_all_buffers()
            logger.info("Final flush completed")
        except Exception as e:
            logger.error(f"Final flush failed: {e}")
            # On shutdown failure, log what's lost
            lost = sum(len(buf) for buf in self.ticker_buffers.values())
            if lost > 0:
                logger.error(f"Lost {lost} trades on shutdown!")

    async def _check_and_send_alert(self, failed_trades_count: int) -> None:
        """Send Telegram alert for processing failures (with anti-spam)."""
        current_time = time.time()

        if current_time - self._last_overflow_alert_time > self._overflow_alert_cooldown:
            notifier = get_notifier()
            if notifier:
                try:
                    # Calculate total buffered trades
                    total_buffered = sum(len(buf) for buf in self.ticker_buffers.values())

                    await notifier.send_alert(
                        message=(
                            f"⚠️ GlobalTradesUpdater: ClickHouse INSERT failed!\n"
                            f"Failed batch: {failed_trades_count:,} trades\n"
                            f"Total buffered: {total_buffered:,} trades\n"
                            f"Consecutive errors: {self.consecutive_errors}\n"
                            f"Trades restored to buffers, will retry."
                        ),
                        level=AlertLevel.WARNING
                    )
                    self._last_overflow_alert_time = current_time
                except Exception as e:
                    logger.error(f"Failed to send Telegram alert: {e}")

    def get_statistics(self) -> GlobalTradesStats:
        """Get current statistics."""
        uptime = time.time() - self.start_time if self.start_time else 0

        return GlobalTradesStats(
            total_trades_processed=self.total_trades_processed,
            batch_count=self.batch_count,
            last_batch_size=self.last_batch_size,
            last_batch_time=self.last_batch_time,
            active_tickers_count=len(self.ticker_buffers),
            queue_overflow_count=self.queue_overflow_count,
            processing_errors_count=self.processing_errors_count,
            uptime_seconds=uptime
        )

    def get_detailed_stats(self) -> Dict[str, Any]:
        """Get detailed statistics including performance metrics."""
        stats = self.get_statistics()

        # Calculate average batch processing time
        avg_batch_time = 0.0
        if self.batch_processing_times:
            avg_batch_time = sum(self.batch_processing_times) / len(self.batch_processing_times)

        # Current buffer sizes
        buffer_sizes = {
            symbol: len(buffer)
            for symbol, buffer in self.ticker_buffers.items()
            if len(buffer) > 0
        }

        # Per-ticker overflow counts
        ticker_overflows = {
            symbol: buffer.overflow_count
            for symbol, buffer in self.ticker_buffers.items()
            if buffer.overflow_count > 0
        }

        # Total currently buffered
        total_buffered = sum(len(buf) for buf in self.ticker_buffers.values())

        return {
            **stats.__dict__,
            'avg_batch_processing_time': avg_batch_time,
            'peak_buffer_sizes': dict(self.peak_buffer_sizes),
            'current_buffer_sizes': buffer_sizes,
            'total_currently_buffered': total_buffered,
            'ticker_overflow_counts': ticker_overflows,
            'consecutive_errors': self.consecutive_errors,
            'restore_count': self.restore_count,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Alias for get_detailed_stats() for API compatibility."""
        return self.get_detailed_stats()

    def is_healthy(self) -> bool:
        """
        Check if GlobalTradesUpdater is healthy.

        Returns:
            True if healthy (running, no critical errors)
        """
        # Unhealthy if too many consecutive errors
        if self.consecutive_errors >= 5:
            return False

        # Unhealthy if not running
        if not self.is_running:
            return False

        # Unhealthy if no batches processed in last 30 seconds (when there are tickers)
        if self.ticker_buffers and self.last_batch_time:
            time_since_last_batch = time.time() - self.last_batch_time
            if time_since_last_batch > 30:
                return False

        return True

    def reset_statistics(self) -> None:
        """Reset all statistics."""
        self.total_trades_processed = 0
        self.batch_count = 0
        self.last_batch_size = 0
        self.last_batch_time = None
        self.queue_overflow_count = 0
        self.processing_errors_count = 0
        self.consecutive_errors = 0
        self.restore_count = 0
        self.batch_processing_times.clear()
        self.peak_buffer_sizes.clear()

        # Reset per-ticker overflow counts
        for buffer in self.ticker_buffers.values():
            buffer.reset_overflow_count()

        logger.info("Statistics reset")