"""
GapChecker - Automatic Gap Detection and Recovery
================================================

Periodically checks for gaps in aggregate_id sequence and triggers automatic recovery.
Uses lightweight SQL queries and dynamic check intervals based on active ticker count.

File: src/core/gap_checker.py
"""

import asyncio
import time
from typing import Optional, List, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass

from loggerino import loggerino

if TYPE_CHECKING:
    from ..storage.clickhouse_manager import ClickHouseManager
    from .shared import Shared

logger = loggerino.get('gap_checker')


@dataclass
class GapInfo:
    """Information about detected gap"""
    symbol: str
    gap_start_id: int
    gap_end_id: int
    gap_size: int
    detected_at: int
    recovery_initiated: bool
    recovery_completed: bool
    recovery_error: Optional[str] = None


@dataclass
class GapCheckerStats:
    """Statistics for gap checker"""
    total_checks_performed: int
    total_gaps_detected: int
    total_gaps_recovered: int
    failed_recoveries: int
    last_check_time: Optional[int]
    current_check_interval: float
    active_tickers_count: int
    uptime_seconds: float


class GapChecker:
    """
    Automatic gap detection and recovery system.

    Features:
    - Dynamic check interval: 30 minutes / number of tickers (min 3 seconds)
    - Lightweight SQL queries checking only last 30 minutes
    - Automatic recovery through HistoricalDownloader
    - Comprehensive statistics and monitoring
    """

    def __init__(
            self,
            clickhouse_manager: 'ClickHouseManager',
            shared: 'Shared',
            base_interval_minutes: int = 30,
            min_interval_seconds: int = 3,
            check_window_minutes: int = 30
    ):
        """
        Initialize GapChecker.

        Args:
            clickhouse_manager: ClickHouseManager instance for data access
            shared: Shared instance for ticker access
            base_interval_minutes: Base interval for full cycle (default: 30 minutes)
            min_interval_seconds: Minimum check interval per ticker (default: 3 seconds)
            check_window_minutes: Time window to check for gaps (default: 30 minutes)
        """
        self.clickhouse_manager = clickhouse_manager
        self.shared = shared
        self.base_interval_minutes = base_interval_minutes
        self.min_interval_seconds = min_interval_seconds
        self.check_window_minutes = check_window_minutes

        # State management
        self._is_running: bool = False
        self._should_stop: bool = False
        self._start_time: Optional[float] = None
        self._check_task: Optional[asyncio.Task] = None

        # Statistics
        self._total_checks_performed: int = 0
        self._total_gaps_detected: int = 0
        self._total_gaps_recovered: int = 0
        self._failed_recoveries: int = 0
        self._last_check_time: Optional[int] = None

        # Gap tracking
        self._detected_gaps: Dict[str, List[GapInfo]] = {}  # symbol -> list of gaps
        self._recovery_in_progress: Dict[str, bool] = {}  # symbol -> recovery status

        logger.info("GapChecker initialized with base_interval={base_interval_minutes}m, "
                   f"min_interval={min_interval_seconds}s, check_window={check_window_minutes}m")

    # ===== LIFECYCLE METHODS =====

    async def start(self) -> None:
        """Start gap checker background task."""
        if self._is_running:
            logger.warning("GapChecker already running")
            return

        logger.info("Starting GapChecker...")
        self._is_running = True
        self._should_stop = False
        self._start_time = time.time()

        # Start background check task
        self._check_task = asyncio.create_task(
            self._check_loop(),
            name="gap_checker_loop"
        )

        logger.info("GapChecker started")

    async def stop(self) -> None:
        """Stop gap checker gracefully."""
        if not self._is_running:
            return

        logger.info("Stopping GapChecker...")
        self._should_stop = True

        # Cancel check task
        if self._check_task and not self._check_task.done():
            self._check_task.cancel()
            try:
                await self._check_task
            except asyncio.CancelledError:
                pass

        self._is_running = False
        logger.info("GapChecker stopped")

    # ===== MAIN CHECK LOOP =====

    async def _check_loop(self) -> None:
        """Main loop for periodic gap checking."""
        logger.info("GapChecker loop started")

        while not self._should_stop:
            try:
                # Get active tickers
                active_symbols = self.shared.get_active_ticker_symbols()

                if not active_symbols:
                    logger.debug("No active tickers, waiting...")
                    await asyncio.sleep(60)  # Wait 1 minute if no tickers
                    continue

                # Calculate dynamic interval
                check_interval = self._calculate_check_interval(len(active_symbols))

                logger.debug(f"Starting gap check cycle for {len(active_symbols)} tickers, "
                           f"interval: {check_interval:.1f}s per ticker")

                # Check each ticker
                for symbol in active_symbols:
                    if self._should_stop:
                        break

                    try:
                        await self._check_ticker_for_gaps(symbol)
                        self._total_checks_performed += 1
                        self._last_check_time = int(time.time() * 1000)

                    except Exception as e:
                        logger.error(f"Gap check failed for {symbol}: {e}")

                    # Wait before checking next ticker
                    await asyncio.sleep(check_interval)

                # After full cycle, log statistics
                stats = self.get_stats()
                logger.info(f"Gap check cycle completed: {stats.total_checks_performed} checks, "
                          f"{stats.total_gaps_detected} gaps detected, "
                          f"{stats.total_gaps_recovered} recovered")

            except asyncio.CancelledError:
                logger.info("GapChecker loop cancelled")
                break
            except Exception as e:
                logger.error(f"GapChecker loop error: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait before retry on error

        logger.info("GapChecker loop ended")

    def _calculate_check_interval(self, ticker_count: int) -> float:
        """
        Calculate dynamic check interval based on active ticker count.

        Formula: (base_interval_minutes * 60) / ticker_count
        Minimum: min_interval_seconds

        Args:
            ticker_count: Number of active tickers

        Returns:
            Check interval in seconds
        """
        if ticker_count == 0:
            return self.min_interval_seconds

        # Calculate interval to complete full cycle in base_interval_minutes
        interval = (self.base_interval_minutes * 60) / ticker_count

        # Enforce minimum interval
        interval = max(interval, self.min_interval_seconds)

        return interval

    # ===== GAP DETECTION =====

    async def _check_ticker_for_gaps(self, symbol: str) -> None:
        """
        Check specific ticker for gaps in aggregate_id sequence.

        Uses lightweight query checking only last check_window_minutes of data.

        Args:
            symbol: Ticker symbol to check
        """
        # Skip if recovery already in progress
        if self._recovery_in_progress.get(symbol, False):
            logger.debug(f"Recovery already in progress for {symbol}, skipping check")
            return

        try:
            # Calculate time window (last check_window_minutes)
            current_time_ms = int(time.time() * 1000)
            window_start_ms = current_time_ms - (self.check_window_minutes * 60 * 1000)

            # Lightweight SQL query to detect gaps
            # Only checks recent data and uses indexed columns
            query = f"""
            WITH recent_trades AS (
                SELECT 
                    aggregate_id,
                    aggregate_id - lag(aggregate_id) OVER (ORDER BY aggregate_id) - 1 as gap_size
                FROM trades_local
                WHERE symbol = '{symbol}'
                  AND timestamp >= fromUnixTimestamp64Milli({window_start_ms})
                ORDER BY aggregate_id
            )
            SELECT 
                lag(aggregate_id) OVER (ORDER BY aggregate_id) + 1 as gap_start_id,
                aggregate_id - 1 as gap_end_id,
                gap_size
            FROM recent_trades
            WHERE gap_size > 0
            ORDER BY gap_start_id
            LIMIT 10
            """

            rows = await self.clickhouse_manager._fetch_with_retry(query)

            if rows and len(rows) > 0:
                # Gaps detected!
                for row in rows:
                    gap_start_id = int(row[0])
                    gap_end_id = int(row[1])
                    gap_size = int(row[2])

                    logger.warning(f"Gap detected in {symbol}: "
                                 f"missing IDs {gap_start_id}-{gap_end_id} ({gap_size} trades)")

                    # Create gap info
                    gap_info = GapInfo(
                        symbol=symbol,
                        gap_start_id=gap_start_id,
                        gap_end_id=gap_end_id,
                        gap_size=gap_size,
                        detected_at=current_time_ms,
                        recovery_initiated=False,
                        recovery_completed=False
                    )

                    # Store gap info
                    if symbol not in self._detected_gaps:
                        self._detected_gaps[symbol] = []
                    self._detected_gaps[symbol].append(gap_info)

                    self._total_gaps_detected += 1

                    # Initiate automatic recovery
                    await self._initiate_gap_recovery(gap_info)

            else:
                logger.debug(f"No gaps found in {symbol}")

        except Exception as e:
            logger.error(f"Gap detection failed for {symbol}: {e}", exc_info=True)

    # ===== GAP RECOVERY =====

    async def _initiate_gap_recovery(self, gap_info: GapInfo) -> None:
        """
        Initiate automatic gap recovery through TickerProcessor.

        Args:
            gap_info: Information about detected gap
        """
        symbol = gap_info.symbol

        try:
            # Mark recovery as in progress
            self._recovery_in_progress[symbol] = True
            gap_info.recovery_initiated = True

            logger.info(f"Initiating gap recovery for {symbol} from ID {gap_info.gap_start_id}")

            # Get TickerProcessor for this symbol
            processor = self.shared.get_ticker_processor(symbol)

            if not processor:
                raise Exception(f"TickerProcessor not found for {symbol}")

            # Trigger gap recovery through processor's callback
            # This will use HistoricalDownloader.recover_from_gap()
            if hasattr(processor, '_handle_gap_recovery'):
                await processor._handle_gap_recovery(gap_info.gap_start_id)

                # Mark as recovered (optimistic)
                gap_info.recovery_completed = True
                self._total_gaps_recovered += 1

                logger.info(f"Gap recovery completed for {symbol}: "
                          f"{gap_info.gap_size} trades recovered")
            else:
                raise Exception(f"TickerProcessor for {symbol} doesn't support gap recovery")

        except Exception as e:
            logger.error(f"Gap recovery failed for {symbol}: {e}", exc_info=True)
            gap_info.recovery_error = str(e)
            self._failed_recoveries += 1

        finally:
            # Clear recovery flag after delay
            await asyncio.sleep(5)  # Give some time before next check
            self._recovery_in_progress[symbol] = False

    # ===== MANUAL OPERATIONS =====

    async def check_symbol_now(self, symbol: str) -> List[GapInfo]:
        """
        Manually trigger gap check for specific symbol.

        Args:
            symbol: Ticker symbol to check

        Returns:
            List of detected gaps
        """
        logger.info(f"Manual gap check triggered for {symbol}")

        await self._check_ticker_for_gaps(symbol)

        return self._detected_gaps.get(symbol, [])

    async def check_all_symbols_now(self) -> Dict[str, List[GapInfo]]:
        """
        Manually trigger gap check for all active symbols.

        Returns:
            Dictionary of symbol -> list of gaps
        """
        logger.info("Manual gap check triggered for all symbols")

        active_symbols = self.shared.get_active_ticker_symbols()

        for symbol in active_symbols:
            await self._check_ticker_for_gaps(symbol)
            await asyncio.sleep(0.5)  # Small delay between checks

        return self._detected_gaps.copy()

    def get_detected_gaps(self, symbol: Optional[str] = None) -> Dict[str, List[GapInfo]]:
        """
        Get detected gaps history.

        Args:
            symbol: Optional symbol filter

        Returns:
            Dictionary of gaps by symbol
        """
        if symbol:
            return {symbol: self._detected_gaps.get(symbol, [])}
        return self._detected_gaps.copy()

    def clear_gap_history(self, symbol: Optional[str] = None) -> None:
        """
        Clear gap history.

        Args:
            symbol: Optional symbol to clear, or None for all
        """
        if symbol:
            if symbol in self._detected_gaps:
                del self._detected_gaps[symbol]
                logger.info(f"Cleared gap history for {symbol}")
        else:
            self._detected_gaps.clear()
            logger.info("Cleared all gap history")

    # ===== STATISTICS AND MONITORING =====

    def get_stats(self) -> GapCheckerStats:
        """
        Get current gap checker statistics.

        Returns:
            GapCheckerStats with current metrics
        """
        uptime = time.time() - (self._start_time or time.time()) if self._start_time else 0
        active_count = len(self.shared.get_active_ticker_symbols())
        current_interval = self._calculate_check_interval(active_count)

        return GapCheckerStats(
            total_checks_performed=self._total_checks_performed,
            total_gaps_detected=self._total_gaps_detected,
            total_gaps_recovered=self._total_gaps_recovered,
            failed_recoveries=self._failed_recoveries,
            last_check_time=self._last_check_time,
            current_check_interval=current_interval,
            active_tickers_count=active_count,
            uptime_seconds=uptime
        )

    def get_detailed_stats(self) -> Dict[str, Any]:
        """
        Get detailed statistics including per-symbol gap counts.

        Returns:
            Dictionary with detailed statistics
        """
        stats = self.get_stats()

        # Calculate per-symbol gap counts
        gaps_by_symbol = {}
        for symbol, gaps in self._detected_gaps.items():
            gaps_by_symbol[symbol] = {
                'total_gaps': len(gaps),
                'recovered': sum(1 for g in gaps if g.recovery_completed),
                'failed': sum(1 for g in gaps if g.recovery_error is not None),
                'in_progress': symbol in self._recovery_in_progress and self._recovery_in_progress[symbol]
            }

        return {
            'overall': {
                'total_checks': stats.total_checks_performed,
                'total_gaps_detected': stats.total_gaps_detected,
                'total_gaps_recovered': stats.total_gaps_recovered,
                'failed_recoveries': stats.failed_recoveries,
                'success_rate': (stats.total_gaps_recovered / stats.total_gaps_detected * 100)
                               if stats.total_gaps_detected > 0 else 100.0,
                'last_check_time': stats.last_check_time,
                'uptime_seconds': stats.uptime_seconds
            },
            'current_state': {
                'is_running': self._is_running,
                'active_tickers': stats.active_tickers_count,
                'check_interval_seconds': stats.current_check_interval,
                'recovery_in_progress_count': sum(1 for v in self._recovery_in_progress.values() if v)
            },
            'per_symbol': gaps_by_symbol
        }

    def is_healthy(self) -> bool:
        """
        Check if gap checker is operating normally.

        Returns:
            True if gap checker is healthy
        """
        # Check if running
        if not self._is_running:
            return False

        # Check if recent check was performed (within last hour)
        if self._last_check_time:
            time_since_last_check = int(time.time() * 1000) - self._last_check_time
            if time_since_last_check > 3600000:  # 1 hour
                return False

        # Check failure rate
        if self._total_gaps_detected > 0:
            failure_rate = self._failed_recoveries / self._total_gaps_detected
            if failure_rate > 0.5:  # More than 50% failures
                return False

        return True

    # ===== STRING REPRESENTATIONS =====

    def __str__(self) -> str:
        """Human-readable string representation."""
        status = 'running' if self._is_running else 'stopped'
        return f"GapChecker({status}, {self._total_gaps_detected} gaps detected)"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (f"GapChecker(running={self._is_running}, "
                f"checks={self._total_checks_performed}, "
                f"gaps_detected={self._total_gaps_detected}, "
                f"gaps_recovered={self._total_gaps_recovered})")
