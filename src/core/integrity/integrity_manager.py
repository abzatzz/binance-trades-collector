"""
IntegrityManager - Data Integrity Coordination
==============================================

Main coordinator for integrity checks and recovery operations.
Uses queue-based processing for startup checks and periodic monitoring.

File: src/core/integrity/integrity_manager.py
"""

import asyncio
import time
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

from loggerino import loggerino

try:
    # Normal import when used as part of package
    from .recovery_state import RecoveryState, RecoveryStatus
    from .integrity_checker import (
        IntegrityChecker, Problem, ProblemType, RecoveryStrategy,
        IntegrityResult, IntegrityStatus, TrustPoint
    )
    from .archive_recovery import ArchiveRecovery, RecoveryResult
except ImportError:
    # Direct import for standalone testing
    from recovery_state import RecoveryState, RecoveryStatus
    from integrity_checker import (
        IntegrityChecker, Problem, ProblemType, RecoveryStrategy,
        IntegrityResult, IntegrityStatus, TrustPoint
    )
    from archive_recovery import ArchiveRecovery, RecoveryResult

logger = loggerino.get('integrity_manager')


@dataclass
class CheckRequest:
    """Request for integrity check."""
    symbol: str
    requested_at: float
    priority: int = 0  # Higher = more priority


class IntegrityManager:
    """
    Coordinates integrity checks and recovery operations.

    Features:
    - Queue-based processing for startup checks
    - Periodic background monitoring
    - Archive recovery coordination
    - Telegram notifications for critical issues
    """

    PERIODIC_CHECK_INTERVAL = 2  # seconds between symbol checks

    def __init__(
            self,
            clickhouse_client,
            recovery_state: RecoveryState,
            telegram_notifier=None
    ):
        """
        Initialize IntegrityManager.

        Args:
            clickhouse_client: ClickHouse client for queries and mutations
            recovery_state: Shared state for coordination with tickers
            telegram_notifier: Optional notifier for alerts
        """
        self.clickhouse = clickhouse_client
        self.recovery_state = recovery_state
        self.telegram = telegram_notifier

        # Components
        self.checker = IntegrityChecker(clickhouse_client)
        self.archive_recovery = ArchiveRecovery(clickhouse_client, telegram_notifier)

        # Queue for startup checks
        self.check_queue: asyncio.Queue[CheckRequest] = asyncio.Queue()

        # State
        self._running = False
        self._queue_worker_task: Optional[asyncio.Task] = None
        self._periodic_checker_task: Optional[asyncio.Task] = None

        # Statistics
        self._symbols_checked = 0
        self._symbols_recovered = 0
        self._total_trades_recovered = 0

        logger.info("IntegrityManager initialized")

    async def start(self):
        """Start the integrity manager workers."""
        if self._running:
            logger.warning("IntegrityManager already running")
            return

        self._running = True

        # Start queue worker
        self._queue_worker_task = asyncio.create_task(
            self._queue_worker(),
            name="integrity_queue_worker"
        )

        logger.info("IntegrityManager started")

    async def start_with_periodic_checker(self, interval_hours: float = 6):
        """Start with both queue worker and periodic checker."""
        await self.start()

        # Start periodic checker
        self._periodic_checker_task = asyncio.create_task(
            self._periodic_checker(interval_hours),
            name="integrity_periodic_checker"
        )

        logger.info(f"Periodic integrity checker started (interval: {interval_hours}h)")

    async def stop(self):
        """Stop the integrity manager."""
        self._running = False

        if self._queue_worker_task:
            self._queue_worker_task.cancel()
            try:
                await self._queue_worker_task
            except asyncio.CancelledError:
                pass

        if self._periodic_checker_task:
            self._periodic_checker_task.cancel()
            try:
                await self._periodic_checker_task
            except asyncio.CancelledError:
                pass

        logger.info("IntegrityManager stopped")

    async def request_check(self, symbol: str, priority: int = 0):
        """
        Request integrity check for a symbol.

        Called by TickerProcessor at startup.

        Args:
            symbol: Symbol to check
            priority: Higher priority = processed first
        """
        request = CheckRequest(
            symbol=symbol,
            requested_at=time.time(),
            priority=priority
        )

        await self.recovery_state.set_status(symbol, RecoveryStatus.QUEUED)
        await self.check_queue.put(request)

        logger.debug(f"Check requested for {symbol} (priority={priority})")

    async def _queue_worker(self):
        """Process check requests from queue."""
        logger.info("Queue worker started")

        while self._running:
            try:
                # Get next request (with timeout to allow shutdown)
                try:
                    request = await asyncio.wait_for(
                        self.check_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # Process the request
                await self._process_symbol(request.symbol)

                self.check_queue.task_done()
                self._symbols_checked += 1

            except asyncio.CancelledError:
                logger.info("Queue worker cancelled")
                break
            except Exception as e:
                logger.error(f"Queue worker error: {e}", exc_info=True)
                await asyncio.sleep(1)

        logger.info("Queue worker stopped")

    async def _periodic_checker(self, interval_hours: float):
        """
        Periodically check all running symbols.

        Args:
            interval_hours: Hours between full check cycles
        """
        logger.info(f"Periodic checker started (interval: {interval_hours}h)")

        while self._running:
            try:
                # Wait for interval
                await asyncio.sleep(interval_hours * 3600)

                if not self._running:
                    break

                logger.info("Starting periodic integrity check cycle")

                # Get all symbols
                symbols = await self._get_all_symbols()

                for symbol in symbols:
                    if not self._running:
                        break

                    # Skip if already being processed
                    if self.recovery_state.is_busy(symbol):
                        continue

                    # Run integrity check
                    result = await self.checker.find_trust_point(symbol)

                    if result.status == IntegrityStatus.VALID:
                        # All OK - nothing to do
                        logger.debug(f"{symbol}: periodic check passed")

                    elif result.status in [IntegrityStatus.NEEDS_RECOVERY, IntegrityStatus.NO_DATA]:
                        logger.warning(f"{symbol}: periodic check found issue - {result.status.value}")

                        # Reset state and process
                        await self.recovery_state.reset(symbol)
                        await self._process_symbol(symbol)

                    elif result.status == IntegrityStatus.ERROR:
                        logger.error(f"{symbol}: periodic check error - {result.error_message}")

                    # Interval between checks
                    await asyncio.sleep(self.PERIODIC_CHECK_INTERVAL)

                logger.info("Periodic integrity check cycle completed")

            except asyncio.CancelledError:
                logger.info("Periodic checker cancelled")
                break
            except Exception as e:
                logger.error(f"Periodic checker error: {e}", exc_info=True)
                await asyncio.sleep(60)

        logger.info("Periodic checker stopped")

    async def _process_symbol(self, symbol: str):
        """
        Process single symbol: check and recover if needed.

        Args:
            symbol: Symbol to process
        """
        start_time = time.time()

        try:
            # 1. Set status to checking
            await self.recovery_state.set_status(symbol, RecoveryStatus.CHECKING)

            # 2. Run integrity check
            result = await self.checker.find_trust_point(symbol)

            # 3. Handle result based on status
            if result.status == IntegrityStatus.VALID:
                # All OK - ticker can proceed
                logger.info(f"{symbol}: integrity check passed ({result.check_duration_seconds:.2f}s)")
                await self.recovery_state.set_ready(symbol)
                return

            if result.status == IntegrityStatus.ERROR:
                # Check failed - set error state
                logger.error(f"{symbol}: integrity check error - {result.error_message}")
                await self.recovery_state.set_error(symbol, result.error_message)
                if self.telegram:
                    await self._send_error_notification(symbol, result.error_message)
                return

            if result.status == IntegrityStatus.NO_DATA:
                # No data - need full download
                logger.info(f"{symbol}: no data found, needs full download")
                strategy = self.checker.determine_strategy(result)

                # For NO_DATA, ticker will handle via HistoricalDownloader
                await self.recovery_state.set_ready(symbol)
                return

            if result.status == IntegrityStatus.NEEDS_RECOVERY:
                # Found trust point - need recovery
                trust_point = result.trust_point
                strategy = self.checker.determine_strategy(result)

                logger.info(
                    f"{symbol}: recovery needed, "
                    f"trust_point={trust_point.last_valid_day}, "
                    f"recovery_start={trust_point.recovery_start_day}, "
                    f"needs_archive={strategy.needs_archive}"
                )

                # 4. DELETE data from recovery start day
                deleted_count = await self._delete_trades_from_day(symbol, strategy.delete_from_day)
                logger.info(f"{symbol}: deleted {deleted_count:,} trades from {strategy.delete_from_day}")

                # 5. Archive recovery if needed
                if strategy.needs_archive:
                    await self.recovery_state.set_status(symbol, RecoveryStatus.ARCHIVE_RECOVERY)

                    result = await self.archive_recovery.recover(
                        symbol=symbol,
                        from_date=datetime.strptime(strategy.archive_from_day, "%Y-%m-%d").date(),
                        to_date=datetime.strptime(strategy.archive_to_day, "%Y-%m-%d").date()
                    )

                    if not result.success:
                        # Recovery failed - ticker should NOT proceed
                        await self.recovery_state.set_error(
                            symbol,
                            f"Archive recovery failed: {result.error}"
                        )
                        return

                    self._symbols_recovered += 1
                    self._total_trades_recovered += result.trades_inserted

                    # Set ready with stats
                    await self.recovery_state.set_ready(
                        symbol,
                        trades_recovered=result.trades_inserted,
                        days_processed=result.days_processed
                    )
                else:
                    # No archive needed - ticker will do API recovery
                    await self.recovery_state.set_ready(symbol)

                duration = time.time() - start_time
                logger.info(f"{symbol}: integrity processing completed in {duration:.1f}s")

        except Exception as e:
            logger.error(f"{symbol}: integrity processing failed - {e}", exc_info=True)
            await self.recovery_state.set_error(symbol, str(e))

            # Send Telegram alert
            if self.telegram:
                await self._send_error_notification(symbol, str(e))

    async def _delete_trades_from_day(self, symbol: str, from_day: str) -> int:
        """
        Delete all trades from specified day onwards.

        Args:
            symbol: Symbol to delete
            from_day: Delete trades from this day (YYYY-MM-DD format)

        Returns:
            Number of trades deleted
        """
        # Count trades to delete
        count_query = f"""
            SELECT count()
            FROM trades_local
            WHERE symbol = '{symbol}'
              AND toDate(timestamp) >= '{from_day}'
        """

        try:
            result = await self.clickhouse.fetch(count_query)
            count = result[0][0] if result else 0

            if count == 0:
                logger.info(f"{symbol}: no trades to delete from {from_day}")
                return 0

            # Delete trades
            delete_query = f"""
                ALTER TABLE trades_local
                DELETE WHERE symbol = '{symbol}'
                  AND toDate(timestamp) >= '{from_day}'
            """

            await self.clickhouse.execute(delete_query)

            # Wait for mutation to complete
            await self._wait_for_mutations()

            return count

        except Exception as e:
            logger.error(f"{symbol}: delete failed - {e}")
            raise

    async def _wait_for_mutations(self, timeout: float = 300):
        """
        Wait for pending mutations to complete.

        Args:
            timeout: Maximum seconds to wait
        """
        start_time = time.time()

        while time.time() - start_time < timeout:
            query = "SELECT count() FROM system.mutations WHERE is_done = 0"
            result = await self.clickhouse.fetch(query)
            pending = result[0][0] if result else 0

            if pending == 0:
                return

            logger.debug(f"Waiting for {pending} mutations to complete...")
            await asyncio.sleep(1)

        logger.warning(f"Mutation wait timeout after {timeout}s")

    async def _get_all_symbols(self) -> List[str]:
        """Get list of all symbols in database."""
        query = """
            SELECT DISTINCT symbol
            FROM trades_local
            ORDER BY symbol
        """

        try:
            result = await self.clickhouse.fetch(query)
            return [row[0] for row in result] if result else []
        except Exception as e:
            logger.error(f"Failed to get symbols: {e}")
            return []

    async def _send_error_notification(self, symbol: str, error: str):
        """Send Telegram notification for processing error."""
        try:
            from ..telegram_notifier import AlertLevel
            await self.telegram.send_alert(
                level=AlertLevel.CRITICAL,
                title="Integrity Check Failed",
                message=(
                    f"Symbol: {symbol}\n"
                    f"Error: {error}\n"
                    f"Action required: Manual intervention needed"
                )
            )
        except Exception as e:
            logger.warning(f"Failed to send error notification: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """Get manager statistics."""
        return {
            'running': self._running,
            'queue_size': self.check_queue.qsize(),
            'symbols_checked': self._symbols_checked,
            'symbols_recovered': self._symbols_recovered,
            'total_trades_recovered': self._total_trades_recovered,
            'recovery_state_summary': self.recovery_state.get_summary()
        }

    async def wait_for_queue_empty(self, timeout: Optional[float] = None):
        """
        Wait until check queue is empty.

        Useful for waiting until all startup checks complete.

        Args:
            timeout: Maximum seconds to wait (None = forever)
        """
        try:
            await asyncio.wait_for(
                self.check_queue.join(),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"Queue wait timeout, {self.check_queue.qsize()} items remaining")
