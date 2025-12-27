"""
HealthMonitor - System Health Monitoring
========================================

Periodic checks for disk space, ClickHouse system logs, ticker health,
data lag, and system performance. Sends Telegram alerts when issues detected.

File: src/core/health_monitor.py
"""

import asyncio
import time
from typing import Optional, Dict, Any, TYPE_CHECKING

from loggerino import loggerino
from .telegram_notifier import get_notifier, AlertLevel

if TYPE_CHECKING:
    from .shared import Shared

logger = loggerino.get('health_monitor')


class HealthMonitor:
    """
    Monitors system health and sends alerts.

    Checks:
    - Disk space (alert if < 10% free)
    - ClickHouse system logs size (alert if > 10GB, auto-cleanup)
    - Stopped/failed tickers count
    - Data lag (how far behind real-time)
    - Buffer overflow count
    - Write throughput
    """

    def __init__(self, shared: 'Shared', check_interval_seconds: int = 300):
        """
        Initialize HealthMonitor.

        Args:
            shared: Shared instance with access to all components
            check_interval_seconds: How often to run checks (default 5 min)
        """
        self.shared = shared
        self.check_interval = check_interval_seconds
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

        # Alert cooldowns (don't spam)
        self._last_disk_alert_time: float = 0
        self._last_logs_alert_time: float = 0
        self._last_tickers_alert_time: float = 0
        self._last_lag_alert_time: float = 0
        self._alert_cooldown: float = 1800  # 30 minutes

        # Thresholds
        self.disk_free_threshold_percent = 10  # Alert if < 10%
        self.system_logs_threshold_gb = 10  # Alert if > 10GB
        self.stopped_tickers_threshold = 5  # Alert if > 5 tickers stopped
        self.data_lag_threshold_seconds = 300  # Alert if > 5 minutes behind

        # Daily summary tracking
        self._last_daily_summary_date: Optional[str] = None
        self._daily_trades_start: int = 0

        # Throughput tracking
        self._last_trades_count: int = 0
        self._last_check_time: float = 0

        logger.info(f"HealthMonitor initialized (interval: {check_interval_seconds}s)")

    async def start(self) -> None:
        """Start health monitoring loop."""
        if self.is_running:
            return
        self.is_running = True
        self._task = asyncio.create_task(self._monitoring_loop())
        logger.info("HealthMonitor started")

    async def stop(self) -> None:
        """Stop health monitoring."""
        self.is_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("HealthMonitor stopped")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        # Initial delay to let system start up
        await asyncio.sleep(60)

        while self.is_running:
            try:
                await self._perform_health_checks()
            except Exception as e:
                logger.error(f"Health check error: {e}")

            await asyncio.sleep(self.check_interval)

    async def _perform_health_checks(self) -> None:
        """Perform all health checks."""
        current_time = time.time()

        logger.debug("Running health checks...")

        # Check disk space
        await self._check_disk_space(current_time)

        # Check system logs size
        await self._check_system_logs(current_time)

        # Check ticker health
        await self._check_ticker_health(current_time)

        # Check data lag
        await self._check_data_lag(current_time)

        # Log throughput
        self._log_throughput(current_time)

        # Check daily summary
        await self._check_daily_summary(current_time)

        logger.debug("Health checks completed")

    async def _check_disk_space(self, current_time: float) -> None:
        """Check disk free space."""
        try:
            clickhouse = self.shared.clickhouse_manager
            if not clickhouse or not clickhouse.client:
                return

            result = await clickhouse.client.fetchrow("""
                SELECT free_space, total_space
                FROM system.disks 
                WHERE name = 'default'
            """)

            if result:
                free_space = result[0]
                total_space = result[1]
                free_percent = (free_space / total_space) * 100 if total_space else 0
                free_gb = free_space / (1024 ** 3)

                if free_percent < self.disk_free_threshold_percent:
                    logger.warning(f"Low disk space: {free_percent:.1f}% free ({free_gb:.1f}GB)")

                    if current_time - self._last_disk_alert_time > self._alert_cooldown:
                        notifier = get_notifier()
                        if notifier:
                            await notifier.send_alert(
                                message=f"Low disk space: {free_percent:.1f}% free ({free_gb:.1f}GB remaining)",
                                level=AlertLevel.CRITICAL
                            )
                            self._last_disk_alert_time = current_time
                else:
                    logger.debug(f"Disk space OK: {free_percent:.1f}% free ({free_gb:.1f}GB)")

        except Exception as e:
            logger.error(f"Failed to check disk space: {e}")

    async def _check_system_logs(self, current_time: float) -> None:
        """Check ClickHouse system logs size and auto-cleanup if needed."""
        try:
            clickhouse = self.shared.clickhouse_manager
            if not clickhouse or not clickhouse.client:
                return

            result = await clickhouse.client.fetchrow("""
                SELECT sum(bytes_on_disk) as total_bytes
                FROM system.parts
                WHERE active AND database = 'system'
            """)

            if result:
                total_bytes = result[0] or 0
                total_gb = total_bytes / (1024 ** 3)
                threshold_bytes = self.system_logs_threshold_gb * (1024 ** 3)

                if total_bytes > threshold_bytes:
                    logger.warning(f"System logs too large: {total_gb:.2f}GB")

                    # Auto-cleanup
                    await self._cleanup_system_logs()

                    if current_time - self._last_logs_alert_time > self._alert_cooldown:
                        notifier = get_notifier()
                        if notifier:
                            await notifier.send_alert(
                                message=f"System logs were {total_gb:.1f}GB. Auto-cleanup performed.",
                                level=AlertLevel.WARNING
                            )
                            self._last_logs_alert_time = current_time
                else:
                    logger.debug(f"System logs OK: {total_gb:.2f}GB")

        except Exception as e:
            logger.error(f"Failed to check system logs: {e}")

    async def _cleanup_system_logs(self) -> None:
        """Truncate ClickHouse system log tables."""
        tables = [
            'system.processors_profile_log',
            'system.text_log',
            'system.query_log',
            'system.trace_log',
            'system.part_log',
            'system.metric_log',
            'system.asynchronous_metric_log'
        ]

        clickhouse = self.shared.clickhouse_manager
        if not clickhouse or not clickhouse.client:
            return

        for table in tables:
            try:
                await clickhouse.client.execute(f"TRUNCATE TABLE {table}")
                logger.info(f"Truncated {table}")
            except Exception as e:
                logger.error(f"Failed to truncate {table}: {e}")

        logger.info("System logs cleanup completed")

    async def _check_ticker_health(self, current_time: float) -> None:
        """Check how many tickers are stopped/failed."""
        try:
            total_tickers = len(self.shared.tickers)
            running_count = 0
            stopped_count = 0

            for symbol, task in self.shared.tickers.items():
                if task.done():
                    stopped_count += 1
                else:
                    processor = getattr(task, 'processor', None)
                    if processor and processor._state.is_active():
                        running_count += 1
                    else:
                        stopped_count += 1

            logger.debug(f"Tickers: {running_count} running, {stopped_count} stopped, {total_tickers} total")

            if stopped_count > self.stopped_tickers_threshold:
                logger.warning(f"Many tickers stopped: {stopped_count}/{total_tickers}")

                if current_time - self._last_tickers_alert_time > self._alert_cooldown:
                    notifier = get_notifier()
                    if notifier:
                        await notifier.send_alert(
                            message=f"Tickers health issue: {stopped_count} stopped out of {total_tickers}",
                            level=AlertLevel.WARNING
                        )
                        self._last_tickers_alert_time = current_time

        except Exception as e:
            logger.error(f"Failed to check ticker health: {e}")

    async def _check_data_lag(self, current_time: float) -> None:
        """Check how far behind real-time the data is."""
        try:
            clickhouse = self.shared.clickhouse_manager
            if not clickhouse or not clickhouse.client:
                return

            result = await clickhouse.client.fetchrow("""
                SELECT toUnixTimestamp(now()) - toUnixTimestamp(max(timestamp)) as lag_seconds
                FROM trades_local 
                WHERE symbol = 'BTCUSDT'
            """)

            if result:
                lag_seconds = result[0] or 0

                if lag_seconds > self.data_lag_threshold_seconds:
                    logger.warning(f"Data lag: {lag_seconds}s behind real-time")

                    if current_time - self._last_lag_alert_time > self._alert_cooldown:
                        notifier = get_notifier()
                        if notifier:
                            await notifier.send_alert(
                                message=f"Data lag: {lag_seconds // 60}m {lag_seconds % 60}s behind real-time (BTCUSDT)",
                                level=AlertLevel.WARNING
                            )
                            self._last_lag_alert_time = current_time
                else:
                    logger.debug(f"Data lag OK: {lag_seconds}s")

        except Exception as e:
            logger.error(f"Failed to check data lag: {e}")

    def _log_throughput(self, current_time: float) -> None:
        """Log write throughput statistics."""
        try:
            if not self.shared.global_trades_updater:
                return

            current_trades = self.shared.global_trades_updater.total_trades_processed

            if self._last_check_time > 0:
                elapsed = current_time - self._last_check_time
                trades_diff = current_trades - self._last_trades_count
                trades_per_second = trades_diff / elapsed if elapsed > 0 else 0

                logger.info(f"Throughput: {trades_per_second:.0f} trades/sec, "
                            f"total: {current_trades:,}")

            self._last_trades_count = current_trades
            self._last_check_time = current_time

        except Exception as e:
            logger.error(f"Failed to log throughput: {e}")

    async def _check_daily_summary(self, current_time: float) -> None:
        """Send daily summary at midnight UTC."""
        from datetime import datetime, timezone

        current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        # Check if we already sent summary today
        if self._last_daily_summary_date == current_date:
            return

        # Check if it's around midnight UTC (0:00 - 0:10)
        current_hour = datetime.now(timezone.utc).hour
        current_minute = datetime.now(timezone.utc).minute

        if current_hour == 0 and current_minute < 10:
            await self._send_daily_summary()
            self._last_daily_summary_date = current_date

    async def _send_daily_summary(self) -> None:
        """Send daily statistics summary."""
        try:
            notifier = get_notifier()
            if not notifier:
                return

            # Get statistics
            total_tickers = len(self.shared.tickers)
            running_count = sum(
                1 for task in self.shared.tickers.values()
                if not task.done() and hasattr(task, 'processor') and task.processor._state.is_active()
            )

            # Trades today
            current_trades = 0
            if self.shared.global_trades_updater:
                current_trades = self.shared.global_trades_updater.total_trades_processed

            trades_today = current_trades - self._daily_trades_start
            self._daily_trades_start = current_trades

            # Disk space
            disk_info = ""
            try:
                clickhouse = self.shared.clickhouse_manager
                if clickhouse and clickhouse.client:
                    result = await clickhouse.client.fetchrow("""
                        SELECT free_space, total_space FROM system.disks WHERE name = 'default'
                    """)
                    if result:
                        free_gb = result[0] / (1024 ** 3)
                        free_pct = (result[0] / result[1]) * 100 if result[1] else 0
                        disk_info = f"Disk: {free_gb:.1f}GB free ({free_pct:.1f}%)"
            except:
                pass

            message = (
                f"📊 Daily Summary\n\n"
                f"Tickers: {running_count}/{total_tickers} running\n"
                f"Trades today: {trades_today:,}\n"
                f"{disk_info}"
            )

            await notifier.send_alert(
                message=message,
                level=AlertLevel.INFO
            )

            logger.info(f"Daily summary sent: {trades_today:,} trades, {running_count}/{total_tickers} tickers")

        except Exception as e:
            logger.error(f"Failed to send daily summary: {e}")

    async def get_health_status(self) -> Dict[str, Any]:
        """Get current health status (for API endpoint)."""
        try:
            clickhouse = self.shared.clickhouse_manager

            # Disk space
            disk_result = await clickhouse.client.fetchrow("""
                SELECT free_space, total_space
                FROM system.disks WHERE name = 'default'
            """) if clickhouse and clickhouse.client else None

            # System logs
            logs_result = await clickhouse.client.fetchrow("""
                SELECT sum(bytes_on_disk) as total_bytes
                FROM system.parts
                WHERE active AND database = 'system'
            """) if clickhouse and clickhouse.client else None

            # Ticker counts
            total_tickers = len(self.shared.tickers)
            running_count = sum(
                1 for task in self.shared.tickers.values()
                if not task.done() and hasattr(task, 'processor') and task.processor._state.is_active()
            )

            # Buffer stats
            buffer_overflow_count = 0
            if self.shared.global_trades_updater:
                buffer_overflow_count = self.shared.global_trades_updater.queue_overflow_count

            disk_free = disk_result[0] if disk_result else 0
            disk_total = disk_result[1] if disk_result else 0
            logs_bytes = logs_result[0] if logs_result else 0

            return {
                'status': 'healthy' if running_count > 0 else 'unhealthy',
                'disk': {
                    'free_bytes': disk_free,
                    'total_bytes': disk_total,
                    'free_percent': round((disk_free / disk_total * 100), 1) if disk_total else 0,
                    'free_gb': round(disk_free / (1024 ** 3), 1)
                },
                'system_logs_gb': round(logs_bytes / (1024 ** 3), 2),
                'tickers': {
                    'total': total_tickers,
                    'running': running_count,
                    'stopped': total_tickers - running_count
                },
                'buffer_overflow_count': buffer_overflow_count,
                'clickhouse': 'connected' if clickhouse and clickhouse.is_connected else 'disconnected'
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }
