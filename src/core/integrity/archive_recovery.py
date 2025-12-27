"""
ArchiveRecovery - Binance Archive Download and Recovery
========================================================

Downloads historical trade data from Binance public archives,
validates with SHA256 checksum, and inserts into ClickHouse.

File: src/core/integrity/archive_recovery.py
"""

import io
import csv
import time
import zipfile
import hashlib
import aiohttp
import asyncio
from typing import Optional, List
from dataclasses import dataclass, field
from datetime import date, timedelta

from loggerino import loggerino


logger = loggerino.get('archive_recovery')


@dataclass
class RecoveryResult:
    """Result of archive recovery operation."""
    success: bool
    days_processed: int
    trades_inserted: int
    duration_seconds: float
    failed_date: Optional[date] = None
    error: Optional[str] = None
    errors: List[str] = field(default_factory=list)


@dataclass
class AggTradeRecord:
    """Single aggregated trade record from archive."""
    aggregate_id: int
    price: float
    quantity: float
    first_trade_id: int
    last_trade_id: int
    timestamp: int
    is_buyer_maker: bool


class ArchiveRecovery:
    """
    Downloads and inserts historical trade data from Binance archives.

    Features:
    - SHA256 checksum validation
    - Retry logic with configurable attempts
    - Batch INSERT for performance
    - Progress logging
    """

    BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"

    # Retry configuration
    MAX_DOWNLOAD_ATTEMPTS = 30
    MAX_VALIDATION_ATTEMPTS = 5
    RETRY_DELAY = 2  # seconds

    # Batch configuration - CSV FORMAT handles large batches efficiently
    BATCH_SIZE = 50_000

    def __init__(self, clickhouse_client, telegram_notifier=None):
        """
        Initialize ArchiveRecovery.

        Args:
            clickhouse_client: ClickHouse client for INSERT operations
            telegram_notifier: Optional notifier for alerts
        """
        self.clickhouse = clickhouse_client
        self.telegram = telegram_notifier
        logger.info("ArchiveRecovery initialized (CSV FORMAT mode - 69x faster)")

    async def recover(
            self,
            symbol: str,
            from_date: date,
            to_date: date
    ) -> RecoveryResult:
        """
        Recover data from archives for date range.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSDT')
            from_date: Start date (inclusive)
            to_date: End date (inclusive)

        Returns:
            RecoveryResult with success status and statistics
        """
        start_time = time.time()
        total_trades = 0
        days_processed = 0

        # Generate list of dates
        dates = self._generate_dates(from_date, to_date)
        total_days = len(dates)

        logger.info(f"Starting archive recovery for {symbol}: {from_date} → {to_date} ({total_days} days)")

        # Send Telegram notification
        if self.telegram:
            await self._send_start_notification(symbol, from_date, to_date, total_days)

        # Process each day
        for i, current_date in enumerate(dates):
            try:
                trades_count = await self._process_day(symbol, current_date)
                total_trades += trades_count
                days_processed += 1

                # Log progress
                logger.info(
                    f"{symbol}: day {i + 1}/{total_days} ({current_date}) - "
                    f"{trades_count:,} trades inserted"
                )

                # Periodic Telegram update (every 10 days)
                if self.telegram and days_processed % 10 == 0:
                    await self._send_progress_notification(
                        symbol, days_processed, total_days, total_trades
                    )

            except Exception as e:
                error_msg = f"Failed to process {current_date}: {e}"
                logger.error(f"{symbol}: {error_msg}")

                # Send critical alert
                if self.telegram:
                    await self._send_failure_notification(symbol, current_date, str(e))

                return RecoveryResult(
                    success=False,
                    days_processed=days_processed,
                    trades_inserted=total_trades,
                    duration_seconds=time.time() - start_time,
                    failed_date=current_date,
                    error=error_msg
                )

        duration = time.time() - start_time

        logger.info(
            f"Archive recovery completed for {symbol}: "
            f"{days_processed} days, {total_trades:,} trades, {duration:.1f}s"
        )

        # Send success notification
        if self.telegram:
            await self._send_success_notification(
                symbol, days_processed, total_trades, duration
            )

        return RecoveryResult(
            success=True,
            days_processed=days_processed,
            trades_inserted=total_trades,
            duration_seconds=duration
        )

    def _generate_dates(self, from_date: date, to_date: date) -> List[date]:
        """Generate list of dates in range."""
        dates = []
        current = from_date
        while current <= to_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates

    async def _process_day(self, symbol: str, current_date: date) -> int:
        """
        Process single day: download, validate, parse, insert.

        Args:
            symbol: Trading symbol
            current_date: Date to process

        Returns:
            Number of trades inserted

        Raises:
            Exception if processing fails after all retries
        """
        date_str = current_date.strftime("%Y-%m-%d")
        logger.info(f"{symbol}: Starting download for {date_str}...")

        for validation_attempt in range(self.MAX_VALIDATION_ATTEMPTS):
            try:
                # 1. Download ZIP
                zip_data = await self._download_file(symbol, date_str, ".zip")
                logger.debug(f"{symbol}: Downloaded ZIP for {date_str} ({len(zip_data):,} bytes)")

                # 2. Download CHECKSUM
                checksum_data = await self._download_file(symbol, date_str, ".zip.CHECKSUM")

                # 3. Validate checksum
                if not self._validate_checksum(zip_data, checksum_data):
                    logger.warning(
                        f"{symbol}: checksum validation failed for {date_str} "
                        f"(attempt {validation_attempt + 1}/{self.MAX_VALIDATION_ATTEMPTS})"
                    )
                    continue

                # 4. Extract ZIP
                csv_content = self._extract_zip(zip_data)

                # 5. Parse and INSERT
                trades_count = await self._parse_and_insert(symbol, csv_content)

                return trades_count

            except zipfile.BadZipFile as e:
                logger.warning(
                    f"{symbol}: bad zip file for {date_str} "
                    f"(attempt {validation_attempt + 1}/{self.MAX_VALIDATION_ATTEMPTS}): {e}"
                )
                continue

        # All validation attempts failed
        raise Exception(
            f"Checksum/ZIP validation failed after {self.MAX_VALIDATION_ATTEMPTS} attempts"
        )

    async def _download_file(self, symbol: str, date_str: str, suffix: str) -> bytes:
        """
        Download file with retry logic.

        Args:
            symbol: Trading symbol
            date_str: Date string (YYYY-MM-DD)
            suffix: File suffix (.zip or .zip.CHECKSUM)

        Returns:
            File content as bytes

        Raises:
            Exception if download fails after all retries
        """
        filename = f"{symbol}-aggTrades-{date_str}{suffix}"
        url = f"{self.BASE_URL}/{symbol}/{filename}"

        logger.info(f"{symbol}: Downloading {filename}...")

        for attempt in range(self.MAX_DOWNLOAD_ATTEMPTS):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as response:

                        if response.status == 200:
                            data = await response.read()
                            size_mb = len(data) / (1024 * 1024)
                            logger.info(f"{symbol}: Downloaded {filename} ({size_mb:.1f} MB)")
                            return data

                        elif response.status == 404:
                            raise FileNotFoundError(f"File not found: {url}")

                        else:
                            logger.warning(
                                f"Download failed (attempt {attempt + 1}/{self.MAX_DOWNLOAD_ATTEMPTS}): "
                                f"status {response.status} for {filename}"
                            )

            except aiohttp.ClientError as e:
                logger.warning(
                    f"Download error (attempt {attempt + 1}/{self.MAX_DOWNLOAD_ATTEMPTS}): "
                    f"{e} for {filename}"
                )

            except asyncio.TimeoutError:
                logger.warning(
                    f"Download timeout (attempt {attempt + 1}/{self.MAX_DOWNLOAD_ATTEMPTS}): "
                    f"{filename}"
                )

            await asyncio.sleep(self.RETRY_DELAY)

        raise Exception(
            f"Failed to download {filename} after {self.MAX_DOWNLOAD_ATTEMPTS} attempts"
        )

    def _validate_checksum(self, zip_data: bytes, checksum_data: bytes) -> bool:
        """
        Validate ZIP file against SHA256 checksum.

        Args:
            zip_data: ZIP file content
            checksum_data: CHECKSUM file content

        Returns:
            True if checksum matches, False otherwise
        """
        try:
            # Parse expected checksum from file
            # Format: "hash  filename" or "hash filename"
            checksum_line = checksum_data.decode('utf-8').strip()
            expected_hash = checksum_line.split()[0].lower()

            # Calculate actual checksum
            actual_hash = hashlib.sha256(zip_data).hexdigest().lower()

            if expected_hash == actual_hash:
                return True

            logger.warning(
                f"Checksum mismatch: expected {expected_hash[:16]}..., "
                f"got {actual_hash[:16]}..."
            )
            return False

        except Exception as e:
            logger.warning(f"Checksum validation error: {e}")
            return False

    def _extract_zip(self, zip_data: bytes) -> bytes:
        """
        Extract CSV content from ZIP archive.

        Args:
            zip_data: ZIP file content

        Returns:
            CSV content as bytes

        Raises:
            zipfile.BadZipFile if ZIP is corrupted
        """
        with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
            # Archive contains single CSV file
            filenames = zf.namelist()
            if not filenames:
                raise Exception("ZIP archive is empty")

            return zf.read(filenames[0])

    async def _parse_and_insert(self, symbol: str, csv_content: bytes) -> int:
        """
        Parse CSV and insert trades into ClickHouse.
        Optimized with csv.reader for ~20x faster parsing.

        Args:
            symbol: Trading symbol
            csv_content: CSV file content

        Returns:
            Number of trades inserted
        """
        trades_batch = []
        total_inserted = 0
        parse_errors = 0

        start_parse = time.time()

        # Use csv.reader for fast parsing
        text_content = csv_content.decode('utf-8')
        reader = csv.reader(io.StringIO(text_content))

        # Count lines for progress (fast estimate from content)
        total_lines = text_content.count('\n')
        logger.info(f"{symbol}: Parsing ~{total_lines:,} lines from CSV...")

        for row in reader:
            # Skip header
            if not row or row[0] in ('agg_trade_id', 'aggregate_id'):
                continue

            # Skip incomplete rows
            if len(row) < 7:
                parse_errors += 1
                continue

            try:
                trade = AggTradeRecord(
                    aggregate_id=int(row[0]),
                    price=float(row[1]),
                    quantity=float(row[2]),
                    first_trade_id=int(row[3]),
                    last_trade_id=int(row[4]),
                    timestamp=int(row[5]),
                    is_buyer_maker=row[6].strip().lower() == 'true'
                )
                trades_batch.append(trade)
            except (IndexError, ValueError) as e:
                parse_errors += 1
                continue

            # Batch INSERT
            if len(trades_batch) >= self.BATCH_SIZE:
                await self._bulk_insert(symbol, trades_batch)
                total_inserted += len(trades_batch)
                if total_inserted % 100_000 == 0:
                    logger.info(f"{symbol}: Inserted {total_inserted:,}/{total_lines:,} trades...")
                trades_batch = []

        # Insert remaining
        if trades_batch:
            await self._bulk_insert(symbol, trades_batch)
            total_inserted += len(trades_batch)

        parse_time = time.time() - start_parse

        if parse_errors > 0:
            logger.warning(f"{symbol}: {parse_errors} lines failed to parse")

        logger.info(f"{symbol}: Completed inserting {total_inserted:,} trades in {parse_time:.1f}s")
        return total_inserted

    def _parse_csv_line(self, line: str) -> Optional[AggTradeRecord]:
        """
        Parse single CSV line into AggTradeRecord.

        CSV format:
        aggregate_id,price,quantity,first_trade_id,last_trade_id,timestamp,is_buyer_maker

        Args:
            line: CSV line

        Returns:
            AggTradeRecord or None if parse failed
        """
        try:
            parts = line.split(',')
            if len(parts) < 7:
                return None

            return AggTradeRecord(
                aggregate_id=int(parts[0]),
                price=float(parts[1]),
                quantity=float(parts[2]),
                first_trade_id=int(parts[3]),
                last_trade_id=int(parts[4]),
                timestamp=int(parts[5]),
                is_buyer_maker=parts[6].strip().lower() == 'true'
            )

        except (IndexError, ValueError) as e:
            logger.debug(f"Failed to parse line: {line[:50]}... - {e}")
            return None

    async def _bulk_insert(self, symbol: str, trades: List[AggTradeRecord]) -> None:
        """
        Bulk INSERT trades into ClickHouse using CSV FORMAT (69x faster than VALUES).

        Args:
            symbol: Trading symbol
            trades: List of trades to insert
        """
        if not trades:
            return

        start_time = time.time()

        # Build CSV data (fastest format according to benchmarks)
        lines = []
        for t in trades:
            lines.append(f"{symbol},{t.aggregate_id},{t.price},{t.quantity},{t.timestamp},{1 if t.is_buyer_maker else 0}")
        csv_data = "\n".join(lines)

        # Execute via HTTP with FORMAT CSV
        query = "INSERT INTO trades_local (symbol,aggregate_id,price,quantity,timestamp,is_buyer_maker) FORMAT CSV"

        await self.clickhouse.execute_with_data(query, csv_data)

        elapsed = time.time() - start_time
        logger.debug(f"{symbol}: Batch INSERT {len(trades):,} trades in {elapsed:.2f}s")

    # ===== TELEGRAM NOTIFICATIONS =====

    async def _send_start_notification(
            self,
            symbol: str,
            from_date: date,
            to_date: date,
            total_days: int
    ):
        """Send Telegram notification when recovery starts."""
        try:
            from ..telegram_notifier import AlertLevel
            await self.telegram.send_alert(
                level=AlertLevel.INFO,
                title="Archive Recovery Started",
                message=(
                    f"Symbol: {symbol}\n"
                    f"Period: {from_date} → {to_date}\n"
                    f"Days: {total_days}"
                )
            )
        except Exception as e:
            logger.warning(f"Failed to send start notification: {e}")

    async def _send_progress_notification(
            self,
            symbol: str,
            days_processed: int,
            total_days: int,
            total_trades: int
    ):
        """Send Telegram progress update."""
        try:
            from ..telegram_notifier import AlertLevel
            await self.telegram.send_alert(
                level=AlertLevel.INFO,
                title="Archive Recovery Progress",
                message=(
                    f"Symbol: {symbol}\n"
                    f"Progress: {days_processed}/{total_days} days\n"
                    f"Trades: {total_trades:,}"
                )
            )
        except Exception as e:
            logger.warning(f"Failed to send progress notification: {e}")

    async def _send_success_notification(
            self,
            symbol: str,
            days_processed: int,
            total_trades: int,
            duration: float
    ):
        """Send Telegram notification on success."""
        try:
            from ..telegram_notifier import AlertLevel

            minutes = int(duration // 60)
            seconds = int(duration % 60)
            duration_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

            await self.telegram.send_alert(
                level=AlertLevel.INFO,
                title="Archive Recovery Completed",
                message=(
                    f"Symbol: {symbol}\n"
                    f"Days: {days_processed}\n"
                    f"Trades: {total_trades:,}\n"
                    f"Duration: {duration_str}"
                )
            )
        except Exception as e:
            logger.warning(f"Failed to send success notification: {e}")

    async def _send_failure_notification(
            self,
            symbol: str,
            failed_date: date,
            error: str
    ):
        """Send Telegram critical alert on failure."""
        try:
            from ..telegram_notifier import AlertLevel
            await self.telegram.send_alert(
                level=AlertLevel.CRITICAL,
                title="Archive Recovery FAILED",
                message=(
                    f"Symbol: {symbol}\n"
                    f"Failed date: {failed_date}\n"
                    f"Error: {error}\n"
                    f"Action required: Manual intervention needed"
                )
            )
        except Exception as e:
            logger.warning(f"Failed to send failure notification: {e}")