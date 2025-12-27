"""
ClickHouseManager - High-Performance ClickHouse Storage Manager
=============================================================

Drop-in replacement for BinaryFileManager with pure ClickHouse backend.
Supports minute-based timeframes and integrated ClickHouseConfig system.

File: src/storage/clickhouse_manager.py
"""

import asyncio
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set, Tuple

import aiohttp
from loggerino import loggerino
import aiochclient
from aiohttp import ClientSession

from src.models.aggtrade import AggTrade
from src.models.config import ClickHouseConfig

from .telegram_notifier import get_notifier, AlertLevel

logger = loggerino.get('clickhouse_manager')


class ClickHouseManager:
    """
    ClickHouse storage manager with BinaryFileManager-compatible interface.

    Features:
    - Drop-in replacement for BinaryFileManager
    - High-performance batch inserts via buffer table
    - Dynamic materialized views for OHLCV data with minute-based timeframes
    - Automatic partitioning by month and symbol hash
    - Compatible async/sync interface with existing components
    """

    def __init__(self, clickhouse_config: ClickHouseConfig):
        """
        Initialize ClickHouseManager for centralized storage.

        Args:
            clickhouse_config: ClickHouse configuration from Config system
        """
        self.config = clickhouse_config

        # Client session and connection
        self._session: Optional[ClientSession] = None
        self.client: Optional[aiochclient.ChClient] = None
        self.is_connected = False

        # Statistics tracking
        self._total_trades_written = 0
        self._total_bytes_written = 0
        self._connection_time: Optional[float] = None

        # Materialized views tracking
        self._created_mvs: Dict[int, str] = {}  # timeframe_minutes -> mv_name

        # Lock for MV creation to prevent race conditions
        self._mv_creation_lock = asyncio.Lock()

        # Telegram alert anti-spam
        self._last_disconnect_alert_time: float = 0
        self._disconnect_alert_cooldown: float = 600  # 10 minutes

        logger.info(f"ClickHouseManager initialized")

    async def batch_insert_all_symbols(self, all_trades_data: List[List[Any]]) -> int:
        """
        Single batch insert for all symbols using CSV FORMAT (69x faster).

        Args:
            all_trades_data: List of trade records [symbol, aggregate_id, price, quantity,
                           first_trade_id, last_trade_id, timestamp, is_buyer_maker]

        Returns:
            Number of trades inserted
        """
        if not all_trades_data:
            return 0

        try:
            target_table = "trades_buffer" if self.config.buffer_enabled else "trades_local"

            # Build CSV data (fastest INSERT format)
            lines = []
            for row in all_trades_data:
                # row = [symbol, aggregate_id, price, quantity, first_trade_id, last_trade_id, timestamp, is_buyer_maker]
                lines.append(f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]}")
            csv_data = "\n".join(lines)

            query = f"INSERT INTO {target_table} (symbol, aggregate_id, price, quantity, first_trade_id, last_trade_id, timestamp, is_buyer_maker) FORMAT CSV"

            await self.execute_with_data(query, csv_data)

            if self.config.verbose_trade_logging:
                logger.debug(f"Batch inserted {len(all_trades_data)} trades for all symbols (CSV FORMAT)")

            return len(all_trades_data)

        except Exception as e:
            logger.error(f"Batch insert failed for {len(all_trades_data)} trades: {e}")
            raise

    async def ensure_connected(self) -> None:
        """Ensure ClickHouse connection is established."""
        logger.debug(f"ensure_connected called, is_connected={self.is_connected}")

        # Проверяем совместимость с текущим loop
        if self.is_connected and self.client and self._session:
            try:
                current_loop = asyncio.get_running_loop()
                # Проверяем, привязана ли сессия к другому loop
                if hasattr(self._session, '_loop') and self._session._loop is not current_loop:
                    logger.debug("Session bound to different loop - forcing reconnection")
                    await self._force_reconnect()
                    return
                elif not self._session.closed:
                    logger.debug("Already connected in same loop, returning")
                    return
            except RuntimeError:
                # Нет активного loop - странная ситуация для async метода
                logger.warning("No running loop in async context")

        # Принудительно пересоздаем соединение
        await self._force_reconnect()

    async def _force_reconnect(self) -> None:
        """Force reconnection by recreating session and client."""
        logger.debug("Force reconnecting to ClickHouse")

        # Закрываем старую сессию
        if self._session and not self._session.closed:
            try:
                await self._session.close()
            except Exception as e:
                logger.debug(f"Error closing old session: {e}")

        # Сбрасываем состояние
        self._session = None
        self.client = None
        self.is_connected = False

        try:
            logger.debug("Creating new session and client...")

            # TCP Connector with keepalive and connection pooling
            connector = aiohttp.TCPConnector(
                limit=self.config.connection_pool_size,
                limit_per_host=self.config.max_connections_per_host,
                keepalive_timeout=280,  # Slightly less than ClickHouse server (300s)
                enable_cleanup_closed=True,
                force_close=False,
                ttl_dns_cache=300,  # Cache DNS for 5 minutes
            )

            # Separate timeouts for different operations
            timeout = aiohttp.ClientTimeout(
                total=self.config.query_timeout_seconds,  # Total timeout for long queries
                connect=self.config.connection_timeout_seconds,  # Connection timeout
                sock_connect=self.config.connection_timeout_seconds,
                sock_read=self.config.query_timeout_seconds,
            )

            self._session = ClientSession(
                timeout=timeout,
                connector=connector,
            )

            self.client = aiochclient.ChClient(
                session=self._session,
                url=self.config.get_connection_url(),
                database=self.config.database,
                user=self.config.username,
                password=self.config.password,
                compress_response=self.config.enable_http_compression
            )

            # Тестируем соединение
            await self.client.execute("SELECT 1")
            self.is_connected = True
            self._connection_time = time.time()

            # Убеждаемся в схеме БД
            await self._ensure_database_schema()
            logger.debug(f"Successfully reconnected to ClickHouse")

        except Exception as e:
            logger.error(f"Failed to reconnect to ClickHouse: {e}")
            await self._cleanup_connection()

            # Send disconnect alert (with anti-spam)
            current_time = time.time()
            if current_time - self._last_disconnect_alert_time > self._disconnect_alert_cooldown:
                notifier = get_notifier()
                if notifier:
                    try:
                        await notifier.send_alert(
                            message=f"ClickHouse connection failed: {str(e)[:100]}",
                            level=AlertLevel.CRITICAL
                        )
                        self._last_disconnect_alert_time = current_time
                    except Exception as notify_error:
                        logger.error(f"Failed to send disconnect notification: {notify_error}")

            raise

    async def _cleanup_connection(self) -> None:
        """Clean up ClickHouse connection."""
        self.is_connected = False
        self.client = None

        if self._session:
            await self._session.close()
            self._session = None

    async def close(self) -> None:
        """Close ClickHouse connection."""
        if self.is_connected:
            logger.info("ClickHouseManager closing...")
            await self._cleanup_connection()
            logger.info("ClickHouseManager closed")

    async def _execute_with_retry(self, query: str, *args, max_retries: Optional[int] = None) -> Any:
        """Execute with automatic reconnection on failure."""
        if max_retries is None:
            max_retries = self.config.max_retries

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                return await self.client.execute(query, *args)
            except Exception as e:
                last_error = e
                if attempt < max_retries and self._is_connection_error(e):
                    delay = self.config.retry_delay_seconds * (self.config.retry_backoff_multiplier ** attempt)
                    logger.warning(f"Query failed (attempt {attempt + 1}/{max_retries + 1}), reconnecting in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    await self._force_reconnect()
                    continue
                raise
        raise last_error

    async def _fetchrow_with_retry(self, query: str, max_retries: Optional[int] = None) -> Any:
        """Fetchrow with automatic reconnection on failure."""
        if max_retries is None:
            max_retries = self.config.max_retries

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                return await self.client.fetchrow(query)
            except Exception as e:
                last_error = e
                if attempt < max_retries and self._is_connection_error(e):
                    delay = self.config.retry_delay_seconds * (self.config.retry_backoff_multiplier ** attempt)
                    logger.warning(f"Fetchrow failed (attempt {attempt + 1}/{max_retries + 1}), reconnecting in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    await self._force_reconnect()
                    continue
                raise
        raise last_error

    async def _fetch_with_retry(self, query: str, max_retries: Optional[int] = None) -> Optional[List]:
        """Fetch with automatic reconnection on failure."""
        if max_retries is None:
            max_retries = self.config.max_retries

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                return await self.client.fetch(query)
            except Exception as e:
                last_error = e
                if attempt < max_retries and self._is_connection_error(e):
                    delay = self.config.retry_delay_seconds * (self.config.retry_backoff_multiplier ** attempt)
                    logger.warning(f"Fetch failed (attempt {attempt + 1}/{max_retries + 1}), reconnecting in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    await self._force_reconnect()
                    continue
                raise
        raise last_error

    async def _fetchval_with_retry(self, query: str, max_retries: Optional[int] = None) -> Any:
        """Fetchval with automatic reconnection on failure."""
        if max_retries is None:
            max_retries = self.config.max_retries

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                return await self.client.fetchval(query)
            except Exception as e:
                last_error = e
                if attempt < max_retries and self._is_connection_error(e):
                    delay = self.config.retry_delay_seconds * (self.config.retry_backoff_multiplier ** attempt)
                    logger.warning(f"Fetchval failed (attempt {attempt + 1}/{max_retries + 1}), reconnecting in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    await self._force_reconnect()
                    continue
                raise
        raise last_error

    # ===== PUBLIC QUERY METHODS =====

    async def execute(self, query: str, *args) -> Any:
        """
        Execute a query with automatic retry on connection errors.

        Args:
            query: SQL query to execute
            *args: Query arguments

        Returns:
            Query result
        """
        return await self._execute_with_retry(query, *args)

    async def fetch(self, query: str) -> Optional[List]:
        """
        Fetch all rows from a query with automatic retry.

        Args:
            query: SQL query to execute

        Returns:
            List of rows or None
        """
        return await self._fetch_with_retry(query)

    async def fetchrow(self, query: str) -> Any:
        """
        Fetch single row from a query with automatic retry.

        Args:
            query: SQL query to execute

        Returns:
            Single row or None
        """
        return await self._fetchrow_with_retry(query)

    async def fetchval(self, query: str) -> Any:
        """
        Fetch single value from a query with automatic retry.

        Args:
            query: SQL query to execute

        Returns:
            Single value or None
        """
        return await self._fetchval_with_retry(query)

    async def execute_with_data(self, query: str, data: str, max_retries: Optional[int] = None) -> None:
        """
        Execute INSERT query with data payload (for FORMAT CSV/JSONEachRow/etc).
        69x faster than VALUES for bulk inserts.

        Args:
            query: INSERT query with FORMAT clause (e.g., "INSERT INTO table FORMAT CSV")
            data: Data payload as string
            max_retries: Optional max retry count
        """
        if max_retries is None:
            max_retries = self.config.max_retries

        await self.ensure_connected()

        last_error = None
        for attempt in range(max_retries + 1):
            try:
                url = self.config.get_connection_url()
                params = {
                    'query': query,
                    'user': self.config.username,
                    'password': self.config.password,
                    'database': self.config.database
                }

                async with self._session.post(
                    url,
                    params=params,
                    data=data.encode('utf-8'),
                    headers={'Content-Type': 'text/csv'}
                ) as response:
                    if response.status == 200:
                        return
                    else:
                        error_text = await response.text()
                        raise Exception(f"HTTP {response.status}: {error_text}")

            except Exception as e:
                last_error = e
                if attempt < max_retries and self._is_connection_error(e):
                    delay = self.config.retry_delay_seconds * (self.config.retry_backoff_multiplier ** attempt)
                    logger.warning(f"execute_with_data failed (attempt {attempt + 1}/{max_retries + 1}), reconnecting in {delay:.1f}s: {e}")
                    await asyncio.sleep(delay)
                    await self._force_reconnect()
                    continue
                raise
        raise last_error

    def _is_connection_healthy(self) -> bool:
        """Quick connection health check without network calls."""
        return (
                self.is_connected and
                self._session and
                not self._session.closed and
                self.client is not None
        )

    @staticmethod
    def _is_connection_error(error: Exception) -> bool:
        """Check if error is connection-related and retryable."""
        error_str = str(error).lower()

        # Check by exception type
        if isinstance(error, (
                aiohttp.ClientError,
                aiohttp.ServerDisconnectedError,
                aiochclient.ChClientError,
                ConnectionError,
                ConnectionResetError,
                BrokenPipeError,
                OSError,
                asyncio.TimeoutError,
        )):
            return True

        # Check by error message
        retryable_messages = [
            'server disconnected',
            'connection reset',
            'broken pipe',
            'connection refused',
            'network unreachable',
            'timed out',
            'timeout',
        ]
        return any(msg in error_str for msg in retryable_messages)

    async def _ensure_database_schema(self) -> None:
        """Ensure database and required tables exist."""
        try:
            # Create database if not exists
            await self.client.execute(f"CREATE DATABASE IF NOT EXISTS {self.config.database}")

            # Ensure we're using the correct database
            await self.client.execute(f"USE {self.config.database}")

            # Create main trades table if not exists
            await self._create_trades_table()

            # Fix broken partitions before proceeding
            await self._fix_broken_partitions()

            # Create buffer table if enabled
            if self.config.buffer_enabled:
                await self._create_buffer_table()

            await self._cleanup_all_orphaned_mvs()

            logger.debug("Database schema ensured")

        except Exception as e:
            logger.error(f"Failed to ensure database schema: {e}")
            raise

    async def _create_trades_table(self) -> None:
        """Create main trades table with optimized structure."""
        partition_clause = "toYYYYMM(timestamp)"
        if self.config.partition_by_symbol_hash:
            partition_clause += f", cityHash64(symbol) % {self.config.symbol_hash_buckets}"

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS trades_local (
            symbol LowCardinality(String),
            aggregate_id UInt64,
            price Float64 CODEC(DoubleDelta, {self.config.compression_method.upper()}),
            quantity Float64 CODEC(DoubleDelta, {self.config.compression_method.upper()}),
            first_trade_id UInt64,
            last_trade_id UInt64,
            timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, {self.config.compression_method.upper()}),
            is_buyer_maker UInt8,
            insert_time DateTime DEFAULT now() CODEC({self.config.compression_method.upper()})
        ) ENGINE = MergeTree()
        PARTITION BY ({partition_clause})
        ORDER BY (symbol, timestamp, aggregate_id)
        SETTINGS 
            index_granularity = 8192,
            max_threads = {self.config.max_threads},
            max_suspicious_broken_parts = 1000
        """

        await self.client.execute(create_table_query)

        # Add TTL if auto cleanup is enabled
        # if self.config.enable_auto_cleanup and self.config.trades_retention_days > 0:
        #     ttl_query = f"""
        #     ALTER TABLE trades_local
        #     MODIFY TTL timestamp + INTERVAL {self.config.trades_retention_days} DAY
        #     """
        #     try:
        #         await self.client.execute(ttl_query)
        #     except Exception as e:
        #         logger.debug(f"TTL setup note: {e}")

    async def _create_buffer_table(self) -> None:
        """Create buffer table for high-performance inserts."""
        if not self.config.buffer_enabled:
            return

        buffer_config = self.config.get_buffer_table_config()

        create_buffer_query = f"""
        CREATE TABLE IF NOT EXISTS trades_buffer AS trades_local
        ENGINE = Buffer({self.config.database}, trades_local, 
                       {buffer_config['num_layers']}, 
                       {buffer_config['min_time']}, {buffer_config['max_time']},
                       {buffer_config['min_rows']}, {buffer_config['max_rows']},
                       {buffer_config['min_bytes']}, {buffer_config['max_bytes']})
        """

        await self.client.execute(create_buffer_query)

    async def cleanup_all_mvs(self) -> int:
        """
        Manually clean up all OHLCV materialized views.

        Returns:
            Number of MVs dropped
        """
        try:
            query = f"""
            SELECT name 
            FROM system.tables 
            WHERE database = '{self.config.database}'
              AND name LIKE 'ohlcv_%_mv'
              AND engine = 'MaterializedView'
            """

            rows = await self._fetch_with_retry(query)
            dropped_count = 0

            for row in rows:
                mv_name = row[0]
                try:
                    await self.client.execute(f"DROP VIEW IF EXISTS {mv_name}")
                    dropped_count += 1
                    logger.info(f"Dropped MV: {mv_name}")
                except Exception as e:
                    logger.error(f"Failed to drop MV {mv_name}: {e}")

            # Clear tracking
            self._created_mvs.clear()

            logger.info(f"Cleaned up {dropped_count} MVs")
            return dropped_count

        except Exception as e:
            logger.error(f"Failed to cleanup all MVs: {e}")
            return 0

    async def _fix_broken_partitions(self) -> None:
        """
        Automatically fix broken partitions on startup WITHOUT data loss.

        Handles cases where ClickHouse has broken partitions after unexpected shutdown.
        """
        try:
            # Check if table exists and is accessible
            check_query = f"""
            SELECT engine 
            FROM system.tables 
            WHERE database = '{self.config.database}' 
              AND name = 'trades_local'
            """

            try:
                result = await self.client.fetchval(check_query)

                if result:
                    # Table exists and is accessible - check for broken parts
                    broken_query = f"""
                    SELECT COUNT(*) 
                    FROM system.detached_parts
                    WHERE database = '{self.config.database}'
                      AND table = 'trades_local'
                    """

                    try:
                        broken_count = await self.client.fetchval(broken_query)

                        if broken_count and broken_count > 0:
                            logger.warning(f"Found {broken_count} broken partitions, attempting to fix...")

                            # Increase limit and reload table
                            await self.client.execute(f"""
                                ALTER TABLE trades_local 
                                MODIFY SETTING max_suspicious_broken_parts = 1000
                            """)

                            await self.client.execute("DETACH TABLE trades_local")
                            await asyncio.sleep(1)
                            await self.client.execute("ATTACH TABLE trades_local")

                            logger.info(f"Successfully fixed {broken_count} broken partitions")
                        else:
                            logger.debug("No broken partitions found")

                    except Exception as e:
                        logger.debug(f"Could not check detached parts: {e}")

                    return

            except Exception as e:
                # Table exists but cannot be accessed (broken state)
                error_str = str(e)

                if "TOO_MANY_UNEXPECTED_DATA_PARTS" in error_str or "ASYNC_LOAD_WAIT_FAILED" in error_str:
                    logger.error("Table trades_local has broken partitions preventing load")
                    logger.warning("Attempting to fix WITHOUT data loss...")

                    try:
                        # Try to modify settings even if table is not fully loaded
                        await self.client.execute(f"""
                            ALTER TABLE trades_local 
                            MODIFY SETTING max_suspicious_broken_parts = 1000
                        """)

                        logger.info("Increased partition limit, reloading table...")

                        await self.client.execute("DETACH TABLE trades_local")
                        await asyncio.sleep(2)
                        await self.client.execute("ATTACH TABLE trades_local")

                        logger.info("Successfully fixed broken partitions")

                    except Exception as fix_error:
                        logger.error(f"Could not auto-fix broken partitions: {fix_error}")
                        logger.error("=" * 60)
                        logger.error("MANUAL FIX REQUIRED:")
                        logger.error("1. Stop data-provider")
                        logger.error("2. Stop ClickHouse: sudo service clickhouse-server stop")
                        logger.error("3. Edit: sudo nano /var/lib/clickhouse/metadata/trades/trades_local.sql")
                        logger.error("4. Change: SETTINGS index_granularity = 8192")
                        logger.error("   To: SETTINGS index_granularity = 8192, max_suspicious_broken_parts = 1000")
                        logger.error("5. Start ClickHouse: sudo service clickhouse-server start")
                        logger.error("6. Start data-provider")
                        logger.error("=" * 60)
                        raise RuntimeError("Cannot fix broken partitions automatically - manual intervention required")
                else:
                    # Different error - re-raise
                    raise

        except Exception as e:
            # Table doesn't exist yet or other non-critical error
            logger.debug(f"Partition check skipped: {e}")

    # ===== BINARY FILE MANAGER COMPATIBLE INTERFACE =====

    async def write_trades(self, symbol: str, trades: List[AggTrade]) -> int:
        """
        Write trades to ClickHouse for specific symbol.

        Args:
            symbol: Ticker symbol
            trades: List of AggTrade instances

        Returns:
            Number of trades written
        """
        if not trades:
            return 0

        try:
            # Convert trades to ClickHouse format
            data_rows = []
            for trade in trades:
                dt_str = datetime.fromtimestamp(
                    trade.timestamp / 1000,
                    tz=timezone.utc
                ).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                data_rows.append([
                    symbol,  # Теперь используем переданный symbol
                    trade.aggregate_id,
                    trade.price,
                    trade.quantity,
                    trade.first_trade_id,
                    trade.last_trade_id,
                    dt_str,
                    1 if trade.is_buyer_maker else 0
                ])

            target_table = "trades_buffer" if self.config.buffer_enabled else "trades_local"

            batch_size = min(len(data_rows), self.config.max_insert_block_size)
            for i in range(0, len(data_rows), batch_size):
                batch = data_rows[i:i + batch_size]
                query = f"""
                INSERT INTO {target_table} 
                (symbol, aggregate_id, price, quantity, first_trade_id, last_trade_id, timestamp, is_buyer_maker)
                VALUES
                """
                await self._execute_with_retry(query, *batch)

            if self.config.verbose_trade_logging:
                logger.debug(f"Wrote {len(trades)} trades for {symbol}")

            return len(trades)

        except Exception as e:
            logger.error(f"Failed to write trades for {symbol}: {e}")
            raise

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """
        Read trades from ClickHouse within time range.

        Args:
            symbol: Ticker symbol
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            List of AggTrade instances
        """
        # Convert timestamps to datetime strings
        start_dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
        start_dt_str = start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_dt = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)
        end_dt_str = end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        try:
            query = f"""
            SELECT 
                aggregate_id,
                price,
                quantity,
                first_trade_id,
                last_trade_id,
                toUnixTimestamp64Milli(timestamp) as timestamp_ms,
                is_buyer_maker
            FROM trades_local
            WHERE symbol = '{symbol}'
              AND timestamp >= '{start_dt_str}'
              AND timestamp <= '{end_dt_str}'
            ORDER BY timestamp, aggregate_id
            """

            rows = await self._fetch_with_retry(query)

            trades = []
            for row in rows:
                trade = AggTrade(
                    aggregate_id=row[0],
                    price=float(row[1]),
                    quantity=float(row[2]),
                    first_trade_id=row[3],
                    last_trade_id=row[4],
                    timestamp=int(row[5]),
                    is_buyer_maker=bool(row[6])
                )
                trades.append(trade)

            logger.debug(f"Read {len(trades)} trades for {symbol}")
            return trades

        except Exception as e:
            logger.error(f"Failed to read trades for {symbol}: {e}")
            raise

    async def get_file_stats(self, symbol: str) -> Dict[str, Any]:
        """Async implementation of get_file_stats for specific symbol."""
        try:
            stats_query = f"""
            SELECT 
                COUNT(*) as total_trades,
                MIN(toUnixTimestamp64Milli(timestamp)) as first_trade_time,
                MAX(toUnixTimestamp64Milli(timestamp)) as last_trade_time
            FROM trades_local
            WHERE symbol = '{symbol}'
            """

            result = await self._fetchrow_with_retry(stats_query)

            if result:
                total_trades = result[0] or 0
                first_time = result[1]
                last_time = result[2]
            else:
                total_trades = 0
                first_time = None
                last_time = None

            return {
                'symbol': symbol,
                'total_files': 1,
                'compressed_files': 1,
                'estimated_trades': total_trades,
                'first_trade_time': first_time,
                'last_trade_time': last_time,
                'date_range': {
                    'start': first_time,
                    'end': last_time
                } if first_time and last_time else None,
                'buffer_enabled': self.config.buffer_enabled
            }

        except Exception as e:
            logger.error(f"Failed to get file stats for {symbol}: {e}")
            return {
                'symbol': symbol,
                'total_files': 0,
                'estimated_trades': 0,
                'error': str(e)
            }

    async def get_available_dates(self, symbol: str) -> List[str]:
        """
        Async implementation of get_available_dates for specific symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            List of available date strings
        """
        try:
            query = f"""
            SELECT DISTINCT toDate(timestamp) as trade_date
            FROM trades_local
            WHERE symbol = '{symbol}'
            ORDER BY trade_date
            """

            rows = await self._fetch_with_retry(query)
            dates = [str(row[0]) for row in rows]

            return dates

        except Exception as e:
            logger.error(f"Failed to get available dates for {symbol}: {e}")
            return []

    # ===== CLICKHOUSE-SPECIFIC METHODS =====
    async def _cleanup_all_orphaned_mvs(self) -> None:
        """Clean up all orphaned materialized views on startup."""
        try:
            # Найти все существующие OHLCV MV
            query = f"""
            SELECT name 
            FROM system.tables 
            WHERE database = '{self.config.database}'
              AND name LIKE 'ohlcv_%_mv'
              AND engine = 'MaterializedView'
            """

            rows = await self._fetch_with_retry(query)

            if rows:
                logger.info(f"Found {len(rows)} orphaned MV to cleanup on startup")

                for row in rows:
                    mv_name = row[0]
                    try:
                        await self.client.execute(f"DROP VIEW IF EXISTS {mv_name}")
                        logger.info(f"Dropped orphaned MV: {mv_name}")
                    except Exception as e:
                        logger.error(f"Failed to drop orphaned MV {mv_name}: {e}")

                logger.info(f"Completed cleanup of {len(rows)} orphaned MVs")
            else:
                logger.debug("No orphaned MVs found on startup")

        except Exception as e:
            logger.error(f"Failed to cleanup orphaned MVs: {e}")

    async def create_ticker_mvs(self, symbol: str, timeframes_minutes: List[int]) -> None:
        """
        Create materialized views for OHLCV data with minute-based timeframes.
        Automatically populates MV with existing historical data.

        Args:
            symbol: Ticker symbol
            timeframes_minutes: List of timeframes in minutes (e.g., [1, 5, 15, 60, 240, 1440])
        """
        for minutes in timeframes_minutes:
            try:
                mv_name = f"ohlcv_{minutes}_{symbol.lower()}_mv"

                # Universal interval function - epoch-aligned for all timeframes
                interval_func = f"toStartOfInterval(timestamp, INTERVAL {minutes} MINUTE)"

                create_mv_query = f"""
                CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}
                ENGINE = AggregatingMergeTree()
                PARTITION BY toYYYYMM(candle_time)
                ORDER BY (symbol, candle_time)
                SETTINGS index_granularity = 8192
                AS SELECT
                  '{symbol}' as symbol,
                  {interval_func} as candle_time,
                  argMinState(price, timestamp) as open_state,
                  maxState(price) as high_state,
                  minState(price) as low_state,
                  argMaxState(price, timestamp) as close_state,
                  sumState(quantity) as volume_state,
                  countState() as trades_count_state,
                  sumState(CASE WHEN is_buyer_maker = 0 THEN quantity ELSE 0 END) as taker_buy_volume_state,
                  sumState(CASE WHEN is_buyer_maker = 1 THEN quantity ELSE 0 END) as taker_sell_volume_state
                FROM trades_local
                WHERE symbol = '{symbol}'
                GROUP BY candle_time
                """

                await self._execute_with_retry(create_mv_query)
                self._created_mvs[minutes] = mv_name

                logger.info(f"Created MV: {mv_name} for {minutes}m timeframe")

                # Populate MV with existing historical data
                await self._populate_mv_historical_data(symbol, minutes, interval_func)

            except Exception as e:
                logger.error(f"Failed to create MV for {symbol} {minutes}m: {e}")

    async def _populate_mv_historical_data(self, symbol: str, minutes: int, interval_func: str) -> None:
        """
        Populate materialized view with existing historical data.

        Args:
            symbol: Ticker symbol
            minutes: Timeframe in minutes
            interval_func: ClickHouse interval function
        """
        try:
            mv_name = f"ohlcv_{minutes}_{symbol.lower()}_mv"

            # Verify MV exists before populating
            check_query = f"EXISTS TABLE {mv_name}"
            mv_exists = await self._fetchval_with_retry(check_query)
            if not mv_exists:
                logger.error(f"Cannot populate {mv_name} - table does not exist")
                return

            # Check if we have existing data for this symbol
            count_query = f"SELECT COUNT(*) FROM trades_local WHERE symbol = '{symbol}'"
            trade_count = await self._fetchval_with_retry(count_query)

            if trade_count == 0:
                logger.debug(f"No historical data to populate for {symbol}")
                return

            # Populate MV with historical data
            populate_query = f"""
            INSERT INTO {mv_name}
            SELECT
              '{symbol}' as symbol,
              {interval_func} as candle_time,
                argMinState(price, timestamp) as open_state,
                maxState(price) as high_state,
                minState(price) as low_state,
                argMaxState(price, timestamp) as close_state,
                sumState(quantity) as volume_state,
                countState() as trades_count_state,
                sumState(CASE WHEN is_buyer_maker = 0 THEN quantity ELSE 0 END) as taker_buy_volume_state,
                sumState(CASE WHEN is_buyer_maker = 1 THEN quantity ELSE 0 END) as taker_sell_volume_state
            FROM trades_local
            WHERE symbol = '{symbol}'
            GROUP BY candle_time
            ORDER BY candle_time
            """

            await self._execute_with_retry(populate_query)

            # Get populated candle count
            candle_count_query = f"SELECT COUNT(*) FROM {mv_name}"
            candle_count = await self._fetchval_with_retry(candle_count_query)

            logger.info(f"Populated {mv_name} with {candle_count} candles from {trade_count} historical trades")

        except Exception as e:
            logger.error(f"Failed to populate historical data for {symbol} {minutes}m: {e}")

    async def get_historical_klines(
            self,
            symbol: str,
            timeframe_minutes: int,
            start_time: Optional[int] = None,
            limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """Get historical OHLCV data from materialized views."""
        mv_name = f"ohlcv_{timeframe_minutes}_{symbol.lower()}_mv"

        try:
            if start_time is None:
                # Case 1: No start_time - return latest candles
                query = f"""
                SELECT 
                    toUnixTimestamp(candle_time) * 1000 as timestamp,
                    argMinMerge(open_state) as open,
                    maxMerge(high_state) as high,
                    minMerge(low_state) as low,
                    argMaxMerge(close_state) as close,
                    sumMerge(volume_state) as volume,
                    countMerge(trades_count_state) as trades_count,
                    sumMerge(taker_buy_volume_state) as taker_buy_volume,
                    sumMerge(taker_sell_volume_state) as taker_sell_volume
                FROM {mv_name}
                GROUP BY candle_time
                ORDER BY candle_time DESC
                LIMIT {limit}
                """

                rows = await self._fetch_with_retry(query)
                rows = list(reversed(rows))

            else:
                # Case 2 & 3: start_time provided
                start_dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
                start_dt_str = start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

                earliest_query = f"SELECT MIN(candle_time) as earliest_time FROM {mv_name}"
                earliest_result = await self._fetchval_with_retry(earliest_query)

                if earliest_result is None:
                    return []

                earliest_timestamp = int(earliest_result.timestamp() * 1000)

                if start_time < earliest_timestamp:
                    # Case 2: start_time is before earliest data
                    query = f"""
                    SELECT 
                        toUnixTimestamp(candle_time) * 1000 as timestamp,
                        argMinMerge(open_state) as open,
                        maxMerge(high_state) as high,
                        minMerge(low_state) as low,
                        argMaxMerge(close_state) as close,
                        sumMerge(volume_state) as volume,
                        countMerge(trades_count_state) as trades_count,
                        sumMerge(taker_buy_volume_state) as taker_buy_volume,
                        sumMerge(taker_sell_volume_state) as taker_sell_volume
                    FROM {mv_name}
                    GROUP BY candle_time
                    ORDER BY candle_time ASC
                    LIMIT {limit}
                    """
                else:
                    # Case 3: start_time is valid
                    query = f"""
                    SELECT 
                        toUnixTimestamp(candle_time) * 1000 as timestamp,
                        argMinMerge(open_state) as open,
                        maxMerge(high_state) as high,
                        minMerge(low_state) as low,
                        argMaxMerge(close_state) as close,
                        sumMerge(volume_state) as volume,
                        countMerge(trades_count_state) as trades_count,
                        sumMerge(taker_buy_volume_state) as taker_buy_volume,
                        sumMerge(taker_sell_volume_state) as taker_sell_volume
                    FROM {mv_name}
                    WHERE candle_time >= '{start_dt_str}'
                    GROUP BY candle_time
                    ORDER BY candle_time ASC
                    LIMIT {limit}
                    """

                rows = await self._fetch_with_retry(query)

            # Convert rows to klines format
            klines = []
            for row in rows:
                kline = {
                    'timestamp': int(row[0]),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': float(row[5]),
                    'trades_count': int(row[6]),
                    'taker_buy_volume': float(row[7]) if row[7] is not None else 0.0,
                    'taker_sell_volume': float(row[8]) if row[8] is not None else 0.0,
                    'timeframe_minutes': timeframe_minutes
                }
                klines.append(kline)

            logger.debug(f"Retrieved {len(klines)} {timeframe_minutes}m klines for {symbol}")
            return klines

        except Exception as e:
            logger.error(f"Failed to get historical klines for {symbol} {timeframe_minutes}m: {e}")
            return []

    async def get_historical_klines_fast(
            self,
            symbol: str,
            timeframe_minutes: int,
            limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        Get historical OHLCV data using 1m projection (fast).

        Note: taker_buy_volume and taker_sell_volume are not included
        for performance reasons (projection doesn't have is_buyer_maker).
        """
        try:
            if timeframe_minutes == 1:
                query = f"""
                SELECT 
                    toUnixTimestamp(candle_time) * 1000 as timestamp,
                    open, high, low, close, volume, trades_count
                FROM (
                    SELECT 
                        toStartOfMinute(timestamp) as candle_time,
                        argMin(price, timestamp) as open,
                        max(price) as high,
                        min(price) as low,
                        argMax(price, timestamp) as close,
                        sum(quantity) as volume,
                        count() as trades_count
                    FROM trades_local
                    WHERE symbol = '{symbol}'
                    GROUP BY candle_time
                    ORDER BY candle_time DESC
                    LIMIT {limit}
                )
                ORDER BY timestamp ASC
                """
            else:
                # Two-level aggregation for other timeframes
                query = f"""
                        SELECT 
                            toUnixTimestamp(candle_tf) * 1000 as timestamp,
                            argMin(open, candle_time) as open,
                            max(high) as high,
                            min(low) as low,
                            argMax(close, candle_time) as close,
                            sum(volume) as volume,
                            sum(trades_count) as trades_count
                        FROM (
                            SELECT 
                                toStartOfMinute(timestamp) as candle_time,
                                argMin(price, timestamp) as open,
                                max(price) as high,
                                min(price) as low,
                                argMax(price, timestamp) as close,
                                sum(quantity) as volume,
                                count() as trades_count
                            FROM trades_local
                            WHERE symbol = '{symbol}'
                            GROUP BY candle_time
                        )
                        GROUP BY toStartOfInterval(candle_time, INTERVAL {timeframe_minutes} MINUTE) as candle_tf
                        ORDER BY candle_tf DESC
                        LIMIT {limit}
                        """

            rows = await self._fetch_with_retry(query)
            rows = list(reversed(rows))

            return [
                {
                    'timestamp': int(row[0]),
                    'open': float(row[1]),
                    'high': float(row[2]),
                    'low': float(row[3]),
                    'close': float(row[4]),
                    'volume': float(row[5]),
                    'trades_count': int(row[6]),
                    'taker_buy_volume': None,
                    'taker_sell_volume': None
                }
                for row in rows
            ]

        except Exception as e:
            logger.error(f"Failed to get historical klines fast for {symbol}:{timeframe_minutes}m: {e}")
            return []

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """
        Get the last aggregate_id for specific symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            Last aggregate_id or None if no data exists
        """
        try:
            query = f"""
            SELECT MAX(aggregate_id) as max_id
            FROM trades_local 
            WHERE symbol = '{symbol}'
            """

            result = await self._fetchval_with_retry(query)
            return int(result) if result is not None else None

        except Exception as e:
            logger.error(f"Failed to get last aggregate_id for {symbol}: {e}")
            return None

    async def batch_get_last_aggregate_ids(self, symbols: List[str]) -> Dict[str, Optional[int]]:
        """
        Get last aggregate_ids for multiple symbols in SINGLE batch query.

        Critical optimization: Replaces N individual SELECT MAX() queries
        with one grouped query. Essential for SATA disk performance.

        Args:
            symbols: List of symbol names

        Returns:
            Dictionary: symbol -> last_aggregate_id (or None if no data)
        """
        if not symbols:
            return {}

        try:
            symbols_list = "','".join(symbols)

            query = f"""
            SELECT 
                symbol,
                MAX(aggregate_id) as max_id
            FROM trades_local 
            WHERE symbol IN ('{symbols_list}')
            GROUP BY symbol
            """

            rows = await self._fetch_with_retry(query)

            # Initialize result with None for all symbols
            result = {symbol: None for symbol in symbols}

            # Fill in actual values from query results
            for row in rows:
                symbol = row[0]
                max_id = int(row[1]) if row[1] is not None else None
                result[symbol] = max_id

            logger.debug(f"Batch retrieved last aggregate_ids for {len(symbols)} symbols, {len(rows)} with data")
            return result

        except Exception as e:
            logger.error(f"Failed to batch get last aggregate_ids for {len(symbols)} symbols: {e}")
            # Fallback: return None for all symbols
            return {symbol: None for symbol in symbols}

    async def get_latest_candle(self, symbol: str, timeframe_minutes: int) -> Optional[Dict[str, Any]]:
        """
        Get the latest candle for a specific timeframe.

        Args:
            symbol: Ticker symbol
            timeframe_minutes: Timeframe in minutes

        Returns:
            Latest candle dictionary or None
        """
        klines = await self.get_historical_klines(symbol, timeframe_minutes, start_time=None, limit=1)
        return klines[0] if klines else None

    async def get_ohlcv_range(
            self,
            symbol: str,
            timeframe_minutes: int,
            start_time: int,
            end_time: int
    ) -> List[Dict[str, Any]]:
        """
        Get OHLCV data within exact time range.
        Works with or without materialized views.
        """
        mv_name = f"ohlcv_{timeframe_minutes}_{symbol.lower()}_mv"

        start_dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc)
        start_dt_str = start_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        end_dt_str = end_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Check if MV exists
        check_query = f"EXISTS TABLE {mv_name}"
        mv_exists = await self._fetchval_with_retry(check_query)

        if mv_exists:
            # Use MV (fast)
            query = f"""
            SELECT 
                toUnixTimestamp(candle_time) * 1000 as timestamp,
                argMinMerge(open_state) as open,
                maxMerge(high_state) as high,
                minMerge(low_state) as low,
                argMaxMerge(close_state) as close,
                sumMerge(volume_state) as volume,
                countMerge(trades_count_state) as trades_count
            FROM {mv_name}
            WHERE candle_time >= '{start_dt_str}'
              AND candle_time <= '{end_dt_str}'
            GROUP BY candle_time
            ORDER BY candle_time ASC
            """
        else:
            # Calculate on-the-fly using intDiv for alignment
            interval_seconds = timeframe_minutes * 60
            query = f"""
            SELECT 
                intDiv(toUnixTimestamp(timestamp), {interval_seconds}) * {interval_seconds} * 1000 as candle_timestamp,
                argMin(price, timestamp) as open,
                max(price) as high,
                min(price) as low,
                argMax(price, timestamp) as close,
                sum(quantity) as volume,
                count() as trades_count
            FROM trades_local
            WHERE symbol = '{symbol}'
              AND timestamp >= '{start_dt_str}'
              AND timestamp <= '{end_dt_str}'
            GROUP BY intDiv(toUnixTimestamp(timestamp), {interval_seconds})
            ORDER BY candle_timestamp ASC
            """

        rows = await self._fetch_with_retry(query)

        klines = []
        for row in rows:
            kline = {
                'timestamp': int(row[0]),
                'open': float(row[1]),
                'high': float(row[2]),
                'low': float(row[3]),
                'close': float(row[4]),
                'volume': float(row[5]),
                'trades_count': int(row[6])
            }
            klines.append(kline)

        logger.debug(f"Retrieved {len(klines)} {timeframe_minutes}m candles for {symbol} ({'MV' if mv_exists else 'on-the-fly'})")
        return klines

    async def drop_ticker_mvs(self, symbol: str, timeframes_minutes: Optional[List[int]] = None) -> None:
        """
        Drop materialized views for a ticker.

        Args:
            symbol: Ticker symbol
            timeframes_minutes: Specific timeframes to drop, or None for all
        """
        # If no specific timeframes provided, drop all created MVs for this symbol
        if timeframes_minutes is None:
            timeframes_minutes = list(self._created_mvs.keys())

        for minutes in timeframes_minutes:
            mv_name = f"ohlcv_{minutes}_{symbol.lower()}_mv"

            try:
                await self.client.execute(f"DROP VIEW IF EXISTS {mv_name}")
                if minutes in self._created_mvs:
                    del self._created_mvs[minutes]
                logger.info(f"Dropped materialized view: {mv_name}")

            except Exception as e:
                logger.error(f"Failed to drop MV {mv_name}: {e}")

    async def batch_get_current_candles_realtime(
            self,
            queries: Set[Tuple[str, int]]
    ) -> Dict[Tuple[str, int], Optional[Dict[str, Any]]]:
        """Batch real-time query for multiple (symbol, timeframe) pairs."""
        if not queries:
            return {}

        # current_time_ms = int(time.time() * 1000)

        current_time_query = "SELECT toUnixTimestamp(now('UTC')) * 1000"
        current_time_ms = int(await self._fetchval_with_retry(current_time_query))
        results = {}

        # Группируем по таймфреймам
        by_timeframe = {}
        for symbol, tf in queries:
            if tf not in by_timeframe:
                by_timeframe[tf] = []
            by_timeframe[tf].append(symbol)

        for timeframe_minutes, symbols in by_timeframe.items():
            interval_ms = timeframe_minutes * 60 * 1000
            candle_start_ms = (current_time_ms // interval_ms) * interval_ms
            candle_end_ms = candle_start_ms + interval_ms

            if self.config.verbose_trade_logging:
                logger.info(f"DEBUG batch query: tf={timeframe_minutes}m, current_ms={current_time_ms}, "
                            f"candle_start_ms={candle_start_ms}, candle_end_ms={candle_end_ms}")

            # Конвертация для проверки
            start_dt = datetime.fromtimestamp(candle_start_ms / 1000, tz=timezone.utc)
            end_dt = datetime.fromtimestamp(candle_end_ms / 1000, tz=timezone.utc)
            if self.config.verbose_trade_logging:
                logger.info(f"DEBUG timestamps: start={start_dt.isoformat()}, end={end_dt.isoformat()}")

            start_dt_str = datetime.fromtimestamp(candle_start_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            end_dt_str = datetime.fromtimestamp(candle_end_ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

            symbols_list = "','".join(symbols)

            # Используем fromUnixTimestamp64Milli для конвертации миллисекунд в DateTime64
            query = f"""
            SELECT 
                symbol,
                {candle_start_ms} as candle_time,
                argMin(price, timestamp) as open,
                max(price) as high,
                min(price) as low,
                argMax(price, timestamp) as close,
                sum(quantity) as volume,
                count() as trades_count,
                sum(CASE WHEN is_buyer_maker = 0 THEN quantity ELSE 0 END) as taker_buy_volume,
                sum(CASE WHEN is_buyer_maker = 1 THEN quantity ELSE 0 END) as taker_sell_volume
            FROM trades_local
            WHERE symbol IN ('{symbols_list}')
              AND timestamp >= '{start_dt_str}'
            AND timestamp < '{end_dt_str}'
            GROUP BY symbol
            """
            if self.config.verbose_trade_logging:
                logger.info(f"Executing batch query for {timeframe_minutes}m: {symbols}")

            try:
                rows = await self._fetch_with_retry(query)
                if self.config.verbose_trade_logging:
                    logger.info(f"Query returned {len(rows)} rows for {timeframe_minutes}m")

                for row in rows:
                    symbol = row[0]
                    results[(symbol, timeframe_minutes)] = {
                        "candle_time": int(row[1]),
                        "open": float(row[2]),
                        "high": float(row[3]),
                        "low": float(row[4]),
                        "close": float(row[5]),
                        "volume": float(row[6]),
                        "trades_count": int(row[7]),
                        "taker_buy_volume": float(row[8]) if row[8] is not None else 0.0,
                        "taker_sell_volume": float(row[9]) if row[9] is not None else 0.0
                    }

            except Exception as e:
                logger.error(f"Batch realtime query failed for {timeframe_minutes}m: {e}")

        return results

    async def create_custom_mv(
            self,
            symbol: str,
            timeframe_minutes: int,
            populate_historical: bool = True
    ) -> bool:
        """Create custom MV for specific timeframe with optional historical population."""
        async with self._mv_creation_lock:
            try:
                mv_name = f"ohlcv_{timeframe_minutes}_{symbol.lower()}_mv"

                # Check if MV already exists
                check_query = f"EXISTS TABLE {mv_name}"
                exists = await self.client.fetchval(check_query)

                if exists:
                    logger.debug(f"MV {mv_name} already exists, reusing")
                    return True  # MV exists - this is success

                # Create MV structure
                interval_func = f"toStartOfInterval(timestamp, INTERVAL {timeframe_minutes} MINUTE)"

                create_mv_query = f"""
                CREATE MATERIALIZED VIEW {mv_name}
                ENGINE = AggregatingMergeTree()
                PARTITION BY toYYYYMM(candle_time)
                ORDER BY (symbol, candle_time)
                SETTINGS index_granularity = 8192
                AS SELECT
                  '{symbol}' as symbol,
                  {interval_func} as candle_time,
                  argMinState(price, timestamp) as open_state,
                  maxState(price) as high_state,
                  minState(price) as low_state,
                  argMaxState(price, timestamp) as close_state,
                  sumState(quantity) as volume_state,
                  countState() as trades_count_state,
                  sumState(CASE WHEN is_buyer_maker = 0 THEN quantity ELSE 0 END) as taker_buy_volume_state,
                  sumState(CASE WHEN is_buyer_maker = 1 THEN quantity ELSE 0 END) as taker_sell_volume_state
                FROM trades_local
                WHERE symbol = '{symbol}'
                GROUP BY candle_time
                """

                await self.client.execute(create_mv_query)
                self._created_mvs[timeframe_minutes] = mv_name

                # Populate historical data if requested
                if populate_historical:
                    await self._populate_mv_historical_data(symbol, timeframe_minutes, interval_func)

                logger.info(f"Created custom MV: {mv_name}")
                return True

            except Exception as e:
                logger.error(f"Failed to create custom MV {symbol}:{timeframe_minutes}m: {e}")
                return False

    async def drop_custom_mv(self, symbol: str, timeframe_minutes: int) -> bool:
        """Drop custom MV for specific symbol and timeframe."""
        try:
            mv_name = f"ohlcv_{timeframe_minutes}_{symbol.lower()}_mv"

            drop_query = f"DROP VIEW IF EXISTS {mv_name}"
            await self.client.execute(drop_query)

            # Remove from tracking
            if timeframe_minutes in self._created_mvs:
                del self._created_mvs[timeframe_minutes]

            logger.info(f"Dropped custom MV: {mv_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to drop custom MV {symbol}:{timeframe_minutes}m: {e}")
            return False

    async def list_ticker_mvs(self, symbol: str) -> List[Dict[str, Any]]:
        """List all MV for specific symbol with statistics."""
        try:
            query = f"""
            SELECT 
                name,
                engine,
                total_rows,
                total_bytes
            FROM system.tables
            WHERE database = '{self.config.database}'
              AND name LIKE 'ohlcv_%_{symbol.lower()}_mv'
              AND engine = 'MaterializedView'
            ORDER BY name
            """

            rows = await self._fetch_with_retry(query)

            mvs = []
            for row in rows:
                # Extract timeframe from name: ohlcv_{timeframe}_{symbol}_mv
                name_parts = row[0].split('_')
                if len(name_parts) >= 2 and name_parts[1].isdigit():
                    timeframe = int(name_parts[1])
                    mvs.append({
                        'name': row[0],
                        'timeframe_minutes': timeframe,
                        'engine': row[1],
                        'total_rows': row[2] or 0,
                        'total_bytes': row[3] or 0
                    })

            return mvs

        except Exception as e:
            logger.error(f"Failed to list MV for {symbol}: {e}")
            return []

    async def populate_mv_historical(self, symbol: str, timeframe_minutes: int) -> bool:
        """
        Populate MV with historical data.

        Args:
            symbol: Ticker symbol
            timeframe_minutes: Timeframe in minutes

        Returns:
            True if successful
        """
        try:
            interval_func = f"toStartOfInterval(timestamp, INTERVAL {timeframe_minutes} MINUTE)"
            await self._populate_mv_historical_data(symbol, timeframe_minutes, interval_func)
            logger.info(f"Populated MV historical data for {symbol}:{timeframe_minutes}m")
            return True
        except Exception as e:
            logger.error(f"Failed to populate MV historical data for {symbol}:{timeframe_minutes}m: {e}")
            return False

    async def health_check(self) -> Dict[str, Any]:
        """
        Perform ClickHouse health check.

        Returns:
            Dictionary with health status
        """
        try:
            # Test basic connectivity
            result = await self.client.fetchval("SELECT 1")
            if result != 1:
                raise Exception("Basic connectivity test failed")

            # Get system info
            try:
                system_info = await self.client.fetchrow("""
                    SELECT 
                        version() as version,
                        uptime() as uptime
                """)
                version = system_info[0] if system_info else 'unknown'
                uptime = system_info[1] if system_info else 0
            except Exception:
                version = 'unknown'
                uptime = 0

            # Test write capability to buffer table
            test_write = True
            try:
                if self.config.buffer_enabled:
                    await self.client.execute("SELECT 1 FROM trades_buffer LIMIT 1")
                else:
                    await self.client.execute("SELECT 1 FROM {self.config.database}.trades_local LIMIT 1")
            except Exception:
                test_write = False

            return {
                'status': 'healthy',
                'connected': self.is_connected,
                'version': version,
                'uptime_seconds': uptime,
                'materialized_views': len(self._created_mvs),
                'trades_written': self._total_trades_written,
                'buffer_enabled': self.config.buffer_enabled,
                'can_write': test_write,
                'connection_time': self._connection_time
            }

        except Exception as e:
            return {
                'status': 'unhealthy',
                'connected': False,
                'error': str(e),
            }

    async def optimize_tables(self) -> None:
        """Optimize ClickHouse tables for better performance."""
        try:
            # Force buffer flush
            if self.config.buffer_enabled:
                await self.client.execute("OPTIMIZE TABLE trades_buffer")

            # Optimize main table
            await self.client.execute("OPTIMIZE TABLE trades_local FINAL")

            # Optimize materialized views
            for mv_name in self._created_mvs.values():
                await self.client.execute(f"OPTIMIZE TABLE {mv_name} FINAL")

            logger.info(f"Optimized tables")

        except Exception as e:
            logger.error(f"Failed to optimize tables: {e}")

    # ===== ASYNC CONTEXT MANAGER SUPPORT =====

    async def __aenter__(self):
        """Async context manager entry."""
        await self.ensure_connected()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._cleanup_connection()

    # ===== STRING REPRESENTATIONS =====

    def __str__(self) -> str:
        status = 'connected' if self.is_connected else 'disconnected'
        return f"ClickHouseManager({status})"

    def __repr__(self) -> str:
        return (
            f"ClickHouseManager(connected={self.is_connected}, "
            f"buffer_enabled={self.config.buffer_enabled})"
        )
