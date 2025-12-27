"""
TickerSynchronizer - Binance Exchange Info Management
=====================================================

Simple ticker synchronization from Binance futures API with direct state management.
Compares exchangeInfo with active tickers and adds/removes as needed.

File: src/coordinators/ticker_synchronizer.py
"""

import asyncio
import time
from typing import Dict, Set, Optional

from loggerino import loggerino
from binance import AsyncClient

from ..models.config import ProcessingConfig
from ..models.exchange_info import BinanceExchangeInfo
from .telegram_notifier import get_notifier, AlertLevel

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .shared import Shared

logger = loggerino.get('ticker_synchronizer')


def is_valid_symbol(symbol: str) -> bool:
    """Validate symbol: only 0-9 and A-Z allowed."""
    return bool(symbol) and all(c.isdigit() or 'A' <= c <= 'Z' for c in symbol)


class TickerSynchronizer:
    """
    Simple Binance futures ticker synchronization coordinator.

    Responsibilities:
    - Fetch exchangeInfo from Binance API every 5 minutes
    - Filter symbols based on configuration and trading status
    - Compare with active tickers in Shared
    - Add new tickers, remove delisted tickers
    - Direct integration with Shared (no complex callbacks)
    """

    def __init__(
            self,
            binance_client: AsyncClient,
            shared: 'Shared',
            config: ProcessingConfig
    ):
        """
        Initialize TickerSynchronizer.

        Args:
            binance_client: Binance AsyncClient instance
            shared: Shared resource manager instance
            config: Processing configuration
        """
        self.binance_client = binance_client
        self.shared = shared
        self.config = config

        # Synchronization state
        self._is_running: bool = False
        self._sync_task: Optional[asyncio.Task] = None
        self._last_sync_time: Optional[int] = None

        # Current exchange info cache
        self._current_exchange_info: Dict[str, BinanceExchangeInfo] = {}

        # Sync configuration
        self.sync_interval_seconds: int = 300  # 5 minutes
        self.initial_sync_delay: int = 5  # Delay before first sync

        # Error handling
        self.max_consecutive_failures: int = 5
        self._consecutive_failures: int = 0

        logger.info("TickerSynchronizer initialized")

    # ===== LIFECYCLE METHODS =====

    async def start(self) -> bool:
        """
        Start ticker synchronization service.

        Returns:
            True if started successfully, False otherwise
        """
        if self._is_running:
            logger.warning("TickerSynchronizer already running")
            return True

        try:
            logger.info("Starting TickerSynchronizer...")

            # Perform initial sync immediately
            await self._sync_cycle()

            # Start periodic sync task
            self._sync_task = asyncio.create_task(
                self._sync_loop(),
                name="ticker_synchronizer"
            )

            self._is_running = True
            logger.info("TickerSynchronizer started successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to start TickerSynchronizer: {e}", exc_info=True)
            return False

    async def stop(self) -> None:
        """Stop ticker synchronization service."""
        if not self._is_running:
            logger.warning("TickerSynchronizer not running")
            return

        try:
            logger.info("Stopping TickerSynchronizer...")

            self._is_running = False

            # Cancel sync task
            if self._sync_task and not self._sync_task.done():
                self._sync_task.cancel()
                try:
                    await self._sync_task
                except asyncio.CancelledError:
                    pass

            self._sync_task = None
            logger.info("TickerSynchronizer stopped")

        except Exception as e:
            logger.error(f"Error stopping TickerSynchronizer: {e}")

    @property
    def is_running(self) -> bool:
        """Check if synchronizer is running."""
        return self._is_running

    # ===== SYNC LOOP =====

    async def _sync_loop(self) -> None:
        """Main synchronization loop."""
        logger.info(f"Sync loop started, interval: {self.sync_interval_seconds}s")

        try:
            # Initial delay to let system initialize
            await asyncio.sleep(self.initial_sync_delay)

            while self._is_running:
                try:
                    await self._sync_cycle()
                    self._consecutive_failures = 0

                    # Wait for next sync
                    await asyncio.sleep(self.sync_interval_seconds)

                except asyncio.CancelledError:
                    logger.info("Sync loop cancelled")
                    break
                except Exception as e:
                    self._consecutive_failures += 1
                    logger.error(
                        f"Sync cycle failed (attempt {self._consecutive_failures}): {e}",
                        exc_info=True
                    )

                    # Stop if too many failures
                    if self._consecutive_failures >= self.max_consecutive_failures:
                        logger.error("Too many consecutive sync failures, stopping")
                        self._is_running = False
                        break

                    # Exponential backoff on failures
                    backoff_delay = min(60, 5 * (2 ** (self._consecutive_failures - 1)))
                    await asyncio.sleep(backoff_delay)

        except Exception as e:
            logger.error(f"Sync loop error: {e}", exc_info=True)

        logger.info("Sync loop ended")

    async def _sync_cycle(self) -> None:
        """Single synchronization cycle."""
        logger.debug("Starting sync cycle")
        sync_start = time.time()

        try:
            # Step 1: Fetch fresh exchange info
            exchange_info = await self._fetch_exchange_info()

            # Step 2: Filter symbols based on config
            target_symbols = self._filter_target_symbols(exchange_info)

            # Step 3: Get currently active symbols from Shared
            active_symbols = set(self.shared.get_active_ticker_symbols())

            # Step 4: Calculate differences
            to_add = target_symbols - active_symbols
            to_remove = active_symbols - target_symbols

            # Step 5: Log changes
            if to_add or to_remove:
                logger.info(
                    f"Ticker changes detected - Add: {len(to_add)}, Remove: {len(to_remove)}"
                )
                if to_add:
                    logger.info(f"Adding tickers: {sorted(to_add)}")
                if to_remove:
                    logger.info(f"Removing tickers: {sorted(to_remove)}")

            # Step 6: Apply changes
            await self._add_tickers(to_add, exchange_info)
            await self._remove_tickers(to_remove)

            # Update cache and timestamp
            self._current_exchange_info = exchange_info
            self._last_sync_time = int(time.time() * 1000)

            sync_duration = time.time() - sync_start
            logger.debug(f"Sync cycle completed in {sync_duration:.2f}s")

        except Exception as e:
            logger.error(f"Sync cycle failed: {e}", exc_info=True)
            raise

    # ===== EXCHANGE INFO OPERATIONS =====

    async def _fetch_exchange_info(self) -> Dict[str, BinanceExchangeInfo]:
        """
        Fetch exchange info from Binance API.

        Returns:
            Dictionary of symbol -> BinanceExchangeInfo
        """
        logger.debug("Fetching exchange info from Binance")

        try:
            response = await self.shared.weight_coordinator.request(
                "exchangeInfo",
                expected_weight=10
            )

            if not response or 'symbols' not in response:
                raise ValueError("Invalid exchange info response")

            exchange_info = {}
            for symbol_data in response['symbols']:
                try:
                    symbol_info = BinanceExchangeInfo.from_binance_dict(symbol_data)
                    exchange_info[symbol_info.symbol] = symbol_info
                except Exception as e:
                    logger.warning(f"Failed to parse symbol {symbol_data.get('symbol', 'unknown')}: {e}")

            logger.info(f"Fetched exchange info for {len(exchange_info)} symbols")
            return exchange_info

        except Exception as e:
            logger.error(f"Failed to fetch exchange info: {e}")
            raise

    def _filter_target_symbols(self, exchange_info: Dict[str, BinanceExchangeInfo]) -> Set[str]:
        """
        Filter symbols based on configuration criteria.

        Args:
            exchange_info: All exchange info from API

        Returns:
            Set of symbols that should be active
        """
        target_symbols = set()

        for symbol, info in exchange_info.items():
            # Validate symbol format (only 0-9 and A-Z)
            if not is_valid_symbol(symbol):
                continue

            # Check if symbol should be loaded based on config
            if not self.config.should_load_ticker(symbol):
                continue

            # Only include trading symbols
            if not info.is_tradeable():
                continue

            # Only include perpetual contracts (skip delivery contracts)
            if not info.is_perpetual():
                continue

            target_symbols.add(symbol)

        logger.debug(f"Filtered to {len(target_symbols)} target symbols")
        return target_symbols

    # ===== TICKER MANAGEMENT =====

    async def _add_tickers(self, symbols_to_add: Set[str], exchange_info: Dict[str, BinanceExchangeInfo]) -> None:
        """
        Add new ticker tasks to Shared.

        Args:
            symbols_to_add: Set of symbols to add
            exchange_info: Exchange info for creating ticker models
        """
        if not symbols_to_add:
            return

        logger.info(f"Adding {len(symbols_to_add)} new tickers")

        # Send notification about new tickers
        notifier = get_notifier()
        if notifier and len(symbols_to_add) > 0:
            try:
                symbols_list = ', '.join(sorted(symbols_to_add)[:10])
                extra = f" (+{len(symbols_to_add) - 10} more)" if len(symbols_to_add) > 10 else ""
                await notifier.send_alert(
                    message=f"New tickers added: {len(symbols_to_add)}\n\n{symbols_list}{extra}",
                    level=AlertLevel.INFO
                )
            except Exception as e:
                logger.error(f"Failed to send new tickers notification: {e}")

        for i, symbol in enumerate(symbols_to_add):
            try:
                if symbol not in exchange_info:
                    logger.warning(f"Symbol {symbol} not found in exchange info")
                    continue

                # Create ticker model
                ticker_model = exchange_info[symbol].to_ticker_model()

                # Add to Shared
                success = await self.shared.add_ticker(symbol, ticker_model)
                if success:
                    logger.debug(f"Added ticker: {symbol}")
                else:
                    logger.warning(f"Failed to add ticker: {symbol}")

                # Staggered startup: small delay between tickers to spread connections
                # 574 tickers × 0.1s = ~57 seconds total, spreads 24h disconnect window
                if i < len(symbols_to_add) - 1:
                    await asyncio.sleep(0.1)

            except Exception as e:
                logger.error(f"Error adding ticker {symbol}: {e}")

    async def _remove_tickers(self, symbols_to_remove: Set[str]) -> None:
        """
        Remove ticker tasks from Shared.

        Args:
            symbols_to_remove: Set of symbols to remove
        """
        if not symbols_to_remove:
            return

        logger.info(f"Removing {len(symbols_to_remove)} tickers")

        # Send notification about removed tickers (delisting)
        notifier = get_notifier()
        if notifier and len(symbols_to_remove) > 0:
            try:
                symbols_list = ', '.join(sorted(symbols_to_remove))
                await notifier.send_alert(
                    message=f"Tickers removed (delisted): {len(symbols_to_remove)}\n\n{symbols_list}",
                    level=AlertLevel.WARNING
                )
            except Exception as e:
                logger.error(f"Failed to send removed tickers notification: {e}")

        for symbol in symbols_to_remove:
            try:
                success = await self.shared.remove_ticker(symbol, graceful=True)
                if success:
                    logger.info(f"Removed ticker: {symbol}")
                else:
                    logger.warning(f"Failed to remove ticker: {symbol}")

            except Exception as e:
                logger.error(f"Error removing ticker {symbol}: {e}")

    # ===== PUBLIC ACCESS METHODS =====

    def get_current_exchange_info(self) -> Dict[str, BinanceExchangeInfo]:
        """
        Get current cached exchange info.

        Returns:
            Dictionary of symbol -> BinanceExchangeInfo
        """
        return self._current_exchange_info.copy()

    def get_ticker_info(self, symbol: str) -> Optional[BinanceExchangeInfo]:
        """
        Get exchange info for specific symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            BinanceExchangeInfo or None if not found
        """
        return self._current_exchange_info.get(symbol.upper())

    def get_last_sync_time(self) -> Optional[int]:
        """
        Get timestamp of last successful sync.

        Returns:
            Timestamp in milliseconds or None if no sync yet
        """
        return self._last_sync_time

    def get_target_symbols_count(self) -> int:
        """
        Get count of symbols that should be active based on current exchange info.

        Returns:
            Number of target symbols
        """
        if not self._current_exchange_info:
            return 0
        return len(self._filter_target_symbols(self._current_exchange_info))

    # ===== MANUAL OPERATIONS =====

    async def force_sync(self) -> bool:
        """
        Force immediate synchronization cycle.

        Returns:
            True if sync successful, False otherwise
        """
        logger.info("Force sync requested")

        try:
            await self._sync_cycle()
            logger.info("Force sync completed successfully")
            return True
        except Exception as e:
            logger.error(f"Force sync failed: {e}")
            return False

    async def refresh_ticker(self, symbol: str) -> bool:
        """
        Refresh specific ticker (remove and re-add).

        Args:
            symbol: Ticker symbol to refresh

        Returns:
            True if refresh successful, False otherwise
        """
        symbol = symbol.upper()
        logger.info(f"Refreshing ticker: {symbol}")

        try:
            # Check if symbol exists in current exchange info
            if symbol not in self._current_exchange_info:
                logger.warning(f"Symbol {symbol} not found in exchange info")
                return False

            # Remove and re-add
            await self.shared.remove_ticker(symbol, graceful=True)
            await asyncio.sleep(1)  # Brief delay

            ticker_model = self._current_exchange_info[symbol].to_ticker_model()
            success = await self.shared.add_ticker(symbol, ticker_model)

            if success:
                logger.info(f"Ticker {symbol} refreshed successfully")
            else:
                logger.warning(f"Failed to refresh ticker {symbol}")

            return success

        except Exception as e:
            logger.error(f"Error refreshing ticker {symbol}: {e}")
            return False

    # ===== STATUS AND MONITORING =====

    def get_sync_status(self) -> Dict[str, any]:
        """
        Get synchronization status information.

        Returns:
            Dictionary with sync status
        """
        return {
            'running': self._is_running,
            'last_sync_time': self._last_sync_time,
            'consecutive_failures': self._consecutive_failures,
            'sync_interval_seconds': self.sync_interval_seconds,
            'total_symbols_in_cache': len(self._current_exchange_info),
            'target_symbols_count': self.get_target_symbols_count(),
            'active_tickers_count': len(self.shared.get_active_ticker_symbols()) if hasattr(self.shared, 'get_active_ticker_symbols') else 0
        }

    def __str__(self) -> str:
        """Human-readable string representation."""
        status = 'running' if self._is_running else 'stopped'
        return f"TickerSynchronizer({status}, {len(self._current_exchange_info)} symbols)"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"TickerSynchronizer(running={self._is_running}, "
            f"symbols={len(self._current_exchange_info)}, "
            f"failures={self._consecutive_failures})"
        )
