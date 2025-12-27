"""
Shared - Central Resource Manager
=================================

Central coordinator for shared resources in the Data Provider system.
Manages Binance client, coordinators, and active ticker tasks lifecycle with
comprehensive monitoring and health management.

File: src/core/shared.py
"""

import asyncio
import time
from typing import Dict, List, Optional, Any, TYPE_CHECKING

from loggerino import loggerino
from binance import AsyncClient

from ..models.config import Config
from ..models.ticker_model import TickerModel
from .weight_coordinator import WeightCoordinator
from .clickhouse_manager import ClickHouseManager

if TYPE_CHECKING:
    from .ticker_synchronizer import TickerSynchronizer
    from ..api.rest_api import RestAPIServer
from .ticker_processor import TickerProcessor
from .global_trades_updater import GlobalTradesUpdater
from .trades_interfaces import TradesWriter, TradesWriterImpl
from .health_monitor import HealthMonitor
from .integrity import RecoveryState, IntegrityManager
# from .gap_checker import GapChecker


logger = loggerino.get('shared')


class Shared:
    """
    Central manager for shared resources across the Data Provider system.

    Responsibilities:
    - Binance client initialization and management
    - Coordinator lifecycle (WeightCoordinator, TickerSynchronizer)
    - Active ticker task management (TickerProcessor instances)
    - Comprehensive health monitoring and statistics
    - Graceful system shutdown with proper resource cleanup
    """

    def __init__(self, config: Config):
        """
        Initialize Shared with configuration.

        Args:
            config: System configuration
        """
        self.config = config

        # Initialization state
        self._is_initialized: bool = False
        self._initialization_time: Optional[int] = None

        # Shared resources
        self.binance_client: Optional[AsyncClient] = None
        self.weight_coordinator: Optional[WeightCoordinator] = None
        self.ticker_synchronizer: Optional[TickerSynchronizer] = None
        self.rest_api: Optional[RestAPIServer] = None

        self.clickhouse_manager: Optional[ClickHouseManager] = None
        self.global_trades_updater: Optional[GlobalTradesUpdater] = None
        self.gap_checker: Optional[GapChecker] = None
        self.health_monitor: Optional[HealthMonitor] = None
        self.recovery_state: Optional[RecoveryState] = None
        self.integrity_manager: Optional[IntegrityManager] = None

        # Active ticker tasks - single source of truth
        self.tickers: Dict[str, asyncio.Task] = {}  # symbol -> TickerProcessor task

        # Cache for last_aggregate_ids (preloaded on startup)
        self._last_aggregate_ids_cache: Dict[str, Optional[int]] = {}

        logger.info("Shared initialized")

    # ===== LIFECYCLE METHODS =====

    async def initialize(self) -> bool:
        """
        Initialize all shared resources in correct order.

        Returns:
            True if initialization successful, False otherwise
        """
        if self._is_initialized:
            logger.warning("Shared already initialized")
            return True

        try:
            self._initialization_time = int(time.time() * 1000)

            # Phase 1: Create core components
            await self._initialize_binance_client()
            await self._create_coordinators()

            # Phase 2: Start coordinators (TickerSynchronizer will auto-add tickers)
            await self._start_coordinators()

            self._is_initialized = True
            logger.info(f"Shared initialized successfully with {len(self.tickers)} active tickers")
            return True

        except Exception as e:
            logger.error(f"Failed to initialize Shared: {e}", exc_info=True)
            await self._cleanup_on_failure()
            return False

    async def shutdown(self) -> None:
        """
        Graceful shutdown of all resources with proper cleanup order.
        """
        if not self._is_initialized:
            logger.warning("Shared not initialized, skipping shutdown")
            return

        try:
            logger.info("Starting Shared shutdown...")

            # Step 1: Stop all ticker tasks
            await self._shutdown_ticker_tasks()

            # Step 2: Stop coordinators
            await self._shutdown_coordinators()

            self._is_initialized = False
            logger.info("Shared shutdown completed")

        except Exception as e:
            logger.error(f"Error during Shared shutdown: {e}", exc_info=True)

    # ===== INITIALIZATION STEPS =====

    async def _initialize_binance_client(self) -> None:
        """Initialize Binance AsyncClient."""
        logger.info("Initializing Binance client...")

        if not self.config.binance.is_configured():
            raise ValueError("Binance API credentials not configured")

        try:
            self.binance_client = await AsyncClient.create(
                self.config.binance.api_key,
                self.config.binance.secret_key,
                testnet=self.config.binance.use_testnet
            )
            logger.info("Binance client initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize Binance client: {e}")
            raise

    async def _create_coordinators(self) -> None:
        """Create coordinators without starting them."""
        logger.info("Creating coordinators...")

        # Create ClickHouseManager FIRST (needed by others)
        self.clickhouse_manager = ClickHouseManager(self.config.clickhouse)
        logger.info("ClickHouseManager created")

        # Create GapChecker
        # self.gap_checker = GapChecker(
        #     clickhouse_manager=self.clickhouse_manager,
        #     shared=self,
        #     base_interval_minutes=30,  # или из конфига
        #     min_interval_seconds=3,
        #     check_window_minutes=30
        # )
        # logger.info("GapChecker created")

        # Create GlobalTradesUpdater
        self.global_trades_updater = GlobalTradesUpdater(
            clickhouse_manager=self.clickhouse_manager,
            config=self.config.processing
        )
        logger.info("GlobalTradesUpdater created")

        # Create RecoveryState for integrity coordination
        self.recovery_state = RecoveryState()
        logger.info("RecoveryState created")

        # Create IntegrityManager
        from .telegram_notifier import get_notifier
        self.integrity_manager = IntegrityManager(
            clickhouse_client=self.clickhouse_manager,
            recovery_state=self.recovery_state,
            telegram_notifier=get_notifier()
        )
        logger.info("IntegrityManager created")

        # Create WeightCoordinator with proper configuration
        self.weight_coordinator = WeightCoordinator(
            binance_client=self.binance_client,
            config=self.config
        )

        # Create TickerSynchronizer
        from .ticker_synchronizer import TickerSynchronizer
        self.ticker_synchronizer = TickerSynchronizer(
            binance_client=self.binance_client,
            shared=self,  # Pass self for direct access
            config=self.config.processing
        )

        # Create REST API Server
        from ..api.rest_api import RestAPIServer
        self.rest_api = RestAPIServer(
            shared=self,
            config=self.config
        )
        logger.info("REST API server created")

        logger.info("Coordinators created successfully")

    async def _start_coordinators(self) -> None:
        """Start coordinators in proper order and perform initial sync."""
        logger.info("Starting coordinators...")
        # Ensure ClickHouse connection is established once at startup
        if self.clickhouse_manager:
            await self.clickhouse_manager.ensure_connected()
            logger.info("ClickHouse connection established")

        # Start GlobalTradesUpdater FIRST
        if self.global_trades_updater:
            await self.global_trades_updater.start()
            logger.info("GlobalTradesUpdater started")

        # ===== BATCH PRELOAD last_aggregate_ids =====
        await self._preload_last_aggregate_ids()
        # ============================================

        # Start WeightCoordinator (required by other components)
        if self.weight_coordinator:
            await self.weight_coordinator.start()
            logger.info("WeightCoordinator started")

        # Start GapChecker
        # if self.gap_checker:
        #     await self.gap_checker.start()
        #     logger.info("GapChecker started")

        # Start REST API server
        if self.rest_api:
            await self.rest_api.start_server()
            logger.info("REST API server started")

        # Start TickerSynchronizer - this will trigger initial sync and add tickers
        if self.ticker_synchronizer:
            await self.ticker_synchronizer.start()
            logger.info("TickerSynchronizer started")

        # Start HealthMonitor
        self.health_monitor = HealthMonitor(self, check_interval_seconds=300)
        await self.health_monitor.start()
        logger.info("HealthMonitor started")

        # Start IntegrityManager
        if self.integrity_manager:
            await self.integrity_manager.start_with_periodic_checker(interval_hours=6)
            logger.info("IntegrityManager started")

        logger.info("All coordinators started successfully")

    async def _preload_last_aggregate_ids(self) -> None:
        """
        Preload last_aggregate_ids for all symbols in single batch query.

        Critical optimization: Prevents 700+ individual queries during startup.
        This is called BEFORE TickerSynchronizer starts adding tickers.
        """
        try:
            logger.info("Preloading last aggregate IDs for all symbols...")

            # Get all TRADEABLE symbols from Binance
            if not self.binance_client:
                logger.warning("Binance client not available, skipping preload")
                return

            # Fetch exchange info directly
            from ..models.exchange_info import BinanceExchangeInfo

            logger.info("Fetching exchange info from Binance...")
            exchange_info_data = await self.binance_client.futures_exchange_info()

            # Parse and filter tradeable symbols
            all_tickers = []
            for symbol_data in exchange_info_data.get('symbols', []):
                try:
                    exchange_info = BinanceExchangeInfo.from_binance_dict(symbol_data)
                    ticker_model = exchange_info.to_ticker_model()

                    # Apply same filters as TickerSynchronizer
                    if ticker_model.is_tradeable() and ticker_model.is_usdt_pair():
                        all_tickers.append(ticker_model)
                except Exception as e:
                    logger.debug(f"Skipped symbol during preload: {e}")

            if not all_tickers:
                logger.warning("No tradeable tickers found for preload")
                return

            all_symbols = [ticker.symbol for ticker in all_tickers]

            logger.info(f"Batch loading last aggregate IDs for {len(all_symbols)} symbols...")
            start_time = time.time()

            # Single batch query for all symbols
            last_ids = await self.clickhouse_manager.batch_get_last_aggregate_ids(all_symbols)

            duration = time.time() - start_time
            symbols_with_data = sum(1 for v in last_ids.values() if v is not None)

            logger.info(
                f"Preloaded last aggregate IDs in {duration:.2f}s: "
                f"{symbols_with_data}/{len(all_symbols)} symbols have data"
            )

            # Store in cache
            self._last_aggregate_ids_cache = last_ids

        except Exception as e:
            logger.error(f"Failed to preload last aggregate IDs: {e}", exc_info=True)
            # Non-critical - processors will fallback to individual queries
            self._last_aggregate_ids_cache = {}

    def get_trades_writer(self) -> TradesWriter:
        """
        Get TradesWriter interface for TickerProcessor components.

        Returns:
            TradesWriter implementation using GlobalTradesUpdater and ClickHouseManager
        """
        if not self.global_trades_updater or not self.clickhouse_manager:
            raise RuntimeError("Shared not initialized - call initialize() first")

        return TradesWriterImpl(
            global_trades_updater=self.global_trades_updater,
            clickhouse_manager=self.clickhouse_manager
        )

    # ===== TICKER MANAGEMENT =====

    async def add_ticker(self, symbol: str, ticker_model: TickerModel) -> bool:
        """
        Add new ticker and start processing task.

        Args:
            symbol: Ticker symbol
            ticker_model: TickerModel instance

        Returns:
            True if ticker added successfully
        """
        symbol = symbol.upper()

        if symbol in self.tickers and not self.tickers[symbol].done():
            logger.warning(f"Ticker {symbol} already active")
            return False

        try:
            ticker_log_file = self.config.monitoring.get_ticker_log_path(symbol)
            logger_name = f'ticker_processor_{symbol}'

            # Directory is automatically created by get_ticker_log_path()

            # Try to create ticker-specific logger
            try:
                ticker_logger = loggerino.create(logger_name, ticker_log_file)
                ticker_logger.info(f"Ticker logger initialized for {symbol}")
                logger.debug(f"Created ticker logger: {ticker_log_file}")
            except Exception as e:
                logger.error(f"Failed to create ticker logger for {symbol}: {e}")
                logger_name = 'system'  # Use system logger as fallback

            # Get cached last_aggregate_id if available
            initial_last_id = self._last_aggregate_ids_cache.get(symbol)

            # Create TickerProcessor with TradesWriter interface
            processor = TickerProcessor(
                ticker_model=ticker_model,
                binance_client=self.binance_client,
                weight_coordinator=self.weight_coordinator,
                trades_writer=self.get_trades_writer(),
                config=self.config,
                logger_name=logger_name,
                initial_last_aggregate_id=initial_last_id,
                integrity_manager=self.integrity_manager,
                recovery_state=self.recovery_state,
                central_websocket_manager=getattr(self.rest_api, 'websocket_manager', None) if self.rest_api else None
            )

            task = asyncio.create_task(
                processor.run(),
                name=f"ticker_processor_{symbol}"
            )

            # Store processor reference in task for future access
            task.processor = processor

            self.tickers[symbol] = task
            logger.debug(f"Started ticker task for {symbol}")
            return True

        except Exception as e:
            logger.error(f"Failed to add ticker {symbol}: {e}")
            return False

    async def remove_ticker(self, symbol: str, graceful: bool = True) -> bool:
        """
        Remove ticker and stop processing task.

        Args:
            symbol: Ticker symbol
            graceful: Whether to wait for graceful shutdown

        Returns:
            True if ticker removed successfully
        """
        symbol = symbol.upper()

        if symbol not in self.tickers:
            logger.warning(f"Ticker {symbol} not found")
            return False

        try:
            task = self.tickers[symbol]

            if graceful and not task.done():
                # Send shutdown signal to TickerProcessor
                if hasattr(task, 'processor'):
                    await task.processor.shutdown()
                else:
                    # Fallback for tasks without processor reference
                    task.cancel()
                    try:
                        await asyncio.wait_for(task, timeout=30.0)
                    except asyncio.TimeoutError:
                        logger.warning(f"Ticker {symbol} shutdown timeout, forcing cancel")
                        task.cancel()
            else:
                task.cancel()

            # Clean up task
            if task.done():
                try:
                    await task  # Retrieve any exceptions
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Ticker {symbol} task error: {e}")

            del self.tickers[symbol]
            logger.info(f"Removed ticker task for {symbol}")
            return True

        except Exception as e:
            logger.error(f"Failed to remove ticker {symbol}: {e}")
            return False

    # ===== SHUTDOWN STEPS =====

    async def _shutdown_ticker_tasks(self) -> None:
        """Shutdown all ticker processing tasks with graceful handling."""
        logger.info("Shutting down ticker tasks...")

        if not self.tickers:
            return

        # First attempt graceful shutdown for all tasks
        shutdown_tasks = []
        for symbol, task in self.tickers.items():
            if not task.done() and hasattr(task, 'processor'):
                logger.debug(f"Gracefully shutting down ticker {symbol}")
                shutdown_tasks.append(task.processor.shutdown())
            elif not task.done():
                logger.debug(f"Cancelling ticker task for {symbol}")
                task.cancel()

        # Wait for graceful shutdowns
        if shutdown_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*shutdown_tasks, return_exceptions=True),
                    timeout=45.0
                )
            except asyncio.TimeoutError:
                logger.warning("Some ticker tasks graceful shutdown timeout")

        # Force cancel any remaining tasks
        remaining_tasks = []
        for symbol, task in self.tickers.items():
            if not task.done():
                logger.debug(f"Force cancelling ticker task for {symbol}")
                task.cancel()
                remaining_tasks.append(task)

        # Wait for force cancellations
        if remaining_tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*remaining_tasks, return_exceptions=True),
                    timeout=15.0
                )
            except asyncio.TimeoutError:
                logger.warning("Force ticker tasks shutdown timeout")

        # Clean up and log results
        for symbol, task in list(self.tickers.items()):
            try:
                if task.done():
                    await task  # Retrieve exceptions
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.error(f"Ticker {symbol} shutdown error: {e}")

        self.tickers.clear()
        logger.info("All ticker tasks shut down")

    async def _shutdown_coordinators(self) -> None:
        """Shutdown coordinators in reverse order."""
        logger.info("Shutting down coordinators...")

        # Stop REST API server first
        if self.rest_api:
            try:
                await self.rest_api.stop_server()
                self.rest_api = None
                logger.info("REST API server stopped")
            except Exception as e:
                logger.error(f"Error stopping REST API server: {e}")

        # Stop TickerSynchronizer
        if self.ticker_synchronizer:
            try:
                await self.ticker_synchronizer.stop()
                self.ticker_synchronizer = None
                logger.info("TickerSynchronizer stopped")
            except Exception as e:
                logger.error(f"Error stopping TickerSynchronizer: {e}")

        # Stop GapChecker
        # if self.gap_checker:
        #     try:
        #         await self.gap_checker.stop()
        #         self.gap_checker = None
        #         logger.info("GapChecker stopped")
        #     except Exception as e:
        #         logger.error(f"Error stopping GapChecker: {e}")

        # Stop WeightCoordinator
        if self.weight_coordinator:
            try:
                await self.weight_coordinator.shutdown()
                self.weight_coordinator = None
                logger.info("WeightCoordinator stopped")
            except Exception as e:
                logger.error(f"Error stopping WeightCoordinator: {e}")

        # Stop GlobalTradesUpdater
        if self.global_trades_updater:
            try:
                await self.global_trades_updater.stop()
                self.global_trades_updater = None
                logger.info("GlobalTradesUpdater stopped")
            except Exception as e:
                logger.error(f"Error stopping GlobalTradesUpdater: {e}")

        # Close ClickHouseManager LAST
        if self.clickhouse_manager:
            try:
                await self.clickhouse_manager.close()
                self.clickhouse_manager = None
                logger.info("ClickHouseManager closed")
            except Exception as e:
                logger.error(f"Error closing ClickHouseManager: {e}")

        # Stop HealthMonitor
        if self.health_monitor:
            try:
                await self.health_monitor.stop()
                self.health_monitor = None
                logger.info("HealthMonitor stopped")
            except Exception as e:
                logger.error(f"Error stopping HealthMonitor: {e}")

        # Stop IntegrityManager
        if self.integrity_manager:
            await self.integrity_manager.stop()
            logger.info("IntegrityManager stopped")

    # ===== UTILITY METHODS =====

    async def _cleanup_on_failure(self) -> None:
        """Cleanup resources on initialization failure."""
        try:
            await self.shutdown()
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    # ===== PUBLIC ACCESS METHODS =====

    def get_ticker_model(self, symbol: str) -> Optional[TickerModel]:
        """
        Get TickerModel for specific symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            TickerModel instance or None if ticker not active
        """
        symbol = symbol.upper()
        task = self.tickers.get(symbol)

        if task and not task.done() and hasattr(task, 'processor'):
            return task.processor.ticker_model

        return None

    def get_ticker_processor(self, symbol: str) -> Optional[TickerProcessor]:
        """
        Get TickerProcessor for specific symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            TickerProcessor instance or None if not found
        """
        symbol = symbol.upper()
        task = self.tickers.get(symbol)

        if task and not task.done() and hasattr(task, 'processor'):
            return task.processor

        return None

    def get_ticker_processor_stats(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive TickerProcessor statistics for specific symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            Dictionary with processor stats or None if not found
        """
        processor = self.get_ticker_processor(symbol)
        if processor:
            stats = processor.get_stats()
            return {
                'symbol': symbol,
                'state': stats.state.name,
                'start_time': stats.start_time,
                'historical_trades_loaded': stats.historical_trades_loaded,
                'websocket_trades_processed': stats.websocket_trades_processed,
                'last_trade_time': stats.last_trade_time,
                'last_error': stats.last_error,
                'error_count': stats.error_count,
                'binance_error_count': stats.binance_error_count,
                'recoverable_error_count': stats.recoverable_error_count,
                'gap_recoveries': stats.gap_recoveries,
                'websocket_reconnections': stats.websocket_reconnections,
                'uptime_seconds': stats.uptime_seconds,
                'is_healthy': processor.is_healthy()
            }
        return None

    def get_ticker_comprehensive_status(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive status for specific ticker including all component stats.

        Args:
            symbol: Ticker symbol

        Returns:
            Dictionary with comprehensive status or None if not found
        """
        processor = self.get_ticker_processor(symbol)
        if processor:
            return processor.get_comprehensive_status()
        return None

    def pause_ticker(self, symbol: str) -> bool:
        """
        Pause ticker processing.

        Args:
            symbol: Ticker symbol

        Returns:
            True if pause was successful
        """
        processor = self.get_ticker_processor(symbol)
        if processor:
            return processor.pause()
        return False

    def resume_ticker(self, symbol: str) -> bool:
        """
        Resume ticker processing.

        Args:
            symbol: Ticker symbol

        Returns:
            True if resume was successful
        """
        processor = self.get_ticker_processor(symbol)
        if processor:
            return processor.resume()
        return False

    def is_ticker_active(self, symbol: str) -> bool:
        """
        Check if ticker is currently active.

        Args:
            symbol: Ticker symbol

        Returns:
            True if ticker has active processing task
        """
        symbol = symbol.upper()
        task = self.tickers.get(symbol)
        return task is not None and not task.done()

    def get_active_ticker_symbols(self) -> List[str]:
        """Get list of all active ticker symbols."""
        return [
            symbol for symbol, task in self.tickers.items()
            if not task.done()
        ]

    def get_gap_checker_stats(self) -> Optional[Dict[str, Any]]:
        """Get GapChecker statistics."""
        if self.gap_checker:
            return self.gap_checker.get_detailed_stats()
        return None

    async def trigger_gap_check(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Manually trigger gap check."""
        if not self.gap_checker:
            return {'error': 'GapChecker not available'}

        if symbol:
            gaps = await self.gap_checker.check_symbol_now(symbol)
            return {'symbol': symbol, 'gaps_detected': len(gaps), 'gaps': gaps}
        else:
            gaps = await self.gap_checker.check_all_symbols_now()
            return {'gaps_by_symbol': {s: len(g) for s, g in gaps.items()}}

    # ===== COORDINATOR ACCESS METHODS =====

    def get_weight_coordinator_stats(self) -> Optional[Dict[str, Any]]:
        """
        Get WeightCoordinator statistics.

        Returns:
            Dictionary with weight coordinator stats or None if not available
        """
        if self.weight_coordinator:
            stats = self.weight_coordinator.get_stats()
            return {
                'current_weight_usage': stats.current_weight_usage,
                'available_weight': stats.available_weight,
                'critical_queue_size': stats.critical_queue_size,
                'normal_queue_size': stats.normal_queue_size,
                'total_requests_processed': stats.total_requests_processed,
                'requests_per_minute': stats.requests_per_minute,
                'last_request_time': stats.last_request_time,
                'is_rate_limited': stats.is_rate_limited,
                'recovery_end_time': stats.recovery_end_time
            }
        return None

    def get_ticker_synchronizer_stats(self) -> Optional[Dict[str, Any]]:
        """
        Get TickerSynchronizer statistics.

        Returns:
            Dictionary with ticker synchronizer stats or None if not available
        """
        if self.ticker_synchronizer:
            sync_status = self.ticker_synchronizer.get_sync_status()
            return {
                'running': sync_status['running'],
                'last_sync_time': sync_status['last_sync_time'],
                'consecutive_failures': sync_status['consecutive_failures'],
                'sync_interval_seconds': sync_status['sync_interval_seconds'],
                'total_symbols_in_cache': sync_status['total_symbols_in_cache'],
                'target_symbols_count': sync_status['target_symbols_count'],
                'active_tickers_count': sync_status['active_tickers_count']
            }
        return None

    def get_global_trades_stats(self) -> Optional[Dict[str, Any]]:
        """Get GlobalTradesUpdater statistics."""
        if self.global_trades_updater:
            stats = self.global_trades_updater.get_stats()
            return {
                'total_trades_processed': stats['total_trades_processed'],
                'batch_count': stats['batch_count'],
                'last_batch_size': stats['last_batch_size'],
                'last_batch_time': stats['last_batch_time'],
                'active_tickers_count': stats['active_tickers_count'],
                'processing_errors_count': stats['processing_errors_count'],
                'uptime_seconds': stats['uptime_seconds'],
                'is_healthy': self.global_trades_updater.is_healthy()
            }
        return None

    def get_clickhouse_stats(self) -> Optional[Dict[str, Any]]:
        """Get ClickHouseManager statistics."""
        if self.clickhouse_manager:
            return {
                'connected': self.clickhouse_manager.is_connected,
                'buffer_enabled': self.clickhouse_manager.config.buffer_enabled,
                'compression_enabled': self.clickhouse_manager.config.enable_compression
            }
        return None

    # ===== STATUS AND MONITORING =====

    async def health_check(self, detailed: bool = False) -> Dict[str, Any]:
        """
        Perform comprehensive health check of all components.

        Args:
            detailed: Include detailed processor information (default: False for production)

        Returns:
            Dictionary with health status
        """
        health = {
            'initialized': self._is_initialized,
            'timestamp': int(time.time() * 1000),
            'uptime_ms': int(time.time() * 1000) - (self._initialization_time or 0)
        }

        # Binance client health
        health['binance_client'] = {
            'connected': self.binance_client is not None,
            'testnet': self.config.binance.use_testnet if self.binance_client else None
        }

        # WeightCoordinator health
        if self.weight_coordinator:
            weight_stats = self.weight_coordinator.get_stats()
            health['weight_coordinator'] = {
                'active': True,
                'healthy': self.weight_coordinator.is_healthy(),
                'current_weight_usage': weight_stats.current_weight_usage,
                'available_weight': weight_stats.available_weight,
                'queue_sizes': {
                    'critical': weight_stats.critical_queue_size,
                    'normal': weight_stats.normal_queue_size,
                    'total_pending': weight_stats.critical_queue_size + weight_stats.normal_queue_size
                },
                'is_rate_limited': weight_stats.is_rate_limited,
                'requests_processed': weight_stats.total_requests_processed
            }
        else:
            health['weight_coordinator'] = {'active': False}

        # REST API health
        if self.rest_api:
            api_stats = self.get_rest_api_stats()
            health['rest_api'] = {
                'active': self.rest_api.is_running,
                'stats': api_stats
            }
        else:
            health['rest_api'] = {'active': False}

        # TickerSynchronizer health
        if self.ticker_synchronizer:
            sync_stats = self.get_ticker_synchronizer_stats()
            health['ticker_synchronizer'] = {
                'active': sync_stats['running'] if sync_stats else False,
                'stats': sync_stats
            }
        else:
            health['ticker_synchronizer'] = {'active': False}

        # GlobalTradesUpdater health
        if self.global_trades_updater:
            trades_stats = self.get_global_trades_stats()
            health['global_trades_updater'] = {
                'active': True,
                'healthy': self.global_trades_updater.is_healthy(),
                'stats': trades_stats
            }
        else:
            health['global_trades_updater'] = {'active': False}

        # ClickHouse health
        if self.clickhouse_manager:
            ch_stats = self.get_clickhouse_stats()
            health['clickhouse_manager'] = {
                'active': True,
                'stats': ch_stats
            }
        else:
            health['clickhouse_manager'] = {'active': False}

        # GapChecker health
        if self.gap_checker:
            gap_stats = self.get_gap_checker_stats()
            health['gap_checker'] = {
                'active': True,
                'healthy': self.gap_checker.is_healthy(),
                'stats': gap_stats
            }
        else:
            health['gap_checker'] = {'active': False}

        # Ticker tasks health
        active_tickers = self.get_active_ticker_symbols()
        completed_tasks = sum(1 for task in self.tickers.values() if task.done() and not task.cancelled())
        failed_tasks = sum(1 for task in self.tickers.values() if task.done() and task.exception())
        cancelled_tasks = sum(1 for task in self.tickers.values() if task.cancelled())

        # Analyze processor states and health
        healthy_processors = 0
        error_processors = 0
        processor_states_count = {}
        processor_details = {}

        for symbol in active_tickers:
            processor = self.get_ticker_processor(symbol)
            if processor:
                if processor.is_healthy():
                    healthy_processors += 1
                else:
                    error_processors += 1

                # Count states for summary
                state_name = processor.state.name
                processor_states_count[state_name] = processor_states_count.get(state_name, 0) + 1

                # Collect details only if detailed=True
                if detailed:
                    stats = processor.get_stats()
                    processor_details[symbol] = {
                        'state': stats.state.name,
                        'historical_trades': stats.historical_trades_loaded,
                        'websocket_trades': stats.websocket_trades_processed,
                        'errors': stats.error_count,
                        'binance_errors': stats.binance_error_count,
                        'gap_recoveries': stats.gap_recoveries,
                        'uptime_seconds': stats.uptime_seconds
                    }

        health['tickers'] = {
            'total_tasks': len(self.tickers),
            'active_tasks': len(active_tickers),
            'completed_tasks': completed_tasks,
            'failed_tasks': failed_tasks,
            'cancelled_tasks': cancelled_tasks,
            'healthy_processors': healthy_processors,
            'error_processors': error_processors,
            'state_summary': processor_states_count
        }

        # Add detailed info only if requested
        if detailed:
            health['tickers']['processor_details'] = processor_details

        # Overall system health assessment
        # During initial startup with many tickers downloading, weight coordinator may be under pressure
        # This is normal and shouldn't mark the system as unhealthy
        weight_healthy = (
                self.weight_coordinator is None or
                self.weight_coordinator.is_healthy() or
                (processor_states_count.get('DOWNLOADING_HISTORICAL', 0) > 10)  # Allow unhealthy weight during mass download
        )

        components_healthy = (
                self._is_initialized and
                self.binance_client is not None and
                weight_healthy and
                len(active_tickers) > 0 and
                failed_tasks == 0 and
                error_processors == 0
        )

        health['overall_status'] = 'healthy' if components_healthy else 'degraded' if weight_healthy else 'unhealthy'
        health['health_score'] = self._calculate_health_score(health)

        return health

    def _calculate_health_score(self, health: Dict[str, Any]) -> float:
        """
        Calculate overall system health score (0.0 to 1.0).

        Args:
            health: Health check results

        Returns:
            Health score between 0.0 (critical) and 1.0 (perfect)
        """
        score = 0.0
        max_score = 0.0

        # Basic initialization (20%)
        max_score += 0.2
        if health['initialized']:
            score += 0.2

        # Binance client (15%)
        max_score += 0.15
        if health['binance_client']['connected']:
            score += 0.15

        # WeightCoordinator (20%)
        max_score += 0.2
        if health['weight_coordinator']['active']:
            score += 0.1
            if health['weight_coordinator']['healthy']:
                score += 0.1

        # TickerSynchronizer (10%)
        max_score += 0.1
        if health['ticker_synchronizer']['active']:
            score += 0.1

        # Ticker processors (35%)
        max_score += 0.35
        ticker_health = health['tickers']
        if ticker_health['active_tasks'] > 0:
            healthy_ratio = ticker_health['healthy_processors'] / ticker_health['active_tasks']
            score += 0.35 * healthy_ratio

        return score / max_score if max_score > 0 else 0.0

    @property
    def is_initialized(self) -> bool:
        """Check if Shared is initialized."""
        return self._is_initialized

    def get_system_info(self) -> Dict[str, Any]:
        """
        Get basic system information.

        Returns:
            Dictionary with system info
        """
        return {
            'initialized': self._is_initialized,
            'initialization_time': self._initialization_time,
            'total_ticker_tasks': len(self.tickers),
            'active_tickers': len(self.get_active_ticker_symbols()),
            'binance_client_connected': self.binance_client is not None,
            'weight_coordinator_status': 'active' if self.weight_coordinator is not None else 'inactive',
            'ticker_synchronizer_status': 'active' if self.ticker_synchronizer and self.ticker_synchronizer.is_running else 'inactive',
            'rest_api_status': 'active' if self.rest_api and self.rest_api.is_running else 'inactive',
            'environment': self.config.environment
        }

    def get_rest_api_stats(self) -> Optional[Dict[str, Any]]:
        """Get REST API server statistics."""
        if self.rest_api:
            return self.rest_api.get_server_stats()
        return None

    def __str__(self) -> str:
        """Human-readable string representation."""
        status = 'initialized' if self._is_initialized else 'not_initialized'
        active_count = len(self.get_active_ticker_symbols())
        return f"Shared({status}, {active_count} active tickers)"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"Shared(initialized={self._is_initialized}, "
            f"total_tasks={len(self.tickers)}, "
            f"active_tasks={len(self.get_active_ticker_symbols())}, "
            f"weight_coordinator={'active' if self.weight_coordinator else 'none'}, "
            f"ticker_synchronizer={'active' if self.ticker_synchronizer else 'none'})"
        )
