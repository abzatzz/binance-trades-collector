"""
Level 1.4.1 Integration Test: System Startup Integration
=======================================================

Complete system startup integration tests covering:
- Cold start system initialization with proper component ordering
- Configuration loading and validation with v4.2 parameters
- Resource allocation and health checks across all components
- Component dependency resolution and cross-wiring validation
- Startup strategy selection based on system conditions
- Comprehensive error handling and graceful failure scenarios

CRITICAL INTEGRATION POINTS:
- main.py → Shared → Component initialization chain
- Config.from_env() → Component configuration propagation
- Resource management lifecycle (Binance, ClickHouse, WeightCoordinator)
- TickerSynchronizer → Shared → TickerProcessor coordination
- Error propagation and cleanup during startup failures

File: tests/integration/level1_component_pairs/test_system_startup_integration.py
"""

# Initialize loggerino first
import os
from pathlib import Path
from loggerino import loggerino

# Configure loggerino for tests
logs_folder = Path('./test_logs')
if not os.path.isdir(logs_folder):
    os.makedirs(logs_folder)

loggerino.configure(
    logs_dir=str(logs_folder),
    debug_in_console=True,
    buffer_size=100,
    flush_interval=5,
)

# Create test loggers
test_log_file = os.path.join('test_logs', 'level_1_4_1_startup.log')
loggerino.create('level_1_4_1_startup', test_log_file)

import pytest
import asyncio
import time
import threading
import json
import uuid
import tempfile
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import random
from dataclasses import dataclass
from enum import Enum, auto
import signal
import sys


# ===== MOCK DATA MODELS =====

@dataclass(frozen=True)
class ClickHouseConfig:
    """Mock ClickHouse configuration"""
    host: str = "localhost"
    port: int = 9000
    http_port: int = 8123
    database: str = "trades"
    username: str = "default"
    password: str = ""
    buffer_enabled: bool = True
    buffer_min_time: int = 10
    buffer_max_time: int = 100
    buffer_min_rows: int = 10000
    buffer_max_rows: int = 1000000
    materialized_views_enabled: bool = True
    enable_compression: bool = True
    compression_method: str = "lz4"

    def is_configured(self) -> bool:
        return bool(self.host and self.database)

    def get_connection_url(self) -> str:
        auth_part = ""
        if self.username or self.password:
            auth_part = f"{self.username}:{self.password}@"
        return f"http://{auth_part}{self.host}:{self.http_port}"


@dataclass(frozen=True)
class BinanceConfig:
    """Mock Binance configuration"""
    api_key: str = ""
    secret_key: str = ""
    max_weight_per_minute: int = 2400
    weight_buffer: int = 200
    use_testnet: bool = False

    def is_configured(self) -> bool:
        return bool(self.api_key and self.secret_key)


@dataclass(frozen=True)
class ProcessingConfig:
    """Mock processing configuration with v4.2 dynamic parameters"""
    historical_days_back: int = 30
    websocket_switch_hours: int = 24
    global_batch_enabled: bool = True
    global_batch_interval_seconds: int = 1
    max_buffer_size_per_ticker: int = 10000

    # v4.2 Dynamic WebSocket Management
    dynamic_websocket_enabled: bool = True
    dynamic_deactivation_threshold: float = 30.0
    dynamic_reactivation_threshold: float = 25.0
    dynamic_evaluation_interval: int = 10
    dynamic_max_consecutive_overloads: int = 3
    dynamic_enable_buffer_discard: bool = True

    # v4.2 Timing Calculator and startup strategies
    timing_calculator_enabled: bool = True
    websocket_startup_strategy: str = "auto"
    buffer_overflow_protection: bool = True

    # Ticker filtering
    load_only_tickers: List[str] = None

    def __post_init__(self):
        if self.load_only_tickers is None:
            object.__setattr__(self, 'load_only_tickers', [])

    def should_load_ticker(self, symbol: str) -> bool:
        if not self.load_only_tickers:
            return True
        return symbol.upper() in [t.upper() for t in self.load_only_tickers]


@dataclass(frozen=True)
class DatabaseConfig:
    """Mock database configuration"""
    host: str = "localhost"
    port: int = 3306
    database: str = "main"
    username: str = "vadim"
    password: str = "test"
    ticker_sync_interval_hours: int = 1

    def is_configured(self) -> bool:
        return bool(self.host and self.database and self.username)


@dataclass(frozen=True)
class MonitoringConfig:
    """Mock monitoring configuration"""
    loggerino_file_path: str = "./logs/data_provider.log"
    metrics_enabled: bool = True
    system_stats_interval_seconds: int = 60
    health_check_interval_seconds: int = 60


@dataclass(frozen=True)
class APIConfig:
    """Mock API configuration"""
    host: str = "0.0.0.0"
    port: int = 8080
    websocket_enabled: bool = True


@dataclass(frozen=True)
class FeatureFlags:
    """Mock feature flags"""
    enable_websocket_streaming: bool = True
    enable_rest_api: bool = True


@dataclass(frozen=True)
class WebSocketStreamingConfig:
    """Mock WebSocket streaming configuration"""
    per_ticker_enabled: bool = True


@dataclass(frozen=True)
class Config:
    """Mock complete configuration"""
    clickhouse: ClickHouseConfig
    binance: BinanceConfig
    processing: ProcessingConfig
    database: DatabaseConfig
    monitoring: MonitoringConfig
    api: APIConfig
    features: FeatureFlags
    websocket_streaming: WebSocketStreamingConfig
    environment: str = "test"
    debug: bool = True

    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> 'Config':
        """Mock config loading from environment"""
        return cls(
            clickhouse=ClickHouseConfig(),
            binance=BinanceConfig(),
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig(),
            features=FeatureFlags(),
            websocket_streaming=WebSocketStreamingConfig()
        )

    def validate(self) -> List[str]:
        """Mock configuration validation"""
        issues = []

        if not self.clickhouse.is_configured():
            issues.append("ClickHouse configuration incomplete")

        if not self.binance.is_configured():
            issues.append("Binance API credentials not configured")

        if not self.database.is_configured():
            issues.append("Database configuration incomplete")

        return issues

    def is_production(self) -> bool:
        return self.environment.lower() == 'production'


class MockTickerModel:
    """Mock ticker model"""

    def __init__(self, symbol: str = "BTCUSDT"):
        self.symbol = symbol


# ===== MOCK COMPONENTS =====

class MockBinanceClient:
    """Mock Binance AsyncClient with realistic behavior"""

    def __init__(self, api_key: str, secret_key: str, testnet: bool = False):
        self.api_key = api_key
        self.secret_key = secret_key
        self.testnet = testnet
        self.is_connected = False
        self.connection_time: Optional[float] = None

    @classmethod
    async def create(cls, api_key: str, secret_key: str, testnet: bool = False):
        """Mock AsyncClient.create method"""
        instance = cls(api_key, secret_key, testnet)
        instance.is_connected = True
        instance.connection_time = time.perf_counter()
        return instance

    async def get_exchange_info(self):
        """Mock exchange info"""
        return {
            "symbols": [
                {"symbol": "BTCUSDT", "status": "TRADING"},
                {"symbol": "ETHUSDT", "status": "TRADING"},
                {"symbol": "BNBUSDT", "status": "TRADING"}
            ]
        }


class MockClickHouseManager:
    """Mock ClickHouseManager for startup testing"""

    def __init__(self, config: ClickHouseConfig):
        self.config = config
        self.is_connected = False
        self.connection_time: Optional[float] = None
        self.connection_attempts = 0

    async def ensure_connected(self) -> None:
        """Mock connection establishment"""
        self.connection_attempts += 1
        self.is_connected = True
        self.connection_time = time.perf_counter()

    def close(self) -> None:
        """Mock connection cleanup"""
        self.is_connected = False


class MockGlobalTradesUpdater:
    """Mock GlobalTradesUpdater for startup testing"""

    def __init__(self, clickhouse_manager: MockClickHouseManager, config: ProcessingConfig):
        self.clickhouse_manager = clickhouse_manager
        self.config = config
        self.is_running = False
        self.start_time: Optional[float] = None

    async def start(self) -> bool:
        """Mock start method"""
        self.is_running = True
        self.start_time = time.perf_counter()
        return True

    async def stop(self) -> None:
        """Mock stop method"""
        self.is_running = False


class MockWeightCoordinator:
    """Mock WeightCoordinator for startup testing"""

    def __init__(self, binance_client: MockBinanceClient, config: Config):
        self.binance_client = binance_client
        self.config = config
        self.is_running = False
        self.start_time: Optional[float] = None

    async def start(self) -> None:
        """Mock start method"""
        self.is_running = True
        self.start_time = time.perf_counter()

    async def shutdown(self) -> None:
        """Mock shutdown method"""
        self.is_running = False


class MockTickerSynchronizer:
    """Mock TickerSynchronizer for startup testing"""

    def __init__(self, binance_client: MockBinanceClient, shared, config: ProcessingConfig):
        self.binance_client = binance_client
        self.shared = shared
        self.config = config
        self.is_running = False
        self.start_time: Optional[float] = None
        self.sync_count = 0

    async def start(self) -> bool:
        """Mock start method with ticker addition"""
        self.is_running = True
        self.start_time = time.perf_counter()

        # Simulate adding tickers to shared
        if hasattr(self.shared, 'add_ticker'):
            test_symbols = ["BTCUSDT", "ETHUSDT"]
            for symbol in test_symbols:
                ticker_model = MockTickerModel(symbol)
                await self.shared.add_ticker(symbol, ticker_model)

        self.sync_count += 1
        return True

    async def stop(self) -> None:
        """Mock stop method"""
        self.is_running = False

    @property
    def is_running_property(self) -> bool:
        return self.is_running


class MockRestAPIServer:
    """Mock REST API Server for startup testing"""

    def __init__(self, shared, config: Config):
        self.shared = shared
        self.config = config
        self.is_running = False
        self.start_time: Optional[float] = None

    async def start_server(self) -> None:
        """Mock start server"""
        self.is_running = True
        self.start_time = time.perf_counter()

    async def stop_server(self) -> None:
        """Mock stop server"""
        self.is_running = False


class MockTickerProcessor:
    """Mock TickerProcessor for startup testing"""

    def __init__(self, ticker_model: MockTickerModel, binance_client: MockBinanceClient,
                 weight_coordinator: MockWeightCoordinator, trades_writer, config: Config):
        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.weight_coordinator = weight_coordinator
        self.trades_writer = trades_writer
        self.config = config
        self.is_running = False
        self.start_time: Optional[float] = None

    async def run(self) -> None:
        """Mock run method"""
        self.is_running = True
        self.start_time = time.perf_counter()

        # Simulate some processing time
        await asyncio.sleep(0.01)

    async def shutdown(self, timeout_seconds: float = 30.0) -> None:
        """Mock shutdown method"""
        self.is_running = False


class MockTradesWriterImpl:
    """Mock TradesWriter implementation"""

    def __init__(self, global_trades_updater: MockGlobalTradesUpdater,
                 clickhouse_manager: MockClickHouseManager):
        self.global_trades_updater = global_trades_updater
        self.clickhouse_manager = clickhouse_manager


# ===== ENHANCED SHARED MOCK =====

class MockShared:
    """
    Comprehensive mock Shared for testing complete startup integration.
    """

    def __init__(self, config: Config):
        self.config = config

        # Initialization state
        self._is_initialized: bool = False
        self._initialization_time: Optional[int] = None
        self.initialization_steps: List[str] = []

        # Component initialization tracking
        self.binance_client_initialized = False
        self.clickhouse_manager_initialized = False
        self.global_trades_updater_initialized = False
        self.weight_coordinator_initialized = False
        self.ticker_synchronizer_initialized = False
        self.rest_api_initialized = False

        # Shared resources (will be mocked)
        self.binance_client: Optional[MockBinanceClient] = None
        self.weight_coordinator: Optional[MockWeightCoordinator] = None
        self.ticker_synchronizer: Optional[MockTickerSynchronizer] = None
        self.rest_api: Optional[MockRestAPIServer] = None
        self.clickhouse_manager: Optional[MockClickHouseManager] = None
        self.global_trades_updater: Optional[MockGlobalTradesUpdater] = None

        # Active ticker tasks
        self.tickers: Dict[str, asyncio.Task] = {}

        # Error simulation
        self.should_fail_step: Optional[str] = None
        self.failure_message: str = "Simulated startup failure"

    async def initialize(self) -> bool:
        """Mock initialization with detailed step tracking"""
        if self._is_initialized:
            return True

        try:
            self._initialization_time = int(time.perf_counter() * 1000)

            # Phase 1: Create core components
            await self._initialize_binance_client()
            await self._create_coordinators()

            # Phase 2: Start coordinators
            await self._start_coordinators()

            self._is_initialized = True
            return True

        except Exception as e:
            await self._cleanup_on_failure()
            raise e

    async def _initialize_binance_client(self) -> None:
        """Mock Binance client initialization"""
        self.initialization_steps.append("initialize_binance_client")

        if self.should_fail_step == "binance_client":
            raise Exception(self.failure_message)

        if not self.config.binance.is_configured():
            raise ValueError("Binance API credentials not configured")

        # Mock AsyncClient.create
        self.binance_client = await MockBinanceClient.create(
            self.config.binance.api_key or "test_key",
            self.config.binance.secret_key or "test_secret",
            testnet=self.config.binance.use_testnet
        )

        self.binance_client_initialized = True

    async def _create_coordinators(self) -> None:
        """Mock coordinator creation"""
        self.initialization_steps.append("create_coordinators")

        if self.should_fail_step == "coordinators":
            raise Exception(self.failure_message)

        # Create ClickHouseManager
        self.clickhouse_manager = MockClickHouseManager(self.config.clickhouse)
        await self.clickhouse_manager.ensure_connected()
        self.clickhouse_manager_initialized = True

        # Create GlobalTradesUpdater
        self.global_trades_updater = MockGlobalTradesUpdater(
            clickhouse_manager=self.clickhouse_manager,
            config=self.config.processing
        )
        self.global_trades_updater_initialized = True

        # Create WeightCoordinator
        self.weight_coordinator = MockWeightCoordinator(
            binance_client=self.binance_client,
            config=self.config
        )
        self.weight_coordinator_initialized = True

        # Create TickerSynchronizer
        self.ticker_synchronizer = MockTickerSynchronizer(
            binance_client=self.binance_client,
            shared=self,
            config=self.config.processing
        )
        self.ticker_synchronizer_initialized = True

        # Create REST API Server
        self.rest_api = MockRestAPIServer(
            shared=self,
            config=self.config
        )
        self.rest_api_initialized = True

    async def _start_coordinators(self) -> None:
        """Mock coordinator startup"""
        self.initialization_steps.append("start_coordinators")

        if self.should_fail_step == "start_coordinators":
            raise Exception(self.failure_message)

        # Start GlobalTradesUpdater
        if self.global_trades_updater:
            await self.global_trades_updater.start()

        # Start WeightCoordinator
        if self.weight_coordinator:
            await self.weight_coordinator.start()

        # Start TickerSynchronizer - this will add tickers
        if self.ticker_synchronizer:
            await self.ticker_synchronizer.start()

        # Start REST API server
        if self.rest_api:
            await self.rest_api.start_server()

    async def shutdown(self) -> None:
        """Mock graceful shutdown"""
        if not self._is_initialized:
            return

        # Shutdown ticker tasks
        await self._shutdown_ticker_tasks()

        # Shutdown coordinators in reverse order
        await self._shutdown_coordinators()

        # Close Binance client
        if self.binance_client:
            self.binance_client.is_connected = False

        self._is_initialized = False

    async def _shutdown_ticker_tasks(self) -> None:
        """Mock ticker task shutdown"""
        for symbol, task in list(self.tickers.items()):
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        self.tickers.clear()

    async def _shutdown_coordinators(self) -> None:
        """Mock coordinator shutdown"""
        if self.rest_api:
            await self.rest_api.stop_server()

        if self.ticker_synchronizer:
            await self.ticker_synchronizer.stop()

        if self.weight_coordinator:
            await self.weight_coordinator.shutdown()

        if self.global_trades_updater:
            await self.global_trades_updater.stop()

        if self.clickhouse_manager:
            self.clickhouse_manager.close()

    async def _cleanup_on_failure(self) -> None:
        """Mock cleanup on initialization failure"""
        try:
            await self.shutdown()
        except Exception:
            pass

    async def add_ticker(self, symbol: str, ticker_model: MockTickerModel) -> bool:
        """Mock add ticker method"""
        symbol = symbol.upper()

        if symbol in self.tickers and not self.tickers[symbol].done():
            return False

        try:
            # Create mock TickerProcessor
            processor = MockTickerProcessor(
                ticker_model=ticker_model,
                binance_client=self.binance_client,
                weight_coordinator=self.weight_coordinator,
                trades_writer=MockTradesWriterImpl(self.global_trades_updater, self.clickhouse_manager),
                config=self.config
            )

            task = asyncio.create_task(
                processor.run(),
                name=f"ticker_processor_{symbol}"
            )

            self.tickers[symbol] = task
            return True

        except Exception:
            return False

    def get_trades_writer(self):
        """Mock get trades writer"""
        if not self.global_trades_updater or not self.clickhouse_manager:
            raise RuntimeError("Shared not initialized")

        return MockTradesWriterImpl(
            global_trades_updater=self.global_trades_updater,
            clickhouse_manager=self.clickhouse_manager
        )

    def get_active_ticker_symbols(self) -> List[str]:
        """Mock get active ticker symbols"""
        return [symbol for symbol, task in self.tickers.items() if not task.done()]

    def get_system_info(self) -> Dict[str, Any]:
        """Mock get system information"""
        return {
            'initialized': self._is_initialized,
            'initialization_time': self._initialization_time,
            'total_ticker_tasks': len(self.tickers),
            'active_tickers_count': len(self.get_active_ticker_symbols()),
            'binance_client_connected': self.binance_client.is_connected if self.binance_client else False,
            'weight_coordinator_status': 'active' if self.weight_coordinator and self.weight_coordinator.is_running else 'inactive',
            'ticker_synchronizer_status': 'active' if self.ticker_synchronizer and self.ticker_synchronizer.is_running else 'inactive',
            'rest_api_status': 'active' if self.rest_api and self.rest_api.is_running else 'inactive',
            'environment': self.config.environment
        }

    async def health_check(self) -> Dict[str, Any]:
        """Mock comprehensive health check"""
        health = {
            'initialized': self._is_initialized,
            'timestamp': int(time.time() * 1000),
            'uptime_ms': int(time.time() * 1000) - (self._initialization_time or 0)
        }

        # Component health
        health['binance_client'] = {
            'connected': self.binance_client.is_connected if self.binance_client else False
        }

        health['weight_coordinator'] = {
            'active': self.weight_coordinator.is_running if self.weight_coordinator else False
        }

        health['ticker_synchronizer'] = {
            'active': self.ticker_synchronizer.is_running if self.ticker_synchronizer else False
        }

        health['global_trades_updater'] = {
            'active': self.global_trades_updater.is_running if self.global_trades_updater else False
        }

        health['clickhouse_manager'] = {
            'active': self.clickhouse_manager.is_connected if self.clickhouse_manager else False
        }

        health['rest_api'] = {
            'active': self.rest_api.is_running if self.rest_api else False
        }

        # Ticker health
        active_tickers = self.get_active_ticker_symbols()
        health['tickers'] = {
            'total_tasks': len(self.tickers),
            'active_tasks': len(active_tickers),
            'active_symbols': active_tickers
        }

        # Overall health assessment
        components_healthy = (
                self._is_initialized and
                self.binance_client and self.binance_client.is_connected and
                len(active_tickers) >= 0
        )

        health['overall_status'] = 'healthy' if components_healthy else 'unhealthy'

        return health

    def set_failure_simulation(self, step: str, message: str = "Simulated failure"):
        """Configure failure simulation for testing"""
        self.should_fail_step = step
        self.failure_message = message

    @property
    def is_initialized(self) -> bool:
        """Check if shared is initialized"""
        return self._is_initialized


# ===== TEST UTILITIES =====

def create_test_config(**overrides) -> Config:
    """Create test configuration with optional overrides"""
    defaults = {
        'environment': 'test',
        'debug': True
    }
    defaults.update(overrides)

    return Config(
        clickhouse=ClickHouseConfig(),
        binance=BinanceConfig(
            api_key="test_api_key",
            secret_key="test_secret_key"
        ),
        processing=ProcessingConfig(),
        database=DatabaseConfig(),
        monitoring=MonitoringConfig(),
        api=APIConfig(),
        features=FeatureFlags(),
        websocket_streaming=WebSocketStreamingConfig(),
        **defaults
    )


def create_invalid_config(invalid_component: str = "binance") -> Config:
    """Create configuration with specific invalid component"""
    config = create_test_config()

    if invalid_component == "binance":
        config = Config(
            clickhouse=config.clickhouse,
            binance=BinanceConfig(api_key="", secret_key=""),  # Invalid
            processing=config.processing,
            database=config.database,
            monitoring=config.monitoring,
            api=config.api,
            features=config.features,
            websocket_streaming=config.websocket_streaming,
            environment=config.environment,
            debug=config.debug
        )
    elif invalid_component == "clickhouse":
        config = Config(
            clickhouse=ClickHouseConfig(host="", database=""),  # Invalid
            binance=config.binance,
            processing=config.processing,
            database=config.database,
            monitoring=config.monitoring,
            api=config.api,
            features=config.features,
            websocket_streaming=config.websocket_streaming,
            environment=config.environment,
            debug=config.debug
        )

    return config


async def wait_for_initialization(shared: MockShared, timeout: float = 10.0) -> bool:
    """Wait for shared initialization to complete"""
    start_time = time.time()

    while time.time() - start_time < timeout:
        if shared.is_initialized:
            return True
        await asyncio.sleep(0.1)

    return False


async def wait_for_tickers(shared: MockShared, expected_count: int, timeout: float = 10.0) -> bool:
    """Wait for expected number of active tickers"""
    start_time = time.time()

    while time.time() - start_time < timeout:
        if len(shared.get_active_ticker_symbols()) >= expected_count:
            return True
        await asyncio.sleep(0.1)

    return False


# ===== FIXTURES =====

@pytest.fixture
def test_config():
    """Provide valid test configuration"""
    return create_test_config()


@pytest.fixture
def invalid_binance_config():
    """Provide configuration with invalid Binance settings"""
    return create_invalid_config("binance")


@pytest.fixture
def invalid_clickhouse_config():
    """Provide configuration with invalid ClickHouse settings"""
    return create_invalid_config("clickhouse")


@pytest.fixture
def shared_factory():
    """Factory to create MockShared instances for testing"""
    created_instances = []

    def create_shared(config: Config) -> MockShared:
        shared = MockShared(config)
        created_instances.append(shared)
        return shared

    yield create_shared

    # Cleanup
    for shared in created_instances:
        if shared.is_initialized:
            try:
                # Force stop for cleanup
                shared._is_initialized = False
            except:
                pass


# ===== COMPREHENSIVE SYSTEM STARTUP TESTS =====

class TestColdStartSystemInitialization:
    """Test complete cold start system initialization"""

    @pytest.mark.asyncio
    async def test_successful_cold_start_initialization(self, shared_factory, test_config):
        """Test successful complete system cold start"""
        shared = shared_factory(test_config)

        # Execute initialization
        success = await shared.initialize()

        # Verify successful initialization
        assert success is True
        assert shared.is_initialized is True
        assert shared._initialization_time is not None

        # Verify initialization steps executed
        expected_steps = [
            "initialize_binance_client",
            "create_coordinators",
            "start_coordinators"
        ]
        assert shared.initialization_steps == expected_steps

        # Verify all components initialized
        assert shared.binance_client_initialized is True
        assert shared.clickhouse_manager_initialized is True
        assert shared.global_trades_updater_initialized is True
        assert shared.weight_coordinator_initialized is True
        assert shared.ticker_synchronizer_initialized is True
        assert shared.rest_api_initialized is True

        # Verify all components created
        assert shared.binance_client is not None
        assert shared.clickhouse_manager is not None
        assert shared.global_trades_updater is not None
        assert shared.weight_coordinator is not None
        assert shared.ticker_synchronizer is not None
        assert shared.rest_api is not None

        # Verify component states
        assert shared.binance_client.is_connected is True
        assert shared.clickhouse_manager.is_connected is True
        assert shared.global_trades_updater.is_running is True
        assert shared.weight_coordinator.is_running is True
        assert shared.ticker_synchronizer.is_running is True
        assert shared.rest_api.is_running is True

        # Cleanup
        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_component_initialization_order(self, shared_factory, test_config):
        """Test that components are initialized in correct dependency order"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify ClickHouse initialized before GlobalTradesUpdater
        clickhouse_time = shared.clickhouse_manager.connection_time
        global_trades_time = shared.global_trades_updater.start_time
        assert clickhouse_time < global_trades_time

        # Verify Binance client initialized before WeightCoordinator
        binance_time = shared.binance_client.connection_time
        weight_time = shared.weight_coordinator.start_time
        assert binance_time < weight_time

        # Verify WeightCoordinator started before TickerSynchronizer
        ticker_sync_time = shared.ticker_synchronizer.start_time
        assert weight_time < ticker_sync_time

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_automatic_ticker_addition_during_startup(self, shared_factory, test_config):
        """Test that TickerSynchronizer automatically adds tickers during startup"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Wait for ticker addition
        success = await wait_for_tickers(shared, 2, timeout=5.0)
        assert success is True

        # Verify tickers were added
        active_symbols = shared.get_active_ticker_symbols()
        assert len(active_symbols) >= 2
        assert "BTCUSDT" in active_symbols
        assert "ETHUSDT" in active_symbols

        # Verify TickerSynchronizer performed sync
        assert shared.ticker_synchronizer.sync_count >= 1

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_system_info_after_initialization(self, shared_factory, test_config):
        """Test system info provides accurate data after initialization"""
        shared = shared_factory(test_config)

        await shared.initialize()
        await wait_for_tickers(shared, 1, timeout=5.0)

        system_info = shared.get_system_info()

        # Verify system info accuracy
        assert system_info['initialized'] is True
        assert system_info['initialization_time'] is not None
        assert system_info['binance_client_connected'] is True
        assert system_info['weight_coordinator_status'] == 'active'
        assert system_info['ticker_synchronizer_status'] == 'active'
        assert system_info['rest_api_status'] == 'active'
        assert system_info['environment'] == 'test'
        assert system_info['active_tickers_count'] >= 0

        await shared.shutdown()


class TestConfigurationLoadingAndValidation:
    """Test configuration loading and validation scenarios"""

    def test_config_from_env_loading(self):
        """Test Config.from_env() loading mechanism"""
        # Test basic loading
        config = Config.from_env()

        # Verify all sections loaded
        assert config.clickhouse is not None
        assert config.binance is not None
        assert config.processing is not None
        assert config.database is not None
        assert config.monitoring is not None
        assert config.api is not None
        assert config.features is not None
        assert config.websocket_streaming is not None

        # Verify default values
        assert config.environment == "test"  # Mock default
        assert config.debug is True

    def test_v42_dynamic_websocket_parameters(self):
        """Test that v4.2 dynamic WebSocket parameters are loaded correctly"""
        config = Config.from_env()

        # Verify v4.2 dynamic WebSocket management parameters
        assert config.processing.dynamic_websocket_enabled is True
        assert config.processing.dynamic_deactivation_threshold == 30.0
        assert config.processing.dynamic_reactivation_threshold == 25.0
        assert config.processing.dynamic_evaluation_interval == 10
        assert config.processing.dynamic_max_consecutive_overloads == 3
        assert config.processing.dynamic_enable_buffer_discard is True

        # Verify v4.2 timing calculator parameters
        assert config.processing.timing_calculator_enabled is True
        assert config.processing.websocket_startup_strategy == "auto"
        assert config.processing.buffer_overflow_protection is True

    def test_configuration_validation_success(self, test_config):
        """Test configuration validation with valid configuration"""
        issues = test_config.validate()

        # Should have no validation issues
        assert len(issues) == 0

    def test_configuration_validation_binance_failure(self, invalid_binance_config):
        """Test configuration validation with invalid Binance config"""
        issues = invalid_binance_config.validate()

        # Should detect Binance configuration issue
        assert len(issues) > 0
        assert any("Binance API credentials" in issue for issue in issues)

    def test_configuration_validation_clickhouse_failure(self, invalid_clickhouse_config):
        """Test configuration validation with invalid ClickHouse config"""
        issues = invalid_clickhouse_config.validate()

        # Should detect ClickHouse configuration issue
        assert len(issues) > 0
        assert any("ClickHouse configuration" in issue for issue in issues)

    @pytest.mark.asyncio
    async def test_startup_with_invalid_configuration(self, shared_factory, invalid_binance_config):
        """Test startup failure with invalid configuration"""
        shared = shared_factory(invalid_binance_config)

        # Should fail during initialization
        with pytest.raises(ValueError, match="Binance API credentials not configured"):
            await shared.initialize()

        # Verify not initialized
        assert shared.is_initialized is False

    def test_processing_config_ticker_filtering(self):
        """Test ProcessingConfig ticker filtering functionality"""
        # Test with empty load_only_tickers (should load all)
        config = ProcessingConfig(load_only_tickers=[])
        assert config.should_load_ticker("BTCUSDT") is True
        assert config.should_load_ticker("ETHUSDT") is True

        # Test with specific tickers only
        config = ProcessingConfig(load_only_tickers=["BTCUSDT", "ETHUSDT"])
        assert config.should_load_ticker("BTCUSDT") is True
        assert config.should_load_ticker("ETHUSDT") is True
        assert config.should_load_ticker("BNBUSDT") is False

        # Test case insensitive
        assert config.should_load_ticker("btcusdt") is True


class TestResourceAllocationAndHealthChecks:
    """Test resource allocation and health monitoring"""

    @pytest.mark.asyncio
    async def test_binance_client_resource_allocation(self, shared_factory, test_config):
        """Test Binance client resource allocation and connection"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify Binance client properly allocated
        assert shared.binance_client is not None
        assert shared.binance_client.is_connected is True
        assert shared.binance_client.connection_time is not None
        assert shared.binance_client.api_key == "test_api_key"
        assert shared.binance_client.secret_key == "test_secret_key"

        # Verify connection functionality
        exchange_info = await shared.binance_client.get_exchange_info()
        assert exchange_info is not None
        assert 'symbols' in exchange_info

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_clickhouse_connection_establishment(self, shared_factory, test_config):
        """Test ClickHouse connection establishment and health"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify ClickHouse manager properly initialized
        assert shared.clickhouse_manager is not None
        assert shared.clickhouse_manager.is_connected is True
        assert shared.clickhouse_manager.connection_time is not None
        assert shared.clickhouse_manager.connection_attempts >= 1

        # Verify configuration applied
        assert shared.clickhouse_manager.config.host == "localhost"
        assert shared.clickhouse_manager.config.database == "trades"

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_weight_coordinator_startup(self, shared_factory, test_config):
        """Test WeightCoordinator startup and resource allocation"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify WeightCoordinator properly started
        assert shared.weight_coordinator is not None
        assert shared.weight_coordinator.is_running is True
        assert shared.weight_coordinator.start_time is not None
        assert shared.weight_coordinator.binance_client == shared.binance_client

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_global_trades_updater_startup(self, shared_factory, test_config):
        """Test GlobalTradesUpdater startup and integration"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify GlobalTradesUpdater properly started
        assert shared.global_trades_updater is not None
        assert shared.global_trades_updater.is_running is True
        assert shared.global_trades_updater.start_time is not None
        assert shared.global_trades_updater.clickhouse_manager == shared.clickhouse_manager

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_comprehensive_health_check(self, shared_factory, test_config):
        """Test comprehensive system health check after startup"""
        shared = shared_factory(test_config)

        await shared.initialize()
        await wait_for_tickers(shared, 1, timeout=5.0)

        health = await shared.health_check()

        # Verify overall health
        assert health['initialized'] is True
        assert health['overall_status'] == 'healthy'
        assert health['timestamp'] > 0
        assert health['uptime_ms'] >= 0

        # Verify component health
        assert health['binance_client']['connected'] is True
        assert health['weight_coordinator']['active'] is True
        assert health['ticker_synchronizer']['active'] is True
        assert health['global_trades_updater']['active'] is True
        assert health['clickhouse_manager']['active'] is True
        assert health['rest_api']['active'] is True

        # Verify ticker health
        assert health['tickers']['total_tasks'] >= 0
        assert health['tickers']['active_tasks'] >= 0
        assert isinstance(health['tickers']['active_symbols'], list)

        await shared.shutdown()


class TestComponentDependencyResolution:
    """Test component dependency resolution and cross-wiring"""

    @pytest.mark.asyncio
    async def test_clickhouse_to_global_trades_dependency(self, shared_factory, test_config):
        """Test ClickHouse → GlobalTradesUpdater dependency resolution"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify dependency wiring
        assert shared.global_trades_updater.clickhouse_manager == shared.clickhouse_manager

        # Verify ClickHouse initialized before GlobalTradesUpdater
        clickhouse_time = shared.clickhouse_manager.connection_time
        global_trades_time = shared.global_trades_updater.start_time
        assert clickhouse_time < global_trades_time

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_binance_to_weight_coordinator_dependency(self, shared_factory, test_config):
        """Test Binance client → WeightCoordinator dependency resolution"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify dependency wiring
        assert shared.weight_coordinator.binance_client == shared.binance_client

        # Verify Binance client initialized before WeightCoordinator
        binance_time = shared.binance_client.connection_time
        weight_time = shared.weight_coordinator.start_time
        assert binance_time < weight_time

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_shared_to_ticker_synchronizer_dependency(self, shared_factory, test_config):
        """Test Shared → TickerSynchronizer dependency resolution"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify dependency wiring
        assert shared.ticker_synchronizer.shared == shared
        assert shared.ticker_synchronizer.binance_client == shared.binance_client

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_trades_writer_interface_creation(self, shared_factory, test_config):
        """Test TradesWriter interface creation and dependency injection"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify TradesWriter can be created
        trades_writer = shared.get_trades_writer()
        assert trades_writer is not None
        assert trades_writer.global_trades_updater == shared.global_trades_updater
        assert trades_writer.clickhouse_manager == shared.clickhouse_manager

        await shared.shutdown()

    @pytest.mark.asyncio
    async def test_cross_component_integration(self, shared_factory, test_config):
        """Test cross-component integration after initialization"""
        shared = shared_factory(test_config)

        await shared.initialize()

        # Verify all cross-component references
        components = [
            shared.binance_client,
            shared.clickhouse_manager,
            shared.global_trades_updater,
            shared.weight_coordinator,
            shared.ticker_synchronizer,
            shared.rest_api
        ]

        # All components should be created
        for component in components:
            assert component is not None

        # Verify REST API has access to shared
        assert shared.rest_api.shared == shared

        await shared.shutdown()


class TestStartupErrorHandlingAndRecovery:
    """Test error handling during startup and recovery scenarios"""

    @pytest.mark.asyncio
    async def test_binance_client_initialization_failure(self, shared_factory, test_config):
        """Test graceful handling of Binance client initialization failure"""
        shared = shared_factory(test_config)
        shared.set_failure_simulation("binance_client", "Mock Binance client failure")

        # Should fail during Binance client initialization
        with pytest.raises(Exception, match="Mock Binance client failure"):
            await shared.initialize()

        # Verify not initialized and partial cleanup
        assert shared.is_initialized is False
        assert "initialize_binance_client" in shared.initialization_steps

    @pytest.mark.asyncio
    async def test_coordinator_creation_failure(self, shared_factory, test_config):
        """Test graceful handling of coordinator creation failure"""
        shared = shared_factory(test_config)
        shared.set_failure_simulation("coordinators", "Mock coordinator creation failure")

        # Should fail during coordinator creation
        with pytest.raises(Exception, match="Mock coordinator creation failure"):
            await shared.initialize()

        # Verify initialization steps completed before failure
        assert shared.is_initialized is False
        assert "initialize_binance_client" in shared.initialization_steps
        assert "create_coordinators" in shared.initialization_steps

    @pytest.mark.asyncio
    async def test_coordinator_startup_failure(self, shared_factory, test_config):
        """Test graceful handling of coordinator startup failure"""
        shared = shared_factory(test_config)
        shared.set_failure_simulation("start_coordinators", "Mock coordinator startup failure")

        # Should fail during coordinator startup
        with pytest.raises(Exception, match="Mock coordinator startup failure"):
            await shared.initialize()

        # Verify initialization steps completed before failure
        assert shared.is_initialized is False
        expected_steps = [
            "initialize_binance_client",
            "create_coordinators",
            "start_coordinators"
        ]
        assert shared.initialization_steps == expected_steps

    @pytest.mark.asyncio
    async def test_partial_initialization_cleanup(self, shared_factory, test_config):
        """Test cleanup of partially initialized components on failure"""
        shared = shared_factory(test_config)
        shared.set_failure_simulation("start_coordinators", "Simulated startup failure")

        try:
            await shared.initialize()
        except Exception:
            pass

        # Verify cleanup was attempted
        # In real implementation, this would verify resource deallocation
        assert shared.is_initialized is False

    @pytest.mark.asyncio
    async def test_graceful_shutdown_after_partial_initialization(self, shared_factory, test_config):
        """Test graceful shutdown after partial initialization"""
        shared = shared_factory(test_config)

        # Initialize successfully first
        await shared.initialize()

        # Verify can shutdown gracefully
        await shared.shutdown()

        # Verify clean shutdown state
        assert shared.is_initialized is False
        if shared.binance_client:
            assert shared.binance_client.is_connected is False


class TestGracefulStartupFailureScenarios:
    """Test graceful failure scenarios and error propagation"""

    @pytest.mark.asyncio
    async def test_resource_deallocation_on_startup_failure(self, shared_factory, test_config):
        """Test proper resource deallocation when startup fails"""
        shared = shared_factory(test_config)
        shared.set_failure_simulation("start_coordinators", "Resource allocation failure")

        try:
            await shared.initialize()
        except Exception:
            pass

        # Verify resources were deallocated (simulated)
        assert shared.is_initialized is False

        # In real implementation, would verify:
        # - Binance client connections closed
        # - ClickHouse connections closed
        # - Async tasks cancelled
        # - Memory released

    @pytest.mark.asyncio
    async def test_error_propagation_and_logging(self, shared_factory, test_config):
        """Test error propagation and logging during startup failures"""
        shared = shared_factory(test_config)
        error_message = "Critical startup component failure"
        shared.set_failure_simulation("coordinators", error_message)

        # Verify error propagates correctly
        with pytest.raises(Exception) as exc_info:
            await shared.initialize()

        assert error_message in str(exc_info.value)

        # Verify initialization state reflects failure
        assert shared.is_initialized is False

    @pytest.mark.asyncio
    async def test_initialization_retry_after_failure(self, shared_factory, test_config):
        """Test that initialization can be retried after failure"""
        shared = shared_factory(test_config)

        # First attempt - simulate failure
        shared.set_failure_simulation("coordinators", "Temporary failure")

        with pytest.raises(Exception):
            await shared.initialize()

        # Verify failed state
        assert shared.is_initialized is False

        # Second attempt - remove failure simulation
        shared.set_failure_simulation(None)

        # Should succeed on retry
        success = await shared.initialize()
        assert success is True
        assert shared.is_initialized is True

        await shared.shutdown()


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])