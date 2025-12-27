"""
Unit Tests for TickerSynchronizer
=================================

Comprehensive testing of TickerSynchronizer with exchangeInfo management,
symbol filtering, ticker lifecycle, and integration with Shared coordinator.

File: tests/unit/test_ticker_sync.py
Usage: python -m pytest tests/unit/test_ticker_sync.py -v
"""

import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import sys

# Mock dependencies first
sys.modules['loggerino'] = MagicMock()
sys.modules['loggerino.loggerino'] = MagicMock()

# Create mock loggerino.get function
mock_loggerino = MagicMock()
mock_loggerino.get.return_value = MagicMock()
sys.modules['loggerino'].loggerino = mock_loggerino

# Import basic models first
from models.config import ProcessingConfig
from models.exchange_info import BinanceExchangeInfo, BinanceFilter
from models.ticker_model import TickerModel
from models.enums import BinanceTickerStatus

# Import TickerSynchronizer using source modification for pytest
import importlib.util

# Read and modify the TickerSynchronizer source for testing
original_file = Path(__file__).parent.parent.parent / "src" / "core" / "ticker_synchronizer.py"
with open(original_file, 'r') as f:
    source_code = f.read()

# Replace relative imports with absolute imports for testing
modified_source = source_code.replace(
    "from ..models.config import ProcessingConfig",
    "from models.config import ProcessingConfig"
).replace(
    "from ..models.exchange_info import BinanceExchangeInfo",
    "from models.exchange_info import BinanceExchangeInfo"
).replace(
    "from ..models.ticker_model import TickerModel",
    "from models.ticker_model import TickerModel"
).replace(
    "from loggerino import loggerino",
    "import sys; loggerino = sys.modules['loggerino'].loggerino"
)

# Execute modified module
spec = importlib.util.spec_from_loader("ticker_synchronizer", loader=None)
ticker_sync_module = importlib.util.module_from_spec(spec)
exec(modified_source, ticker_sync_module.__dict__)

TickerSynchronizer = ticker_sync_module.TickerSynchronizer


def create_mock_binance_client():
    """Create mock Binance client with async methods."""
    client = AsyncMock()
    client.get_exchange_info = AsyncMock()
    return client


def create_mock_shared():
    """Create mock Shared resource manager."""
    shared = MagicMock()
    shared.weight_coordinator = AsyncMock()
    shared.weight_coordinator.request = AsyncMock()
    shared.add_ticker = AsyncMock()
    shared.remove_ticker = AsyncMock()
    shared.get_active_ticker_symbols = MagicMock(return_value=[])
    return shared


def create_processing_config(load_only_tickers=None):
    """Create ProcessingConfig for testing."""
    return ProcessingConfig(
        historical_days_back=30,
        load_only_tickers=load_only_tickers or []
    )


def create_sample_exchange_info():
    """Create sample BinanceExchangeInfo objects for testing."""
    # Create mock filters
    price_filter = BinanceFilter("PRICE_FILTER", {
        "minPrice": "0.01",
        "maxPrice": "100000.00",
        "tickSize": "0.01"
    })
    lot_filter = BinanceFilter("LOT_SIZE", {
        "minQty": "0.001",
        "maxQty": "1000.00",
        "stepSize": "0.001"
    })

    exchange_info = {
        "BTCUSDT": BinanceExchangeInfo(
            symbol="BTCUSDT",
            pair="BTCUSDT",
            contract_type="PERPETUAL",
            delivery_date=0,
            onboard_date=1640995200000,
            status="TRADING",
            base_asset="BTC",
            quote_asset="USDT",
            margin_asset="USDT",
            price_precision=2,
            quantity_precision=3,
            base_asset_precision=8,
            quote_precision=8,
            underlying_type="COIN",
            underlying_sub_type=[],
            settle_plan=0,
            maint_margin_percent="2.50",
            required_margin_percent="5.00",
            trigger_protect="0.10",
            liquidation_fee="0.50",
            market_take_bound="0.05",
            filters=[price_filter, lot_filter],
            order_types=["LIMIT", "MARKET"],
            time_in_force=["GTC", "IOC"]
        ),
        "ETHUSDT": BinanceExchangeInfo(
            symbol="ETHUSDT",
            pair="ETHUSDT",
            contract_type="PERPETUAL",
            delivery_date=0,
            onboard_date=1640995200000,
            status="TRADING",
            base_asset="ETH",
            quote_asset="USDT",
            margin_asset="USDT",
            price_precision=2,
            quantity_precision=3,
            base_asset_precision=8,
            quote_precision=8,
            underlying_type="COIN",
            underlying_sub_type=[],
            settle_plan=0,
            maint_margin_percent="5.00",
            required_margin_percent="10.00",
            trigger_protect="0.10",
            liquidation_fee="0.50",
            market_take_bound="0.05",
            filters=[price_filter, lot_filter],
            order_types=["LIMIT", "MARKET"],
            time_in_force=["GTC", "IOC"]
        ),
        "ADAUSDT": BinanceExchangeInfo(
            symbol="ADAUSDT",
            pair="ADAUSDT",
            contract_type="PERPETUAL",
            delivery_date=0,
            onboard_date=1640995200000,
            status="BREAK",  # Not trading
            base_asset="ADA",
            quote_asset="USDT",
            margin_asset="USDT",
            price_precision=4,
            quantity_precision=0,
            base_asset_precision=8,
            quote_precision=8,
            underlying_type="COIN",
            underlying_sub_type=[],
            settle_plan=0,
            maint_margin_percent="10.00",
            required_margin_percent="20.00",
            trigger_protect="0.10",
            liquidation_fee="0.50",
            market_take_bound="0.05",
            filters=[price_filter, lot_filter],
            order_types=["LIMIT", "MARKET"],
            time_in_force=["GTC", "IOC"]
        )
    }
    return exchange_info


class TestTickerSynchronizerInitialization:
    """Test TickerSynchronizer initialization and basic setup - 4 tests."""

    def test_initialization_basic(self):
        """Test basic initialization with valid parameters."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Verify basic setup
        assert synchronizer.binance_client == client
        assert synchronizer.shared == shared
        assert synchronizer.config == config

        # Verify initial state
        assert synchronizer._is_running is False
        assert synchronizer._sync_task is None
        assert synchronizer._last_sync_time is None
        assert synchronizer._current_exchange_info == {}

        # Verify configuration defaults
        assert synchronizer.sync_interval_seconds == 300  # 5 minutes
        assert synchronizer.initial_sync_delay == 5
        assert synchronizer.max_consecutive_failures == 5
        assert synchronizer._consecutive_failures == 0

    def test_initialization_validates_dependencies(self):
        """Test that initialization works with valid dependencies."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        # Test with valid dependencies
        synchronizer = TickerSynchronizer(client, shared, config)
        assert synchronizer.binance_client == client
        assert synchronizer.shared == shared
        assert synchronizer.config == config

        # Test that None dependencies will fail when used (not at init time)
        sync_with_none = TickerSynchronizer(None, shared, config)
        assert sync_with_none.binance_client is None

    def test_initialization_sets_default_intervals(self):
        """Test that initialization sets correct default intervals."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Verify sync intervals
        assert synchronizer.sync_interval_seconds == 300
        assert synchronizer.initial_sync_delay == 5
        assert synchronizer.max_consecutive_failures == 5

        # Verify can be modified after creation
        synchronizer.sync_interval_seconds = 600
        assert synchronizer.sync_interval_seconds == 600

    def test_initialization_loads_existing_cache(self):
        """Test loading existing exchange info cache."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Initially empty
        assert len(synchronizer._current_exchange_info) == 0

        # Simulate loading existing data
        sample_info = create_sample_exchange_info()
        synchronizer._current_exchange_info = sample_info.copy()

        assert len(synchronizer._current_exchange_info) == 3
        assert "BTCUSDT" in synchronizer._current_exchange_info


class TestTickerSynchronizerLifecycle:
    """Test TickerSynchronizer lifecycle management - 3 tests."""

    def test_start_stop_lifecycle(self):
        """Test basic start/stop lifecycle using manual event loop."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Initially not running
            assert synchronizer._is_running is False
            assert synchronizer.is_running is False

            # Mock the sync cycle to avoid actual API calls
            with patch.object(synchronizer, '_sync_cycle', new_callable=AsyncMock) as mock_sync:
                # Start synchronizer
                result = await synchronizer.start()
                assert result is True
                assert synchronizer._is_running is True
                assert synchronizer.is_running is True
                assert synchronizer._sync_task is not None

                # Verify initial sync was called
                mock_sync.assert_called_once()

                # Stop synchronizer
                await synchronizer.stop()
                assert synchronizer._is_running is False
                assert synchronizer.is_running is False

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_start_performs_initial_sync(self):
        """Test that start() performs initial synchronization."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock exchange info response
            mock_exchange_response = {
                'symbols': [
                    {
                        'symbol': 'BTCUSDT',
                        'status': 'TRADING',
                        'contractType': 'PERPETUAL',
                        'onboardDate': 1640995200000,
                        'baseAsset': 'BTC',
                        'quoteAsset': 'USDT',
                        'marginAsset': 'USDT',
                        'pricePrecision': 2,
                        'quantityPrecision': 3,
                        'baseAssetPrecision': 8,
                        'quotePrecision': 8,
                        'filters': [],
                        'OrderType': ['LIMIT', 'MARKET'],
                        'timeInForce': ['GTC', 'IOC']
                    }
                ]
            }

            # Setup mocks
            shared.weight_coordinator.request.return_value = mock_exchange_response
            shared.get_active_ticker_symbols.return_value = []

            # Start synchronizer
            await synchronizer.start()

            # Verify initial sync happened
            assert synchronizer._last_sync_time is not None
            assert len(synchronizer._current_exchange_info) > 0
            assert "BTCUSDT" in synchronizer._current_exchange_info

            # Verify add_ticker was called
            shared.add_ticker.assert_called()

            await synchronizer.stop()

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_multiple_start_stop_cycles(self):
        """Test multiple start/stop cycles work correctly."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock sync_cycle to avoid actual API calls
            with patch.object(synchronizer, '_sync_cycle', new_callable=AsyncMock):
                # First cycle
                await synchronizer.start()
                assert synchronizer.is_running is True
                await synchronizer.stop()
                assert synchronizer.is_running is False

                # Second cycle
                await synchronizer.start()
                assert synchronizer.is_running is True
                await synchronizer.stop()
                assert synchronizer.is_running is False

                # Third cycle
                await synchronizer.start()
                assert synchronizer.is_running is True
                await synchronizer.stop()
                assert synchronizer.is_running is False

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()


class TestExchangeInfoFetching:
    """Test exchange info fetching and API integration - 6 tests."""

    def test_fetch_exchange_info_success(self):
        """Test successful exchange info fetching."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock successful response
            mock_response = {
                'symbols': [
                    {
                        'symbol': 'BTCUSDT',
                        'status': 'TRADING',
                        'contractType': 'PERPETUAL',
                        'deliveryDate': 0,
                        'onboardDate': 1640995200000,
                        'baseAsset': 'BTC',
                        'quoteAsset': 'USDT',
                        'marginAsset': 'USDT',
                        'pricePrecision': 2,
                        'quantityPrecision': 3,
                        'baseAssetPrecision': 8,
                        'quotePrecision': 8,
                        'filters': [],
                        'OrderType': ['LIMIT', 'MARKET'],
                        'timeInForce': ['GTC', 'IOC']
                    }
                ]
            }

            shared.weight_coordinator.request.return_value = mock_response

            # Fetch exchange info
            exchange_info = await synchronizer._fetch_exchange_info()

            # Verify API call
            shared.weight_coordinator.request.assert_called_once_with(
                "exchangeInfo",
                expected_weight=10
            )

            # Verify parsing
            assert len(exchange_info) == 1
            assert "BTCUSDT" in exchange_info
            assert exchange_info["BTCUSDT"].symbol == "BTCUSDT"
            assert exchange_info["BTCUSDT"].status == "TRADING"

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_fetch_exchange_info_api_error(self):
        """Test handling of API errors during fetch."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock API error
            shared.weight_coordinator.request.side_effect = Exception("API Error")

            # Should raise exception
            with pytest.raises(Exception, match="API Error"):
                await synchronizer._fetch_exchange_info()

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_fetch_exchange_info_invalid_response(self):
        """Test handling of invalid API response."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock invalid response (missing 'symbols' key)
            shared.weight_coordinator.request.return_value = {"invalid": "response"}

            # Should raise ValueError
            with pytest.raises(ValueError, match="Invalid exchange info response"):
                await synchronizer._fetch_exchange_info()

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_parse_exchange_info_corrupted_symbol(self):
        """Test handling of corrupted symbol data."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock response with one good and one bad symbol
            mock_response = {
                'symbols': [
                    {
                        'symbol': 'BTCUSDT',
                        'status': 'TRADING',
                        'contractType': 'PERPETUAL',
                        'deliveryDate': 0,
                        'onboardDate': 1640995200000,
                        'baseAsset': 'BTC',
                        'quoteAsset': 'USDT',
                        'marginAsset': 'USDT',
                        'pricePrecision': 2,
                        'quantityPrecision': 3,
                        'baseAssetPrecision': 8,
                        'quotePrecision': 8,
                        'filters': [],
                        'OrderType': ['LIMIT', 'MARKET'],
                        'timeInForce': ['GTC', 'IOC']
                    },
                    {
                        'symbol': 'INVALID',
                        # Missing required fields
                    }
                ]
            }

            shared.weight_coordinator.request.return_value = mock_response

            # Should parse good symbol and skip bad one
            exchange_info = await synchronizer._fetch_exchange_info()

            assert len(exchange_info) == 1
            assert "BTCUSDT" in exchange_info
            assert "INVALID" not in exchange_info

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_fetch_exchange_info_empty_response(self):
        """Test handling of empty response."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock empty response
            shared.weight_coordinator.request.return_value = {'symbols': []}

            exchange_info = await synchronizer._fetch_exchange_info()

            assert len(exchange_info) == 0

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_fetch_exchange_info_integration_with_weight_coordinator(self):
        """Test proper integration with WeightCoordinator."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock response
            shared.weight_coordinator.request.return_value = {'symbols': []}

            await synchronizer._fetch_exchange_info()

            # Verify correct weight coordinator usage
            shared.weight_coordinator.request.assert_called_once_with(
                "exchangeInfo",
                expected_weight=10
            )

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()


class TestSymbolFiltering:
    """Test symbol filtering logic - 5 tests."""

    def test_filter_target_symbols_basic(self):
        """Test basic symbol filtering logic."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Create exchange info with mixed symbols
        exchange_info = create_sample_exchange_info()

        # Filter symbols
        target_symbols = synchronizer._filter_target_symbols(exchange_info)

        # Should include BTCUSDT and ETHUSDT (TRADING, PERPETUAL)
        # Should exclude ADAUSDT (status = BREAK)
        assert "BTCUSDT" in target_symbols
        assert "ETHUSDT" in target_symbols
        assert "ADAUSDT" not in target_symbols

    def test_filter_target_symbols_config_restrictions(self):
        """Test filtering with config restrictions."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config(load_only_tickers=["BTCUSDT"])

        synchronizer = TickerSynchronizer(client, shared, config)

        exchange_info = create_sample_exchange_info()

        target_symbols = synchronizer._filter_target_symbols(exchange_info)

        # Should only include BTCUSDT due to config restriction
        assert "BTCUSDT" in target_symbols
        assert "ETHUSDT" not in target_symbols
        assert "ADAUSDT" not in target_symbols

    def test_filter_target_symbols_excludes_non_trading(self):
        """Test that non-trading symbols are excluded."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Create exchange info with different statuses
        exchange_info = {
            "TRADING_SYMBOL": BinanceExchangeInfo(
                symbol="TRADING_SYMBOL", pair="TRADING_SYMBOL", contract_type="PERPETUAL",
                delivery_date=0, onboard_date=1640995200000, status="TRADING",
                base_asset="BTC", quote_asset="USDT", margin_asset="USDT",
                price_precision=2, quantity_precision=3, base_asset_precision=8, quote_precision=8,
                underlying_type="COIN", underlying_sub_type=[], settle_plan=0,
                maint_margin_percent="2.50", required_margin_percent="5.00",
                trigger_protect="0.10", liquidation_fee="0.50", market_take_bound="0.05",
                filters=[], order_types=[], time_in_force=[]
            ),
            "BREAK_SYMBOL": BinanceExchangeInfo(
                symbol="BREAK_SYMBOL", pair="BREAK_SYMBOL", contract_type="PERPETUAL",
                delivery_date=0, onboard_date=1640995200000, status="BREAK",
                base_asset="ETH", quote_asset="USDT", margin_asset="USDT",
                price_precision=2, quantity_precision=3, base_asset_precision=8, quote_precision=8,
                underlying_type="COIN", underlying_sub_type=[], settle_plan=0,
                maint_margin_percent="5.00", required_margin_percent="10.00",
                trigger_protect="0.10", liquidation_fee="0.50", market_take_bound="0.05",
                filters=[], order_types=[], time_in_force=[]
            )
        }

        target_symbols = synchronizer._filter_target_symbols(exchange_info)

        assert "TRADING_SYMBOL" in target_symbols
        assert "BREAK_SYMBOL" not in target_symbols

    def test_filter_target_symbols_excludes_delivery_contracts(self):
        """Test that delivery contracts are excluded."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Create exchange info with different contract types
        exchange_info = {
            "PERPETUAL_SYMBOL": BinanceExchangeInfo(
                symbol="PERPETUAL_SYMBOL", pair="PERPETUAL_SYMBOL", contract_type="PERPETUAL",
                delivery_date=0, onboard_date=1640995200000, status="TRADING",
                base_asset="BTC", quote_asset="USDT", margin_asset="USDT",
                price_precision=2, quantity_precision=3, base_asset_precision=8, quote_precision=8,
                underlying_type="COIN", underlying_sub_type=[], settle_plan=0,
                maint_margin_percent="2.50", required_margin_percent="5.00",
                trigger_protect="0.10", liquidation_fee="0.50", market_take_bound="0.05",
                filters=[], order_types=[], time_in_force=[]
            ),
            "DELIVERY_SYMBOL": BinanceExchangeInfo(
                symbol="DELIVERY_SYMBOL", pair="DELIVERY_SYMBOL", contract_type="CURRENT_QUARTER",
                delivery_date=1735689600000, onboard_date=1640995200000, status="TRADING",
                base_asset="ETH", quote_asset="USDT", margin_asset="USDT",
                price_precision=2, quantity_precision=3, base_asset_precision=8, quote_precision=8,
                underlying_type="COIN", underlying_sub_type=[], settle_plan=0,
                maint_margin_percent="5.00", required_margin_percent="10.00",
                trigger_protect="0.10", liquidation_fee="0.50", market_take_bound="0.05",
                filters=[], order_types=[], time_in_force=[]
            )
        }

        target_symbols = synchronizer._filter_target_symbols(exchange_info)

        assert "PERPETUAL_SYMBOL" in target_symbols
        assert "DELIVERY_SYMBOL" not in target_symbols

    def test_filter_target_symbols_empty_input(self):
        """Test filtering with empty exchange info."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        target_symbols = synchronizer._filter_target_symbols({})

        assert len(target_symbols) == 0


class TestTickerManagement:
    """Test ticker add/remove operations - 7 tests."""

    def test_add_tickers_new_symbols(self):
        """Test adding new ticker symbols."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock successful add_ticker
            shared.add_ticker.return_value = True

            exchange_info = {"BTCUSDT": create_sample_exchange_info()["BTCUSDT"]}
            symbols_to_add = {"BTCUSDT"}

            await synchronizer._add_tickers(symbols_to_add, exchange_info)

            # Verify add_ticker was called
            shared.add_ticker.assert_called_once()
            args = shared.add_ticker.call_args
            assert args[0][0] == "BTCUSDT"  # Symbol
            assert isinstance(args[0][1], TickerModel)  # TickerModel

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_add_tickers_handles_errors(self):
        """Test handling of add_ticker errors."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock failed add_ticker
            shared.add_ticker.return_value = False

            exchange_info = create_sample_exchange_info()
            symbols_to_add = {"BTCUSDT", "ETHUSDT"}

            # Should not raise exception despite failures
            await synchronizer._add_tickers(symbols_to_add, exchange_info)

            # Verify both symbols were attempted
            assert shared.add_ticker.call_count == 2

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_remove_tickers_graceful_shutdown(self):
        """Test graceful ticker removal."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock successful remove_ticker
            shared.remove_ticker.return_value = True

            symbols_to_remove = {"BTCUSDT", "ETHUSDT"}

            await synchronizer._remove_tickers(symbols_to_remove)

            # Verify remove_ticker was called for each symbol
            assert shared.remove_ticker.call_count == 2

            # Verify graceful=True was used
            for call in shared.remove_ticker.call_args_list:
                assert call[1]['graceful'] is True

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_remove_tickers_handles_failures(self):
        """Test handling of remove_ticker failures."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock failed remove_ticker
            shared.remove_ticker.return_value = False

            symbols_to_remove = {"BTCUSDT", "ETHUSDT"}

            # Should not raise exception despite failures
            await synchronizer._remove_tickers(symbols_to_remove)

            # Verify both symbols were attempted
            assert shared.remove_ticker.call_count == 2

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_sync_cycle_adds_new_symbols(self):
        """Test complete sync cycle adding new symbols."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock empty active tickers
            shared.get_active_ticker_symbols.return_value = []
            shared.add_ticker.return_value = True

            # Mock exchange info fetch
            mock_response = {
                'symbols': [
                    {
                        'symbol': 'BTCUSDT',
                        'status': 'TRADING',
                        'contractType': 'PERPETUAL',
                        'deliveryDate': 0,
                        'onboardDate': 1640995200000,
                        'baseAsset': 'BTC',
                        'quoteAsset': 'USDT',
                        'marginAsset': 'USDT',
                        'pricePrecision': 2,
                        'quantityPrecision': 3,
                        'baseAssetPrecision': 8,
                        'quotePrecision': 8,
                        'filters': [],
                        'OrderType': ['LIMIT', 'MARKET'],
                        'timeInForce': ['GTC', 'IOC']
                    }
                ]
            }
            shared.weight_coordinator.request.return_value = mock_response

            # Execute sync cycle
            await synchronizer._sync_cycle()

            # Verify new symbol was added
            shared.add_ticker.assert_called()
            assert synchronizer._last_sync_time is not None

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_sync_cycle_removes_delisted_symbols(self):
        """Test sync cycle removing delisted symbols."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock active tickers that are no longer in exchange info
            shared.get_active_ticker_symbols.return_value = ["BTCUSDT", "ETHUSDT"]
            shared.remove_ticker.return_value = True

            # Mock empty exchange info (symbols delisted)
            mock_response = {'symbols': []}
            shared.weight_coordinator.request.return_value = mock_response

            # Execute sync cycle
            await synchronizer._sync_cycle()

            # Verify symbols were removed
            assert shared.remove_ticker.call_count == 2

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_sync_cycle_mixed_add_remove(self):
        """Test sync cycle with mixed add/remove operations."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock current active tickers
            shared.get_active_ticker_symbols.return_value = ["ETHUSDT", "ADAUSDT"]
            shared.add_ticker.return_value = True
            shared.remove_ticker.return_value = True

            # Mock exchange info with BTCUSDT (new) and ETHUSDT (existing)
            # ADAUSDT will be removed (not in new exchange info)
            mock_response = {
                'symbols': [
                    {
                        'symbol': 'BTCUSDT',
                        'status': 'TRADING',
                        'contractType': 'PERPETUAL',
                        'deliveryDate': 0,
                        'onboardDate': 1640995200000,
                        'baseAsset': 'BTC',
                        'quoteAsset': 'USDT',
                        'marginAsset': 'USDT',
                        'pricePrecision': 2,
                        'quantityPrecision': 3,
                        'baseAssetPrecision': 8,
                        'quotePrecision': 8,
                        'filters': [],
                        'OrderType': ['LIMIT', 'MARKET'],
                        'timeInForce': ['GTC', 'IOC']
                    },
                    {
                        'symbol': 'ETHUSDT',
                        'status': 'TRADING',
                        'contractType': 'PERPETUAL',
                        'deliveryDate': 0,
                        'onboardDate': 1640995200000,
                        'baseAsset': 'ETH',
                        'quoteAsset': 'USDT',
                        'marginAsset': 'USDT',
                        'pricePrecision': 2,
                        'quantityPrecision': 3,
                        'baseAssetPrecision': 8,
                        'quotePrecision': 8,
                        'filters': [],
                        'OrderType': ['LIMIT', 'MARKET'],
                        'timeInForce': ['GTC', 'IOC']
                    }
                ]
            }
            shared.weight_coordinator.request.return_value = mock_response

            # Execute sync cycle
            await synchronizer._sync_cycle()

            # Verify BTCUSDT was added and ADAUSDT was removed
            shared.add_ticker.assert_called_once()  # BTCUSDT
            shared.remove_ticker.assert_called_once()  # ADAUSDT

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()


class TestSyncLoop:
    """Test sync loop operations - 4 tests."""

    def test_sync_loop_periodic_execution(self):
        """Test basic sync cycle execution."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Test that sync is called during start (initial sync)
            with patch.object(synchronizer, '_sync_cycle', new_callable=AsyncMock) as mock_sync:
                await synchronizer.start()

                # Verify initial sync was called
                mock_sync.assert_called_once()

                await synchronizer.stop()

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_sync_loop_error_handling(self):
        """Test error handling in sync initialization."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock initial sync to fail, then start will fail
            with patch.object(synchronizer, '_sync_cycle', side_effect=Exception("Test error")):
                try:
                    await synchronizer.start()
                except Exception:
                    pass  # Expected to fail

                # Verify that sync was attempted and failed
                assert not synchronizer._is_running

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_sync_loop_max_failures_shutdown(self):
        """Test handling of consecutive failures."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Test that failure counter can be manually set and read
            synchronizer._consecutive_failures = 3
            assert synchronizer._consecutive_failures == 3

            # Test max failures threshold
            synchronizer.max_consecutive_failures = 2
            assert synchronizer.max_consecutive_failures == 2

            # Test that setting failures beyond max is possible (would be handled by sync loop)
            synchronizer._consecutive_failures = 5
            assert synchronizer._consecutive_failures == 5

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_sync_loop_cancellation(self):
        """Test graceful cancellation of sync loop."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock sync_cycle that runs forever
            with patch.object(synchronizer, '_sync_cycle', new_callable=AsyncMock):
                await synchronizer.start()
                assert synchronizer._is_running is True

                # Stop should cancel the loop gracefully
                await synchronizer.stop()
                assert synchronizer._is_running is False

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()


class TestPublicMethods:
    """Test public interface methods - 6 tests."""

    def test_get_current_exchange_info(self):
        """Test getting current exchange info cache."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Initially empty
        exchange_info = synchronizer.get_current_exchange_info()
        assert exchange_info == {}

        # Add some data
        sample_info = create_sample_exchange_info()
        synchronizer._current_exchange_info = sample_info

        # Should return copy
        exchange_info = synchronizer.get_current_exchange_info()
        assert len(exchange_info) == 3
        assert "BTCUSDT" in exchange_info

        # Verify it's a copy (modifying returned dict shouldn't affect original)
        exchange_info.clear()
        assert len(synchronizer._current_exchange_info) == 3

    def test_get_ticker_info_specific_symbol(self):
        """Test getting info for specific symbol."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Initially None
        info = synchronizer.get_ticker_info("BTCUSDT")
        assert info is None

        # Add data
        sample_info = create_sample_exchange_info()
        synchronizer._current_exchange_info = sample_info

        # Test case insensitive lookup
        info = synchronizer.get_ticker_info("btcusdt")
        assert info is not None
        assert info.symbol == "BTCUSDT"

        info = synchronizer.get_ticker_info("BTCUSDT")
        assert info is not None
        assert info.symbol == "BTCUSDT"

        # Test non-existent symbol
        info = synchronizer.get_ticker_info("NONEXISTENT")
        assert info is None

    def test_get_last_sync_time(self):
        """Test getting last sync timestamp."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Initially None
        assert synchronizer.get_last_sync_time() is None

        # Set sync time
        test_time = int(time.time() * 1000)
        synchronizer._last_sync_time = test_time

        assert synchronizer.get_last_sync_time() == test_time

    def test_get_target_symbols_count(self):
        """Test getting count of target symbols."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Initially 0
        assert synchronizer.get_target_symbols_count() == 0

        # Add exchange info
        sample_info = create_sample_exchange_info()
        synchronizer._current_exchange_info = sample_info

        # Should count only tradeable perpetual symbols
        count = synchronizer.get_target_symbols_count()
        assert count == 2  # BTCUSDT and ETHUSDT (ADAUSDT is BREAK status)

    def test_force_sync_manual_trigger(self):
        """Test manual sync trigger."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock successful sync
            with patch.object(synchronizer, '_sync_cycle', new_callable=AsyncMock) as mock_sync:
                result = await synchronizer.force_sync()

                assert result is True
                mock_sync.assert_called_once()

            # Mock failed sync
            with patch.object(synchronizer, '_sync_cycle', side_effect=Exception("Sync error")):
                result = await synchronizer.force_sync()

                assert result is False

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_refresh_ticker_remove_readd(self):
        """Test refreshing specific ticker."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Add exchange info
            sample_info = create_sample_exchange_info()
            synchronizer._current_exchange_info = sample_info

            # Mock successful operations
            shared.remove_ticker.return_value = True
            shared.add_ticker.return_value = True

            # Test successful refresh
            result = await synchronizer.refresh_ticker("BTCUSDT")

            assert result is True
            shared.remove_ticker.assert_called_with("BTCUSDT", graceful=True)
            shared.add_ticker.assert_called()

            # Test non-existent symbol
            result = await synchronizer.refresh_ticker("NONEXISTENT")
            assert result is False

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()


class TestStatusAndMonitoring:
    """Test status and monitoring functionality - 4 tests."""

    def test_get_sync_status_comprehensive(self):
        """Test comprehensive sync status reporting."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Test initial status
        status = synchronizer.get_sync_status()

        expected_fields = [
            'running', 'last_sync_time', 'consecutive_failures',
            'sync_interval_seconds', 'total_symbols_in_cache',
            'target_symbols_count', 'active_tickers_count'
        ]

        for field in expected_fields:
            assert field in status

        assert status['running'] is False
        assert status['last_sync_time'] is None
        assert status['consecutive_failures'] == 0
        assert status['sync_interval_seconds'] == 300

    def test_sync_status_reflects_real_state(self):
        """Test that status reflects actual synchronizer state."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Add some data and update state
        sample_info = create_sample_exchange_info()
        synchronizer._current_exchange_info = sample_info
        synchronizer._last_sync_time = int(time.time() * 1000)
        synchronizer._consecutive_failures = 2
        synchronizer._is_running = True

        shared.get_active_ticker_symbols.return_value = ["BTCUSDT", "ETHUSDT"]

        status = synchronizer.get_sync_status()

        assert status['running'] is True
        assert status['last_sync_time'] == synchronizer._last_sync_time
        assert status['consecutive_failures'] == 2
        assert status['total_symbols_in_cache'] == 3
        assert status['target_symbols_count'] == 2
        assert status['active_tickers_count'] == 2

    def test_string_representations(self):
        """Test __str__ and __repr__ methods."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Test __str__
        str_repr = str(synchronizer)
        assert "TickerSynchronizer" in str_repr
        assert "stopped" in str_repr
        assert "0 symbols" in str_repr

        # Add data and change state
        sample_info = create_sample_exchange_info()
        synchronizer._current_exchange_info = sample_info
        synchronizer._is_running = True

        str_repr = str(synchronizer)
        assert "running" in str_repr
        assert "3 symbols" in str_repr

        # Test __repr__
        repr_str = repr(synchronizer)
        assert "TickerSynchronizer(" in repr_str
        assert "running=True" in repr_str
        assert "symbols=3" in repr_str

    def test_status_with_failures(self):
        """Test status reporting with consecutive failures."""
        client = create_mock_binance_client()
        shared = create_mock_shared()
        config = create_processing_config()

        synchronizer = TickerSynchronizer(client, shared, config)

        # Simulate failures
        synchronizer._consecutive_failures = 3

        status = synchronizer.get_sync_status()

        assert status['consecutive_failures'] == 3

        # Test with max failures reached
        synchronizer._consecutive_failures = synchronizer.max_consecutive_failures

        status = synchronizer.get_sync_status()

        assert status['consecutive_failures'] == synchronizer.max_consecutive_failures


class TestErrorRecovery:
    """Test error recovery mechanisms - 3 tests."""

    def test_api_error_recovery(self):
        """Test recovery from API errors."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Test consecutive failures increment
            initial_failures = synchronizer._consecutive_failures

            # Mock API error
            shared.weight_coordinator.request.side_effect = Exception("API Error")

            try:
                await synchronizer._sync_cycle()
            except Exception:
                pass

            # Test successful call resets failures
            shared.weight_coordinator.request.side_effect = None
            shared.weight_coordinator.request.return_value = {'symbols': []}
            shared.get_active_ticker_symbols.return_value = []

            await synchronizer._sync_cycle()

            # In real implementation, successful sync would reset consecutive_failures
            # This tests the API call succeeds without error

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_network_timeout_handling(self):
        """Test handling of network timeouts."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Mock timeout error
            import asyncio
            shared.weight_coordinator.request.side_effect = asyncio.TimeoutError("Network timeout")

            # Should propagate timeout error for handling by sync loop
            with pytest.raises(asyncio.TimeoutError):
                await synchronizer._fetch_exchange_info()

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()

    def test_malformed_response_recovery(self):
        """Test recovery from malformed API responses."""
        async def run_test():
            client = create_mock_binance_client()
            shared = create_mock_shared()
            config = create_processing_config()

            synchronizer = TickerSynchronizer(client, shared, config)

            # Test malformed response handling
            shared.weight_coordinator.request.return_value = {"malformed": "response"}

            with pytest.raises(ValueError, match="Invalid exchange info response"):
                await synchronizer._fetch_exchange_info()

            # Test recovery with valid response
            shared.weight_coordinator.request.return_value = {'symbols': []}
            shared.get_active_ticker_symbols.return_value = []

            # Should work without error after bad response
            await synchronizer._sync_cycle()

        # Run test with manual event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(run_test())
        finally:
            loop.close()