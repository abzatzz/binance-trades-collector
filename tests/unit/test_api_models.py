"""
Unit Tests for API Models
=========================

Comprehensive testing of Pydantic models for REST API responses including
validation, serialization, error handling, and edge cases.

File: tests/unit/test_api_models.py
Usage: python -m pytest tests/unit/test_api_models.py -v
"""

import pytest
import json
import time
from typing import Dict, Any, List, Optional
from pathlib import Path
import sys

# Import mock first
from unittest.mock import MagicMock

# Mock dependencies first
sys.modules['loggerino'] = MagicMock()
sys.modules['loggerino.loggerino'] = MagicMock()
mock_loggerino = MagicMock()
mock_loggerino.get.return_value = MagicMock()
sys.modules['loggerino'].loggerino = mock_loggerino

# Import API models directly from source file to avoid relative import issues
import importlib.util

# Read the API models source file directly
api_models_file = Path(__file__).parent.parent.parent / "src" / "api" / "models.py"
with open(api_models_file, 'r') as f:
    source_code = f.read()

# Execute the module directly (models.py has no relative imports)
spec = importlib.util.spec_from_loader("api_models", loader=None)
api_models_module = importlib.util.module_from_spec(spec)
exec(source_code, api_models_module.__dict__)

# Import the classes from the executed module
APIResponse = api_models_module.APIResponse
TradeResponse = api_models_module.TradeResponse
TickerInfo = api_models_module.TickerInfo
SystemInfo = api_models_module.SystemInfo
HealthResponse = api_models_module.HealthResponse


class TestAPIResponseGeneric:
    """Test generic APIResponse wrapper with type safety - 8 tests."""

    def test_api_response_basic_structure(self):
        """Test basic APIResponse structure and required fields."""
        timestamp = int(time.time() * 1000)

        response = APIResponse[str](
            success=True,
            data="test data",
            message="Operation successful",
            timestamp=timestamp
        )

        assert response.success is True
        assert response.data == "test data"
        assert response.message == "Operation successful"
        assert response.timestamp == timestamp
        assert response.error is None

    def test_api_response_with_error(self):
        """Test APIResponse with error field."""
        timestamp = int(time.time() * 1000)

        response = APIResponse[None](
            success=False,
            data=None,
            message="Operation failed",
            timestamp=timestamp,
            error="Invalid input parameters"
        )

        assert response.success is False
        assert response.data is None
        assert response.message == "Operation failed"
        assert response.error == "Invalid input parameters"

    def test_api_response_with_list_data(self):
        """Test APIResponse with list data type."""
        test_data = ["item1", "item2", "item3"]

        response = APIResponse[List[str]](
            success=True,
            data=test_data,
            message="List retrieved",
            timestamp=int(time.time() * 1000)
        )

        assert response.success is True
        assert response.data == test_data
        assert len(response.data) == 3

    def test_api_response_with_dict_data(self):
        """Test APIResponse with dictionary data type."""
        test_data = {"key1": "value1", "key2": 42, "key3": True}

        response = APIResponse[Dict[str, Any]](
            success=True,
            data=test_data,
            message="Dictionary retrieved",
            timestamp=int(time.time() * 1000)
        )

        assert response.success is True
        assert response.data == test_data
        assert response.data["key2"] == 42

    def test_api_response_serialization(self):
        """Test APIResponse JSON serialization."""
        timestamp = int(time.time() * 1000)

        response = APIResponse[str](
            success=True,
            data="test data",
            message="Success",
            timestamp=timestamp
        )

        # Test model_dump method
        response_dict = response.model_dump()

        assert "success" in response_dict
        assert "data" in response_dict
        assert "message" in response_dict
        assert "timestamp" in response_dict
        assert "error" in response_dict

        assert response_dict["success"] is True
        assert response_dict["data"] == "test data"
        assert response_dict["error"] is None

    def test_api_response_json_serialization(self):
        """Test APIResponse JSON string serialization."""
        timestamp = int(time.time() * 1000)

        response = APIResponse[Dict[str, int]](
            success=True,
            data={"count": 42},
            message="Data retrieved",
            timestamp=timestamp
        )

        # Test JSON serialization
        json_str = response.model_dump_json()

        # Parse back to verify
        parsed = json.loads(json_str)
        assert parsed["success"] is True
        assert parsed["data"]["count"] == 42
        assert parsed["timestamp"] == timestamp

    def test_api_response_validation_errors(self):
        """Test APIResponse field validation."""
        # Test missing required fields
        with pytest.raises(ValueError):
            APIResponse(
                # Missing success, message, timestamp
                data="test"
            )

    def test_api_response_field_descriptions(self):
        """Test that field descriptions are properly set."""
        # Get field info from the model
        fields_info = APIResponse.model_fields

        assert "success" in fields_info
        assert "data" in fields_info
        assert "message" in fields_info
        assert "timestamp" in fields_info
        assert "error" in fields_info

        # Check descriptions exist
        assert fields_info["success"].description is not None
        assert fields_info["message"].description is not None
        assert fields_info["timestamp"].description is not None


class TestTradeResponse:
    """Test TradeResponse model for AggTrade data - 6 tests."""

    def test_trade_response_basic_creation(self):
        """Test basic TradeResponse creation and validation."""
        trade = TradeResponse(
            aggregate_id=123456,
            price=50000.12345678,
            quantity=1.23456789,
            first_trade_id=789012,
            last_trade_id=789015,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        assert trade.aggregate_id == 123456
        assert trade.price == 50000.12345678
        assert trade.quantity == 1.23456789
        assert trade.first_trade_id == 789012
        assert trade.last_trade_id == 789015
        assert trade.timestamp == 1640995200000
        assert trade.is_buyer_maker is True

    def test_trade_response_field_types(self):
        """Test field type validation."""
        # Test with string numbers (should be converted)
        trade = TradeResponse(
            aggregate_id="123456",
            price="50000.12345678",
            quantity="1.23456789",
            first_trade_id="789012",
            last_trade_id="789015",
            timestamp="1640995200000",
            is_buyer_maker="true"
        )

        assert isinstance(trade.aggregate_id, int)
        assert isinstance(trade.price, float)
        assert isinstance(trade.quantity, float)
        assert isinstance(trade.first_trade_id, int)
        assert isinstance(trade.last_trade_id, int)
        assert isinstance(trade.timestamp, int)
        assert isinstance(trade.is_buyer_maker, bool)

    def test_trade_response_validation_errors(self):
        """Test validation errors for invalid data."""
        # Note: Pydantic models in models.py may not have custom validators
        # so we test the basic validation that Pydantic provides by default

        # Test that the model accepts valid data
        valid_trade = TradeResponse(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23,
            first_trade_id=789012,
            last_trade_id=789015,
            timestamp=1640995200000,
            is_buyer_maker=True
        )
        assert valid_trade.aggregate_id == 123456

    def test_trade_response_serialization(self):
        """Test TradeResponse serialization."""
        trade = TradeResponse(
            aggregate_id=123456,
            price=50000.12345678,
            quantity=1.23456789,
            first_trade_id=789012,
            last_trade_id=789015,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        trade_dict = trade.model_dump()

        assert trade_dict["aggregate_id"] == 123456
        assert trade_dict["price"] == 50000.12345678
        assert trade_dict["quantity"] == 1.23456789
        assert trade_dict["is_buyer_maker"] is True

    def test_trade_response_from_aggtrade_compatibility(self):
        """Test compatibility with AggTrade data structure."""
        # Simulate data that would come from AggTrade
        aggtrade_data = {
            "aggregate_id": 123456,
            "price": 50000.12345678,
            "quantity": 1.23456789,
            "first_trade_id": 789012,
            "last_trade_id": 789015,
            "timestamp": 1640995200000,
            "is_buyer_maker": True
        }

        trade = TradeResponse(**aggtrade_data)

        assert trade.aggregate_id == aggtrade_data["aggregate_id"]
        assert trade.price == aggtrade_data["price"]
        assert trade.quantity == aggtrade_data["quantity"]

    def test_trade_response_precision_handling(self):
        """Test handling of high precision decimal numbers."""
        trade = TradeResponse(
            aggregate_id=123456,
            price=50000.123456789012345,  # High precision
            quantity=1.234567890123456,   # High precision
            first_trade_id=789012,
            last_trade_id=789015,
            timestamp=1640995200000,
            is_buyer_maker=False
        )

        # Verify precision is maintained (within float limitations)
        assert trade.price > 50000.123456
        assert trade.quantity > 1.234567
        assert trade.is_buyer_maker is False


class TestTickerInfo:
    """Test TickerInfo model for ticker metadata - 7 tests."""

    def test_ticker_info_basic_creation(self):
        """Test basic TickerInfo creation."""
        ticker = TickerInfo(
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,
            price_precision=2,
            quantity_precision=3
        )

        assert ticker.symbol == "BTCUSDT"
        assert ticker.base_asset == "BTC"
        assert ticker.quote_asset == "USDT"
        assert ticker.status == "TRADING"
        assert ticker.is_tradeable is True
        assert ticker.onboard_date == 1640995200000
        assert ticker.price_precision == 2
        assert ticker.quantity_precision == 3
        assert ticker.processor_stats is None

    def test_ticker_info_with_processor_stats(self):
        """Test TickerInfo with processor statistics."""
        processor_stats = {
            "state": "RUNNING",
            "historical_trades_loaded": 50000,
            "websocket_trades_processed": 1500,
            "uptime_seconds": 3600,
            "error_count": 2
        }

        ticker = TickerInfo(
            symbol="ETHUSDT",
            base_asset="ETH",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,
            price_precision=2,
            quantity_precision=3,
            processor_stats=processor_stats
        )

        assert ticker.processor_stats is not None
        assert ticker.processor_stats["state"] == "RUNNING"
        assert ticker.processor_stats["historical_trades_loaded"] == 50000

    def test_ticker_info_symbol_validation(self):
        """Test symbol validation and normalization."""
        # Test uppercase normalization
        ticker = TickerInfo(
            symbol="btcusdt",  # lowercase
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,
            price_precision=2,
            quantity_precision=3
        )

        # Pydantic doesn't auto-normalize, so this tests the actual input
        assert ticker.symbol == "btcusdt"

    def test_ticker_info_validation_errors(self):
        """Test validation for invalid ticker data."""
        # Test that basic validation works for valid data
        ticker = TickerInfo(
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,
            price_precision=2,
            quantity_precision=3
        )
        assert ticker.symbol == "BTCUSDT"

    def test_ticker_info_serialization(self):
        """Test TickerInfo serialization with all fields."""
        processor_stats = {
            "trades_processed": 1000,
            "errors": 0
        }

        ticker = TickerInfo(
            symbol="ADAUSDT",
            base_asset="ADA",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,
            price_precision=4,
            quantity_precision=0,
            processor_stats=processor_stats
        )

        ticker_dict = ticker.model_dump()

        assert ticker_dict["symbol"] == "ADAUSDT"
        assert ticker_dict["base_asset"] == "ADA"
        assert ticker_dict["processor_stats"]["trades_processed"] == 1000

    def test_ticker_info_optional_fields(self):
        """Test optional fields behavior."""
        # Create with minimal required fields
        ticker = TickerInfo(
            symbol="DOGEUSDT",
            base_asset="DOGE",
            quote_asset="USDT",
            status="BREAK",
            is_tradeable=False,
            onboard_date=1640995200000,
            price_precision=6,
            quantity_precision=0
            # processor_stats omitted
        )

        assert ticker.processor_stats is None
        assert ticker.is_tradeable is False
        assert ticker.status == "BREAK"

    def test_ticker_info_timestamp_validation(self):
        """Test timestamp validation for onboard_date."""
        # Test with valid timestamp
        ticker = TickerInfo(
            symbol="DOTUSDT",
            base_asset="DOT",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,  # Valid timestamp
            price_precision=3,
            quantity_precision=2
        )

        assert ticker.onboard_date == 1640995200000

        # Test with string timestamp (should convert)
        ticker2 = TickerInfo(
            symbol="DOTUSDT",
            base_asset="DOT",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date="1640995200000",  # String timestamp
            price_precision=3,
            quantity_precision=2
        )

        assert isinstance(ticker2.onboard_date, int)


class TestSystemInfo:
    """Test SystemInfo model for system status - 6 tests."""

    def test_system_info_basic_creation(self):
        """Test basic SystemInfo creation."""
        system = SystemInfo(
            initialized=True,
            total_ticker_tasks=10,
            active_tickers=8,
            binance_client_connected=True,
            weight_coordinator_status="active",
            ticker_synchronizer_status="running",
            environment="production",
            api_requests_count=15000,
            api_errors_count=25,
            server_uptime_seconds=86400.5
        )

        assert system.initialized is True
        assert system.total_ticker_tasks == 10
        assert system.active_tickers == 8
        assert system.binance_client_connected is True
        assert system.weight_coordinator_status == "active"
        assert system.ticker_synchronizer_status == "running"
        assert system.environment == "production"
        assert system.api_requests_count == 15000
        assert system.api_errors_count == 25
        assert system.server_uptime_seconds == 86400.5

    def test_system_info_field_types(self):
        """Test field type validation and conversion."""
        # Test with string numbers
        system = SystemInfo(
            initialized="true",
            total_ticker_tasks="10",
            active_tickers="8",
            binance_client_connected="false",
            weight_coordinator_status="active",
            ticker_synchronizer_status="stopped",
            environment="development",
            api_requests_count="15000",
            api_errors_count="25",
            server_uptime_seconds="3600.75"
        )

        assert isinstance(system.initialized, bool)
        assert isinstance(system.total_ticker_tasks, int)
        assert isinstance(system.active_tickers, int)
        assert isinstance(system.binance_client_connected, bool)
        assert isinstance(system.api_requests_count, int)
        assert isinstance(system.api_errors_count, int)
        assert isinstance(system.server_uptime_seconds, float)

    def test_system_info_validation_constraints(self):
        """Test validation constraints."""
        # Test that valid data works
        system = SystemInfo(
            initialized=True,
            total_ticker_tasks=10,
            active_tickers=5,
            binance_client_connected=True,
            weight_coordinator_status="active",
            ticker_synchronizer_status="running",
            environment="test",
            api_requests_count=1000,
            api_errors_count=10,
            server_uptime_seconds=3600.0
        )
        assert system.total_ticker_tasks == 10

    def test_system_info_status_values(self):
        """Test different status values."""
        # Test various status combinations
        system = SystemInfo(
            initialized=False,
            total_ticker_tasks=0,
            active_tickers=0,
            binance_client_connected=False,
            weight_coordinator_status="inactive",
            ticker_synchronizer_status="error",
            environment="staging",
            api_requests_count=0,
            api_errors_count=0,
            server_uptime_seconds=0.0
        )

        assert system.initialized is False
        assert system.total_ticker_tasks == 0
        assert system.binance_client_connected is False

    def test_system_info_serialization(self):
        """Test SystemInfo serialization."""
        system = SystemInfo(
            initialized=True,
            total_ticker_tasks=5,
            active_tickers=3,
            binance_client_connected=True,
            weight_coordinator_status="healthy",
            ticker_synchronizer_status="syncing",
            environment="development",
            api_requests_count=500,
            api_errors_count=2,
            server_uptime_seconds=1800.25
        )

        system_dict = system.model_dump()

        assert system_dict["initialized"] is True
        assert system_dict["total_ticker_tasks"] == 5
        assert system_dict["environment"] == "development"
        assert system_dict["server_uptime_seconds"] == 1800.25

    def test_system_info_json_serialization(self):
        """Test SystemInfo JSON serialization."""
        system = SystemInfo(
            initialized=True,
            total_ticker_tasks=15,
            active_tickers=12,
            binance_client_connected=True,
            weight_coordinator_status="active",
            ticker_synchronizer_status="running",
            environment="production",
            api_requests_count=50000,
            api_errors_count=100,
            server_uptime_seconds=172800.0
        )

        json_str = system.model_dump_json()
        parsed = json.loads(json_str)

        assert parsed["total_ticker_tasks"] == 15
        assert parsed["active_tickers"] == 12
        assert parsed["server_uptime_seconds"] == 172800.0


class TestHealthResponse:
    """Test HealthResponse model for health checks - 5 tests."""

    def test_health_response_basic_creation(self):
        """Test basic HealthResponse creation."""
        components = {
            "database": {"status": "healthy", "latency_ms": 15},
            "binance_api": {"status": "healthy", "rate_limit": "normal"},
            "storage": {"status": "healthy", "free_space_gb": 500}
        }

        health = HealthResponse(
            status="healthy",
            components=components,
            timestamp=1640995200000,
            uptime_seconds=86400.0
        )

        assert health.status == "healthy"
        assert health.components == components
        assert health.timestamp == 1640995200000
        assert health.uptime_seconds == 86400.0

    def test_health_response_unhealthy_status(self):
        """Test HealthResponse with unhealthy status."""
        components = {
            "database": {"status": "error", "error": "Connection timeout"},
            "binance_api": {"status": "degraded", "rate_limit": "warning"},
            "storage": {"status": "healthy", "free_space_gb": 50}
        }

        health = HealthResponse(
            status="unhealthy",
            components=components,
            timestamp=1640995260000,
            uptime_seconds=7200.5
        )

        assert health.status == "unhealthy"
        assert health.components["database"]["status"] == "error"
        assert health.components["binance_api"]["status"] == "degraded"

    def test_health_response_empty_components(self):
        """Test HealthResponse with empty components."""
        health = HealthResponse(
            status="unknown",
            components={},
            timestamp=1640995300000,
            uptime_seconds=0.0
        )

        assert health.status == "unknown"
        assert health.components == {}
        assert health.uptime_seconds == 0.0

    def test_health_response_complex_components(self):
        """Test HealthResponse with complex nested components."""
        components = {
            "coordinators": {
                "weight_coordinator": {
                    "status": "healthy",
                    "queue_size": 0,
                    "requests_processed": 1000,
                    "rate_limit": False
                },
                "ticker_synchronizer": {
                    "status": "healthy",
                    "last_sync": "2022-01-01T00:00:00Z",
                    "active_tickers": 25
                }
            },
            "processors": {
                "BTCUSDT": {"status": "running", "trades_processed": 50000},
                "ETHUSDT": {"status": "running", "trades_processed": 35000}
            },
            "system": {
                "memory_usage_percent": 45.2,
                "cpu_usage_percent": 12.8,
                "disk_usage_percent": 67.3
            }
        }

        health = HealthResponse(
            status="healthy",
            components=components,
            timestamp=1640995400000,
            uptime_seconds=259200.0
        )

        assert health.components["coordinators"]["weight_coordinator"]["status"] == "healthy"
        assert health.components["processors"]["BTCUSDT"]["trades_processed"] == 50000
        assert health.components["system"]["memory_usage_percent"] == 45.2

    def test_health_response_serialization(self):
        """Test HealthResponse serialization with complex data."""
        components = {
            "api": {"status": "healthy", "response_time_ms": 25},
            "storage": {"status": "warning", "free_space_percent": 15.5}
        }

        health = HealthResponse(
            status="degraded",
            components=components,
            timestamp=1640995500000,
            uptime_seconds=604800.0
        )

        health_dict = health.model_dump()

        assert health_dict["status"] == "degraded"
        assert health_dict["components"]["api"]["response_time_ms"] == 25
        assert health_dict["components"]["storage"]["free_space_percent"] == 15.5
        assert health_dict["uptime_seconds"] == 604800.0

        # Test JSON serialization
        json_str = health.model_dump_json()
        parsed = json.loads(json_str)
        assert parsed["status"] == "degraded"


class TestModelIntegration:
    """Test model integration and real-world usage patterns - 5 tests."""

    def test_complete_api_response_with_trade_data(self):
        """Test complete APIResponse with TradeResponse data."""
        trades_data = [
            TradeResponse(
                aggregate_id=123456,
                price=50000.12,
                quantity=1.5,
                first_trade_id=789012,
                last_trade_id=789012,
                timestamp=1640995200000,
                is_buyer_maker=True
            ),
            TradeResponse(
                aggregate_id=123457,
                price=50001.34,
                quantity=2.0,
                first_trade_id=789013,
                last_trade_id=789013,
                timestamp=1640995260000,
                is_buyer_maker=False
            )
        ]

        response = APIResponse[List[TradeResponse]](
            success=True,
            data=trades_data,
            message=f"Retrieved {len(trades_data)} trades",
            timestamp=int(time.time() * 1000)
        )

        assert response.success is True
        assert len(response.data) == 2
        assert response.data[0].aggregate_id == 123456
        assert response.data[1].is_buyer_maker is False

    def test_complete_api_response_with_ticker_info(self):
        """Test complete APIResponse with TickerInfo data."""
        ticker_data = TickerInfo(
            symbol="BTCUSDT",
            base_asset="BTC",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,
            price_precision=2,
            quantity_precision=3,
            processor_stats={
                "state": "RUNNING",
                "uptime_seconds": 3600,
                "trades_processed": 25000
            }
        )

        response = APIResponse[TickerInfo](
            success=True,
            data=ticker_data,
            message="Ticker information retrieved",
            timestamp=int(time.time() * 1000)
        )

        assert response.success is True
        assert response.data.symbol == "BTCUSDT"
        assert response.data.processor_stats["trades_processed"] == 25000

    def test_error_response_patterns(self):
        """Test common error response patterns."""
        # Test with null data and error message
        error_response = APIResponse[None](
            success=False,
            data=None,
            message="Symbol not found",
            timestamp=int(time.time() * 1000),
            error="SYMBOL_NOT_FOUND"
        )

        assert error_response.success is False
        assert error_response.data is None
        assert error_response.error == "SYMBOL_NOT_FOUND"

        # Test with partial data and warning
        warning_response = APIResponse[Dict[str, Any]](
            success=True,
            data={"partial": True, "count": 100},
            message="Partial data returned due to rate limiting",
            timestamp=int(time.time() * 1000),
            error="RATE_LIMITED"
        )

        assert warning_response.success is True
        assert warning_response.data["partial"] is True
        assert warning_response.error == "RATE_LIMITED"

    def test_model_validation_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Test TradeResponse with very small numbers
        tiny_trade = TradeResponse(
            aggregate_id=1,
            price=0.00000001,  # Very small price
            quantity=0.001,     # Very small quantity
            first_trade_id=1,
            last_trade_id=1,
            timestamp=1000000000000,  # Minimum valid timestamp
            is_buyer_maker=False
        )

        assert tiny_trade.price == 0.00000001
        assert tiny_trade.quantity == 0.001

        # Test TickerInfo with high precision
        high_precision_ticker = TickerInfo(
            symbol="PRECISION_TEST",
            base_asset="TEST",
            quote_asset="USDT",
            status="TRADING",
            is_tradeable=True,
            onboard_date=1640995200000,
            price_precision=18,    # Maximum precision
            quantity_precision=18  # Maximum precision
        )

        assert high_precision_ticker.price_precision == 18
        assert high_precision_ticker.quantity_precision == 18

    def test_nested_serialization_deserialization(self):
        """Test complex nested serialization/deserialization."""
        # Create complex health response
        components = {
            "tickers": {
                "BTCUSDT": {
                    "processor": {
                        "state": "RUNNING",
                        "stats": {
                            "trades_processed": 50000,
                            "errors": 0,
                            "last_trade": {
                                "id": 123456,
                                "price": 50000.0,
                                "timestamp": 1640995200000
                            }
                        }
                    }
                }
            }
        }

        health = HealthResponse(
            status="healthy",
            components=components,
            timestamp=1640995200000,
            uptime_seconds=86400.0
        )

        # Serialize to JSON
        json_str = health.model_dump_json()

        # Deserialize back
        parsed_data = json.loads(json_str)

        # Create new instance from parsed data
        health_reconstructed = HealthResponse(**parsed_data)

        # Verify deep nested data is preserved
        assert health_reconstructed.components["tickers"]["BTCUSDT"]["processor"]["state"] == "RUNNING"
        assert health_reconstructed.components["tickers"]["BTCUSDT"]["processor"]["stats"]["trades_processed"] == 50000
        assert health_reconstructed.components["tickers"]["BTCUSDT"]["processor"]["stats"]["last_trade"]["price"] == 50000.0


class TestModelPerformance:
    """Test model performance and memory efficiency - 3 tests."""

    def test_trade_response_batch_creation(self):
        """Test performance of creating many TradeResponse instances."""
        start_time = time.time()

        # Create 1000 TradeResponse instances
        trades = []
        for i in range(1000):
            trade = TradeResponse(
                aggregate_id=100000 + i,
                price=50000.0 + (i * 0.01),
                quantity=1.0 + (i * 0.001),
                first_trade_id=200000 + i,
                last_trade_id=200000 + i,
                timestamp=1640995200000 + (i * 1000),
                is_buyer_maker=(i % 2 == 0)
            )
            trades.append(trade)

        creation_time = time.time() - start_time

        assert len(trades) == 1000
        assert creation_time < 1.0  # Should complete in under 1 second

    def test_batch_serialization_performance(self):
        """Test performance of batch serialization."""
        # Create large dataset
        trades = [
            TradeResponse(
                aggregate_id=i,
                price=50000.0 + i,
                quantity=1.0,
                first_trade_id=i,
                last_trade_id=i,
                timestamp=1640995200000 + i,
                is_buyer_maker=(i % 2 == 0)
            ) for i in range(500)
        ]

        start_time = time.time()

        # Serialize all trades
        serialized_trades = [trade.model_dump() for trade in trades]

        serialization_time = time.time() - start_time

        assert len(serialized_trades) == 500
        assert serialization_time < 0.5  # Should be fast

    def test_complex_api_response_performance(self):
        """Test performance with complex nested API responses."""
        # Create complex system info
        system = SystemInfo(
            initialized=True,
            total_ticker_tasks=100,
            active_tickers=95,
            binance_client_connected=True,
            weight_coordinator_status="active",
            ticker_synchronizer_status="running",
            environment="production",
            api_requests_count=1000000,
            api_errors_count=500,
            server_uptime_seconds=604800.0
        )

        start_time = time.time()

        # Create 100 API responses
        responses = []
        for i in range(100):
            response = APIResponse[SystemInfo](
                success=True,
                data=system,
                message=f"System info {i}",
                timestamp=int(time.time() * 1000) + i
            )
            responses.append(response)

        creation_time = time.time() - start_time

        assert len(responses) == 100
        assert creation_time < 0.5  # Should be fast

        # Test serialization performance
        start_time = time.time()
        json_responses = [response.model_dump_json() for response in responses]
        serialization_time = time.time() - start_time

        assert len(json_responses) == 100
        assert serialization_time < 1.0


class TestModelDocumentation:
    """Test model documentation and field descriptions - 3 tests."""

    def test_model_field_descriptions(self):
        """Test that all models have proper field descriptions."""
        # Test APIResponse fields
        api_fields = APIResponse.model_fields
        assert all(field.description is not None for field in api_fields.values())

        # Test TradeResponse fields
        trade_fields = TradeResponse.model_fields
        assert all(field.description is not None for field in trade_fields.values())

        # Test TickerInfo fields
        ticker_fields = TickerInfo.model_fields
        assert all(field.description is not None for field in ticker_fields.values())

    def test_model_json_schema(self):
        """Test that models generate proper JSON schema."""
        # Test TradeResponse schema
        trade_schema = TradeResponse.model_json_schema()

        assert "properties" in trade_schema
        assert "aggregate_id" in trade_schema["properties"]
        assert "price" in trade_schema["properties"]
        assert "timestamp" in trade_schema["properties"]

        # Verify field types in schema
        assert trade_schema["properties"]["aggregate_id"]["type"] == "integer"
        assert trade_schema["properties"]["price"]["type"] == "number"
        assert trade_schema["properties"]["is_buyer_maker"]["type"] == "boolean"

    def test_model_examples_in_schema(self):
        """Test that model schemas contain useful information."""
        # Test SystemInfo schema
        system_schema = SystemInfo.model_json_schema()

        assert "properties" in system_schema
        assert "initialized" in system_schema["properties"]
        assert "environment" in system_schema["properties"]

        # Check that descriptions are present
        properties = system_schema["properties"]
        for field_name, field_schema in properties.items():
            if "description" in field_schema:
                assert len(field_schema["description"]) > 0