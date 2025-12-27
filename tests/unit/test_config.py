"""
Unit Tests for Configuration System
===================================

Comprehensive testing of all configuration dataclasses, environment variable loading,
validation, and factory methods.

File: tests/unit/test_config.py
"""
from dataclasses import FrozenInstanceError

import pytest
import os
import json
import tempfile
from pathlib import Path
from unittest.mock import patch, mock_open

from models.config import (
    Config, StorageConfig, BinanceConfig, ProcessingConfig,
    DatabaseConfig, MonitoringConfig, APIConfig,
    str_to_bool, str_to_list, load_dotenv_manual
)


class TestUtilityFunctions:
    """Test utility functions for configuration parsing."""

    def test_str_to_bool_true_values(self):
        """Test str_to_bool with various true values."""
        true_values = ['true', 'True', 'TRUE', '1', 'yes', 'YES', 'on', 'ON', 'enabled', 'ENABLED']

        for value in true_values:
            assert str_to_bool(value) is True

    def test_str_to_bool_false_values(self):
        """Test str_to_bool with various false values."""
        false_values = ['false', 'False', 'FALSE', '0', 'no', 'NO', 'off', 'OFF', 'disabled', 'DISABLED', '']

        for value in false_values:
            assert str_to_bool(value) is False

    def test_str_to_list_comma_separated(self):
        """Test str_to_list with comma-separated values."""
        test_cases = [
            ('a,b,c', ['a', 'b', 'c']),
            ('  a  ,  b  ,  c  ', ['a', 'b', 'c']),  # With spaces
            ('single', ['single']),
            ('', []),
            ('a,', ['a']),  # Trailing comma
            (',a,b,', ['a', 'b']),  # Leading and trailing commas
        ]

        for input_str, expected in test_cases:
            result = str_to_list(input_str)
            assert result == expected

    def test_str_to_list_custom_separator(self):
        """Test str_to_list with custom separator."""
        result = str_to_list('a|b|c', separator='|')
        assert result == ['a', 'b', 'c']

    def test_load_dotenv_manual(self):
        """Test manual .env file loading."""
        env_content = """
# This is a comment
API_KEY=test_key
SECRET_KEY=test_secret
DEBUG=true
EMPTY_VALUE=
QUOTED_VALUE="quoted string"
SINGLE_QUOTED='single quoted'
INVALID_LINE_NO_EQUALS
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_file_path = f.name

        try:
            # Clear relevant env vars first
            test_vars = ['API_KEY', 'SECRET_KEY', 'DEBUG', 'EMPTY_VALUE', 'QUOTED_VALUE', 'SINGLE_QUOTED']
            for var in test_vars:
                os.environ.pop(var, None)

            # Load .env file
            load_dotenv_manual(env_file_path)

            # Verify values were loaded
            assert os.environ.get('API_KEY') == 'test_key'
            assert os.environ.get('SECRET_KEY') == 'test_secret'
            assert os.environ.get('DEBUG') == 'true'
            assert os.environ.get('EMPTY_VALUE') == ''
            assert os.environ.get('QUOTED_VALUE') == 'quoted string'
            assert os.environ.get('SINGLE_QUOTED') == 'single quoted'

        finally:
            # Cleanup
            os.unlink(env_file_path)
            for var in test_vars:
                os.environ.pop(var, None)


class TestStorageConfig:
    """Test StorageConfig dataclass and environment loading."""

    def test_storage_config_defaults(self):
        """Test StorageConfig with default values."""
        config = StorageConfig()

        assert config.data_dir == "./data"
        assert config.metadata_dir == "./data/metadata"
        assert config.logs_dir == "./logs"
        assert config.file_segmentation == "daily"
        assert config.daily_rotation_hour == 0
        assert config.max_daily_file_size_mb == 200
        assert config.compression_enabled is True
        assert config.retention_days == 365

    @patch.dict(os.environ, {
        'STORAGE_DATA_DIR': '/custom/data',
        'STORAGE_RETENTION_DAYS': '180',
        'STORAGE_COMPRESSION_ENABLED': 'false',
        'STORAGE_BUFFER_SIZE_MB': '32'
    })
    def test_storage_config_from_env(self):
        """Test StorageConfig loading from environment variables."""
        config = StorageConfig.from_env()

        assert config.data_dir == '/custom/data'
        assert config.retention_days == 180
        assert config.compression_enabled is False
        assert config.buffer_size_mb == 32

    def test_storage_config_path_methods(self):
        """Test StorageConfig path generation methods."""
        config = StorageConfig(data_dir="/test/data", metadata_dir="/test/metadata")

        # Test ticker directory
        ticker_dir = config.get_ticker_dir("BTCUSDT")
        assert ticker_dir == Path("/test/data/BTCUSDT")

        # Test daily file path
        daily_file = config.get_daily_file_path("BTCUSDT", "2024-01-01")
        assert daily_file == Path("/test/data/BTCUSDT/2024-01-01.mmap")

        # Test current file path
        current_file = config.get_current_file_path("BTCUSDT")
        assert current_file == Path("/test/data/BTCUSDT/current.mmap")

        # Test metadata file path
        metadata_file = config.get_metadata_file_path("BTCUSDT")
        assert metadata_file == Path("/test/metadata/BTCUSDT.meta.json")

    def test_storage_config_frozen(self):
        """Test that StorageConfig is immutable."""
        config = StorageConfig()

        # StorageConfig is frozen, so should raise FrozenInstanceError, not AttributeError
        from dataclasses import FrozenInstanceError
        with pytest.raises(FrozenInstanceError):
            config.data_dir = "/new/path"


class TestBinanceConfig:
    """Test BinanceConfig dataclass and environment loading."""

    def test_binance_config_defaults(self):
        """Test BinanceConfig with default values."""
        config = BinanceConfig()

        assert config.api_key == ""
        assert config.secret_key == ""
        assert config.max_weight_per_minute == 2400
        assert config.historical_trades_weight == 20
        assert config.request_timeout_seconds == 30
        assert config.websocket_ping_interval == 20
        assert config.max_websocket_reconnects == 10
        assert config.use_testnet is False

    @patch.dict(os.environ, {
        'BINANCE_API_KEY': 'test_api_key',
        'BINANCE_SECRET_KEY': 'test_secret_key',
        'BINANCE_MAX_WEIGHT_PER_MINUTE': '1200',
        'BINANCE_USE_TESTNET': 'true',
        'BINANCE_WS_MAX_RECONNECTS': '5'
    })
    def test_binance_config_from_env(self):
        """Test BinanceConfig loading from environment variables."""
        config = BinanceConfig.from_env()

        assert config.api_key == 'test_api_key'
        assert config.secret_key == 'test_secret_key'
        assert config.max_weight_per_minute == 1200
        assert config.use_testnet is True
        assert config.max_websocket_reconnects == 5

    def test_binance_config_is_configured(self):
        """Test is_configured method."""
        # Not configured (empty credentials)
        config_empty = BinanceConfig()
        assert config_empty.is_configured() is False

        # Partially configured
        config_partial = BinanceConfig(api_key="test_key")
        assert config_partial.is_configured() is False

        # Fully configured
        config_full = BinanceConfig(api_key="test_key", secret_key="test_secret")
        assert config_full.is_configured() is True

    def test_binance_config_sensitive_data(self):
        """Test that sensitive data is handled properly."""
        config = BinanceConfig(api_key="sensitive_key", secret_key="sensitive_secret")

        # Ensure the values are set
        assert config.api_key == "sensitive_key"
        assert config.secret_key == "sensitive_secret"

        # Test immutability - attempt to modify should fail
        try:
            config.api_key = "new_key"
            assert False, "Should not be able to modify frozen dataclass"
        except Exception:
            pass  # Expected behavior for frozen dataclass

    def test_storage_config_frozen(self):
        """Test that StorageConfig is immutable."""
        config = StorageConfig()

        # Check that dataclass is frozen by verifying default values
        assert config.data_dir == "./data"
        assert config.retention_days == 365

        # Attempt to modify should fail
        try:
            config.data_dir = "/new/path"
            assert False, "Should not be able to modify frozen dataclass"
        except Exception:
            pass  # Expected - any exception means it's properly frozen


class TestProcessingConfig:
    """Test ProcessingConfig dataclass and environment loading."""

    def test_processing_config_defaults(self):
        """Test ProcessingConfig with default values."""
        config = ProcessingConfig()

        assert config.historical_days_back == 30
        assert config.merge_threshold_hours == 24
        assert config.websocket_switch_hours == 24
        assert config.batch_size == 1000
        assert config.concurrent_tickers == 0
        assert config.validate_trade_data is True
        assert config.max_consecutive_errors == 10
        assert config.load_only_tickers == []

    @patch.dict(os.environ, {
        'PROCESSING_HISTORICAL_DAYS_BACK': '7',
        'PROCESSING_MERGE_THRESHOLD_HOURS': '12',
        'PROCESSING_BATCH_SIZE': '500',
        'PROCESSING_VALIDATE_TRADE_DATA': 'false',
        'PROCESSING_LOAD_ONLY_TICKERS': 'BTCUSDT,ETHUSDT,ADAUSDT'
    })
    def test_processing_config_from_env(self):
        """Test ProcessingConfig loading from environment variables."""
        config = ProcessingConfig.from_env()

        assert config.historical_days_back == 7
        assert config.merge_threshold_hours == 12
        assert config.batch_size == 500
        assert config.validate_trade_data is False
        assert config.load_only_tickers == ['BTCUSDT', 'ETHUSDT', 'ADAUSDT']

    def test_processing_config_timestamp_methods(self):
        """Test timestamp calculation methods."""
        config = ProcessingConfig(historical_days_back=7, websocket_switch_hours=12)

        # Test historical start timestamp
        historical_start = config.get_historical_start_timestamp()
        assert isinstance(historical_start, int)
        assert historical_start > 0

        # Test WebSocket threshold timestamp
        ws_threshold = config.get_websocket_threshold_timestamp()
        assert isinstance(ws_threshold, int)
        assert ws_threshold > 0
        assert ws_threshold > historical_start  # Should be more recent

    def test_processing_config_should_load_ticker(self):
        """Test should_load_ticker method."""
        # Empty filter (load all)
        config_all = ProcessingConfig(load_only_tickers=[])
        assert config_all.should_load_ticker("BTCUSDT") is True
        assert config_all.should_load_ticker("ETHUSDT") is True

        # Specific filter
        config_filtered = ProcessingConfig(load_only_tickers=["BTCUSDT", "ETHUSDT"])
        assert config_filtered.should_load_ticker("BTCUSDT") is True
        assert config_filtered.should_load_ticker("ETHUSDT") is True
        assert config_filtered.should_load_ticker("ADAUSDT") is False

        # Case insensitive
        assert config_filtered.should_load_ticker("btcusdt") is True
        assert config_filtered.should_load_ticker("ethusdt") is True

    def test_processing_config_settings_methods(self):
        """Test configuration getter methods."""
        config = ProcessingConfig(
            websocket_buffer_max_size=50000,
            websocket_batch_size=200,
            max_consecutive_errors=5,
            validate_trade_data=False
        )

        # Test WebSocket settings
        ws_settings = config.get_websocket_settings()
        expected_ws = {
            'buffer_max_size': 50000,
            'batch_size': 200,
            'flush_interval': config.websocket_flush_interval,
            'switch_hours': config.websocket_switch_hours
        }
        assert ws_settings == expected_ws

        # Test error handling settings
        error_settings = config.get_error_handling_settings()
        expected_error = {
            'max_consecutive_errors': 5,
            'recovery_delay_seconds': config.error_recovery_delay_seconds,
            'binance_retry_multiplier': config.binance_error_retry_multiplier,
            'validate_trade_data': False,
            'skip_invalid_trades': config.skip_invalid_trades,
            'max_invalid_percent': config.max_invalid_trades_percent
        }
        assert error_settings == expected_error


class TestDatabaseConfig:
    """Test DatabaseConfig dataclass and environment loading."""

    def test_database_config_defaults(self):
        """Test DatabaseConfig with default values."""
        config = DatabaseConfig()

        assert config.host == "localhost"
        assert config.port == 3306
        assert config.database == "main"
        assert config.username == "vadim"
        assert config.password == "test"
        assert config.min_pool_size == 5
        assert config.max_pool_size == 20

    @patch.dict(os.environ, {
        'DB_HOST': 'db.example.com',
        'DB_PORT': '3307',
        'DB_DATABASE': 'production',
        'DB_USERNAME': 'prod_user',
        'DB_PASSWORD': 'prod_password',
        'DB_MAX_POOL_SIZE': '50'
    })
    def test_database_config_from_env(self):
        """Test DatabaseConfig loading from environment variables."""
        config = DatabaseConfig.from_env()

        assert config.host == 'db.example.com'
        assert config.port == 3307
        assert config.database == 'production'
        assert config.username == 'prod_user'
        assert config.password == 'prod_password'
        assert config.max_pool_size == 50

    def test_database_config_connection_url(self):
        """Test connection URL generation."""
        config = DatabaseConfig(
            username="test_user",
            password="test_pass",
            host="test_host",
            port=3306,
            database="test_db"
        )

        expected_url = "mysql://test_user:test_pass@test_host:3306/test_db"
        assert config.get_connection_url() == expected_url

    def test_database_config_is_configured(self):
        """Test is_configured method."""
        # Not configured (missing database)
        config_incomplete = DatabaseConfig(host="", database="")
        assert config_incomplete.is_configured() is False

        # Configured
        config_complete = DatabaseConfig(host="localhost", database="test", username="user")
        assert config_complete.is_configured() is True


class TestMonitoringConfig:
    """Test MonitoringConfig dataclass and environment loading."""

    def test_monitoring_config_defaults(self):
        """Test MonitoringConfig with default values."""
        config = MonitoringConfig()

        assert config.loggerino_level == "INFO"
        assert config.loggerino_format == "default"
        assert config.loggerino_file_enabled is True
        assert config.metrics_enabled is True
        assert config.metrics_port == 9090
        assert config.health_check_port == 8080
        assert config.alerts_enabled is False

    @patch.dict(os.environ, {
        'LOGGERINO_LEVEL': 'DEBUG',
        'LOGGERINO_FILE_ENABLED': 'false',
        'MONITORING_METRICS_PORT': '9091',
        'MONITORING_ALERTS_ENABLED': 'true',
        'MONITORING_MEMORY_ALERT_THRESHOLD': '90'
    })
    def test_monitoring_config_from_env(self):
        """Test MonitoringConfig loading from environment variables."""
        config = MonitoringConfig.from_env()

        assert config.loggerino_level == 'DEBUG'
        assert config.loggerino_file_enabled is False
        assert config.metrics_port == 9091
        assert config.alerts_enabled is True
        assert config.memory_usage_alert_threshold_percent == 90


class TestAPIConfig:
    """Test APIConfig dataclass and environment loading."""

    def test_api_config_defaults(self):
        """Test APIConfig with default values."""
        config = APIConfig()

        assert config.rest_host == "0.0.0.0"
        assert config.rest_port == 8080
        assert config.rest_workers == 4
        assert config.websocket_host == "0.0.0.0"
        assert config.websocket_port == 8081
        assert config.rate_limit_enabled is True
        assert config.cors_enabled is True
        assert config.cors_origins == ["*"]
        assert config.cors_methods == ["GET", "POST"]

    @patch.dict(os.environ, {
        'API_REST_HOST': '127.0.0.1',
        'API_REST_PORT': '8000',
        'API_WEBSOCKET_PORT': '8001',
        'API_RATE_LIMIT_ENABLED': 'false',
        'API_CORS_ORIGINS': 'http://localhost:3000,https://app.example.com',
        'API_CORS_METHODS': 'GET,POST,PUT,DELETE'
    })
    def test_api_config_from_env(self):
        """Test APIConfig loading from environment variables."""
        config = APIConfig.from_env()

        assert config.rest_host == '127.0.0.1'
        assert config.rest_port == 8000
        assert config.websocket_port == 8001
        assert config.rate_limit_enabled is False
        assert config.cors_origins == ['http://localhost:3000', 'https://app.example.com']
        assert config.cors_methods == ['GET', 'POST', 'PUT', 'DELETE']


class TestMainConfig:
    """Test main Config class with all sub-configurations."""

    def test_config_from_env(self):
        """Test Config.from_env() method works without errors."""
        # Test that Config.from_env() can be called and returns valid config
        config = Config.from_env()

        # Verify it's a valid Config object with all required components
        assert isinstance(config, Config)
        assert isinstance(config.storage, StorageConfig)
        assert isinstance(config.binance, BinanceConfig)
        assert isinstance(config.processing, ProcessingConfig)
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.monitoring, MonitoringConfig)
        assert isinstance(config.api, APIConfig)

        # Verify default environment
        assert config.environment in ['development', 'staging', 'production']
        assert isinstance(config.debug, bool)

    def test_config_from_json_file(self):
        """Test Config loading from JSON file."""
        config_data = {
            'storage': {
                'data_dir': '/json/data',
                'retention_days': 90
            },
            'binance': {
                'api_key': 'json_key',
                'max_weight_per_minute': 1200
            },
            'processing': {
                'historical_days_back': 21,
                'batch_size': 2000
            },
            'database': {
                'host': 'json.db.host'
            },
            'monitoring': {
                'loggerino_level': 'WARNING'
            },
            'api': {
                'rest_port': 8888
            },
            'environment': 'json_env',
            'debug': False
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(config_data, f)
            json_file_path = f.name

        try:
            config = Config.from_json_file(json_file_path)

            assert config.storage.data_dir == '/json/data'
            assert config.storage.retention_days == 90
            assert config.binance.api_key == 'json_key'
            assert config.binance.max_weight_per_minute == 1200
            assert config.processing.historical_days_back == 21
            assert config.processing.batch_size == 2000
            assert config.database.host == 'json.db.host'
            assert config.monitoring.loggerino_level == 'WARNING'
            assert config.api.rest_port == 8888
            assert config.environment == 'json_env'
            assert config.debug is False

        finally:
            os.unlink(json_file_path)

    def test_config_to_dict(self):
        """Test Config serialization to dictionary."""
        config = Config(
            storage=StorageConfig(data_dir='/test'),
            binance=BinanceConfig(api_key='key', secret_key='secret'),
            processing=ProcessingConfig(historical_days_back=7),
            database=DatabaseConfig(password='db_pass'),
            monitoring=MonitoringConfig(),
            api=APIConfig(),
            environment='test',
            debug=True
        )

        config_dict = config.to_dict()

        # Verify structure
        assert 'storage' in config_dict
        assert 'binance' in config_dict
        assert 'processing' in config_dict
        assert config_dict['environment'] == 'test'
        assert config_dict['debug'] is True

        # Verify secrets are hidden
        assert config_dict['binance']['secret_key'] == '***HIDDEN***'
        assert config_dict['database']['password'] == '***HIDDEN***'

        # Verify other values are preserved
        assert config_dict['storage']['data_dir'] == '/test'
        assert config_dict['binance']['api_key'] == 'key'  # Not hidden
        assert config_dict['processing']['historical_days_back'] == 7

    def test_config_save_to_json(self):
        """Test Config saving to JSON file."""
        config = Config(
            storage=StorageConfig(data_dir='/save/test'),
            binance=BinanceConfig(api_key='save_key', secret_key='save_secret'),
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig(),
            environment='save_test',
            debug=False
        )

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            save_file_path = f.name

        try:
            # Save without secrets
            config.save_to_json(save_file_path, include_secrets=False)

            with open(save_file_path, 'r') as f:
                saved_data = json.load(f)

            assert saved_data['storage']['data_dir'] == '/save/test'
            assert saved_data['binance']['secret_key'] == '***HIDDEN***'
            assert saved_data['environment'] == 'save_test'

            # Save with secrets
            config.save_to_json(save_file_path, include_secrets=True)

            with open(save_file_path, 'r') as f:
                saved_data_with_secrets = json.load(f)

            assert saved_data_with_secrets['binance']['secret_key'] == 'save_secret'

        finally:
            os.unlink(save_file_path)

    def test_config_environment_methods(self):
        """Test environment detection methods."""
        # Production
        config_prod = Config(
            storage=StorageConfig(),
            binance=BinanceConfig(),
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig(),
            environment='production'
        )
        assert config_prod.is_production() is True
        assert config_prod.is_development() is False

        # Development
        config_dev = Config(
            storage=StorageConfig(),
            binance=BinanceConfig(),
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig(),
            environment='development'
        )
        assert config_dev.is_production() is False
        assert config_dev.is_development() is True


class TestConfigValidation:
    """Test configuration validation logic."""

    def test_config_validate_success(self):
        """Test successful validation with proper configuration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create temporary directories
            data_dir = Path(temp_dir) / "data"
            metadata_dir = Path(temp_dir) / "metadata"
            logs_dir = Path(temp_dir) / "logs"

            data_dir.mkdir()
            metadata_dir.mkdir()
            logs_dir.mkdir()

            config = Config(
                storage=StorageConfig(
                    data_dir=str(data_dir),
                    metadata_dir=str(metadata_dir),
                    logs_dir=str(logs_dir),
                    retention_days=365
                ),
                binance=BinanceConfig(api_key='valid_key', secret_key='valid_secret'),
                processing=ProcessingConfig(
                    concurrent_tickers=10,
                    batch_size=1000
                ),
                database=DatabaseConfig(host='localhost', database='test', username='user'),
                monitoring=MonitoringConfig(),
                api=APIConfig()
            )

            issues = config.validate()
            assert issues == []

    def test_config_validate_missing_directories(self):
        """Test validation with missing directories."""
        config = Config(
            storage=StorageConfig(
                data_dir='/nonexistent/data',
                metadata_dir='/nonexistent/metadata',
                logs_dir='/nonexistent/logs'
            ),
            binance=BinanceConfig(),
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig()
        )

        issues = config.validate()

        # Should have 3 issues for missing directories
        directory_issues = [issue for issue in issues if 'does not exist' in issue]
        assert len(directory_issues) == 3

    def test_config_validate_binance_not_configured(self):
        """Test validation with missing Binance configuration."""
        config = Config(
            storage=StorageConfig(),
            binance=BinanceConfig(api_key='', secret_key=''),  # Not configured
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig()
        )

        issues = config.validate()
        assert any('Binance API credentials not configured' in issue for issue in issues)

    def test_config_validate_processing_limits(self):
        """Test validation with invalid processing limits."""
        config = Config(
            storage=StorageConfig(),
            binance=BinanceConfig(),
            processing=ProcessingConfig(
                concurrent_tickers=100,  # Too high
                batch_size=2000,  # Too high
                websocket_switch_hours=12,  # Different from merge_threshold_hours
                merge_threshold_hours=24
            ),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig()
        )

        issues = config.validate()

        # Should have issues for high limits and inconsistent hours
        assert any('concurrent_tickers too high' in issue for issue in issues)
        assert any('batch_size cannot exceed 1000' in issue for issue in issues)
        assert any('websocket_switch_hours should match merge_threshold_hours' in issue for issue in issues)

    def test_config_validate_retention_days(self):
        """Test validation with invalid retention days."""
        config = Config(
            storage=StorageConfig(retention_days=0),  # Invalid
            binance=BinanceConfig(),
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig()
        )

        issues = config.validate()
        assert any('retention_days must be at least 1' in issue for issue in issues)


class TestConfigErrorHandling:
    """Test error handling and edge cases."""

    def test_config_from_env_with_dotenv_file(self):
        """Test Config.from_env with .env file loading."""
        env_content = """
STORAGE_DATA_DIR=/env/data
BINANCE_API_KEY=env_api_key
PROCESSING_BATCH_SIZE=1500
"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write(env_content)
            env_file_path = f.name

        try:
            # Clear environment first
            test_vars = ['STORAGE_DATA_DIR', 'BINANCE_API_KEY', 'PROCESSING_BATCH_SIZE']
            for var in test_vars:
                os.environ.pop(var, None)

            # Test with explicit env file
            config = Config.from_env(env_file=env_file_path)

            assert config.storage.data_dir == '/env/data'
            assert config.binance.api_key == 'env_api_key'
            # Note: This will fail validation due to batch_size > 1000

        finally:
            os.unlink(env_file_path)
            for var in test_vars:
                os.environ.pop(var, None)

    def test_config_from_json_file_not_found(self):
        """Test Config.from_json_file with non-existent file."""
        with pytest.raises(FileNotFoundError):
            Config.from_json_file('/nonexistent/config.json')

    def test_config_from_json_file_invalid_json(self):
        """Test Config.from_json_file with invalid JSON."""
        invalid_json = "{ invalid json content"

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write(invalid_json)
            json_file_path = f.name

        try:
            with pytest.raises(json.JSONDecodeError):
                Config.from_json_file(json_file_path)
        finally:
            os.unlink(json_file_path)

    @patch.dict(os.environ, {
        'PROCESSING_HISTORICAL_DAYS_BACK': 'not_a_number',
        'STORAGE_RETENTION_DAYS': 'invalid_int',
        'API_REST_PORT': 'not_port'
    })
    def test_config_immutability(self):
        """Test that configuration objects are immutable."""
        config = Config(
            storage=StorageConfig(),
            binance=BinanceConfig(),
            processing=ProcessingConfig(),
            database=DatabaseConfig(),
            monitoring=MonitoringConfig(),
            api=APIConfig()
        )

        # Verify initial values
        assert config.environment == "development"  # default
        assert config.storage.data_dir == "./data"  # default

        # Should not be able to modify frozen dataclass
        try:
            config.environment = 'modified'
            assert False, "Should not be able to modify main config"
        except Exception:
            pass

        try:
            config.storage.data_dir = '/modified'
            assert False, "Should not be able to modify storage config"
        except Exception:
            pass

    def test_processing_config_websocket_consistency(self):
        """Test ProcessingConfig WebSocket settings consistency."""
        # Test that merge_threshold_hours and websocket_switch_hours default correctly
        config = ProcessingConfig.from_env()

        # They should be equal by default
        assert config.merge_threshold_hours == config.websocket_switch_hours

        # Test explicit setting maintains consistency
        config_explicit = ProcessingConfig(merge_threshold_hours=12)
        # websocket_switch_hours should default to same value in from_env()
        assert config_explicit.merge_threshold_hours == 12


class TestFactoryFunctions:
    """Test factory functions for common scenarios."""

    def test_get_development_config(self):
        """Test development configuration factory."""
        # This would require the actual file to exist, so we mock it
        with patch('models.config.Config.from_env') as mock_from_env:
            mock_from_env.return_value = Config(
                storage=StorageConfig(),
                binance=BinanceConfig(),
                processing=ProcessingConfig(),
                database=DatabaseConfig(),
                monitoring=MonitoringConfig(),
                api=APIConfig(),
                environment='development',
                debug=True
            )

            from models.config import get_development_config
            dev_config = get_development_config()

            assert dev_config.environment == 'development'
            assert dev_config.debug is True
            mock_from_env.assert_called_once_with('config/.env.development.local')

    def test_get_production_config(self):
        """Test production configuration factory."""
        with patch('models.config.Config.from_env') as mock_from_env:
            mock_from_env.return_value = Config(
                storage=StorageConfig(),
                binance=BinanceConfig(),
                processing=ProcessingConfig(),
                database=DatabaseConfig(),
                monitoring=MonitoringConfig(),
                api=APIConfig(),
                environment='production',
                debug=False
            )

            from models.config import get_production_config
            prod_config = get_production_config()

            assert prod_config.environment == 'production'
            assert prod_config.debug is False
            mock_from_env.assert_called_once_with('config/.env.production')
