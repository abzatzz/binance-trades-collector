"""
Configuration Management for Data Provider
==========================================

Pure ClickHouse architecture configuration system using dataclasses with environment variable support.
Optimized for ClickHouse storage, materialized views, and real-time WebSocket streaming.

File: src/models/config.py
"""
import datetime
import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List


def load_dotenv_manual(filepath: str = '.env') -> None:
    """
    Lightweight .env file loader without external dependencies.

    Args:
        filepath: Path to .env file
    """
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    try:
                        key, value = line.split('=', 1)
                        key = key.strip()

                        # Remove inline comments (everything after #)
                        if '#' in value:
                            value = value.split('#')[0]

                        # Clean up value
                        value = value.strip().strip('"').strip("'")
                        os.environ[key] = value
                    except ValueError:
                        print(f"Warning: Invalid line {line_num} in {filepath}: {line}")


def str_to_bool(value: str) -> bool:
    """Convert string to boolean with common variations."""
    return value.lower() in ('true', '1', 'yes', 'on', 'enabled')


def str_to_list(value: str, separator: str = ',') -> List[str]:
    """Convert comma-separated string to list."""
    return [item.strip() for item in value.split(separator) if item.strip()]


# ===== WEBSOCKET UTILITY CONFIGS (moved here to avoid circular import) =====

@dataclass(frozen=True)
class WebSocketConfig:
    """Internal WebSocket configuration extracted from APIConfig."""

    # Server settings
    host: str = "0.0.0.0"
    port: int = 8000

    # Update intervals
    update_interval_seconds: int = 1

    # Connection limits
    max_connections_per_ip: int = 10
    connection_timeout_seconds: int = 300

    # Message settings
    max_message_size: int = 1024 * 1024  # 1MB

    # WebSocket protocol settings
    ping_interval: int = 30  # seconds
    ping_timeout: int = 10  # seconds

    # Heartbeat settings
    enable_heartbeat: bool = True
    heartbeat_interval: int = 60  # seconds


@dataclass(frozen=True)
class APISecurityConfig:
    """Internal security configuration extracted from APIConfig."""

    # IP whitelist
    allowed_ips: List[str]

    # Rate limiting
    enable_rate_limiting: bool = True
    requests_per_minute: int = 1200


# ===== MAIN CONFIG CLASSES =====

@dataclass(frozen=True)
class ClickHouseConfig:
    """
    ClickHouse database configuration for trades storage and OHLCV materialized views.
    Primary storage backend - no file-based storage support.
    """
    # Connection settings
    host: str = ""
    port: int = 9000
    http_port: int = 8123
    database: str = ""
    username: str = ""
    password: str = ""

    # Connection pool settings
    connection_pool_size: int = 10
    max_connections_per_host: int = 20
    connection_timeout_seconds: int = 30
    query_timeout_seconds: int = 300

    # Performance settings
    enable_compression: bool = True
    compression_method: str = "lz4"  # lz4, zstd, gzip
    enable_http_compression: bool = True

    # Buffer table settings (for high-performance inserts)
    buffer_enabled: bool = False
    buffer_num_layers: int = 4
    buffer_min_time: int = 1       # Minimum seconds before flush
    buffer_max_time: int = 3      # Maximum seconds before flush
    buffer_min_rows: int = 50    # Minimum rows before flush
    buffer_max_rows: int = 500  # Maximum rows before flush
    buffer_min_bytes: int = 524288    # 1MB minimum
    buffer_max_bytes: int = 5242880   # 10MB maximum

    # Materialized Views settings
    materialized_views_enabled: bool = True
    default_timeframes_minutes: List[int] = field(default_factory=lambda: [1, 5, 15, 60, 240, 1440])
    mv_refresh_interval_seconds: int = 5  # How often to check for MV updates

    # Data retention settings (ClickHouse TTL)
    trades_retention_days: int = 3650
    enable_auto_cleanup: bool = False

    # Partitioning strategy
    partition_by_symbol_hash: bool = True
    symbol_hash_buckets: int = 16  # Number of symbol hash partitions

    # Performance optimization
    max_insert_block_size: int = 1048576  # 1M rows per insert block
    max_threads: int = 4
    enable_parallel_replicas: bool = False

    # Monitoring and health checks
    health_check_interval_seconds: int = 60
    slow_query_threshold_seconds: int = 10
    enable_query_logging: bool = True

    # Connection retry settings
    max_retries: int = 2
    retry_delay_seconds: float = 1.0
    retry_backoff_multiplier: float = 1.5

    # Detailed logging for each trade processed (for debugging)
    verbose_trade_logging: bool = False

    @classmethod
    def from_env(cls) -> 'ClickHouseConfig':
        """Load ClickHouse configuration from environment variables."""
        timeframes_str = os.getenv('CLICKHOUSE_DEFAULT_TIMEFRAMES_MINUTES', '1,5,15,60,240,1440')
        default_timeframes_minutes = [int(x.strip()) for x in timeframes_str.split(',') if x.strip().isdigit()]

        return cls(
            host=os.getenv('CLICKHOUSE_HOST', ''),
            port=int(os.getenv('CLICKHOUSE_PORT', '9000')),
            http_port=int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123')),
            database=os.getenv('CLICKHOUSE_DATABASE', ''),
            username=os.getenv('CLICKHOUSE_USERNAME', ''),
            password=os.getenv('CLICKHOUSE_PASSWORD', ''),
            connection_pool_size=int(os.getenv('CLICKHOUSE_CONNECTION_POOL_SIZE', '10')),
            max_connections_per_host=int(os.getenv('CLICKHOUSE_MAX_CONNECTIONS_PER_HOST', '20')),
            connection_timeout_seconds=int(os.getenv('CLICKHOUSE_CONNECTION_TIMEOUT', '30')),
            query_timeout_seconds=int(os.getenv('CLICKHOUSE_QUERY_TIMEOUT', '300')),
            enable_compression=str_to_bool(os.getenv('CLICKHOUSE_ENABLE_COMPRESSION', 'true')),
            compression_method=os.getenv('CLICKHOUSE_COMPRESSION_METHOD', 'lz4'),
            enable_http_compression=str_to_bool(os.getenv('CLICKHOUSE_ENABLE_HTTP_COMPRESSION', 'true')),
            buffer_enabled=str_to_bool(os.getenv('CLICKHOUSE_BUFFER_ENABLED', 'false')),
            buffer_num_layers=int(os.getenv('CLICKHOUSE_BUFFER_NUM_LAYERS', '16')),
            buffer_min_time=int(os.getenv('CLICKHOUSE_BUFFER_MIN_TIME', '10')),
            buffer_max_time=int(os.getenv('CLICKHOUSE_BUFFER_MAX_TIME', '100')),
            buffer_min_rows=int(os.getenv('CLICKHOUSE_BUFFER_MIN_ROWS', '10000')),
            buffer_max_rows=int(os.getenv('CLICKHOUSE_BUFFER_MAX_ROWS', '1000000')),
            buffer_min_bytes=int(os.getenv('CLICKHOUSE_BUFFER_MIN_BYTES', '10000000')),
            buffer_max_bytes=int(os.getenv('CLICKHOUSE_BUFFER_MAX_BYTES', '100000000')),
            materialized_views_enabled=str_to_bool(os.getenv('CLICKHOUSE_MATERIALIZED_VIEWS_ENABLED', 'true')),
            default_timeframes_minutes=default_timeframes_minutes,
            mv_refresh_interval_seconds=int(os.getenv('CLICKHOUSE_MV_REFRESH_INTERVAL', '5')),
            trades_retention_days=int(os.getenv('CLICKHOUSE_TRADES_RETENTION_DAYS', '3650')),
            enable_auto_cleanup=str_to_bool(os.getenv('CLICKHOUSE_ENABLE_AUTO_CLEANUP', 'false')),
            partition_by_symbol_hash=str_to_bool(os.getenv('CLICKHOUSE_PARTITION_BY_SYMBOL_HASH', 'true')),
            symbol_hash_buckets=int(os.getenv('CLICKHOUSE_SYMBOL_HASH_BUCKETS', '16')),
            max_insert_block_size=int(os.getenv('CLICKHOUSE_MAX_INSERT_BLOCK_SIZE', '1048576')),
            max_threads=int(os.getenv('CLICKHOUSE_MAX_THREADS', '4')),
            enable_parallel_replicas=str_to_bool(os.getenv('CLICKHOUSE_ENABLE_PARALLEL_REPLICAS', 'false')),
            health_check_interval_seconds=int(os.getenv('CLICKHOUSE_HEALTH_CHECK_INTERVAL', '60')),
            slow_query_threshold_seconds=int(os.getenv('CLICKHOUSE_SLOW_QUERY_THRESHOLD', '10')),
            enable_query_logging=str_to_bool(os.getenv('CLICKHOUSE_ENABLE_QUERY_LOGGING', 'true')),
            max_retries=int(os.getenv('CLICKHOUSE_MAX_RETRIES', '2')),
            retry_delay_seconds=float(os.getenv('CLICKHOUSE_RETRY_DELAY', '1.0')),
            retry_backoff_multiplier=float(os.getenv('CLICKHOUSE_RETRY_BACKOFF_MULTIPLIER', '1.5')),
            verbose_trade_logging=str_to_bool(os.getenv('CLICKHOUSE_VERBOSE_TRADE_LOGGING', 'false'))
        )

    def get_connection_url(self) -> str:
        """Get ClickHouse connection URL for aiochclient."""
        return f"http://{self.host}:{self.http_port}"

    def get_native_connection_params(self) -> Dict[str, Any]:
        """Get native connection parameters for clickhouse-driver."""
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.username,
            'password': self.password,
            'compression': self.compression_method if self.enable_compression else None,
            'connect_timeout': self.connection_timeout_seconds,
            'send_receive_timeout': self.query_timeout_seconds,
            'max_threads': self.max_threads
        }

    def get_buffer_table_config(self) -> Dict[str, Any]:
        """Get buffer table configuration for dynamic creation."""
        if not self.buffer_enabled:
            return {}

        return {
            'num_layers': self.buffer_num_layers,
            'min_time': self.buffer_min_time,
            'max_time': self.buffer_max_time,
            'min_rows': self.buffer_min_rows,
            'max_rows': self.buffer_max_rows,
            'min_bytes': self.buffer_min_bytes,
            'max_bytes': self.buffer_max_bytes
        }

    def is_configured(self) -> bool:
        """Check if ClickHouse configuration is complete."""
        return bool(self.host and self.database)


# =============================================================================
# DEFAULT SYMBOLS LIST
# =============================================================================

@dataclass(frozen=True)
class BinanceConfig:
    """
    Binance API configuration with comprehensive WebSocket and error handling settings.
    """
    # API credentials (should be loaded from secure environment)
    api_key: str = ""
    secret_key: str = ""

    # Rate limiting (Binance futures limits)
    max_weight_per_minute: int = 2400
    weight_buffer: int = 200  # Keep some buffer to avoid hitting limit
    historical_trades_weight: int = 20     # Weight per 1000 trades request
    max_requests_per_second: int = 10

    # Request settings
    request_timeout_seconds: int = 30
    max_retries: int = 3
    retry_delay_seconds: float = 1.0
    backoff_multiplier: float = 2.0

    # WebSocket settings
    websocket_ping_interval: int = 20
    websocket_pong_timeout: int = 10
    websocket_close_timeout: int = 10
    websocket_reconnect_delay: int = 5
    max_websocket_reconnects: int = 10

    # Library settings
    use_testnet: bool = False
    debug_mode: bool = False

    @classmethod
    def from_env(cls) -> 'BinanceConfig':
        """Load Binance configuration from environment variables."""
        return cls(
            api_key=os.getenv('BINANCE_API_KEY', ''),
            secret_key=os.getenv('BINANCE_SECRET_KEY', ''),
            max_weight_per_minute=int(os.getenv('BINANCE_MAX_WEIGHT_PER_MINUTE', '2400')),
            weight_buffer=int(os.getenv('BINANCE_WEIGHT_BUFFER', '200')),
            historical_trades_weight=int(os.getenv('BINANCE_HISTORICAL_TRADES_WEIGHT', '20')),
            max_requests_per_second=int(os.getenv('BINANCE_MAX_REQUESTS_PER_SECOND', '10')),
            request_timeout_seconds=int(os.getenv('BINANCE_REQUEST_TIMEOUT', '30')),
            max_retries=int(os.getenv('BINANCE_MAX_RETRIES', '3')),
            retry_delay_seconds=float(os.getenv('BINANCE_RETRY_DELAY', '1.0')),
            backoff_multiplier=float(os.getenv('BINANCE_BACKOFF_MULTIPLIER', '2.0')),
            websocket_ping_interval=int(os.getenv('BINANCE_WS_PING_INTERVAL', '20')),
            websocket_pong_timeout=int(os.getenv('BINANCE_WS_PONG_TIMEOUT', '10')),
            websocket_close_timeout=int(os.getenv('BINANCE_WS_CLOSE_TIMEOUT', '10')),
            websocket_reconnect_delay=int(os.getenv('BINANCE_WS_RECONNECT_DELAY', '5')),
            max_websocket_reconnects=int(os.getenv('BINANCE_WS_MAX_RECONNECTS', '10')),
            use_testnet=str_to_bool(os.getenv('BINANCE_USE_TESTNET', 'false')),
            debug_mode=str_to_bool(os.getenv('BINANCE_DEBUG_MODE', 'false'))
        )

    def is_configured(self) -> bool:
        """Check if API credentials are provided."""
        return bool(self.api_key and self.secret_key)


@dataclass(frozen=True)
class ProcessingConfig:
    """
    Data processing and historical download configuration with WebSocket coordination.
    """
    # Historical data settings
    historical_days_back: int = 1
    merge_threshold_hours: int = 24  # Hours before current time to start WebSocket
    gap_detection_enabled: bool = True
    auto_recovery_enabled: bool = True

    # WebSocket coordination settings
    websocket_switch_hours: int = 24  # Same as merge_threshold_hours for backward compatibility
    websocket_buffer_max_trades: int = 1000  # Maximum trades in WebSocket buffer
    websocket_batch_size: int = 100  # Trades per batch write in streaming mode
    websocket_flush_interval: int = 10  # Seconds between forced flushes
    max_websocket_reconnects: int = 5

    # GlobalTradesUpdater settings
    global_batch_enabled: bool = True  # Enable GlobalTradesUpdater
    global_batch_interval_seconds: int = 1  # Batch processing interval (строго 1 секунда)
    global_batch_max_size: int = 50000  # Maximum trades per batch (не используется, для справки)
    global_queue_max_size: int = 200000  # Maximum queue size before overflow

    # Batch processing
    batch_size: int = 1000  # Trades per API request (max limit)
    concurrent_tickers: int = 0  # 0 = all available tickers

    # Process management
    ticker_process_timeout_minutes: int = 30  # Process timeout protection
    health_check_interval_seconds: int = 60
    restart_failed_processes: bool = True

    # Memory management
    max_memory_usage_mb: int = 1024  # Per ticker process
    memory_check_interval_seconds: int = 300
    force_gc_interval_minutes: int = 15

    # Data validation
    validate_trade_data: bool = True
    skip_invalid_trades: bool = True  # Skip corrupted data, continue processing
    max_invalid_trades_percent: float = 5.0  # Stop if >5% trades are invalid

    # Error handling
    max_consecutive_errors: int = 10  # Maximum consecutive errors before stopping
    error_recovery_delay_seconds: int = 5  # Base delay for error recovery
    binance_error_retry_multiplier: float = 2.0  # Multiplier for Binance error retries

    # Development settings
    load_only_tickers: List[str] = field(default_factory=list)  # Empty = load all tickers

    # Detailed logging for each trade processed (for debugging)
    verbose_trade_logging: bool = False

    @classmethod
    def from_env(cls) -> 'ProcessingConfig':
        """Load processing configuration from environment variables."""
        merge_threshold = int(os.getenv('PROCESSING_MERGE_THRESHOLD_HOURS', '24'))

        return cls(
            historical_days_back=int(os.getenv('PROCESSING_HISTORICAL_DAYS_BACK', '30')),
            merge_threshold_hours=merge_threshold,
            gap_detection_enabled=str_to_bool(os.getenv('PROCESSING_GAP_DETECTION_ENABLED', 'true')),
            auto_recovery_enabled=str_to_bool(os.getenv('PROCESSING_AUTO_RECOVERY_ENABLED', 'true')),
            websocket_switch_hours=int(os.getenv('PROCESSING_WEBSOCKET_SWITCH_HOURS', str(merge_threshold))),
            websocket_buffer_max_trades=int(os.getenv('PROCESSING_WEBSOCKET_BUFFER_MAX_SIZE', '1000')),
            websocket_batch_size=int(os.getenv('PROCESSING_WEBSOCKET_BATCH_SIZE', '100')),
            websocket_flush_interval=int(os.getenv('PROCESSING_WEBSOCKET_FLUSH_INTERVAL', '10')),
            global_batch_enabled=str_to_bool(os.getenv('PROCESSING_GLOBAL_BATCH_ENABLED', 'true')),
            global_batch_interval_seconds=int(os.getenv('PROCESSING_GLOBAL_BATCH_INTERVAL', '1')),
            global_batch_max_size=int(os.getenv('PROCESSING_GLOBAL_BATCH_MAX_SIZE', '50000')),
            global_queue_max_size=int(os.getenv('PROCESSING_GLOBAL_QUEUE_MAX_SIZE', '200000')),
            batch_size=int(os.getenv('PROCESSING_BATCH_SIZE', '1000')),
            concurrent_tickers=int(os.getenv('PROCESSING_CONCURRENT_TICKERS', '0')),
            ticker_process_timeout_minutes=int(os.getenv('PROCESSING_TICKER_TIMEOUT_MINUTES', '30')),
            health_check_interval_seconds=int(os.getenv('PROCESSING_HEALTH_CHECK_INTERVAL', '60')),
            restart_failed_processes=str_to_bool(os.getenv('PROCESSING_RESTART_FAILED_PROCESSES', 'true')),
            max_memory_usage_mb=int(os.getenv('PROCESSING_MAX_MEMORY_USAGE_MB', '1024')),
            memory_check_interval_seconds=int(os.getenv('PROCESSING_MEMORY_CHECK_INTERVAL', '300')),
            force_gc_interval_minutes=int(os.getenv('PROCESSING_FORCE_GC_INTERVAL_MINUTES', '15')),
            validate_trade_data=str_to_bool(os.getenv('PROCESSING_VALIDATE_TRADE_DATA', 'true')),
            skip_invalid_trades=str_to_bool(os.getenv('PROCESSING_SKIP_INVALID_TRADES', 'true')),
            max_invalid_trades_percent=float(os.getenv('PROCESSING_MAX_INVALID_TRADES_PERCENT', '5.0')),
            max_consecutive_errors=int(os.getenv('PROCESSING_MAX_CONSECUTIVE_ERRORS', '10')),
            error_recovery_delay_seconds=int(os.getenv('PROCESSING_ERROR_RECOVERY_DELAY', '5')),
            binance_error_retry_multiplier=float(os.getenv('PROCESSING_BINANCE_ERROR_RETRY_MULTIPLIER', '2.0')),
            load_only_tickers=str_to_list(os.getenv('PROCESSING_LOAD_ONLY_TICKERS', '')),
            verbose_trade_logging=str_to_bool(os.getenv('VERBOSE_TRADE_LOGGING', 'false'))
        )

    def get_historical_start_timestamp(self) -> int:
        """Calculate historical start timestamp in milliseconds."""
        import time
        days_in_seconds = self.historical_days_back * 24 * 3600
        return int((time.time() - days_in_seconds) * 1000)

    def get_websocket_threshold_timestamp(self) -> int:
        """Calculate WebSocket activation threshold timestamp in milliseconds."""
        import time
        hours_in_seconds = self.websocket_switch_hours * 3600
        return int((time.time() - hours_in_seconds) * 1000)

    def should_load_ticker(self, symbol: str) -> bool:
        """Check if ticker should be loaded based on load_only_tickers filter."""
        if not self.load_only_tickers:  # Empty list = load all
            return True
        return symbol.upper() in [t.upper() for t in self.load_only_tickers]

    @staticmethod
    def get_symbols() -> List[str]:
        """
        Get list of symbols from PROCESSING_LOAD_ONLY_TICKERS.

        Returns:
            List of symbols, or empty list (means load all available from DB)
        """
        env_symbols = os.getenv('PROCESSING_LOAD_ONLY_TICKERS', '')
        if env_symbols:
            return str_to_list(env_symbols)
        return []


@dataclass(frozen=True)
class DatabaseConfig:
    """
    MariaDB database configuration for ticker management.
    """
    host: str = ""
    port: int = 3306
    database: str = ""
    username: str = ""
    password: str = ""

    # Connection pool settings
    min_pool_size: int = 5
    max_pool_size: int = 20
    pool_recycle_seconds: int = 3600
    pool_timeout_seconds: int = 30

    # Query settings
    query_timeout_seconds: int = 30
    charset: str = "utf8mb4"
    autocommit: bool = True

    # Ticker synchronization
    ticker_sync_interval_hours: int = 1    # Check DB every hour
    ticker_table_name: str = "ticker"      # Table name

    @classmethod
    def from_env(cls) -> 'DatabaseConfig':
        """Load database configuration from environment variables."""
        return cls(
            host=os.getenv('DB_HOST', ''),
            port=int(os.getenv('DB_PORT', '3306')),
            database=os.getenv('DB_DATABASE', ''),
            username=os.getenv('DB_USERNAME', ''),
            password=os.getenv('DB_PASSWORD', ''),
            min_pool_size=int(os.getenv('DB_MIN_POOL_SIZE', '5')),
            max_pool_size=int(os.getenv('DB_MAX_POOL_SIZE', '20')),
            pool_recycle_seconds=int(os.getenv('DB_POOL_RECYCLE_SECONDS', '3600')),
            pool_timeout_seconds=int(os.getenv('DB_POOL_TIMEOUT_SECONDS', '30')),
            query_timeout_seconds=int(os.getenv('DB_QUERY_TIMEOUT_SECONDS', '30')),
            charset=os.getenv('DB_CHARSET', 'utf8mb4'),
            autocommit=str_to_bool(os.getenv('DB_AUTOCOMMIT', 'true')),
            ticker_sync_interval_hours=int(os.getenv('DB_TICKER_SYNC_INTERVAL_HOURS', '1')),
            ticker_table_name=os.getenv('DB_TICKER_TABLE_NAME', 'ticker')
        )

    def is_configured(self) -> bool:
        """Check if database configuration is complete."""
        return bool(self.host and self.database and self.username)


@dataclass(frozen=True)
class MonitoringConfig:
    """
    Monitoring configuration with Loggerino integration.
    """
    # Loggerino settings
    loggerino_level: str = "INFO"
    loggerino_format: str = "default"
    loggerino_file_enabled: bool = True
    loggerino_file_path: str = "./logs/system.log"
    loggerino_file_rotation: str = "100 MB"
    loggerino_file_retention: str = "30 days"

    session_folder: str = field(default_factory=lambda: datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S"))

    # Metrics settings
    metrics_enabled: bool = True
    metrics_port: int = 9090
    metrics_host: str = "0.0.0.0"
    metrics_path: str = "/metrics"

    # Health monitoring
    health_check_port: int = 8080
    health_check_path: str = "/health"
    system_stats_interval_seconds: int = 60

    # Alerting (basic)
    alerts_enabled: bool = False
    alert_webhook_url: str = ""
    critical_error_threshold: int = 5
    memory_usage_alert_threshold_percent: int = 85

    @classmethod
    def from_env(cls) -> 'MonitoringConfig':
        """Load monitoring configuration from environment variables."""
        session_folder = datetime.datetime.now().strftime("%d-%m-%Y_%H-%M-%S")
        session_log_path = f"./logs/{session_folder}/system.log"
        return cls(
            loggerino_level=os.getenv('LOGGERINO_LEVEL', 'INFO'),
            loggerino_format=os.getenv('LOGGERINO_FORMAT', 'default'),
            loggerino_file_enabled=str_to_bool(os.getenv('LOGGERINO_FILE_ENABLED', 'true')),
            loggerino_file_path=session_log_path,
            session_folder=session_folder,
            loggerino_file_rotation=os.getenv('LOGGERINO_FILE_ROTATION', '100 MB'),
            loggerino_file_retention=os.getenv('LOGGERINO_FILE_RETENTION', '30 days'),
            metrics_enabled=str_to_bool(os.getenv('MONITORING_METRICS_ENABLED', 'true')),
            metrics_port=int(os.getenv('MONITORING_METRICS_PORT', '9090')),
            metrics_host=os.getenv('MONITORING_METRICS_HOST', '0.0.0.0'),
            metrics_path=os.getenv('MONITORING_METRICS_PATH', '/metrics'),
            health_check_port=int(os.getenv('MONITORING_HEALTH_CHECK_PORT', '8080')),
            health_check_path=os.getenv('MONITORING_HEALTH_CHECK_PATH', '/health'),
            system_stats_interval_seconds=int(os.getenv('MONITORING_SYSTEM_STATS_INTERVAL', '60')),
            alerts_enabled=str_to_bool(os.getenv('MONITORING_ALERTS_ENABLED', 'false')),
            alert_webhook_url=os.getenv('MONITORING_ALERT_WEBHOOK_URL', ''),
            critical_error_threshold=int(os.getenv('MONITORING_CRITICAL_ERROR_THRESHOLD', '5')),
            memory_usage_alert_threshold_percent=int(os.getenv('MONITORING_MEMORY_ALERT_THRESHOLD', '85'))
        )

    def get_session_logs_folder(self) -> str:
        """Get the session-specific logs folder path and ensure it exists."""
        session_path = f"./logs/{self.session_folder}"
        os.makedirs(session_path, exist_ok=True)
        return session_path

    def get_ticker_log_path(self, symbol: str) -> str:
        """Get log file path for specific ticker."""
        tickers_dir = f"./logs/{self.session_folder}/tickers"
        os.makedirs(tickers_dir, exist_ok=True)
        return f"./logs/{self.session_folder}/tickers/{symbol.lower()}.log"


@dataclass(frozen=True)
class FeatureFlags:
    """
    Feature flags for enabling/disabling system capabilities.
    """
    # WebSocket streaming features
    enable_websocket_streaming: bool = True
    enable_per_ticker_websockets: bool = True
    websocket_historical_data: bool = True

    # ClickHouse features
    enable_materialized_views: bool = True
    enable_buffer_tables: bool = True
    enable_compression: bool = True

    # API features
    enable_rest_api: bool = True
    enable_admin_endpoints: bool = True
    enable_metrics_endpoint: bool = True

    # Development features
    enable_debug_logging: bool = False
    enable_query_profiling: bool = False
    enable_test_endpoints: bool = False

    @classmethod
    def from_env(cls) -> 'FeatureFlags':
        """Load feature flags from environment variables."""
        return cls(
            enable_websocket_streaming=str_to_bool(os.getenv('FEATURES_ENABLE_WEBSOCKET_STREAMING', 'true')),
            enable_per_ticker_websockets=str_to_bool(os.getenv('FEATURES_ENABLE_PER_TICKER_WEBSOCKETS', 'true')),
            websocket_historical_data=str_to_bool(os.getenv('FEATURES_WEBSOCKET_HISTORICAL_DATA', 'true')),
            enable_materialized_views=str_to_bool(os.getenv('FEATURES_ENABLE_MATERIALIZED_VIEWS', 'true')),
            enable_buffer_tables=str_to_bool(os.getenv('FEATURES_ENABLE_BUFFER_TABLES', 'true')),
            enable_compression=str_to_bool(os.getenv('FEATURES_ENABLE_COMPRESSION', 'true')),
            enable_rest_api=str_to_bool(os.getenv('FEATURES_ENABLE_REST_API', 'true')),
            enable_admin_endpoints=str_to_bool(os.getenv('FEATURES_ENABLE_ADMIN_ENDPOINTS', 'true')),
            enable_metrics_endpoint=str_to_bool(os.getenv('FEATURES_ENABLE_METRICS_ENDPOINT', 'true')),
            enable_debug_logging=str_to_bool(os.getenv('FEATURES_ENABLE_DEBUG_LOGGING', 'false')),
            enable_query_profiling=str_to_bool(os.getenv('FEATURES_ENABLE_QUERY_PROFILING', 'false')),
            enable_test_endpoints=str_to_bool(os.getenv('FEATURES_ENABLE_TEST_ENDPOINTS', 'false'))
        )


@dataclass(frozen=True)
class TelegramConfig:
    """
    Telegram notification configuration for critical alerts.
    """
    enabled: bool = False
    bot_token: str = ""
    chat_id: str = ""

    # Alert settings
    send_gap_alerts: bool = True
    send_recovery_alerts: bool = True
    send_ticker_stopped_alerts: bool = True

    @classmethod
    def from_env(cls) -> 'TelegramConfig':
        """Load Telegram configuration from environment variables."""
        return cls(
            enabled=str_to_bool(os.getenv('TELEGRAM_ENABLED', 'false')),
            bot_token=os.getenv('TELEGRAM_BOT_TOKEN', ''),
            chat_id=os.getenv('TELEGRAM_CHAT_ID', ''),
            send_gap_alerts=str_to_bool(os.getenv('TELEGRAM_SEND_GAP_ALERTS', 'true')),
            send_recovery_alerts=str_to_bool(os.getenv('TELEGRAM_SEND_RECOVERY_ALERTS', 'true')),
            send_ticker_stopped_alerts=str_to_bool(os.getenv('TELEGRAM_SEND_TICKER_STOPPED_ALERTS', 'true'))
        )

    def is_configured(self) -> bool:
        """Check if Telegram is properly configured."""
        return self.enabled and bool(self.bot_token) and bool(self.chat_id)


@dataclass(frozen=True)
class WebSocketStreamingConfig:
    """
    WebSocket streaming configuration for real-time OHLCV updates.
    """
    # Per-ticker WebSocket settings
    per_ticker_enabled: bool = True
    max_connections_per_ticker: int = 100
    connection_timeout_seconds: int = 30
    ping_interval_seconds: int = 30
    ping_timeout_seconds: int = 10

    # Historical data streaming
    historical_data_enabled: bool = True
    max_historical_limit: int = 1000
    historical_timeout_seconds: int = 60

    # Update broadcasting
    broadcast_interval_milliseconds: int = 100  # Batch updates every 100ms
    max_updates_per_broadcast: int = 100
    enable_compression: bool = True

    # Subscription management
    max_subscriptions_per_connection: int = 50
    subscription_timeout_seconds: int = 5

    # Memory management
    connection_buffer_size_mb: int = 1  # 1MB per connection buffer
    max_total_memory_mb: int = 100      # 100MB total WebSocket memory limit

    @classmethod
    def from_env(cls) -> 'WebSocketStreamingConfig':
        """Load WebSocket streaming configuration from environment variables."""
        return cls(
            per_ticker_enabled=str_to_bool(os.getenv('WS_STREAMING_PER_TICKER_ENABLED', 'true')),
            max_connections_per_ticker=int(os.getenv('WS_STREAMING_MAX_CONNECTIONS_PER_TICKER', '100')),
            connection_timeout_seconds=int(os.getenv('WS_STREAMING_CONNECTION_TIMEOUT', '30')),
            ping_interval_seconds=int(os.getenv('WS_STREAMING_PING_INTERVAL', '30')),
            ping_timeout_seconds=int(os.getenv('WS_STREAMING_PING_TIMEOUT', '10')),
            historical_data_enabled=str_to_bool(os.getenv('WS_STREAMING_HISTORICAL_DATA_ENABLED', 'true')),
            max_historical_limit=int(os.getenv('WS_STREAMING_MAX_HISTORICAL_LIMIT', '1000')),
            historical_timeout_seconds=int(os.getenv('WS_STREAMING_HISTORICAL_TIMEOUT', '60')),
            broadcast_interval_milliseconds=int(os.getenv('WS_STREAMING_BROADCAST_INTERVAL_MS', '100')),
            max_updates_per_broadcast=int(os.getenv('WS_STREAMING_MAX_UPDATES_PER_BROADCAST', '100')),
            enable_compression=str_to_bool(os.getenv('WS_STREAMING_ENABLE_COMPRESSION', 'true')),
            max_subscriptions_per_connection=int(os.getenv('WS_STREAMING_MAX_SUBSCRIPTIONS_PER_CONNECTION', '50')),
            subscription_timeout_seconds=int(os.getenv('WS_STREAMING_SUBSCRIPTION_TIMEOUT', '5')),
            connection_buffer_size_mb=int(os.getenv('WS_STREAMING_CONNECTION_BUFFER_SIZE_MB', '1')),
            max_total_memory_mb=int(os.getenv('WS_STREAMING_MAX_TOTAL_MEMORY_MB', '100'))
        )


@dataclass(frozen=True)
class APIConfig:
    """
    Unified API configuration for both REST and WebSocket endpoints.
    Single FastAPI server handles both REST and WebSocket on same host:port.
    """

    # ===== SERVER SETTINGS (shared by REST and WebSocket) =====
    host: str = "0.0.0.0"
    port: int = 8080
    rest_workers: int = 4
    rest_max_request_size: int = 16 * 1024 * 1024  # 16MB

    # ===== WEBSOCKET SETTINGS =====
    websocket_enabled: bool = True
    websocket_update_interval_seconds: int = 1
    websocket_connection_timeout_seconds: int = 300
    websocket_max_message_size: int = 1024 * 1024  # 1MB
    websocket_ping_interval: int = 30
    websocket_ping_timeout: int = 10
    websocket_enable_heartbeat: bool = True
    websocket_heartbeat_interval: int = 60

    # ===== SHARED SECURITY SETTINGS =====
    allowed_ips: List[str] = field(default_factory=lambda: [
        "127.0.0.1",  # Localhost
        "::1",  # IPv6 localhost
        "192.168.1.100",  # User 1
        "203.0.113.45",  # User 2
        "198.51.100.123",  # User 3
    ])
    max_connections_per_ip: int = 10
    enable_rate_limiting: bool = True
    rate_limit_requests_per_minute: int = 1200
    require_api_key: bool = False
    api_keys: List[str] = field(default_factory=list)

    # ===== CORS SETTINGS =====
    cors_enabled: bool = True
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    cors_methods: List[str] = field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])

    @classmethod
    def from_env(cls) -> 'APIConfig':
        """Load API configuration from environment variables."""
        # Parse allowed IPs
        allowed_ips_str = os.getenv('API_ALLOWED_IPS', '127.0.0.1,::1,192.168.1.100,203.0.113.45,198.51.100.123')
        allowed_ips = [ip.strip() for ip in allowed_ips_str.split(',') if ip.strip()]

        # Parse API keys
        api_keys_str = os.getenv('API_KEYS', '')
        api_keys = [key.strip() for key in api_keys_str.split(',') if key.strip()] if api_keys_str else []

        return cls(
            host=os.getenv('API_REST_HOST', '0.0.0.0'),
            port=int(os.getenv('API_REST_PORT', '8080')),
            rest_workers=int(os.getenv('API_REST_WORKERS', '4')),
            rest_max_request_size=int(os.getenv('API_REST_MAX_REQUEST_SIZE', str(16 * 1024 * 1024))),

            websocket_enabled=str_to_bool(os.getenv('API_WEBSOCKET_ENABLED', 'true')),
            websocket_update_interval_seconds=int(os.getenv('API_WEBSOCKET_UPDATE_INTERVAL', '1')),
            websocket_connection_timeout_seconds=int(os.getenv('API_WEBSOCKET_CONNECTION_TIMEOUT', '300')),
            websocket_max_message_size=int(os.getenv('API_WEBSOCKET_MAX_MESSAGE_SIZE', str(1024 * 1024))),
            websocket_ping_interval=int(os.getenv('API_WEBSOCKET_PING_INTERVAL', '30')),
            websocket_ping_timeout=int(os.getenv('API_WEBSOCKET_PING_TIMEOUT', '10')),
            websocket_enable_heartbeat=str_to_bool(os.getenv('API_WEBSOCKET_ENABLE_HEARTBEAT', 'true')),
            websocket_heartbeat_interval=int(os.getenv('API_WEBSOCKET_HEARTBEAT_INTERVAL', '60')),

            allowed_ips=allowed_ips,
            max_connections_per_ip=int(os.getenv('API_MAX_CONNECTIONS_PER_IP', '10')),
            enable_rate_limiting=str_to_bool(os.getenv('API_ENABLE_RATE_LIMITING', 'true')),
            rate_limit_requests_per_minute=int(os.getenv('API_RATE_LIMIT_RPM', '1200')),
            require_api_key=str_to_bool(os.getenv('API_REQUIRE_API_KEY', 'false')),
            api_keys=api_keys,

            cors_enabled=str_to_bool(os.getenv('API_CORS_ENABLED', 'true')),
            cors_origins=str_to_list(os.getenv('API_CORS_ORIGINS', '*')),
            cors_methods=str_to_list(os.getenv('API_CORS_METHODS', 'GET,POST,PUT,DELETE'))
        )

    def get_websocket_config(self) -> WebSocketConfig:
        """Extract WebSocket-specific configuration for internal use."""
        return WebSocketConfig(
            host=self.host,
            port=self.port,
            update_interval_seconds=self.websocket_update_interval_seconds,
            max_connections_per_ip=self.max_connections_per_ip,
            connection_timeout_seconds=self.websocket_connection_timeout_seconds,
            max_message_size=self.websocket_max_message_size,
            ping_interval=self.websocket_ping_interval,
            ping_timeout=self.websocket_ping_timeout,
            enable_heartbeat=self.websocket_enable_heartbeat,
            heartbeat_interval=self.websocket_heartbeat_interval
        )

    def get_security_config(self) -> APISecurityConfig:
        """Extract security configuration for internal use."""
        return APISecurityConfig(
            allowed_ips=self.allowed_ips,
            enable_rate_limiting=self.enable_rate_limiting,
            requests_per_minute=self.rate_limit_requests_per_minute
        )


class ConfigSingleton:
    """Singleton wrapper for Config to ensure consistent session_folder."""
    _instance = None
    _config = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def get_config(self, env_file: Optional[str] = None) -> 'Config':
        if self._config is None:
            self._config = Config.from_env(env_file)
        return self._config


@dataclass(frozen=True)
class Config:
    """
    Main configuration class for pure ClickHouse architecture.
    """
    clickhouse: ClickHouseConfig
    binance: BinanceConfig
    processing: ProcessingConfig
    database: DatabaseConfig
    monitoring: MonitoringConfig
    api: APIConfig
    features: FeatureFlags
    websocket_streaming: WebSocketStreamingConfig
    telegram: TelegramConfig

    # Environment settings
    environment: str = "development"  # development, staging, production
    debug: bool = False

    ip_patch_enabled: bool = False  # Patch to bind to all interfaces, default false
    ip = "0.0.0.0"

    @classmethod
    def from_env(cls, env_file: Optional[str] = None) -> 'Config':
        """
        Load complete configuration from environment variables.

        Args:
            env_file: Optional path to .env file to load first

        Returns:
            Complete configuration instance
        """
        # Load .env file if specified
        if env_file:
            load_dotenv_manual(env_file)
        else:
            # Try common .env file locations
            for env_path in ['.env', 'config/.env', '../.env']:
                if os.path.exists(env_path):
                    load_dotenv_manual(env_path)
                    break

        return cls(
            clickhouse=ClickHouseConfig.from_env(),
            binance=BinanceConfig.from_env(),
            processing=ProcessingConfig.from_env(),
            database=DatabaseConfig.from_env(),
            monitoring=MonitoringConfig.from_env(),
            api=APIConfig.from_env(),
            features=FeatureFlags.from_env(),
            websocket_streaming=WebSocketStreamingConfig.from_env(),
            telegram=TelegramConfig.from_env(),
            environment=os.getenv('ENVIRONMENT', 'development'),
            debug=str_to_bool(os.getenv('DEBUG', 'false'))
        )

    def validate(self) -> List[str]:
        """Validate configuration and return list of issues."""
        issues = []

        # Check ClickHouse configuration
        if not self.clickhouse.is_configured():
            issues.append("ClickHouse configuration incomplete - host and database required")

        # Check Binance configuration
        if not self.binance.is_configured():
            issues.append("Binance API credentials not configured")

        # Check database configuration
        if not self.database.is_configured():
            issues.append("Database configuration incomplete")

        return issues

    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment.lower() == 'production'

    @staticmethod
    def get_instance(env_file: Optional[str] = None) -> 'Config':
        """Get singleton instance of Config."""
        singleton = ConfigSingleton()
        return singleton.get_config(env_file)
