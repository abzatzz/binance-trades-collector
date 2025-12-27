"""
Main entry point for Data Provider Project
High-performance Binance AggTrades data provider with binary storage

Architecture:
- Daily file segmentation with memory-mapped access
- WebSocket streaming and REST API
- Shared resource management with async task isolation
- WeightCoordinator for API rate limiting
- Target: 500+ trading pairs, ~1M+ trades/day per major ticker
"""

import sys
from pathlib import Path

# Add project root to Python path FIRST
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# PATCH MUST BE APPLIED BEFORE ANY OTHER PROJECT IMPORTS
import loggerino_patch
from loggerino import loggerino

# Now safe to import other modules
import os
import logging

from src.models.config import Config


# Load configuration
# env_file = project_root / 'config' / '.env'
env_file = project_root / '.env'
config = Config.get_instance(env_file=str(env_file))

# Get logging configuration from monitoring config
logs_folder = Path(config.monitoring.get_session_logs_folder())
debug_in_console = config.features.enable_debug_logging
buffer_size = 512  # Default buffer size
flush_interval = 5  # Default flush interval
system_log_file = config.monitoring.loggerino_file_path
log_level = config.monitoring.loggerino_level

# Create logs directory if it doesn't exist
if not os.path.isdir(logs_folder):
    os.makedirs(logs_folder)

# Configure loggerino
loggerino.configure(
    logs_dir=str(logs_folder),
    debug_in_console=debug_in_console,
    buffer_size=buffer_size,
    flush_interval=flush_interval,
)

# Set file log level BEFORE creating loggers
loggerino_patch.set_file_log_level(log_level)

# Create loggers for Data Provider components
loggerino.create('system', system_log_file)
loggerino.create('shared', system_log_file)
loggerino.create('clickhouse_manager', system_log_file)
loggerino.create('weight_coordinator', system_log_file)
loggerino.create('ticker_synchronizer', system_log_file)
loggerino.create('historical_downloader', system_log_file)
loggerino.create('websocket_updater', system_log_file)
loggerino.create('websocket_manager', system_log_file)
loggerino.create('global_trades_updater', system_log_file)
loggerino.create('gap_checker', system_log_file)
loggerino.create('monitoring', system_log_file)
loggerino.create('rest_api', system_log_file)
loggerino.create('telegram_notifier', system_log_file)
loggerino.create('health_monitor', system_log_file)
loggerino.create('recovery_state', system_log_file)
loggerino.create('integrity_checker', system_log_file)
loggerino.create('archive_recovery', system_log_file)
loggerino.create('integrity_manager', system_log_file)

from src.core.telegram_notifier import init_notifier
# Initialize Telegram notifier
if config.telegram.is_configured():
    init_notifier(
        bot_token=config.telegram.bot_token,
        chat_id=config.telegram.chat_id,
        enabled=config.telegram.enabled
    )

logger = loggerino.get('system')
logger.info("Loggerino configured for Data Provider project")

if config.ip_patch_enabled:
    patch_ip(config.ip, logger)

import asyncio
import signal
from typing import Optional

from src.core.shared import Shared

# Global shared instance for signal handlers
shared_instance: Optional[Shared] = None


def signal_handler(signum: int, frame) -> None:
    """Handle shutdown signals"""
    logger = loggerino.get('system')
    logger.info(f"Received signal {signum}, initiating shutdown...")

    if shared_instance and shared_instance.is_initialized:
        # Create new event loop for cleanup if needed
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        # Schedule shutdown
        loop.create_task(shutdown_system())


async def shutdown_system() -> None:
    """Gracefully shutdown the Data Provider system"""
    logger = loggerino.get('system')

    # Send shutdown notification
    from src.core.telegram_notifier import get_notifier, AlertLevel
    notifier = get_notifier()
    if notifier:
        try:
            await notifier.send_alert(
                message="System shutdown initiated",
                level=AlertLevel.INFO
            )
        except Exception as e:
            logger.error(f"Failed to send shutdown notification: {e}")

    if shared_instance:
        logger.info("Stopping Data Provider system...")
        await shared_instance.shutdown()
        logger.info("System stopped successfully")


async def startup_system() -> bool:
    """Start the Data Provider system"""
    global shared_instance
    logger = loggerino.get('system')

    try:
        # Create shared resource manager
        logger.info("Initializing Data Provider system...")
        shared_instance = Shared(config)

        # Initialize the system
        logger.info("Starting Data Provider system...")
        success = await shared_instance.initialize()

        if not success:
            logger.error("Failed to initialize system")
            return False

        # System is now running
        logger.info("=" * 70)
        logger.info("🚀 DATA PROVIDER SYSTEM STARTED SUCCESSFULLY")
        logger.info("=" * 70)

        # Print system info
        system_info = shared_instance.get_system_info()
        logger.info(f"System initialized: {system_info['initialized']}")
        logger.info(f"WeightCoordinator: {system_info['weight_coordinator_status']}")
        logger.info(f"TickerSynchronizer: {system_info['ticker_synchronizer_status']}")
        logger.info(f"Binance connected: {system_info['binance_client_connected']}")
        logger.info(f"Active tickers: {system_info['active_tickers']}")
        logger.info(f"Environment: {system_info['environment']}")

        # Send startup notification
        from src.core.telegram_notifier import get_notifier, AlertLevel
        notifier = get_notifier()
        if notifier:
            try:
                await notifier.send_alert(
                    message=f"System started successfully\n\n"
                            f"Active tickers: {system_info['active_tickers']}\n"
                            f"Environment: {system_info['environment']}",
                    level=AlertLevel.INFO
                )
            except Exception as e:
                logger.error(f"Failed to send startup notification: {e}")

        # List active tickers
        active_symbols = shared_instance.get_active_ticker_symbols()
        logger.info(f"Active tickers: {active_symbols}")

        logger.info(f"Active ticker tasks: {len(shared_instance.tickers)}")

        rest_stats = shared_instance.get_rest_api_stats()
        if rest_stats:
            logger.info(f"REST API server: {rest_stats['host']}:{rest_stats['port']}")
        else:
            logger.info("REST API server: not active")

        return True

    except Exception as e:
        logger.error(f"Startup failed: {e}", exc_info=True)
        return False


async def run_system() -> None:
    """Main system run loop"""
    logger = loggerino.get('system')

    # Start the system
    startup_success = await startup_system()
    if not startup_success:
        logger.error("System startup failed, exiting")
        return

    try:
        # Main event loop - keep system running
        logger.info("System running, press Ctrl+C to stop...")

        # Health check loop
        health_check_interval = config.monitoring.system_stats_interval_seconds

        while shared_instance and shared_instance.is_initialized:
            # Just keep the loop alive - health check available via REST API /health
            await asyncio.sleep(health_check_interval)

    except asyncio.CancelledError:
        logger.info("Main loop cancelled")
    except Exception as e:
        logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
    finally:
        # Ensure cleanup
        await shutdown_system()


def print_startup_banner():
    """Print startup banner with system information"""
    logger = loggerino.get('system')

    logger.info("=" * 70)
    logger.info("📊 STARTING DATA PROVIDER - BINANCE AGGTRADES SYSTEM")
    logger.info("=" * 70)
    logger.info("Project: High-performance binary storage for crypto trading data")
    logger.info(f"Scale: {config.processing.load_only_tickers or 'All'} tickers")
    logger.info("Architecture: Shared resources + ClickHouse storage + Materialized Views")
    logger.info("Features: WebSocket streaming + REST API + Real-time OHLCV updates")
    logger.info(f"Environment: {config.environment}")
    logger.info("=" * 70)


def print_shutdown_banner():
    """Print shutdown banner"""
    logger = loggerino.get('system')

    logger.info("=" * 70)
    logger.info("👋 DATA PROVIDER SYSTEM SHUTDOWN COMPLETE")
    logger.info("=" * 70)


def main() -> None:
    """Main function"""
    # Setup logging
    logger = loggerino.get('system')

    print_startup_banner()

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        # Run the system
        asyncio.run(run_system())

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        print_shutdown_banner()


if __name__ == "__main__":
    main()
