"""
Core components for Data Provider system
=======================================

Contains main processing components and coordinators.
"""

from .shared import Shared
from .weight_coordinator import WeightCoordinator
from .ticker_synchronizer import TickerSynchronizer
from .ticker_processor import TickerProcessor
from .historical_downloader import HistoricalDownloader
from .websocket_updater import WebSocketUpdater
from .clickhouse_manager import ClickHouseManager
from .gap_checker import GapChecker
from .telegram_notifier import get_notifier, AlertLevel
from .health_monitor import HealthMonitor

__version__ = "1.0.0"

__all__ = [
    "Shared",
    "WeightCoordinator",
    "TickerSynchronizer",
    "TickerProcessor",
    "HistoricalDownloader",
    "WebSocketUpdater",
    "ClickHouseManager",
    "GapChecker",
    "AlertLevel",
    "get_notifier",
    "HealthMonitor"
]
