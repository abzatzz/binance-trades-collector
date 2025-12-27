"""
Data models and configuration
============================

Contains all data models, configuration classes, and enums.
"""

from .config import Config
from .aggtrade import AggTrade
from .ticker_model import TickerModel
from .exchange_info import BinanceExchangeInfo
from .binance_errors import BinanceClientError, BinanceAPIError, BinanceWebSocketError
from .enums import BinanceTickerStatus, ProcessorState

__version__ = "1.0.0"

__all__ = [
    "Config",
    "AggTrade",
    "TickerModel",
    "BinanceExchangeInfo",
    "BinanceClientError",
    "BinanceAPIError",
    "BinanceWebSocketError",
    "BinanceTickerStatus",
    "ProcessorState"
]
