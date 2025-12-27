"""
Trades Writer Interfaces
========================

Минимальные интерфейсы для записи trades без привязки к конкретным реализациям.
Позволяет TickerProcessor работать с записью данных без знания о Shared или GlobalTradesUpdater.

File: src/core/trades_interfaces.py
"""
import asyncio
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from ..models.aggtrade import AggTrade


class TradesWriter(ABC):
    """
    Abstract interface for trades writing operations.

    Минимальный интерфейс для записи trades без привязки к конкретной реализации.
    Используется компонентами TickerProcessor для записи данных.
    """

    @abstractmethod
    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add single trade for real-time processing (thread-safe, non-blocking).

        Returns:
            True if trade was successfully added, False if buffer overflow
        """

    @abstractmethod
    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """
        Write batch of trades for historical data.

        Args:
            symbol: Ticker symbol
            trades: List of AggTrade instances

        Returns:
            Number of trades written
        """
        pass

    @abstractmethod
    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """
        Read trades from storage within time range.

        Args:
            symbol: Ticker symbol
            start_time: Start timestamp in milliseconds
            end_time: End timestamp in milliseconds

        Returns:
            List of AggTrade instances
        """
        pass

    @abstractmethod
    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """
        Get last aggregate_id for symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            Last aggregate_id or None if no data
        """
        pass

    @abstractmethod
    async def batch_get_last_aggregate_ids(self, symbols: List[str]) -> Dict[str, Optional[int]]:
        """
        Get last aggregate_ids for multiple symbols in single batch query.

        Optimization for mass initialization scenarios.

        Args:
            symbols: List of symbol names

        Returns:
            Dictionary: symbol -> last_aggregate_id (or None if no data)
        """
        pass

    @abstractmethod
    async def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """
        Get storage statistics for symbol.

        Args:
            symbol: Ticker symbol

        Returns:
            Dictionary with storage statistics
        """
        pass


class TradesWriterImpl(TradesWriter):
    """
    Implementation of TradesWriter interface using GlobalTradesUpdater and ClickHouseManager.

    Конкретная реализация интерфейса, которая использует:
    - GlobalTradesUpdater для real-time trades
    - ClickHouseManager для batch операций и чтения
    """

    def __init__(self, global_trades_updater, clickhouse_manager):
        """
        Initialize TradesWriter implementation.

        Args:
            global_trades_updater: GlobalTradesUpdater instance
            clickhouse_manager: ClickHouseManager instance
        """
        self.global_trades_updater = global_trades_updater
        self.clickhouse_manager = clickhouse_manager

    def add_trade(self, symbol: str, trade: AggTrade) -> bool:
        """
        Add single trade to GlobalTradesUpdater for real-time processing.
        Used by WebSocketUpdater for incoming trades.

        Returns:
            True if trade was successfully added, False if buffer overflow
        """
        return self.global_trades_updater.add_trade(symbol, trade)

    async def write_trades_batch(self, symbol: str, trades: List[AggTrade]) -> int:
        """
        Write batch of trades directly to ClickHouse.

        Used by HistoricalDownloader for historical data.
        """
        return await self.clickhouse_manager.write_trades(symbol, trades)

    async def read_trades(self, symbol: str, start_time: int, end_time: int) -> List[AggTrade]:
        """
        Read trades from ClickHouse storage.

        Used by components that need historical data.
        """
        return await self.clickhouse_manager.read_trades(symbol, start_time, end_time)

    async def get_last_aggregate_id(self, symbol: str) -> Optional[int]:
        """
        Get last aggregate_id from ClickHouse.

        Used by HistoricalDownloader to determine resume point.
        """
        return await self.clickhouse_manager.get_last_aggregate_id(symbol)

    async def batch_get_last_aggregate_ids(self, symbols: List[str]) -> Dict[str, Optional[int]]:
        """
        Get last aggregate_ids for multiple symbols using batch query.

        Delegates to ClickHouseManager's optimized batch query.
        """
        return await self.clickhouse_manager.batch_get_last_aggregate_ids(symbols)

    async def get_storage_stats(self, symbol: str) -> Dict[str, Any]:
        """Get storage statistics from ClickHouse."""
        try:
            return await self.clickhouse_manager.get_file_stats(symbol)
        except Exception as e:
            return {
                'symbol': symbol,
                'error': str(e)
            }
