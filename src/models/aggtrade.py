"""
AggTrade Model for Binance Aggregate Trades
==========================================

High-performance model with binary serialization for efficient storage and processing.
Optimized for memory-mapped files and high-frequency data operations.

File: src/models/aggtrade.py
"""

import struct
import time
from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass(frozen=True, slots=True)
class AggTrade:
    """
    Immutable AggTrade model for Binance aggregate trades.

    Optimized for:
    - Binary serialization (49 bytes per record)
    - Memory-mapped file operations
    - Thread-safe operations (immutable)
    - High-frequency data processing

    Binary format: QddQQQB (49 bytes total)
    - Q: aggregate_id (8 bytes)
    - d: price (8 bytes, float64)
    - d: quantity (8 bytes, float64)
    - Q: first_trade_id (8 bytes)
    - Q: last_trade_id (8 bytes)
    - Q: timestamp (8 bytes)
    - B: is_buyer_maker (1 byte, boolean)
    """

    aggregate_id: int
    price: float
    quantity: float
    first_trade_id: int
    last_trade_id: int
    timestamp: int
    is_buyer_maker: bool

    # Binary format configuration
    BINARY_FORMAT = 'QddQQQB'
    BINARY_STRUCT = struct.Struct(BINARY_FORMAT)
    BINARY_SIZE = BINARY_STRUCT.size  # 49 bytes

    # Validation constants
    MIN_TIMESTAMP = 1000000000000  # Year 2001 in milliseconds
    MAX_TIMESTAMP = 4000000000000  # Year 2096 in milliseconds

    def __post_init__(self):
        """Post-initialization validation"""
        if not self.validate():
            raise ValueError(f"Invalid AggTrade data: {self}")

    @classmethod
    def from_binance_dict(cls, data: Dict[str, Any]) -> 'AggTrade':
        """
        Create AggTrade from Binance API response format.

        Expected format:
        {
            "a": 26129,              # Aggregate trade ID
            "p": "0.01633102",       # Price (string)
            "q": "4.70443515",       # Quantity (string)
            "f": 27781,              # First trade ID
            "l": 27781,              # Last trade ID
            "T": 1498793709153,      # Timestamp
            "m": true                # Is buyer maker
        }

        Args:
            data: Dictionary from Binance API

        Returns:
            AggTrade instance

        Raises:
            ValueError: If data format is invalid
            KeyError: If required fields are missing
        """
        try:
            return cls(
                aggregate_id=int(data['a']),
                price=float(data['p']),
                quantity=float(data['q']),
                first_trade_id=int(data['f']),
                last_trade_id=int(data['l']),
                timestamp=int(data['T']),
                is_buyer_maker=bool(data['m'])
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Invalid Binance trade data: {data}. Error: {e}") from e

    @classmethod
    def from_binance_list(cls, trades_list: List[Dict[str, Any]]) -> List['AggTrade']:
        """
        Batch conversion from list of Binance API responses.

        Args:
            trades_list: List of trade dictionaries from Binance

        Returns:
            List of AggTrade instances

        Raises:
            ValueError: If any trade data is invalid
        """
        if not trades_list:
            return []

        trades = []
        for i, trade_data in enumerate(trades_list):
            try:
                trades.append(cls.from_binance_dict(trade_data))
            except ValueError as e:
                raise ValueError(f"Invalid trade at index {i}: {e}") from e

        return trades

    def to_binary(self) -> bytes:
        """
        Convert to binary format for efficient storage.

        Returns:
            49 bytes of binary data

        Raises:
            struct.error: If data cannot be packed
        """
        try:
            return self.BINARY_STRUCT.pack(
                self.aggregate_id,
                self.price,
                self.quantity,
                self.first_trade_id,
                self.last_trade_id,
                self.timestamp,
                int(self.is_buyer_maker)
            )
        except struct.error as e:
            raise ValueError(f"Failed to serialize trade to binary: {e}") from e

    @classmethod
    def from_binary(cls, data: bytes) -> 'AggTrade':
        """
        Create AggTrade from binary data.

        Args:
            data: 49 bytes of binary data

        Returns:
            AggTrade instance

        Raises:
            ValueError: If binary data is invalid
        """
        if len(data) != cls.BINARY_SIZE:
            raise ValueError(
                f"Invalid binary data size: {len(data)} bytes, expected {cls.BINARY_SIZE}"
            )

        try:
            unpacked = cls.BINARY_STRUCT.unpack(data)
            return cls(
                aggregate_id=unpacked[0],
                price=unpacked[1],
                quantity=unpacked[2],
                first_trade_id=unpacked[3],
                last_trade_id=unpacked[4],
                timestamp=unpacked[5],
                is_buyer_maker=bool(unpacked[6])
            )
        except struct.error as e:
            raise ValueError(f"Failed to deserialize binary trade data: {e}") from e

    @classmethod
    def batch_to_binary(cls, trades: List['AggTrade']) -> bytes:
        """
        Batch conversion to binary format for high-performance operations.

        Args:
            trades: List of AggTrade instances

        Returns:
            Binary data for all trades concatenated
        """
        if not trades:
            return b''

        # Pre-allocate buffer for better performance
        total_size = len(trades) * cls.BINARY_SIZE
        buffer = bytearray(total_size)

        for i, trade in enumerate(trades):
            offset = i * cls.BINARY_SIZE
            trade_data = trade.to_binary()
            buffer[offset:offset + cls.BINARY_SIZE] = trade_data

        return bytes(buffer)

    @classmethod
    def batch_from_binary(cls, data: bytes) -> List['AggTrade']:
        """
        Batch conversion from binary data for high-performance operations.

        Args:
            data: Binary data containing multiple trades

        Returns:
            List of AggTrade instances

        Raises:
            ValueError: If binary data length is invalid
        """
        if len(data) % cls.BINARY_SIZE != 0:
            raise ValueError(
                f"Invalid binary data length: {len(data)} bytes. "
                f"Must be multiple of {cls.BINARY_SIZE}"
            )

        trades = []
        trades_count = len(data) // cls.BINARY_SIZE

        for i in range(trades_count):
            start_offset = i * cls.BINARY_SIZE
            end_offset = start_offset + cls.BINARY_SIZE
            trade_data = data[start_offset:end_offset]
            trades.append(cls.from_binary(trade_data))

        return trades

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert back to Binance API format for external interfaces.

        Returns:
            Dictionary in Binance API format
        """
        return {
            'a': self.aggregate_id,
            'p': f"{self.price:.8f}",  # Match Binance precision
            'q': f"{self.quantity:.8f}",  # Match Binance precision
            'f': self.first_trade_id,
            'l': self.last_trade_id,
            'T': self.timestamp,
            'm': self.is_buyer_maker
        }

    def validate(self) -> bool:
        """
        Validate trade data consistency and reasonable values.

        Returns:
            True if valid, False otherwise
        """
        try:
            # Basic positive value checks
            if (self.aggregate_id <= 0 or
                    self.price <= 0 or
                    self.quantity <= 0 or
                    self.first_trade_id <= 0):
                return False

            # Trade ID consistency
            if self.last_trade_id < self.first_trade_id:
                return False

            # Timestamp reasonableness check
            if not (self.MIN_TIMESTAMP <= self.timestamp <= self.MAX_TIMESTAMP):
                return False

            # Price and quantity reasonableness (avoid extreme values)
            if self.price > 1e10 or self.quantity > 1e15:
                return False

            return True

        except (TypeError, AttributeError):
            return False

    def get_trade_value(self) -> float:
        """
        Calculate total trade value (price * quantity).

        Returns:
            Trade value as float
        """
        return self.price * self.quantity

    def get_datetime_str(self) -> str:
        """
        Convert timestamp to human-readable datetime string.

        Returns:
            ISO format datetime string
        """
        return time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(self.timestamp / 1000))

    def is_recent(self, seconds_ago: int = 3600) -> bool:
        """
        Check if trade occurred within specified time period.

        Args:
            seconds_ago: Time period in seconds (default: 1 hour)

        Returns:
            True if trade is recent
        """
        current_time_ms = int(time.time() * 1000)
        time_threshold = current_time_ms - (seconds_ago * 1000)
        return self.timestamp >= time_threshold

    def __lt__(self, other: 'AggTrade') -> bool:
        """Sort by aggregate_id for merge operations and consistency"""
        return self.aggregate_id < other.aggregate_id

    def __str__(self) -> str:
        """Human-readable string representation"""
        return (
            f"AggTrade(id={self.aggregate_id}, "
            f"price={self.price:.8f}, "
            f"qty={self.quantity:.8f}, "
            f"time={self.get_datetime_str()})"
        )

    def __repr__(self) -> str:
        """Developer-friendly representation"""
        return (
            f"AggTrade(aggregate_id={self.aggregate_id}, "
            f"price={self.price}, quantity={self.quantity}, "
            f"first_trade_id={self.first_trade_id}, "
            f"last_trade_id={self.last_trade_id}, "
            f"timestamp={self.timestamp}, "
            f"is_buyer_maker={self.is_buyer_maker})"
        )


class AggTradeStats:
    """
    Utility class for calculating statistics on AggTrade collections.
    """

    @staticmethod
    def calculate_volume(trades: List[AggTrade]) -> float:
        """Calculate total volume from trades list"""
        return sum(trade.quantity for trade in trades)

    @staticmethod
    def calculate_value(trades: List[AggTrade]) -> float:
        """Calculate total value from trades list"""
        return sum(trade.get_trade_value() for trade in trades)

    @staticmethod
    def get_price_range(trades: List[AggTrade]) -> tuple[float, float]:
        """Get min and max prices from trades list"""
        if not trades:
            return 0.0, 0.0
        prices = [trade.price for trade in trades]
        return min(prices), max(prices)

    @staticmethod
    def get_time_range(trades: List[AggTrade]) -> tuple[int, int]:
        """Get earliest and latest timestamps from trades list"""
        if not trades:
            return 0, 0
        timestamps = [trade.timestamp for trade in trades]
        return min(timestamps), max(timestamps)

    @staticmethod
    def filter_by_time_range(
            trades: List[AggTrade],
            start_time: int,
            end_time: int
    ) -> List[AggTrade]:
        """Filter trades by timestamp range"""
        return [
            trade for trade in trades
            if start_time <= trade.timestamp <= end_time
        ]

    @staticmethod
    def filter_by_value_threshold(
            trades: List[AggTrade],
            min_value: float
    ) -> List[AggTrade]:
        """Filter trades by minimum trade value"""
        return [
            trade for trade in trades
            if trade.get_trade_value() >= min_value
        ]

