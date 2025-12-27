"""
TickerModel for Binance Futures Tickers
=======================================

Lightweight model for ticker metadata from Binance exchangeInfo.
Contains only essential fields for data processing operations.

File: src/models/ticker_model.py
"""

import time
from dataclasses import dataclass
from typing import Dict, Any

from .enums import BinanceTickerStatus


@dataclass(frozen=True, slots=True)
class TickerModel:
    """
    Immutable ticker metadata model from Binance exchangeInfo.

    Contains only essential fields needed for data processing operations.
    Thread-safe due to immutability.

    Note: Create instances via BinanceExchangeInfo.to_ticker_model()
    rather than direct instantiation from API data.
    """

    symbol: str  # "BTCUSDT"
    status: BinanceTickerStatus  # Trading status
    base_asset: str  # "BTC"
    quote_asset: str  # "USDT"
    onboard_date: int  # Timestamp when trading started (milliseconds)
    price_precision: int  # Decimal places for price
    quantity_precision: int  # Decimal places for quantity

    def __post_init__(self):
        """Post-initialization validation."""
        if not self.validate_model():
            raise ValueError(f"Invalid TickerModel data: {self}")

    def get_historical_start_date(self, historical_days_back: int) -> int:
        """
        Calculate the actual start date for historical data loading.

        Takes the maximum of:
        - onboard_date (when trading started)
        - current_time - historical_days_back

        This ensures we don't try to load data from before the ticker existed.

        Args:
            historical_days_back: Number of days to go back from current time

        Returns:
            Timestamp in milliseconds for historical data start
        """
        current_time_ms = int(time.time() * 1000)
        config_start_time = current_time_ms - (historical_days_back * 24 * 60 * 60 * 1000)

        # Use the later of onboard_date or config start time
        return max(self.onboard_date, config_start_time)

    def validate_price(self, price: float) -> bool:
        """
        Validate price according to ticker precision rules.

        Args:
            price: Price value to validate

        Returns:
            True if price is valid for this ticker
        """
        if price <= 0:
            return False

        # Check if price respects the precision rules
        # Convert to string to check decimal places
        price_str = f"{price:.{self.price_precision}f}"

        try:
            # Ensure we can convert back and it matches
            reconstructed_price = float(price_str)
            return abs(price - reconstructed_price) < 1e-10
        except (ValueError, OverflowError):
            return False

    def validate_quantity(self, quantity: float) -> bool:
        """
        Validate quantity according to ticker precision rules.

        Args:
            quantity: Quantity value to validate

        Returns:
            True if quantity is valid for this ticker
        """
        if quantity <= 0:
            return False

        # Check if quantity respects the precision rules
        quantity_str = f"{quantity:.{self.quantity_precision}f}"

        try:
            # Ensure we can convert back and it matches
            reconstructed_quantity = float(quantity_str)
            return abs(quantity - reconstructed_quantity) < 1e-10
        except (ValueError, OverflowError):
            return False

    def validate_model(self) -> bool:
        """
        Validate the ticker model data consistency.

        Returns:
            True if model is valid
        """
        try:
            # Basic field validation
            if not self.symbol or len(self.symbol) < 3:
                return False

            if not self.base_asset or not self.quote_asset:
                return False

            # Timestamp validation (reasonable range)
            min_timestamp = 1000000000000  # Year 2001
            max_timestamp = 4000000000000  # Year 2096
            if not (min_timestamp <= self.onboard_date <= max_timestamp):
                return False

            # Precision validation
            if not (0 <= self.price_precision <= 18):
                return False

            if not (0 <= self.quantity_precision <= 18):
                return False

            return True

        except (TypeError, AttributeError):
            return False

    def is_tradeable(self) -> bool:
        """
        Check if ticker is currently tradeable.

        Returns:
            True if ticker status allows trading
        """
        return self.status.is_tradeable()

    def get_pair_display_name(self) -> str:
        """
        Get human-readable pair name.

        Returns:
            Formatted pair name like "BTC/USDT"
        """
        return f"{self.base_asset}/{self.quote_asset}"

    def is_usdt_pair(self) -> bool:
        """Check if this is a USDT-denominated pair."""
        return self.quote_asset == "USDT"

    def is_busd_pair(self) -> bool:
        """Check if this is a BUSD-denominated pair."""
        return self.quote_asset == "BUSD"

    def get_onboard_date_str(self) -> str:
        """
        Get human-readable onboard date.

        Returns:
            ISO format date string
        """
        return time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(self.onboard_date / 1000))

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dictionary representation
        """
        return {
            'symbol': self.symbol,
            'status': self.status.name,
            'base_asset': self.base_asset,
            'quote_asset': self.quote_asset,
            'onboard_date': self.onboard_date,
            'price_precision': self.price_precision,
            'quantity_precision': self.quantity_precision,
            'pair_display_name': self.get_pair_display_name(),
            'onboard_date_str': self.get_onboard_date_str(),
            'is_tradeable': self.is_tradeable()
        }

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"TickerModel({self.symbol}, {self.status.name}, onboard: {self.get_onboard_date_str()})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"TickerModel(symbol='{self.symbol}', status={self.status.name}, "
            f"base_asset='{self.base_asset}', quote_asset='{self.quote_asset}', "
            f"onboard_date={self.onboard_date}, price_precision={self.price_precision}, "
            f"quantity_precision={self.quantity_precision})"
        )
