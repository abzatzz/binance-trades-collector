"""
Binance Exchange Info Model
===========================

Complete model for Binance futures exchangeInfo API response.
Handles the full structure returned by /fapi/v1/exchangeInfo.

File: src/models/exchange_info.py
"""

from dataclasses import dataclass
from typing import Dict, Any, List, Optional

from .enums import BinanceTickerStatus
from .ticker_model import TickerModel


@dataclass(frozen=True)
class BinanceFilter:
    """
    Binance symbol filter model.

    Handles various filter types like PRICE_FILTER, LOT_SIZE, etc.
    """
    filter_type: str
    data: Dict[str, Any]

    @classmethod
    def from_dict(cls, filter_data: Dict[str, Any]) -> 'BinanceFilter':
        """
        Create filter from exchangeInfo filter object.

        Args:
            filter_data: Filter dictionary from API response

        Returns:
            BinanceFilter instance
        """
        filter_type = filter_data.get('filterType', '')
        # Store all other fields as data
        data = {k: v for k, v in filter_data.items() if k != 'filterType'}
        return cls(filter_type=filter_type, data=data)

    def get_value(self, key: str) -> Optional[Any]:
        """Get specific value from filter data."""
        return self.data.get(key)

    def get_float_value(self, key: str) -> Optional[float]:
        """Get float value from filter data."""
        value = self.data.get(key)
        if value is not None:
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        return None


@dataclass(frozen=True)
class BinanceExchangeInfo:
    """
    Complete Binance futures exchangeInfo symbol model.

    Contains all fields from the API response for future extensibility.
    Can be converted to simplified TickerModel for processing.
    """

    # Core symbol data
    symbol: str
    pair: str
    contract_type: str
    delivery_date: int
    onboard_date: int
    status: str

    # Asset information
    base_asset: str
    quote_asset: str
    margin_asset: str

    # Precision data
    price_precision: int
    quantity_precision: int
    base_asset_precision: int
    quote_precision: int

    # Trading parameters
    underlying_type: str
    underlying_sub_type: List[str]
    settle_plan: int

    # Risk management
    maint_margin_percent: str
    required_margin_percent: str
    trigger_protect: str
    liquidation_fee: str
    market_take_bound: str

    # Filters and order types
    filters: List[BinanceFilter]
    order_types: List[str]
    time_in_force: List[str]

    @classmethod
    def from_binance_dict(cls, data: Dict[str, Any]) -> 'BinanceExchangeInfo':
        """
        Create BinanceExchangeInfo from exchangeInfo API response.

        Args:
            data: Dictionary from /fapi/v1/exchangeInfo symbols array

        Returns:
            BinanceExchangeInfo instance

        Raises:
            ValueError: If required fields are missing
        """
        try:
            # Parse filters
            filters = []
            for filter_data in data.get('filters', []):
                filters.append(BinanceFilter.from_dict(filter_data))

            return cls(
                symbol=str(data['symbol']).upper(),
                pair=str(data.get('pair', data['symbol'])).upper(),
                contract_type=str(data.get('contractType', '')),
                delivery_date=int(data.get('deliveryDate', 0)),
                onboard_date=int(data['onboardDate']),
                status=str(data['status']),
                base_asset=str(data['baseAsset']).upper(),
                quote_asset=str(data['quoteAsset']).upper(),
                margin_asset=str(data.get('marginAsset', data['quoteAsset'])).upper(),
                price_precision=int(data['pricePrecision']),
                quantity_precision=int(data['quantityPrecision']),
                base_asset_precision=int(data.get('baseAssetPrecision', 8)),
                quote_precision=int(data.get('quotePrecision', 8)),
                underlying_type=str(data.get('underlyingType', '')),
                underlying_sub_type=data.get('underlyingSubType', []),
                settle_plan=int(data.get('settlePlan', 0)),
                maint_margin_percent=str(data.get('maintMarginPercent', '0')),
                required_margin_percent=str(data.get('requiredMarginPercent', '0')),
                trigger_protect=str(data.get('triggerProtect', '0')),
                liquidation_fee=str(data.get('liquidationFee', '0')),
                market_take_bound=str(data.get('marketTakeBound', '0')),
                filters=filters,
                order_types=data.get('OrderType', []),
                time_in_force=data.get('timeInForce', [])
            )
        except (KeyError, ValueError, TypeError) as e:
            raise ValueError(f"Invalid Binance exchangeInfo data: {data}. Error: {e}") from e

    def to_ticker_model(self) -> TickerModel:
        """
        Convert to simplified TickerModel for processing.

        Returns:
            TickerModel instance with essential fields
        """
        return TickerModel(
            symbol=self.symbol,
            status=BinanceTickerStatus.from_string(self.status),
            base_asset=self.base_asset,
            quote_asset=self.quote_asset,
            onboard_date=self.onboard_date,
            price_precision=self.price_precision,
            quantity_precision=self.quantity_precision
        )

    def get_filter(self, filter_type: str) -> Optional[BinanceFilter]:
        """
        Get specific filter by type.

        Args:
            filter_type: Filter type to search for

        Returns:
            BinanceFilter instance or None if not found
        """
        for filter_obj in self.filters:
            if filter_obj.filter_type == filter_type:
                return filter_obj
        return None

    def get_price_filter(self) -> Optional[BinanceFilter]:
        """Get PRICE_FILTER for this symbol."""
        return self.get_filter('PRICE_FILTER')

    def get_lot_size_filter(self) -> Optional[BinanceFilter]:
        """Get LOT_SIZE filter for this symbol."""
        return self.get_filter('LOT_SIZE')

    def get_min_notional_filter(self) -> Optional[BinanceFilter]:
        """Get MIN_NOTIONAL filter for this symbol."""
        return self.get_filter('MIN_NOTIONAL')

    def get_min_price(self) -> Optional[float]:
        """Get minimum price from PRICE_FILTER."""
        price_filter = self.get_price_filter()
        if price_filter:
            return price_filter.get_float_value('minPrice')
        return None

    def get_max_price(self) -> Optional[float]:
        """Get maximum price from PRICE_FILTER."""
        price_filter = self.get_price_filter()
        if price_filter:
            return price_filter.get_float_value('maxPrice')
        return None

    def get_tick_size(self) -> Optional[float]:
        """Get tick size from PRICE_FILTER."""
        price_filter = self.get_price_filter()
        if price_filter:
            return price_filter.get_float_value('tickSize')
        return None

    def get_min_qty(self) -> Optional[float]:
        """Get minimum quantity from LOT_SIZE filter."""
        lot_filter = self.get_lot_size_filter()
        if lot_filter:
            return lot_filter.get_float_value('minQty')
        return None

    def get_max_qty(self) -> Optional[float]:
        """Get maximum quantity from LOT_SIZE filter."""
        lot_filter = self.get_lot_size_filter()
        if lot_filter:
            return lot_filter.get_float_value('maxQty')
        return None

    def get_step_size(self) -> Optional[float]:
        """Get step size from LOT_SIZE filter."""
        lot_filter = self.get_lot_size_filter()
        if lot_filter:
            return lot_filter.get_float_value('stepSize')
        return None

    def get_min_notional(self) -> Optional[float]:
        """Get minimum notional from MIN_NOTIONAL filter."""
        notional_filter = self.get_min_notional_filter()
        if notional_filter:
            return notional_filter.get_float_value('notional')
        return None

    def is_perpetual(self) -> bool:
        """Check if this is a perpetual contract."""
        return self.contract_type == 'PERPETUAL'

    def is_tradeable(self) -> bool:
        """Check if symbol is tradeable."""
        return self.status == 'TRADING'

    def supports_order_type(self, order_type: str) -> bool:
        """Check if symbol supports specific order type."""
        return order_type in self.order_types

    def supports_time_in_force(self, tif: str) -> bool:
        """Check if symbol supports specific time in force."""
        return tif in self.time_in_force

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for serialization.

        Returns:
            Dictionary representation
        """
        return {
            'symbol': self.symbol,
            'pair': self.pair,
            'contract_type': self.contract_type,
            'delivery_date': self.delivery_date,
            'onboard_date': self.onboard_date,
            'status': self.status,
            'base_asset': self.base_asset,
            'quote_asset': self.quote_asset,
            'margin_asset': self.margin_asset,
            'price_precision': self.price_precision,
            'quantity_precision': self.quantity_precision,
            'base_asset_precision': self.base_asset_precision,
            'quote_precision': self.quote_precision,
            'underlying_type': self.underlying_type,
            'underlying_sub_type': self.underlying_sub_type,
            'settle_plan': self.settle_plan,
            'maint_margin_percent': self.maint_margin_percent,
            'required_margin_percent': self.required_margin_percent,
            'trigger_protect': self.trigger_protect,
            'liquidation_fee': self.liquidation_fee,
            'market_take_bound': self.market_take_bound,
            'filters': [{'filter_type': f.filter_type, **f.data} for f in self.filters],
            'order_types': self.order_types,
            'time_in_force': self.time_in_force,
            'is_perpetual': self.is_perpetual(),
            'is_tradeable': self.is_tradeable(),
            'min_price': self.get_min_price(),
            'max_price': self.get_max_price(),
            'tick_size': self.get_tick_size(),
            'min_qty': self.get_min_qty(),
            'max_qty': self.get_max_qty(),
            'step_size': self.get_step_size(),
            'min_notional': self.get_min_notional()
        }

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"BinanceExchangeInfo({self.symbol}, {self.status}, {self.contract_type})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (
            f"BinanceExchangeInfo(symbol='{self.symbol}', status='{self.status}', "
            f"contract_type='{self.contract_type}', onboard_date={self.onboard_date})"
        )
