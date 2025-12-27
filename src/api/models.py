"""
Pydantic models for REST API responses
=====================================

Response models for all API endpoints with proper typing and validation.

File: src/api/models.py
"""

from typing import Generic, TypeVar, Optional, Dict, Any, List
from pydantic import BaseModel, Field

T = TypeVar('T')


class APIResponse(BaseModel, Generic[T]):
    """Generic API response wrapper."""
    success: bool = Field(description="Whether the request was successful")
    data: Optional[T] = Field(description="Response data")
    message: str = Field(description="Response message")
    timestamp: int = Field(description="Response timestamp in milliseconds")
    error: Optional[str] = Field(None, description="Error message if any")


class TradeResponse(BaseModel):
    """AggTrade response model."""
    aggregate_id: int = Field(description="Aggregate trade ID")
    price: float = Field(description="Trade price")
    quantity: float = Field(description="Trade quantity")
    first_trade_id: int = Field(description="First trade ID")
    last_trade_id: int = Field(description="Last trade ID")
    timestamp: int = Field(description="Trade timestamp in milliseconds")
    is_buyer_maker: bool = Field(description="Whether buyer is maker")


class TickerInfo(BaseModel):
    """Ticker information response model."""
    symbol: str = Field(description="Trading symbol")
    base_asset: str = Field(description="Base asset")
    quote_asset: str = Field(description="Quote asset")
    status: str = Field(description="Ticker status")
    is_tradeable: bool = Field(description="Whether ticker is tradeable")
    onboard_date: int = Field(description="Onboard timestamp in milliseconds")
    price_precision: int = Field(description="Price decimal precision")
    quantity_precision: int = Field(description="Quantity decimal precision")
    processor_stats: Optional[Dict[str, Any]] = Field(None, description="Processor statistics")


class SystemInfo(BaseModel):
    """System information response model."""
    initialized: bool = Field(description="Whether system is initialized")
    total_ticker_tasks: int = Field(description="Total ticker tasks")
    active_tickers: int = Field(description="Active tickers count")
    binance_client_connected: bool = Field(description="Binance client connection status")
    weight_coordinator_status: str = Field(description="WeightCoordinator status")
    ticker_synchronizer_status: str = Field(description="TickerSynchronizer status")
    environment: str = Field(description="Environment name")
    api_requests_count: int = Field(description="Total API requests processed")
    api_errors_count: int = Field(description="Total API errors")
    server_uptime_seconds: float = Field(description="Server uptime in seconds")


class HealthResponse(BaseModel):
    """Health check response model."""
    status: str = Field(description="Overall health status")
    components: Dict[str, Any] = Field(description="Component health details")
    timestamp: int = Field(description="Health check timestamp")
    uptime_seconds: float = Field(description="System uptime in seconds")


class OHLCVCandle(BaseModel):
    """Single OHLCV candle."""
    candle_time: int = Field(description="Candle open time in milliseconds UTC")
    open: float = Field(description="Open price")
    high: float = Field(description="High price")
    low: float = Field(description="Low price")
    close: float = Field(description="Close price")
    volume: float = Field(description="Volume in base currency")
    trades_count: int = Field(description="Number of trades in candle")
