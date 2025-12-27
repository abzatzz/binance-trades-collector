"""
WebSocket Data Models
====================

Pydantic models for WebSocket message validation and serialization.

File: src/api/websocket_api/websocket_models.py
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Union, Literal
from pydantic import BaseModel, Field, validator


# ===== REQUEST MODELS =====

class TimeframeSubscription(BaseModel):
    """Subscription configuration for a specific timeframe."""
    timeframe: int = Field(..., gt=0, description="Timeframe in minutes")
    initial_candles: int = Field(0, ge=0, le=1000, description="Number of initial historical candles")


class SymbolSubscription(BaseModel):
    """Subscription configuration for a symbol with multiple timeframes."""
    symbol: str = Field(..., description="Trading symbol (e.g., BTCUSDT)")
    timeframes: List[TimeframeSubscription] = Field(..., min_items=1, description="Timeframe configurations")

    @validator('symbol')
    def symbol_must_be_uppercase(cls, v):
        return v.upper()


class SubscriptionRequest(BaseModel):
    """Client subscription request."""
    action: Literal["subscribe"] = Field(..., description="Action type")
    subscriptions: List[SymbolSubscription] = Field(..., min_items=1, description="List of symbol subscriptions")


class UnsubscriptionRequest(BaseModel):
    """Client unsubscription request."""
    action: Literal["unsubscribe"] = Field(..., description="Action type")
    unsubscriptions: List[Dict[str, Any]] = Field(..., description="Unsubscription configurations")


class PingRequest(BaseModel):
    """Client ping request for connection health check."""
    action: Literal["ping"] = Field(..., description="Action type")
    timestamp: Optional[int] = Field(None, description="Client timestamp")


# ===== RESPONSE MODELS =====

class CandleData(BaseModel):
    """OHLCV candle data."""
    candle_time: int = Field(..., description="Candle start timestamp in milliseconds")
    open: float = Field(..., description="Opening price")
    high: float = Field(..., description="Highest price")
    low: float = Field(..., description="Lowest price")
    close: float = Field(..., description="Closing price")
    volume: float = Field(..., description="Volume")
    trades_count: int = Field(..., description="Number of trades")
    taker_buy_volume: Optional[float] = Field(None, description="Taker buy volume")
    taker_sell_volume: Optional[float] = Field(None, description="Taker sell volume")


class DataStatus(BaseModel):
    """Ticker data status information."""
    historical_loading: bool = Field(..., description="Historical data loading in progress")
    gap_recovery: bool = Field(..., description="Gap recovery in progress")
    data_quality: str = Field(..., description="Data quality: complete, partial, loading, unknown")
    last_complete_timestamp: Optional[int] = Field(None, description="Last complete data timestamp")


class InitialHistoryMessage(BaseModel):
    """Message with initial historical candles."""
    type: Literal["initial_history"] = Field("initial_history", description="Message type")
    symbol: str = Field(..., description="Trading symbol")
    timeframe: str = Field(..., description="Timeframe (e.g., '15m')")
    requested_candles: int = Field(..., description="Number of candles requested")
    actual_candles: int = Field(..., description="Number of candles returned")
    candles: List[CandleData] = Field(..., description="Historical candle data")
    data_status: DataStatus = Field(..., description="Data status information")


class OHLCVUpdateMessage(BaseModel):
    """Real-time OHLCV update message."""
    type: Literal["ohlcv_update"] = Field("ohlcv_update", description="Message type")
    symbol: str = Field(..., description="Trading symbol")
    timeframe: str = Field(..., description="Timeframe (e.g., '15m')")
    data: Optional[CandleData] = Field(None, description="Current candle data (null if loading)")
    data_status: DataStatus = Field(..., description="Data status information")


class SubscriptionResultMessage(BaseModel):
    """Subscription result confirmation."""
    type: Literal["subscription_result"] = Field("subscription_result", description="Message type")
    successful_subscriptions: Dict[str, List[Dict[str, int]]] = Field(..., description="Successful subscriptions")
    failed_subscriptions: Optional[List[Dict[str, Any]]] = Field(None, description="Failed subscriptions")
    message: str = Field(..., description="Result message")


class UnsubscriptionResultMessage(BaseModel):
    """Unsubscription result confirmation."""
    type: Literal["unsubscription_result"] = Field("unsubscription_result", description="Message type")
    unsubscribed: List[Dict[str, Any]] = Field(..., description="Successfully unsubscribed items")
    message: str = Field(..., description="Result message")


class ErrorMessage(BaseModel):
    """Error message for client."""
    type: Literal["error"] = Field("error", description="Message type")
    error_code: str = Field(..., description="Error code")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")


class PongMessage(BaseModel):
    """Pong response to ping."""
    type: Literal["pong"] = Field("pong", description="Message type")
    timestamp: int = Field(..., description="Server timestamp")
    client_timestamp: Optional[int] = Field(None, description="Original client timestamp")


class HeartbeatMessage(BaseModel):
    """Server heartbeat message."""
    type: Literal["heartbeat"] = Field("heartbeat", description="Message type")
    timestamp: int = Field(..., description="Server timestamp")
    connections_count: int = Field(..., description="Active connections count")


# ===== DATACLASS MODELS FOR INTERNAL USE =====

@dataclass
class ClientConnection:
    """Internal representation of client connection."""
    client_id: str
    websocket: Any  # WebSocket instance
    subscriptions: Dict[str, set]  # symbol -> set of timeframes
    connected_at: float
    last_ping: Optional[float] = None
    ip_address: str = ""


@dataclass
class SubscriptionStats:
    """Statistics for subscription management."""
    total_subscriptions: int
    active_symbols: int
    active_timeframes: int
    clients_count: int
    mv_references: int


# ===== VALIDATION HELPERS =====

def validate_timeframe(timeframe: int) -> bool:
    """Validate if timeframe is acceptable (1 minute to 1 month)."""
    return 1 <= timeframe <= 43200  # 1 minute to 30 days


def validate_symbol_format(symbol: str) -> bool:
    """Validate symbol format (basic check)."""
    return (
            3 <= len(symbol) <= 20 and
            symbol.isalnum() and
            symbol.isupper()
    )


def create_timeframe_string(minutes: int) -> str:
    """Convert minutes to timeframe string (e.g., 60 -> '60m', 1440 -> '1440m')."""
    return f"{minutes}m"


# ===== MESSAGE UNIONS =====

ClientMessage = Union[
    SubscriptionRequest,
    UnsubscriptionRequest,
    PingRequest
]

ServerMessage = Union[
    InitialHistoryMessage,
    OHLCVUpdateMessage,
    SubscriptionResultMessage,
    UnsubscriptionResultMessage,
    ErrorMessage,
    PongMessage,
    HeartbeatMessage
]


# ===== RESPONSE HELPERS =====

def create_error_message(error_code: str, message: str, details: Optional[Dict] = None) -> ErrorMessage:
    """Helper to create standardized error messages."""
    return ErrorMessage(
        type="error",
        error_code=error_code,
        message=message,
        details=details or {}
    )


def create_subscription_success_message(successful: Dict[str, List[Dict]]) -> SubscriptionResultMessage:
    """Helper to create successful subscription message."""
    return SubscriptionResultMessage(
        type="subscription_result",  # Явно указываем type
        successful_subscriptions=successful,
        message="All subscriptions successful"
    )


def create_subscription_partial_message(
    successful: Dict[str, List[Dict]],
    failed: List[Dict]
) -> SubscriptionResultMessage:
    """Helper to create partial subscription success message."""
    return SubscriptionResultMessage(
        type="subscription_result",  # Явно указываем type
        successful_subscriptions=successful,
        failed_subscriptions=failed,
        message="Some subscriptions failed"
    )

