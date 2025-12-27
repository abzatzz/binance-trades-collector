"""
Binance API Error Models
=======================

Exception classes for handling Binance WebSocket and API errors with comprehensive
error classification and recovery strategies.

File: src/models/binance_errors.py
"""

from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class BinanceErrorSeverity(Enum):
    """Severity levels for Binance errors"""
    LOW = "low"  # Temporary issues, automatic retry
    MEDIUM = "medium"  # Requires reconnection
    HIGH = "high"  # Requires manual intervention
    CRITICAL = "critical"  # System-level issues


@dataclass
class BinanceErrorData:
    """Structured representation of Binance error data"""
    error_code: Optional[int]
    error_message: str
    event_time: Optional[int]
    raw_data: Dict[str, Any]
    severity: BinanceErrorSeverity
    is_recoverable: bool
    retry_after: Optional[int]  # Seconds to wait before retry


class BinanceClientError(Exception):
    """
    Exception for Binance WebSocket and API client errors.

    Provides structured error handling with recovery strategies
    and automatic classification of error severity.
    """

    def __init__(self, error_data: Dict[str, Any]):
        """
        Initialize BinanceClientError with error data from Binance API.

        Args:
            error_data: Dictionary containing error information from Binance
        """
        self.error_data = self._parse_error_data(error_data)

        super().__init__(
            f"Binance {self.error_data.severity.value} error "
            f"{self.error_data.error_code}: {self.error_data.error_message}"
        )

    def _parse_error_data(self, error_data: Dict[str, Any]) -> BinanceErrorData:
        """
        Parse and classify error data from Binance.

        Args:
            error_data: Raw error data from Binance API

        Returns:
            Structured BinanceErrorData
        """
        error_code = error_data.get('c') or error_data.get('code')
        error_message = (
                error_data.get('m') or
                error_data.get('msg') or
                error_data.get('message') or
                'Unknown Binance error'
        )
        event_time = error_data.get('E') or error_data.get('timestamp')

        # Classify error severity and recoverability
        severity, is_recoverable, retry_after = self._classify_error(error_code, error_message)

        return BinanceErrorData(
            error_code=error_code,
            error_message=error_message,
            event_time=event_time,
            raw_data=error_data,
            severity=severity,
            is_recoverable=is_recoverable,
            retry_after=retry_after
        )

    def _classify_error(self, error_code: Optional[int], error_message: str) -> tuple[BinanceErrorSeverity, bool, Optional[int]]:
        """
        Classify error severity and recovery strategy.

        Args:
            error_code: Binance error code
            error_message: Error message text

        Returns:
            Tuple of (severity, is_recoverable, retry_after_seconds)
        """
        if not error_code:
            return BinanceErrorSeverity.MEDIUM, True, 5

        # WebSocket connection errors (recoverable)
        websocket_errors = {
            1000: (BinanceErrorSeverity.LOW, True, 1),  # Normal closure
            1001: (BinanceErrorSeverity.LOW, True, 1),  # Going away
            1002: (BinanceErrorSeverity.MEDIUM, True, 5),  # Protocol error
            1006: (BinanceErrorSeverity.MEDIUM, True, 5),  # Abnormal closure
            1011: (BinanceErrorSeverity.MEDIUM, True, 10),  # Server error
            1012: (BinanceErrorSeverity.MEDIUM, True, 10),  # Service restart
        }

        # API rate limiting errors
        rate_limit_errors = {
            429: (BinanceErrorSeverity.MEDIUM, True, 60),  # Rate limit exceeded
            418: (BinanceErrorSeverity.HIGH, True, 300),  # IP banned
            1003: (BinanceErrorSeverity.HIGH, True, 300),  # Too many requests
        }

        # Authentication errors (not recoverable without intervention)
        auth_errors = {
            401: (BinanceErrorSeverity.HIGH, False, None),  # Unauthorized
            403: (BinanceErrorSeverity.HIGH, False, None),  # Forbidden
            -2014: (BinanceErrorSeverity.HIGH, False, None),  # API key format invalid
            -2015: (BinanceErrorSeverity.HIGH, False, None),  # Invalid API key
        }

        # Server errors (temporarily recoverable)
        server_errors = {
            500: (BinanceErrorSeverity.MEDIUM, True, 30),  # Internal server error
            502: (BinanceErrorSeverity.MEDIUM, True, 30),  # Bad gateway
            503: (BinanceErrorSeverity.MEDIUM, True, 60),  # Service unavailable
            504: (BinanceErrorSeverity.MEDIUM, True, 30),  # Gateway timeout
        }

        # Check error code classifications
        for error_map in [websocket_errors, rate_limit_errors, auth_errors, server_errors]:
            if error_code in error_map:
                return error_map[error_code]

        # Check message content for additional classification
        error_message_lower = error_message.lower()

        if any(keyword in error_message_lower for keyword in ['rate limit', 'too many', 'banned']):
            return BinanceErrorSeverity.HIGH, True, 300

        if any(keyword in error_message_lower for keyword in ['unauthorized', 'invalid key', 'permission']):
            return BinanceErrorSeverity.HIGH, False, None

        if any(keyword in error_message_lower for keyword in ['server error', 'maintenance', 'unavailable']):
            return BinanceErrorSeverity.MEDIUM, True, 60

        # Default classification for unknown errors
        return BinanceErrorSeverity.MEDIUM, True, 10

    def is_recoverable(self) -> bool:
        """Check if error is recoverable through automatic retry/reconnection."""
        return self.error_data.is_recoverable

    def should_retry(self) -> bool:
        """Check if operation should be automatically retried."""
        return self.error_data.is_recoverable

    def get_retry_delay(self) -> int:
        """Get recommended retry delay in seconds."""
        return self.error_data.retry_after or 5

    def is_rate_limit_error(self) -> bool:
        """Check if error is related to rate limiting."""
        if not self.error_data.error_code:
            return False

        rate_limit_codes = [429, 418, 1003]
        return self.error_data.error_code in rate_limit_codes

    def is_authentication_error(self) -> bool:
        """Check if error is related to authentication/authorization."""
        if not self.error_data.error_code:
            return False

        auth_codes = [401, 403, -2014, -2015]
        return self.error_data.error_code in auth_codes

    def is_server_error(self) -> bool:
        """Check if error is a server-side issue."""
        if not self.error_data.error_code:
            return False

        server_codes = [500, 502, 503, 504]
        return self.error_data.error_code in server_codes

    def is_websocket_error(self) -> bool:
        """Check if error is a WebSocket connection issue."""
        if not self.error_data.error_code:
            return False

        websocket_codes = [1000, 1001, 1002, 1006, 1011, 1012]
        return self.error_data.error_code in websocket_codes

    def get_error_context(self) -> Dict[str, Any]:
        """
        Get comprehensive error context for logging and debugging.

        Returns:
            Dictionary with error details and classification
        """
        return {
            'error_code': self.error_data.error_code,
            'error_message': self.error_data.error_message,
            'event_time': self.error_data.event_time,
            'severity': self.error_data.severity.value,
            'is_recoverable': self.error_data.is_recoverable,
            'retry_after': self.error_data.retry_after,
            'is_rate_limit': self.is_rate_limit_error(),
            'is_auth_error': self.is_authentication_error(),
            'is_server_error': self.is_server_error(),
            'is_websocket_error': self.is_websocket_error(),
            'raw_data': self.error_data.raw_data
        }


class BinanceAPIError(BinanceClientError):
    """Specific exception for Binance REST API errors."""

    def __init__(self, error_data: Dict[str, Any], endpoint: str = None):
        super().__init__(error_data)
        self.endpoint = endpoint

    def __str__(self) -> str:
        base_message = super().__str__()
        if self.endpoint:
            return f"{base_message} (endpoint: {self.endpoint})"
        return base_message


class BinanceWebSocketError(BinanceClientError):
    """Specific exception for Binance WebSocket errors."""

    def __init__(self, error_data: Dict[str, Any], symbol: str = None):
        super().__init__(error_data)
        self.symbol = symbol

    def __str__(self) -> str:
        base_message = super().__str__()
        if self.symbol:
            return f"{base_message} (symbol: {self.symbol})"
        return base_message


# Convenience functions for error handling
def handle_binance_error(error_data: Dict[str, Any], context: str = "API") -> BinanceClientError:
    """
    Factory function to create appropriate Binance error based on context.

    Args:
        error_data: Error data from Binance
        context: Context of the error (API, WebSocket, etc.)

    Returns:
        Appropriate BinanceClientError subclass
    """
    if context.lower() == "websocket":
        return BinanceWebSocketError(error_data)
    elif context.lower() == "api":
        return BinanceAPIError(error_data)
    else:
        return BinanceClientError(error_data)


def is_recoverable_error(error: Exception) -> bool:
    """
    Check if any exception is a recoverable Binance error.

    Args:
        error: Exception to check

    Returns:
        True if error is recoverable
    """
    if isinstance(error, BinanceClientError):
        return error.is_recoverable()

    # Other exception types are generally not recoverable
    return False


def get_retry_delay(error: Exception) -> int:
    """
    Get retry delay for any exception.

    Args:
        error: Exception to get retry delay for

    Returns:
        Retry delay in seconds
    """
    if isinstance(error, BinanceClientError):
        return error.get_retry_delay()

    # Default retry delay for non-Binance errors
    return 5
