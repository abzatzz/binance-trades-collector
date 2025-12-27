"""
RecoveryState - Shared State for Integrity Recovery
====================================================

Manages state and events for coordination between IntegrityManager
and TickerProcessors during integrity checks and recovery operations.

File: src/core/integrity/recovery_state.py
"""

import asyncio
from typing import Dict, Optional
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from loggerino import loggerino

logger = loggerino.get('recovery_state')


class RecoveryStatus(Enum):
    """Status of recovery process for a symbol."""
    UNKNOWN = "unknown"
    QUEUED = "queued"
    CHECKING = "checking"
    ARCHIVE_RECOVERY = "archive_recovery"
    READY = "ready"
    ERROR = "error"


@dataclass
class SymbolRecoveryInfo:
    """Information about recovery status for a symbol."""
    status: RecoveryStatus = RecoveryStatus.UNKNOWN
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    trades_recovered: int = 0
    days_processed: int = 0


class RecoveryState:
    """
    Shared state for recovery coordination.

    Thread-safe state management for:
    - Tracking which symbols are being checked/recovered
    - Signaling completion to waiting TickerProcessors
    - Storing recovery results and errors
    """

    def __init__(self):
        self._info: Dict[str, SymbolRecoveryInfo] = {}
        self._events: Dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        logger.info("RecoveryState initialized")

    def _get_info(self, symbol: str) -> SymbolRecoveryInfo:
        """Get or create info for symbol."""
        if symbol not in self._info:
            self._info[symbol] = SymbolRecoveryInfo()
        return self._info[symbol]

    def _get_event(self, symbol: str) -> asyncio.Event:
        """Get or create event for symbol."""
        if symbol not in self._events:
            self._events[symbol] = asyncio.Event()
        return self._events[symbol]

    async def set_status(self, symbol: str, status: RecoveryStatus):
        """Set status for symbol."""
        async with self._lock:
            info = self._get_info(symbol)
            info.status = status

            if status == RecoveryStatus.CHECKING:
                info.started_at = datetime.now(timezone.utc)
                info.error = None

            logger.debug(f"Recovery status for {symbol}: {status.value}")

    async def set_ready(self, symbol: str, trades_recovered: int = 0, days_processed: int = 0):
        """Mark symbol as ready - triggers waiting TickerProcessor."""
        async with self._lock:
            info = self._get_info(symbol)
            info.status = RecoveryStatus.READY
            info.completed_at = datetime.now(timezone.utc)
            info.trades_recovered = trades_recovered
            info.days_processed = days_processed
            info.error = None

            # Signal waiting ticker
            self._get_event(symbol).set()

            logger.info(f"Recovery ready for {symbol}: {trades_recovered:,} trades, {days_processed} days")

    async def set_error(self, symbol: str, error: str):
        """Mark symbol as error - triggers waiting TickerProcessor with error."""
        async with self._lock:
            info = self._get_info(symbol)
            info.status = RecoveryStatus.ERROR
            info.completed_at = datetime.now(timezone.utc)
            info.error = error

            # Signal waiting ticker (it will see error status)
            self._get_event(symbol).set()

            logger.error(f"Recovery error for {symbol}: {error}")

    def get_status(self, symbol: str) -> RecoveryStatus:
        """Get current status for symbol."""
        if symbol in self._info:
            return self._info[symbol].status
        return RecoveryStatus.UNKNOWN

    def get_info(self, symbol: str) -> SymbolRecoveryInfo:
        """Get full info for symbol."""
        return self._get_info(symbol)

    def get_error(self, symbol: str) -> Optional[str]:
        """Get error message if any."""
        if symbol in self._info:
            return self._info[symbol].error
        return None

    def is_busy(self, symbol: str) -> bool:
        """Check if symbol is currently being checked or recovered."""
        status = self.get_status(symbol)
        return status in (
            RecoveryStatus.QUEUED,
            RecoveryStatus.CHECKING,
            RecoveryStatus.ARCHIVE_RECOVERY
        )

    async def wait_for_ready(self, symbol: str, timeout: Optional[float] = None) -> bool:
        """
        Wait until symbol is ready or error.

        Args:
            symbol: Symbol to wait for
            timeout: Maximum seconds to wait (None = forever)

        Returns:
            True if ready, False if error or timeout
        """
        event = self._get_event(symbol)

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
            return self.get_status(symbol) == RecoveryStatus.READY
        except asyncio.TimeoutError:
            logger.warning(f"Timeout waiting for recovery: {symbol}")
            return False

    async def reset(self, symbol: str):
        """Reset state for symbol (for re-checking)."""
        async with self._lock:
            if symbol in self._info:
                self._info[symbol] = SymbolRecoveryInfo()
            if symbol in self._events:
                self._events[symbol].clear()

            logger.debug(f"Recovery state reset for {symbol}")

    def get_all_statuses(self) -> Dict[str, RecoveryStatus]:
        """Get statuses for all known symbols."""
        return {symbol: info.status for symbol, info in self._info.items()}

    def get_busy_symbols(self) -> list:
        """Get list of symbols currently being processed."""
        return [symbol for symbol, info in self._info.items() if self.is_busy(symbol)]

    def get_summary(self) -> dict:
        """Get summary statistics."""
        statuses = list(self._info.values())
        return {
            'total': len(statuses),
            'ready': sum(1 for s in statuses if s.status == RecoveryStatus.READY),
            'checking': sum(1 for s in statuses if s.status == RecoveryStatus.CHECKING),
            'archive_recovery': sum(1 for s in statuses if s.status == RecoveryStatus.ARCHIVE_RECOVERY),
            'error': sum(1 for s in statuses if s.status == RecoveryStatus.ERROR),
            'queued': sum(1 for s in statuses if s.status == RecoveryStatus.QUEUED),
        }
