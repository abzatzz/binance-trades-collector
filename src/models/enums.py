"""
Enumerations for Binance API Data
=================================

Collection of enums used throughout the Data Provider system
for type-safe handling of Binance API responses and system states.

File: src/models/enums.py
"""

from enum import Enum, unique, auto


@unique
class BinanceTickerStatus(Enum):
    """
    Binance futures ticker status enumeration.

    Based on Binance API documentation for futures exchangeInfo.
    """
    PENDING_TRADING = 1
    TRADING = 2
    PRE_DELIVERING = 3
    DELIVERING = 4
    DELIVERED = 5
    PRE_SETTLE = 6
    SETTLING = 7
    CLOSE = 8

    @classmethod
    def from_string(cls, status_str: str) -> 'BinanceTickerStatus':
        """
        Convert string status from Binance API to enum.

        Args:
            status_str: Status string from Binance API

        Returns:
            BinanceTickerStatus enum value

        Raises:
            ValueError: If status string is not recognized
        """
        status_mapping = {
            'PENDING_TRADING': cls.PENDING_TRADING,
            'TRADING': cls.TRADING,
            'PRE_DELIVERING': cls.PRE_DELIVERING,
            'DELIVERING': cls.DELIVERING,
            'DELIVERED': cls.DELIVERED,
            'PRE_SETTLE': cls.PRE_SETTLE,
            'SETTLING': cls.SETTLING,
            'CLOSE': cls.CLOSE
        }

        if status_str not in status_mapping:
            raise ValueError(f"Unknown ticker status: {status_str}")

        return status_mapping[status_str]

    def is_tradeable(self) -> bool:
        """Check if ticker is in tradeable status."""
        return self == BinanceTickerStatus.TRADING


@unique
class ProcessorState(Enum):
    """
    TickerProcessor state enumeration.

    Represents the lifecycle states of ticker processing with auto-generated values.
    """
    INITIALIZING = auto()
    WAITING_INTEGRITY_CHECK = auto()  # NEW
    ARCHIVE_RECOVERY = auto()
    DOWNLOADING_HISTORICAL = auto()
    STARTING_WEBSOCKET = auto()
    RUNNING = auto()
    PAUSED = auto()
    STOPPING = auto()
    STOPPED = auto()
    ERROR = auto()

    def is_active(self) -> bool:
        """Check if processor is in active processing state."""
        return self in (ProcessorState.WAITING_INTEGRITY_CHECK,
                        ProcessorState.ARCHIVE_RECOVERY,
                        ProcessorState.DOWNLOADING_HISTORICAL,
                        ProcessorState.STARTING_WEBSOCKET,
                        ProcessorState.RUNNING)

    def is_terminal(self) -> bool:
        """Check if processor is in terminal (finished) state."""
        return self in (ProcessorState.STOPPED, ProcessorState.ERROR)

    def can_transition_to(self, target_state: 'ProcessorState') -> bool:
        """
        Check if transition to target state is valid.

        Args:
            target_state: Target state to transition to

        Returns:
            True if transition is allowed
        """
        valid_transitions = {
            ProcessorState.INITIALIZING: [ProcessorState.WAITING_INTEGRITY_CHECK, ProcessorState.DOWNLOADING_HISTORICAL, ProcessorState.ERROR, ProcessorState.STOPPING],
            ProcessorState.WAITING_INTEGRITY_CHECK: [ProcessorState.ARCHIVE_RECOVERY, ProcessorState.DOWNLOADING_HISTORICAL, ProcessorState.ERROR,
                                                     ProcessorState.STOPPING],
            ProcessorState.ARCHIVE_RECOVERY: [ProcessorState.DOWNLOADING_HISTORICAL, ProcessorState.ERROR, ProcessorState.STOPPING],
            ProcessorState.DOWNLOADING_HISTORICAL: [ProcessorState.STARTING_WEBSOCKET, ProcessorState.ERROR, ProcessorState.STOPPING],
            ProcessorState.STARTING_WEBSOCKET: [ProcessorState.RUNNING, ProcessorState.ERROR, ProcessorState.STOPPING],
            ProcessorState.RUNNING: [ProcessorState.PAUSED, ProcessorState.STOPPING, ProcessorState.ERROR],
            ProcessorState.PAUSED: [ProcessorState.RUNNING, ProcessorState.STOPPING, ProcessorState.ERROR],
            ProcessorState.STOPPING: [ProcessorState.STOPPED, ProcessorState.ERROR],
            ProcessorState.STOPPED: [],  # Terminal state
            ProcessorState.ERROR: [ProcessorState.STOPPING]  # Can try to stop gracefully
        }

        return target_state in valid_transitions.get(self, [])