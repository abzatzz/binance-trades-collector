"""
Integrity Management Package
============================

Components for data integrity checking and recovery:
- RecoveryState: Shared state for coordination
- IntegrityChecker: SQL checks for gaps/duplicates
- ArchiveRecovery: Download and insert from Binance archives
- IntegrityManager: Main coordinator

File: src/core/integrity/__init__.py
"""

from .recovery_state import RecoveryState, RecoveryStatus, SymbolRecoveryInfo
from .integrity_checker import IntegrityChecker, Problem, ProblemType, RecoveryStrategy
from .archive_recovery import ArchiveRecovery, RecoveryResult
from .integrity_manager import IntegrityManager

__all__ = [
    'RecoveryState',
    'RecoveryStatus',
    'SymbolRecoveryInfo',
    'IntegrityChecker',
    'Problem',
    'ProblemType',
    'RecoveryStrategy',
    'ArchiveRecovery',
    'RecoveryResult',
    'IntegrityManager',
]