"""
Unit Tests for Integrity Components
===================================

Tests for RecoveryState, IntegrityChecker logic, and RecoveryStrategy.

File: tests/unit/test_integrity_unit.py
"""

# Initialize loggerino BEFORE importing project modules
from pathlib import Path
from loggerino import loggerino

# Configure loggerino for tests
logs_folder = Path('./test_logs/unit')
if not logs_folder.exists():
    logs_folder.mkdir(parents=True)

loggerino.configure(
    logs_dir=str(logs_folder),
    debug_in_console=False,
    buffer_size=100,
    flush_interval=5,
)

# Create required loggers
test_log_file = str(logs_folder / 'test_integrity_unit.log')
loggerino.create('recovery_state', test_log_file)
loggerino.create('integrity_checker', test_log_file)
loggerino.create('archive_recovery', test_log_file)
loggerino.create('integrity_manager', test_log_file)

# Now safe to import other modules
import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src" / "core" / "integrity"))

from recovery_state import RecoveryState, RecoveryStatus, SymbolRecoveryInfo
from integrity_checker import (
    IntegrityChecker, Problem, ProblemType, RecoveryStrategy
)



class TestRecoveryState:
    """Tests for RecoveryState."""

    @pytest.fixture
    def state(self):
        return RecoveryState()

    @pytest.mark.asyncio
    async def test_initial_status_is_unknown(self, state):
        """New symbols should have UNKNOWN status."""
        status = state.get_status("BTCUSDT")
        assert status == RecoveryStatus.UNKNOWN

    @pytest.mark.asyncio
    async def test_set_status(self, state):
        """Should set status correctly."""
        await state.set_status("BTCUSDT", RecoveryStatus.CHECKING)
        assert state.get_status("BTCUSDT") == RecoveryStatus.CHECKING

    @pytest.mark.asyncio
    async def test_set_ready_triggers_event(self, state):
        """set_ready should trigger waiting event."""
        # Start waiting task
        wait_result = []

        async def waiter():
            result = await state.wait_for_ready("BTCUSDT", timeout=5.0)
            wait_result.append(result)

        task = asyncio.create_task(waiter())

        # Give task time to start waiting
        await asyncio.sleep(0.1)

        # Set ready
        await state.set_ready("BTCUSDT", trades_recovered=1000, days_processed=5)

        # Wait for task to complete
        await task

        assert wait_result[0] is True
        assert state.get_status("BTCUSDT") == RecoveryStatus.READY

        info = state.get_info("BTCUSDT")
        assert info.trades_recovered == 1000
        assert info.days_processed == 5

    @pytest.mark.asyncio
    async def test_set_error_triggers_event_with_false(self, state):
        """set_error should trigger event but wait_for_ready returns False."""
        wait_result = []

        async def waiter():
            result = await state.wait_for_ready("BTCUSDT", timeout=5.0)
            wait_result.append(result)

        task = asyncio.create_task(waiter())
        await asyncio.sleep(0.1)

        await state.set_error("BTCUSDT", "Test error")
        await task

        assert wait_result[0] is False
        assert state.get_status("BTCUSDT") == RecoveryStatus.ERROR
        assert state.get_error("BTCUSDT") == "Test error"

    @pytest.mark.asyncio
    async def test_is_busy(self, state):
        """is_busy should return True for active states."""
        assert state.is_busy("BTCUSDT") is False

        await state.set_status("BTCUSDT", RecoveryStatus.QUEUED)
        assert state.is_busy("BTCUSDT") is True

        await state.set_status("BTCUSDT", RecoveryStatus.CHECKING)
        assert state.is_busy("BTCUSDT") is True

        await state.set_status("BTCUSDT", RecoveryStatus.ARCHIVE_RECOVERY)
        assert state.is_busy("BTCUSDT") is True

        await state.set_ready("BTCUSDT")
        assert state.is_busy("BTCUSDT") is False

    @pytest.mark.asyncio
    async def test_reset(self, state):
        """reset should clear state and event."""
        await state.set_ready("BTCUSDT", trades_recovered=100)

        await state.reset("BTCUSDT")

        assert state.get_status("BTCUSDT") == RecoveryStatus.UNKNOWN
        info = state.get_info("BTCUSDT")
        assert info.trades_recovered == 0

    @pytest.mark.asyncio
    async def test_get_summary(self, state):
        """get_summary should return correct counts."""
        await state.set_status("BTCUSDT", RecoveryStatus.CHECKING)
        await state.set_status("ETHUSDT", RecoveryStatus.ARCHIVE_RECOVERY)
        await state.set_ready("SOLUSDT")
        await state.set_error("XRPUSDT", "Error")

        summary = state.get_summary()

        assert summary['total'] == 4
        assert summary['checking'] == 1
        assert summary['archive_recovery'] == 1
        assert summary['ready'] == 1
        assert summary['error'] == 1

    @pytest.mark.asyncio
    async def test_wait_timeout(self, state):
        """wait_for_ready should return False on timeout."""
        result = await state.wait_for_ready("BTCUSDT", timeout=0.1)
        assert result is False


class TestRecoveryStrategy:
    """Tests for RecoveryStrategy determination."""

    @pytest.fixture
    def checker(self):
        mock_clickhouse = AsyncMock()
        return IntegrityChecker(mock_clickhouse)

    def test_strategy_gap_in_past_days(self, checker):
        """Gap in past days should require archive recovery."""
        now = datetime.now(timezone.utc)
        gap_time = now - timedelta(days=3)

        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=gap_time,
            count=1000000
        )

        strategy = checker.determine_strategy(problem)

        assert strategy.needs_archive is True
        assert strategy.delete_from.date() == gap_time.date()
        assert strategy.archive_from.date() == gap_time.date()

    def test_strategy_gap_today(self, checker):
        """Gap today should NOT require archive recovery."""
        now = datetime.now(timezone.utc)
        gap_time = now - timedelta(hours=2)  # 2 hours ago, same day

        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=gap_time,
            count=5000
        )

        strategy = checker.determine_strategy(problem)

        assert strategy.needs_archive is False
        assert strategy.delete_from == gap_time

    def test_strategy_duplicate_yesterday(self, checker):
        """Duplicate yesterday should require archive recovery."""
        now = datetime.now(timezone.utc)
        dup_time = now - timedelta(days=1, hours=5)

        problem = Problem(
            type=ProblemType.DUPLICATE,
            symbol="BTCUSDT",
            start_time=dup_time,
            count=100
        )

        strategy = checker.determine_strategy(problem)

        assert strategy.needs_archive is True

    def test_strategy_no_data(self, checker):
        """No data should require archive recovery from 30 days ago."""
        problem = Problem(
            type=ProblemType.NO_DATA,
            symbol="NEWUSDT"
        )

        strategy = checker.determine_strategy(problem)

        assert strategy.needs_archive is True

        now = datetime.now(timezone.utc)
        expected_start = (now - timedelta(days=30)).date()
        assert strategy.archive_from.date() == expected_start


class TestProblemDetection:
    """Tests for problem type detection logic."""

    def test_problem_type_priority(self):
        """Verify problem types have correct priority."""
        # Duplicates should be checked before gaps
        # because duplicates can mask gaps
        assert ProblemType.DUPLICATE.value == "duplicate"
        assert ProblemType.GAP.value == "gap"
        assert ProblemType.LAG.value == "lag"

    def test_problem_description(self):
        """Problem should have description."""
        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=datetime.now(timezone.utc),
            count=68000000,
            description="Gap at 2025-11-01, missing ~68,000,000 trades"
        )

        assert "68,000,000" in problem.description


class TestDateCalculations:
    """Tests for date boundary calculations."""

    @pytest.fixture
    def checker(self):
        mock_clickhouse = AsyncMock()
        return IntegrityChecker(mock_clickhouse)

    def test_delete_from_start_of_day_for_past_gap(self, checker):
        """For past gaps, delete_from should be start of that day."""
        # Gap at 15:30 on Nov 1
        gap_time = datetime(2025, 11, 1, 15, 30, 0, tzinfo=timezone.utc)

        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=gap_time
        )

        # Mock "today" as Dec 22 - use module name directly since we imported it that way
        with patch('integrity_checker.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2025, 12, 22, 14, 0, 0, tzinfo=timezone.utc)
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            strategy = checker.determine_strategy(problem)

        # Should delete from start of Nov 1, not 15:30
        assert strategy.delete_from.hour == 0
        assert strategy.delete_from.minute == 0
        assert strategy.delete_from.day == 1
        assert strategy.delete_from.month == 11

    def test_archive_to_is_yesterday(self, checker):
        """archive_to should be yesterday (today's archive not available)."""
        gap_time = datetime(2025, 11, 1, 15, 30, 0, tzinfo=timezone.utc)

        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=gap_time
        )

        now = datetime(2025, 12, 22, 14, 0, 0, tzinfo=timezone.utc)

        with patch('integrity_checker.datetime') as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

            strategy = checker.determine_strategy(problem)

        expected_to = (now - timedelta(days=1)).date()
        assert strategy.archive_to.date() == expected_to


if __name__ == "__main__":
    pytest.main([__file__, "-v"])