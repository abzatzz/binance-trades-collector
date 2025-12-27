"""
Tests for IntegrityManager
==========================

Tests for queue processing, coordination, and recovery flow.

File: tests/integration/test_integrity_manager.py
"""

# Initialize loggerino BEFORE importing project modules
from pathlib import Path
from loggerino import loggerino

# Configure loggerino for tests
logs_folder = Path('./test_logs/integration')
if not logs_folder.exists():
    logs_folder.mkdir(parents=True)

loggerino.configure(
    logs_dir=str(logs_folder),
    debug_in_console=False,
    buffer_size=100,
    flush_interval=5,
)

# Create required loggers for integrity modules
test_log_file = str(logs_folder / 'test_integrity_manager.log')
loggerino.create('recovery_state', test_log_file)
loggerino.create('integrity_checker', test_log_file)
loggerino.create('archive_recovery', test_log_file)
loggerino.create('integrity_manager', test_log_file)

# Now import test dependencies
import pytest
import asyncio
import time
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

# Add integrity module directly to path (bypasses src.core.__init__.py)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src" / "core" / "integrity"))

# Import directly from module files
from recovery_state import RecoveryState, RecoveryStatus
from integrity_checker import Problem, ProblemType, RecoveryStrategy
from integrity_manager import IntegrityManager


class TestIntegrityManagerQueue:
    """Tests for queue-based processing."""

    @pytest.fixture
    def manager(self):
        mock_clickhouse = AsyncMock()
        recovery_state = RecoveryState()

        mgr = IntegrityManager(
            clickhouse_client=mock_clickhouse,
            recovery_state=recovery_state,
            telegram_notifier=None
        )

        return mgr

    @pytest.mark.asyncio
    async def test_request_check_adds_to_queue(self, manager):
        """request_check should add symbol to queue."""
        await manager.request_check("BTCUSDT", priority=10)

        assert manager.check_queue.qsize() == 1
        assert manager.recovery_state.get_status("BTCUSDT") == RecoveryStatus.QUEUED

    @pytest.mark.asyncio
    async def test_queue_processes_in_order(self, manager):
        """Queue should process requests in order."""
        # Add multiple symbols
        await manager.request_check("BTCUSDT")
        await manager.request_check("ETHUSDT")
        await manager.request_check("SOLUSDT")

        assert manager.check_queue.qsize() == 3

        # Get items from queue
        item1 = await manager.check_queue.get()
        item2 = await manager.check_queue.get()
        item3 = await manager.check_queue.get()

        assert item1.symbol == "BTCUSDT"
        assert item2.symbol == "ETHUSDT"
        assert item3.symbol == "SOLUSDT"

    @pytest.mark.asyncio
    async def test_worker_processes_queue(self, manager):
        """Worker should process items from queue."""
        # Mock checker to return no problems
        manager.checker.check = AsyncMock(return_value=None)

        # Add symbol to queue
        await manager.request_check("BTCUSDT")

        # Start manager (starts worker)
        await manager.start()

        # Wait for processing
        await asyncio.sleep(0.5)

        # Check result
        status = manager.recovery_state.get_status("BTCUSDT")
        assert status == RecoveryStatus.READY

        # Cleanup
        await manager.stop()

    @pytest.mark.asyncio
    async def test_worker_handles_problem(self, manager):
        """Worker should handle problems and trigger recovery."""
        # Mock checker to return a problem
        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=datetime.now(timezone.utc) - timedelta(hours=2),
            count=5000
        )
        manager.checker.check = AsyncMock(return_value=problem)

        # Mock strategy (no archive needed - gap today)
        manager.checker.determine_strategy = MagicMock(return_value=RecoveryStrategy(
            delete_from=datetime.now(timezone.utc) - timedelta(hours=2),
            needs_archive=False
        ))

        # Mock delete
        manager._delete_trades = AsyncMock(return_value=1000)

        # Add symbol
        await manager.request_check("BTCUSDT")

        # Start and process
        await manager.start()
        await asyncio.sleep(0.5)

        # Should be ready (no archive needed)
        status = manager.recovery_state.get_status("BTCUSDT")
        assert status == RecoveryStatus.READY

        await manager.stop()

    @pytest.mark.asyncio
    async def test_worker_triggers_archive_recovery(self, manager):
        """Worker should trigger archive recovery when needed."""
        # Mock checker to return problem needing archive
        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=datetime.now(timezone.utc) - timedelta(days=5),
            count=5000000
        )
        manager.checker.check = AsyncMock(return_value=problem)

        # Mock strategy (archive needed)
        from datetime import date
        manager.checker.determine_strategy = MagicMock(return_value=RecoveryStrategy(
            delete_from=datetime.now(timezone.utc) - timedelta(days=5),
            needs_archive=True,
            archive_from=date.today() - timedelta(days=5),
            archive_to=date.today() - timedelta(days=1)
        ))

        # Mock delete and archive recovery
        manager._delete_trades = AsyncMock(return_value=5000000)

        from archive_recovery import RecoveryResult
        manager.archive_recovery.recover = AsyncMock(return_value=RecoveryResult(
            success=True,
            days_processed=5,
            trades_inserted=5000000,
            duration_seconds=60.0
        ))

        # Add symbol
        await manager.request_check("BTCUSDT")

        # Start and process
        await manager.start()
        await asyncio.sleep(0.5)

        # Should be ready
        status = manager.recovery_state.get_status("BTCUSDT")
        assert status == RecoveryStatus.READY

        # Archive recovery should have been called
        manager.archive_recovery.recover.assert_called_once()

        await manager.stop()


class TestIntegrityManagerError:
    """Tests for error handling."""

    @pytest.fixture
    def manager(self):
        mock_clickhouse = AsyncMock()
        recovery_state = RecoveryState()

        return IntegrityManager(
            clickhouse_client=mock_clickhouse,
            recovery_state=recovery_state,
            telegram_notifier=None
        )

    @pytest.mark.asyncio
    async def test_check_error_sets_error_status(self, manager):
        """Error during check should set ERROR status."""
        # Mock checker to raise exception
        manager.checker.check = AsyncMock(side_effect=Exception("DB connection failed"))

        # Add symbol
        await manager.request_check("BTCUSDT")

        # Start and process
        await manager.start()
        await asyncio.sleep(0.5)

        # Should be in error state
        status = manager.recovery_state.get_status("BTCUSDT")
        assert status == RecoveryStatus.ERROR

        error = manager.recovery_state.get_error("BTCUSDT")
        assert "DB connection failed" in error

        await manager.stop()

    @pytest.mark.asyncio
    async def test_archive_recovery_failure_sets_error(self, manager):
        """Archive recovery failure should set ERROR status."""
        # Mock problem needing archive
        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=datetime.now(timezone.utc) - timedelta(days=5),
            count=5000000
        )
        manager.checker.check = AsyncMock(return_value=problem)

        from datetime import date
        manager.checker.determine_strategy = MagicMock(return_value=RecoveryStrategy(
            delete_from=datetime.now(timezone.utc) - timedelta(days=5),
            needs_archive=True,
            archive_from=date.today() - timedelta(days=5),
            archive_to=date.today() - timedelta(days=1)
        ))

        manager._delete_trades = AsyncMock(return_value=5000000)

        # Mock archive recovery failure
        from archive_recovery import RecoveryResult
        manager.archive_recovery.recover = AsyncMock(return_value=RecoveryResult(
            success=False,
            days_processed=2,
            trades_inserted=1000000,
            duration_seconds=30.0,
            failed_date=date.today() - timedelta(days=3),
            error="Checksum validation failed"
        ))

        # Process
        await manager.request_check("BTCUSDT")
        await manager.start()
        await asyncio.sleep(0.5)

        # Should be in error state
        status = manager.recovery_state.get_status("BTCUSDT")
        assert status == RecoveryStatus.ERROR

        await manager.stop()


class TestIntegrityManagerCoordination:
    """Tests for coordination with TickerProcessor."""

    @pytest.fixture
    def manager(self):
        mock_clickhouse = AsyncMock()
        recovery_state = RecoveryState()

        return IntegrityManager(
            clickhouse_client=mock_clickhouse,
            recovery_state=recovery_state,
            telegram_notifier=None
        )

    @pytest.mark.asyncio
    async def test_ticker_can_wait_for_completion(self, manager):
        """Simulated ticker should be able to wait for completion."""
        # Mock no problems
        manager.checker.check = AsyncMock(return_value=None)

        # Simulate ticker waiting
        wait_completed = []

        async def simulated_ticker():
            # Request check
            await manager.request_check("BTCUSDT")

            # Wait for ready
            result = await manager.recovery_state.wait_for_ready("BTCUSDT", timeout=5.0)
            wait_completed.append(result)

        # Start manager
        await manager.start()

        # Run simulated ticker
        await simulated_ticker()

        assert wait_completed[0] is True

        await manager.stop()

    @pytest.mark.asyncio
    async def test_multiple_tickers_processed_sequentially(self, manager):
        """Multiple tickers should be processed one at a time."""
        process_order = []

        async def mock_check(symbol):
            process_order.append(symbol)
            await asyncio.sleep(0.1)  # Simulate check time
            return None

        manager.checker.check = mock_check

        # Add multiple symbols
        await manager.request_check("BTCUSDT")
        await manager.request_check("ETHUSDT")
        await manager.request_check("SOLUSDT")

        # Start and wait
        await manager.start()
        await asyncio.sleep(0.5)

        # Should be processed in order
        assert process_order == ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        await manager.stop()

    @pytest.mark.asyncio
    async def test_status_transitions_during_processing(self, manager):
        """Status should transition correctly during processing."""
        status_history = []

        async def mock_check(symbol):
            # Record status at check time
            status_history.append(manager.recovery_state.get_status(symbol))
            await asyncio.sleep(0.1)
            return None

        manager.checker.check = mock_check

        # Add symbol
        await manager.request_check("BTCUSDT")

        # Initial status should be QUEUED
        assert manager.recovery_state.get_status("BTCUSDT") == RecoveryStatus.QUEUED

        # Start processing
        await manager.start()
        await asyncio.sleep(0.3)

        # Final status should be READY
        assert manager.recovery_state.get_status("BTCUSDT") == RecoveryStatus.READY

        # During check, status should have been CHECKING
        assert RecoveryStatus.CHECKING in status_history

        await manager.stop()


class TestIntegrityManagerStatistics:
    """Tests for statistics tracking."""

    @pytest.fixture
    def manager(self):
        mock_clickhouse = AsyncMock()
        recovery_state = RecoveryState()

        return IntegrityManager(
            clickhouse_client=mock_clickhouse,
            recovery_state=recovery_state,
            telegram_notifier=None
        )

    @pytest.mark.asyncio
    async def test_statistics_tracked(self, manager):
        """Manager should track statistics."""
        manager.checker.check = AsyncMock(return_value=None)

        # Process some symbols
        for symbol in ["BTCUSDT", "ETHUSDT", "SOLUSDT"]:
            await manager.request_check(symbol)

        await manager.start()
        await asyncio.sleep(0.5)

        stats = manager.get_statistics()

        assert stats['symbols_checked'] == 3
        assert stats['running'] is True

        await manager.stop()

    @pytest.mark.asyncio
    async def test_recovery_statistics(self, manager):
        """Recovery statistics should be tracked."""
        # Mock problem and recovery
        problem = Problem(
            type=ProblemType.GAP,
            symbol="BTCUSDT",
            start_time=datetime.now(timezone.utc) - timedelta(days=2),
            count=1000000
        )
        manager.checker.check = AsyncMock(return_value=problem)

        from datetime import date
        manager.checker.determine_strategy = MagicMock(return_value=RecoveryStrategy(
            delete_from=datetime.now(timezone.utc) - timedelta(days=2),
            needs_archive=True,
            archive_from=date.today() - timedelta(days=2),
            archive_to=date.today() - timedelta(days=1)
        ))

        manager._delete_trades = AsyncMock(return_value=1000000)

        from archive_recovery import RecoveryResult
        manager.archive_recovery.recover = AsyncMock(return_value=RecoveryResult(
            success=True,
            days_processed=2,
            trades_inserted=1000000,
            duration_seconds=30.0
        ))

        await manager.request_check("BTCUSDT")
        await manager.start()
        await asyncio.sleep(0.5)

        stats = manager.get_statistics()

        assert stats['symbols_recovered'] == 1
        assert stats['total_trades_recovered'] == 1000000

        await manager.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])