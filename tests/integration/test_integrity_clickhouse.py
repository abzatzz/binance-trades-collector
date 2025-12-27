"""
Integration Tests for IntegrityChecker with ClickHouse
======================================================

Tests SQL queries for gap/duplicate detection with real ClickHouse.
Measures query performance.

File: tests/integration/test_integrity_clickhouse.py
"""

# Initialize loggerino BEFORE importing project modules
import os
from pathlib import Path
from loggerino import loggerino
from dotenv import load_dotenv

# Load .env file
load_dotenv()

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

# Create ALL required loggers
test_log_file = str(logs_folder / 'test_integrity_clickhouse.log')
loggerino.create('recovery_state', test_log_file)
loggerino.create('integrity_checker', test_log_file)
loggerino.create('archive_recovery', test_log_file)
loggerino.create('integrity_manager', test_log_file)
loggerino.create('clickhouse_manager', test_log_file)
loggerino.create('weight_coordinator', test_log_file)
loggerino.create('ticker_processor', test_log_file)
loggerino.create('historical_downloader', test_log_file)
loggerino.create('websocket_updater', test_log_file)
loggerino.create('global_trades_updater', test_log_file)
loggerino.create('rest_api', test_log_file)
loggerino.create('central_websocket_manager', test_log_file)
loggerino.create('telegram_notifier', test_log_file)
loggerino.create('shared', test_log_file)

# Now import test dependencies
import pytest
import pytest_asyncio
import asyncio
import time
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
from unittest.mock import AsyncMock

# Add paths
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src" / "core" / "integrity"))

# Import directly from module files
from integrity_checker import IntegrityChecker, ProblemType


class MockClickHouseClient:
    """Mock ClickHouse client that simulates real query behavior."""

    def __init__(self, data: List[Tuple] = None, delay: float = 0.0):
        self.data = data or []
        self.delay = delay
        self.queries_executed = []

    async def execute(self, query: str):
        self.queries_executed.append(query)
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        return self.data


class ClickHouseAdapter:
    """Adapter to make ClickHouseManager compatible with IntegrityChecker."""

    def __init__(self, manager):
        self.manager = manager

    async def execute(self, query: str):
        """Execute query and return results as list of tuples."""
        await self.manager.ensure_connected()
        result = await self.manager.client.fetch(query)
        # aiochclient returns list of Row objects, convert to tuples
        return [tuple(row.values()) for row in result]


class TestIntegrityCheckerWithMock:
    """Tests with mock ClickHouse (fast, no external deps)."""

    @pytest.mark.asyncio
    async def test_no_gap_detected(self):
        """When count matches expected, no gap should be detected."""
        mock_client = MockClickHouseClient(
            data=[(1000, 1, 1000)]
        )

        checker = IntegrityChecker(mock_client)
        problem = await checker._check_gaps("BTCUSDT")

        assert problem is None

    @pytest.mark.asyncio
    async def test_gap_detected(self):
        """When count doesn't match expected, gap should be detected."""
        mock_client = MockClickHouseClient(
            data=[(900, 1, 1000)]
        )

        checker = IntegrityChecker(mock_client)

        async def mock_find_gap(*args):
            return {
                'start_time': datetime(2025, 11, 1, 10, 0, 0, tzinfo=timezone.utc),
                'start_id': 501,
                'end_id': 600
            }

        checker._find_first_gap = mock_find_gap

        problem = await checker._check_gaps("BTCUSDT")

        assert problem is not None
        assert problem.type == ProblemType.GAP
        assert problem.count == 100

    @pytest.mark.asyncio
    async def test_no_duplicates(self):
        """When no duplicates exist, should return None."""
        mock_client = MockClickHouseClient(data=[])

        checker = IntegrityChecker(mock_client)
        problem = await checker._check_duplicates("BTCUSDT")

        assert problem is None

    @pytest.mark.asyncio
    async def test_duplicate_detected(self):
        """When duplicates exist, should return problem."""
        mock_client = MockClickHouseClient(
            data=[(12345, 2, 1730000000000)]
        )

        checker = IntegrityChecker(mock_client)

        call_count = [0]

        async def execute_with_count(query):
            call_count[0] += 1
            if call_count[0] == 1:
                return [(12345, 2, 1730000000000)]
            else:
                return [(50,)]

        mock_client.execute = execute_with_count

        problem = await checker._check_duplicates("BTCUSDT")

        assert problem is not None
        assert problem.type == ProblemType.DUPLICATE
        assert problem.start_id == 12345

    @pytest.mark.asyncio
    async def test_no_lag_detected(self):
        """When last trade is recent, no lag should be detected."""
        recent_ts = int((datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp() * 1000)

        mock_client = MockClickHouseClient(
            data=[(recent_ts, 999999)]
        )

        checker = IntegrityChecker(mock_client)
        problem = await checker._check_lag("BTCUSDT")

        assert problem is None

    @pytest.mark.asyncio
    async def test_severe_lag_detected(self):
        """When last trade is very old, lag should be detected."""
        old_ts = int((datetime.now(timezone.utc) - timedelta(hours=25)).timestamp() * 1000)

        mock_client = MockClickHouseClient(
            data=[(old_ts, 999999)]
        )

        checker = IntegrityChecker(mock_client)
        problem = await checker._check_lag("BTCUSDT")

        assert problem is not None
        assert problem.type == ProblemType.LAG

    @pytest.mark.asyncio
    async def test_no_data_detected(self):
        """When no data exists, should return NO_DATA problem."""
        mock_client = MockClickHouseClient(
            data=[(0, None, None)]
        )

        checker = IntegrityChecker(mock_client)
        problem = await checker._check_gaps("BTCUSDT")

        assert problem is not None
        assert problem.type == ProblemType.NO_DATA


class TestQueryPerformance:
    """Tests for query performance."""

    @pytest.mark.asyncio
    async def test_query_latency_acceptable(self):
        """Queries should complete within acceptable time."""
        mock_client = MockClickHouseClient(
            data=[(1000000, 1, 1000000)],
            delay=0.1
        )

        checker = IntegrityChecker(mock_client)

        start = time.time()
        await checker._check_gaps("BTCUSDT")
        duration = time.time() - start

        assert duration >= 0.1
        assert duration < 0.2

    @pytest.mark.asyncio
    async def test_full_check_timing(self):
        """Full check should complete in reasonable time."""
        mock_client = MockClickHouseClient(
            data=[],
            delay=0.05
        )

        checker = IntegrityChecker(mock_client)

        # Mock all checks to return no problems
        checker._check_duplicates = AsyncMock(return_value=None)
        checker._check_gaps = AsyncMock(return_value=None)
        checker._check_lag = AsyncMock(return_value=None)

        start = time.time()
        result = await checker.check("BTCUSDT")
        duration = time.time() - start

        assert duration < 0.5
        assert result is None


class TestIntegrityCheckerWithRealClickHouse:
    """
    Integration tests with real ClickHouse.

    These tests require a running ClickHouse instance.
    Connection settings are loaded from .env file:
    - CLICKHOUSE_HOST
    - CLICKHOUSE_HTTP_PORT
    - CLICKHOUSE_DATABASE
    - CLICKHOUSE_USERNAME
    - CLICKHOUSE_PASSWORD
    """

    @pytest_asyncio.fixture
    async def real_client(self):
        """Create real ClickHouse client using project's ClickHouseManager."""
        from src.core.clickhouse_manager import ClickHouseManager
        from src.models.config import ClickHouseConfig

        # Create config from environment variables
        config = ClickHouseConfig(
            host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
            port=int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123')),
            database=os.getenv('CLICKHOUSE_DATABASE', 'trades'),
            username=os.getenv('CLICKHOUSE_USERNAME', 'default'),
            password=os.getenv('CLICKHOUSE_PASSWORD', '')
        )

        manager = ClickHouseManager(config)

        try:
            await manager.ensure_connected()
            # Return adapter that has execute() method
            adapter = ClickHouseAdapter(manager)
            yield adapter
        except Exception as e:
            pytest.skip(f"ClickHouse not available: {e}")
        finally:
            try:
                await manager.close()
            except:
                pass

    @pytest.mark.asyncio
    async def test_real_gap_check_performance(self, real_client):
        """Test gap check query performance on real ClickHouse."""
        checker = IntegrityChecker(real_client)

        start = time.time()
        stats = await checker.get_stats("BTCUSDT")
        duration = time.time() - start

        print(f"\nGap check query time: {duration:.3f}s")
        print(f"Stats: {stats}")

        assert duration < 5.0, f"Query too slow: {duration:.3f}s"

    @pytest.mark.asyncio
    async def test_real_duplicate_check_performance(self, real_client):
        """Test duplicate check query performance."""
        checker = IntegrityChecker(real_client)

        start = time.time()
        problem = await checker._check_duplicates("BTCUSDT")
        duration = time.time() - start

        print(f"\nDuplicate check query time: {duration:.3f}s")
        print(f"Problem: {problem}")

        assert duration < 10.0, f"Query too slow: {duration:.3f}s"

    @pytest.mark.asyncio
    async def test_real_full_check_performance(self, real_client):
        """Test full integrity check performance."""
        checker = IntegrityChecker(real_client)

        start = time.time()
        problem = await checker.check("BTCUSDT")
        duration = time.time() - start

        print(f"\nFull check time: {duration:.3f}s")
        print(f"Problem: {problem}")

        assert duration < 30.0, f"Full check too slow: {duration:.3f}s"

    @pytest.mark.asyncio
    async def test_multiple_symbols_check(self, real_client):
        """Test checking multiple symbols sequentially."""
        checker = IntegrityChecker(real_client)

        symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

        start = time.time()
        results = {}

        for symbol in symbols:
            try:
                problem = await checker.check(symbol)
                results[symbol] = problem
            except Exception as e:
                results[symbol] = f"Error: {e}"

        duration = time.time() - start

        print(f"\nMultiple symbols check time: {duration:.3f}s")
        for symbol, result in results.items():
            print(f"  {symbol}: {result}")

        avg_time = duration / len(symbols)
        assert avg_time < 15.0, f"Average check too slow: {avg_time:.3f}s"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])