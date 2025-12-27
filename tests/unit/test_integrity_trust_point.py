"""
Unit Tests for Trust Point Functionality
========================================

Tests for new IntegrityChecker methods:
- find_trust_point()
- get_monthly_stats()
- get_daily_stats()
- _find_month_for_daily_analysis()
- _find_trust_point_from_days()

File: tests/unit/test_integrity_trust_point.py
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
test_log_file = str(logs_folder / 'test_integrity_trust_point.log')
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
sys.path.insert(0, str(project_root / "src" / "core" / "integrity"))

from integrity_checker import (
    IntegrityChecker,
    IntegrityStatus,
    IntegrityResult,
    TrustPoint,
    MonthStats,
    DayStats,
    RecoveryStrategy
)


class MockClickHouseClient:
    """Mock ClickHouse client for testing."""

    def __init__(self):
        self.queries = []
        self.responses = {}

    def set_response(self, query_contains: str, response: list):
        """Set response for queries containing specific text."""
        self.responses[query_contains] = response

    async def execute(self, query: str):
        self.queries.append(query)
        for key, response in self.responses.items():
            if key in query:
                return response
        return []


class TestMonthStats:
    """Tests for MonthStats dataclass."""

    def test_month_stats_creation(self):
        """Should create MonthStats correctly."""
        stats = MonthStats(
            month=202510,
            total_count=55410632,
            min_id=2867872924,
            max_id=2923283555,
            sum_ids=160445820256142364,
            gaps_count=0
        )

        assert stats.month == 202510
        assert stats.total_count == 55410632
        assert stats.gaps_count == 0

    def test_month_stats_with_gaps(self):
        """MonthStats with negative gaps_count indicates gaps."""
        stats = MonthStats(
            month=202512,
            total_count=30633924,
            min_id=2991321238,
            max_id=3027696868,
            sum_ids=92243891718234707,
            gaps_count=-5741707  # Negative = gaps
        )

        assert stats.gaps_count < 0

    def test_month_stats_with_duplicates(self):
        """MonthStats with positive gaps_count indicates duplicates."""
        stats = MonthStats(
            month=202512,
            total_count=1000100,
            min_id=1,
            max_id=1000000,
            sum_ids=500000500000,
            gaps_count=100  # Positive = duplicates
        )

        assert stats.gaps_count > 0


class TestDayStats:
    """Tests for DayStats dataclass."""

    def test_day_stats_creation(self):
        """Should create DayStats correctly."""
        stats = DayStats(
            day="2025-10-31",
            total_count=1500000,
            min_id=2922000000,
            max_id=2923283555,
            gaps_count=0
        )

        assert stats.day == "2025-10-31"
        assert stats.gaps_count == 0


class TestTrustPoint:
    """Tests for TrustPoint dataclass."""

    def test_trust_point_creation(self):
        """Should create TrustPoint correctly."""
        tp = TrustPoint(
            last_valid_id=2923283555,
            last_valid_day="2025-10-31",
            recovery_start_day="2025-10-31",
            source="Gap between months 202510 and 202512"
        )

        assert tp.last_valid_id == 2923283555
        assert tp.last_valid_day == "2025-10-31"
        assert tp.recovery_start_day == "2025-10-31"


class TestIntegrityResult:
    """Tests for IntegrityResult dataclass."""

    def test_valid_result(self):
        """Should create VALID result."""
        result = IntegrityResult(
            status=IntegrityStatus.VALID,
            symbol="BTCUSDT",
            check_duration_seconds=3.5
        )

        assert result.status == IntegrityStatus.VALID
        assert result.trust_point is None

    def test_needs_recovery_result(self):
        """Should create NEEDS_RECOVERY result with trust point."""
        tp = TrustPoint(
            last_valid_id=100,
            last_valid_day="2025-10-31",
            recovery_start_day="2025-10-31",
            source="Test"
        )

        result = IntegrityResult(
            status=IntegrityStatus.NEEDS_RECOVERY,
            symbol="BTCUSDT",
            trust_point=tp
        )

        assert result.status == IntegrityStatus.NEEDS_RECOVERY
        assert result.trust_point is not None

    def test_no_data_result(self):
        """Should create NO_DATA result."""
        result = IntegrityResult(
            status=IntegrityStatus.NO_DATA,
            symbol="NEWUSDT",
            error_message="No data found for symbol"
        )

        assert result.status == IntegrityStatus.NO_DATA

    def test_error_result(self):
        """Should create ERROR result."""
        result = IntegrityResult(
            status=IntegrityStatus.ERROR,
            symbol="BTCUSDT",
            error_message="Database connection failed"
        )

        assert result.status == IntegrityStatus.ERROR
        assert "Database" in result.error_message


class TestGetMonthlyStats:
    """Tests for get_monthly_stats() method."""

    @pytest.fixture
    def checker(self):
        mock_client = MockClickHouseClient()
        return IntegrityChecker(mock_client), mock_client

    @pytest.mark.asyncio
    async def test_returns_empty_list_when_no_data(self, checker):
        """Should return empty list when no data."""
        checker_instance, mock_client = checker
        mock_client.set_response("toYYYYMM", [])

        result = await checker_instance.get_monthly_stats("BTCUSDT")

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_month_stats_list(self, checker):
        """Should return list of MonthStats."""
        checker_instance, mock_client = checker
        mock_client.set_response("toYYYYMM", [
            (202509, 27229666, 2840643258, 2867872923, 77720494482112773, 0),
            (202510, 55410632, 2867872924, 2923283555, 160445820256142364, 0),
            (202512, 30633924, 2991321238, 3027696868, 92243891718234707, -5741707),
        ])

        result = await checker_instance.get_monthly_stats("BTCUSDT")

        assert len(result) == 3
        assert result[0].month == 202509
        assert result[1].month == 202510
        assert result[1].gaps_count == 0
        assert result[2].gaps_count == -5741707


class TestGetDailyStats:
    """Tests for get_daily_stats() method."""

    @pytest.fixture
    def checker(self):
        mock_client = MockClickHouseClient()
        return IntegrityChecker(mock_client), mock_client

    @pytest.mark.asyncio
    async def test_returns_day_stats_for_month(self, checker):
        """Should return daily stats for specific month."""
        checker_instance, mock_client = checker
        mock_client.set_response("toYYYYMM(timestamp) = 202510", [
            ("2025-10-30", 1800000, 2921000000, 2922800000, 0),
            ("2025-10-31", 1500000, 2922800001, 2923283555, 0),
        ])

        result = await checker_instance.get_daily_stats("BTCUSDT", 202510)

        assert len(result) == 2
        assert result[0].day == "2025-10-30"
        assert result[1].day == "2025-10-31"


class TestFindMonthForDailyAnalysis:
    """Tests for _find_month_for_daily_analysis() method."""

    @pytest.fixture
    def checker(self):
        mock_client = AsyncMock()
        return IntegrityChecker(mock_client)

    def test_all_months_valid_returns_none(self, checker):
        """When all months valid with valid transitions, return None."""
        monthly_stats = [
            MonthStats(202509, 1000, 1, 1000, 500500, 0),
            MonthStats(202510, 1000, 1001, 2000, 1501500, 0),
            MonthStats(202511, 1000, 2001, 3000, 2501500, 0),
        ]

        month, reason = checker._find_month_for_daily_analysis(monthly_stats)

        assert month is None

    def test_month_with_gaps_returns_that_month(self, checker):
        """When month has gaps_count != 0, return that month."""
        monthly_stats = [
            MonthStats(202509, 1000, 1, 1000, 500500, 0),
            MonthStats(202510, 900, 1001, 2000, 1350450, -100),  # Has gaps
            MonthStats(202511, 1000, 2001, 3000, 2501500, 0),
        ]

        month, reason = checker._find_month_for_daily_analysis(monthly_stats)

        assert month == 202510
        assert "gaps_count" in reason

    def test_invalid_transition_returns_current_month(self, checker):
        """When transition to next month is invalid, return current month."""
        monthly_stats = [
            MonthStats(202509, 1000, 1, 1000, 500500, 0),
            MonthStats(202510, 1000, 1001, 2000, 1501500, 0),
            # Gap: next min_id should be 2001, but it's 3001
            MonthStats(202511, 1000, 3001, 4000, 3501500, 0),
        ]

        month, reason = checker._find_month_for_daily_analysis(monthly_stats)

        assert month == 202510
        assert "Gap" in reason

    def test_valid_transition_but_next_has_gaps(self, checker):
        """When transition valid but next month has gaps, return next month."""
        monthly_stats = [
            MonthStats(202509, 1000, 1, 1000, 500500, 0),
            MonthStats(202510, 1000, 1001, 2000, 1501500, 0),
            MonthStats(202511, 900, 2001, 3000, 2401400, -100),  # Valid transition, but gaps inside
        ]

        month, reason = checker._find_month_for_daily_analysis(monthly_stats)

        assert month == 202511
        assert "gaps_count" in reason

    def test_missing_month_detected(self, checker):
        """Should detect missing month (gap between months)."""
        # October ends at 2923283555, December starts at 2991321238
        # November is completely missing
        monthly_stats = [
            MonthStats(202510, 55410632, 2867872924, 2923283555, 160445820256142364, 0),
            MonthStats(202512, 30633924, 2991321238, 3027696868, 92243891718234707, -5741707),
        ]

        month, reason = checker._find_month_for_daily_analysis(monthly_stats)

        assert month == 202510  # Last valid month before gap
        assert "Gap" in reason


class TestFindTrustPointFromDays:
    """Tests for _find_trust_point_from_days() method."""

    @pytest.fixture
    def checker(self):
        mock_client = AsyncMock()
        return IntegrityChecker(mock_client)

    def test_all_days_valid_returns_none(self, checker):
        """When all days valid with valid transitions, return None."""
        daily_stats = [
            DayStats("2025-10-29", 1000, 1, 1000, 0),
            DayStats("2025-10-30", 1000, 1001, 2000, 0),
            DayStats("2025-10-31", 1000, 2001, 3000, 0),
        ]
        monthly_stats = [
            MonthStats(202510, 3000, 1, 3000, 4501500, 0),
        ]

        result = checker._find_trust_point_from_days(daily_stats, monthly_stats, 202510)

        assert result is None

    def test_day_with_gaps_returns_previous_day(self, checker):
        """When day has gaps, trust point is previous day."""
        daily_stats = [
            DayStats("2025-10-29", 1000, 1, 1000, 0),
            DayStats("2025-10-30", 1000, 1001, 2000, 0),
            DayStats("2025-10-31", 900, 2001, 3000, -100),  # Has gaps
        ]
        monthly_stats = [
            MonthStats(202510, 2900, 1, 3000, 4351450, -100),
        ]

        result = checker._find_trust_point_from_days(daily_stats, monthly_stats, 202510)

        assert result is not None
        assert result.last_valid_day == "2025-10-30"
        assert result.recovery_start_day == "2025-10-30"
        assert result.last_valid_id == 2000

    def test_gap_between_days_returns_previous_day(self, checker):
        """When gap between days, trust point is previous day."""
        daily_stats = [
            DayStats("2025-10-29", 1000, 1, 1000, 0),
            DayStats("2025-10-30", 1000, 1001, 2000, 0),
            # Gap: next min_id should be 2001, but it's 2501
            DayStats("2025-10-31", 500, 2501, 3000, 0),
        ]
        monthly_stats = [
            MonthStats(202510, 2500, 1, 3000, 3751250, -500),
        ]

        result = checker._find_trust_point_from_days(daily_stats, monthly_stats, 202510)

        assert result is not None
        assert result.last_valid_day == "2025-10-30"
        assert result.last_valid_id == 2000
        assert "Gap" in result.source

    def test_first_day_with_gaps_uses_previous_month(self, checker):
        """When first day has gaps, use previous month as trust point."""
        daily_stats = [
            DayStats("2025-11-01", 900, 3001, 4000, -100),  # First day has gaps
            DayStats("2025-11-02", 1000, 4001, 5000, 0),
        ]
        monthly_stats = [
            MonthStats(202510, 3000, 1, 3000, 4501500, 0),  # Previous month OK
            MonthStats(202511, 1900, 3001, 5000, 7601900, -100),
        ]

        result = checker._find_trust_point_from_days(daily_stats, monthly_stats, 202511)

        assert result is not None
        assert result.last_valid_id == 3000  # Previous month's max_id
        assert "2025-10-31" in result.last_valid_day

    def test_gap_between_months_detected(self, checker):
        """When gap between previous month and first day, detect it."""
        daily_stats = [
            # First day starts at wrong ID (gap from previous month)
            DayStats("2025-11-01", 1000, 3501, 4500, 0),
            DayStats("2025-11-02", 1000, 4501, 5500, 0),
        ]
        monthly_stats = [
            MonthStats(202510, 3000, 1, 3000, 4501500, 0),  # Ends at 3000
            MonthStats(202511, 2000, 3501, 5500, 9001000, 0),  # Starts at 3501 - gap!
        ]

        result = checker._find_trust_point_from_days(daily_stats, monthly_stats, 202511)

        assert result is not None
        assert result.last_valid_id == 3000
        assert "Gap" in result.source


class TestFindTrustPoint:
    """Tests for main find_trust_point() method."""

    @pytest.fixture
    def checker(self):
        mock_client = MockClickHouseClient()
        return IntegrityChecker(mock_client), mock_client

    @pytest.mark.asyncio
    async def test_no_data_returns_no_data_status(self, checker):
        """When no data exists, return NO_DATA status."""
        checker_instance, mock_client = checker
        mock_client.set_response("toYYYYMM", [])

        result = await checker_instance.find_trust_point("NEWUSDT")

        assert result.status == IntegrityStatus.NO_DATA
        assert result.trust_point is None

    @pytest.mark.asyncio
    async def test_valid_data_returns_valid_status(self, checker):
        """When all data valid, return VALID status."""
        checker_instance, mock_client = checker

        # Monthly stats - all valid
        mock_client.set_response("toYYYYMM(timestamp) as month", [
            (202509, 1000, 1, 1000, 500500, 0),
            (202510, 1000, 1001, 2000, 1501500, 0),
        ])

        # Daily stats for last month - all valid
        mock_client.set_response("toYYYYMM(timestamp) = 202510", [
            ("2025-10-30", 500, 1001, 1500, 0),
            ("2025-10-31", 500, 1501, 2000, 0),
        ])

        result = await checker_instance.find_trust_point("BTCUSDT")

        assert result.status == IntegrityStatus.VALID
        assert result.trust_point is None

    @pytest.mark.asyncio
    async def test_gap_detected_returns_needs_recovery(self, checker):
        """When gap detected, return NEEDS_RECOVERY with trust point."""
        checker_instance, mock_client = checker

        # Monthly stats - gap between Oct and Dec (Nov missing)
        mock_client.set_response("toYYYYMM(timestamp) as month", [
            (202510, 1000, 1, 1000, 500500, 0),
            (202512, 800, 2001, 3000, 2000800, -200),  # Gap from Oct
        ])

        # Daily stats for October
        mock_client.set_response("toYYYYMM(timestamp) = 202510", [
            ("2025-10-30", 500, 1, 500, 0),
            ("2025-10-31", 500, 501, 1000, 0),
        ])

        result = await checker_instance.find_trust_point("BTCUSDT")

        assert result.status == IntegrityStatus.NEEDS_RECOVERY
        assert result.trust_point is not None
        assert result.trust_point.last_valid_id == 1000

    @pytest.mark.asyncio
    async def test_error_returns_error_status(self, checker):
        """When error occurs, return ERROR status."""
        checker_instance, _ = checker

        # Make execute raise exception
        async def raise_error(query):
            raise Exception("Database connection failed")

        checker_instance.clickhouse.execute = raise_error

        result = await checker_instance.find_trust_point("BTCUSDT")

        assert result.status == IntegrityStatus.ERROR
        assert "Database" in result.error_message


class TestDetermineStrategyWithIntegrityResult:
    """Tests for determine_strategy() with IntegrityResult."""

    @pytest.fixture
    def checker(self):
        mock_client = AsyncMock()
        return IntegrityChecker(mock_client)

    def test_valid_result_returns_none(self, checker):
        """VALID result should return None (no recovery needed)."""
        result = IntegrityResult(
            status=IntegrityStatus.VALID,
            symbol="BTCUSDT"
        )

        strategy = checker.determine_strategy(result)

        assert strategy is None

    def test_error_result_returns_none(self, checker):
        """ERROR result should return None."""
        result = IntegrityResult(
            status=IntegrityStatus.ERROR,
            symbol="BTCUSDT",
            error_message="Failed"
        )

        strategy = checker.determine_strategy(result)

        assert strategy is None

    def test_no_data_result_returns_full_recovery(self, checker):
        """NO_DATA result should return 30-day archive recovery."""
        result = IntegrityResult(
            status=IntegrityStatus.NO_DATA,
            symbol="NEWUSDT"
        )

        strategy = checker.determine_strategy(result)

        assert strategy is not None
        assert strategy.needs_archive is True
        # Should start from 30 days ago
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        start = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
        assert strategy.archive_from_day == start

    def test_needs_recovery_past_day(self, checker):
        """NEEDS_RECOVERY with past day should require archive."""
        past_day = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d")

        result = IntegrityResult(
            status=IntegrityStatus.NEEDS_RECOVERY,
            symbol="BTCUSDT",
            trust_point=TrustPoint(
                last_valid_id=1000,
                last_valid_day=past_day,
                recovery_start_day=past_day,
                source="Test"
            )
        )

        strategy = checker.determine_strategy(result)

        assert strategy is not None
        assert strategy.needs_archive is True
        assert strategy.delete_from_day == past_day
        assert strategy.archive_from_day == past_day

    def test_needs_recovery_today(self, checker):
        """NEEDS_RECOVERY with today should NOT require archive."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        result = IntegrityResult(
            status=IntegrityStatus.NEEDS_RECOVERY,
            symbol="BTCUSDT",
            trust_point=TrustPoint(
                last_valid_id=1000,
                last_valid_day=today,
                recovery_start_day=today,
                source="Test"
            )
        )

        strategy = checker.determine_strategy(result)

        assert strategy is not None
        assert strategy.needs_archive is False
        assert strategy.delete_from_day == today


class TestMonthToLastDay:
    """Tests for _month_to_last_day() static method."""

    def test_january(self):
        result = IntegrityChecker._month_to_last_day(202501)
        assert result == "2025-01-31"

    def test_february_non_leap(self):
        result = IntegrityChecker._month_to_last_day(202502)
        assert result == "2025-02-28"

    def test_february_leap(self):
        result = IntegrityChecker._month_to_last_day(202402)  # 2024 is leap year
        assert result == "2024-02-29"

    def test_april(self):
        result = IntegrityChecker._month_to_last_day(202504)
        assert result == "2025-04-30"

    def test_december(self):
        result = IntegrityChecker._month_to_last_day(202512)
        assert result == "2025-12-31"


class TestRealWorldScenario:
    """Tests simulating real-world data scenarios."""

    @pytest.fixture
    def checker(self):
        mock_client = MockClickHouseClient()
        return IntegrityChecker(mock_client), mock_client

    @pytest.mark.asyncio
    async def test_november_missing_scenario(self, checker):
        """
        Real scenario: October OK, November missing, December has gaps.
        Should find trust point at end of October.
        """
        checker_instance, mock_client = checker

        # Monthly stats matching real data
        mock_client.set_response("toYYYYMM(timestamp) as month", [
            (202510, 55410632, 2867872924, 2923283555, 160445820256142364, 0),
            (202512, 30633924, 2991321238, 3027696868, 92243891718234707, -5741707),
        ])

        # Daily stats for October (all valid)
        mock_client.set_response("toYYYYMM(timestamp) = 202510", [
            ("2025-10-30", 1800000, 2921483556, 2923283555, 0),
            ("2025-10-31", 1800000, 2919683556, 2921483555, 0),
        ])

        result = await checker_instance.find_trust_point("BTCUSDT")

        assert result.status == IntegrityStatus.NEEDS_RECOVERY
        assert result.trust_point is not None
        # Trust point should be last day of October
        assert "2025-10" in result.trust_point.last_valid_day

    @pytest.mark.asyncio
    async def test_gap_1000_pattern(self, checker):
        """
        Scenario: Day has exactly 1000 missing (one API request lost).
        """
        checker_instance, mock_client = checker

        mock_client.set_response("toYYYYMM(timestamp) as month", [
            (202512, 29000, 1, 30000, 435500000, -1000),  # 1000 gaps
        ])

        mock_client.set_response("toYYYYMM(timestamp) = 202512", [
            ("2025-12-01", 10000, 1, 10000, 0),
            ("2025-12-02", 9000, 10001, 20000, -1000),  # Day with gap
            ("2025-12-03", 10000, 20001, 30000, 0),
        ])

        result = await checker_instance.find_trust_point("BTCUSDT")

        assert result.status == IntegrityStatus.NEEDS_RECOVERY
        assert result.trust_point.last_valid_day == "2025-12-01"
        assert result.trust_point.last_valid_id == 10000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
