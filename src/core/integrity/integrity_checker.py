"""
IntegrityChecker - Data Integrity Verification
==============================================

SQL-based checks for detecting data integrity issues:
- Gaps (missing trades)
- Duplicates (repeated aggregate_ids)
- Lag (data not up to date)

Uses month/day analysis to find trust point for recovery.

File: src/core/integrity/integrity_checker.py
"""

import time
from typing import Optional, List
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum

from loggerino import loggerino

logger = loggerino.get('integrity_checker')


class ProblemType(Enum):
    """Type of integrity problem."""
    NONE = "none"
    GAP = "gap"
    DUPLICATE = "duplicate"
    LAG = "lag"
    NO_DATA = "no_data"


class IntegrityStatus(Enum):
    """Status of integrity check result."""
    VALID = "valid"              # All data is valid
    NEEDS_RECOVERY = "needs_recovery"  # Found trust point, needs recovery
    NO_DATA = "no_data"          # No data exists for symbol
    ERROR = "error"              # Error during check


@dataclass
class Problem:
    """Detected integrity problem."""
    type: ProblemType
    symbol: str
    start_time: Optional[datetime] = None
    start_id: Optional[int] = None
    end_id: Optional[int] = None
    count: int = 0
    description: str = ""


@dataclass
class RecoveryStrategy:
    """Strategy for recovering from a problem."""
    delete_from_day: str  # YYYY-MM-DD format - delete all data from this day
    needs_archive: bool
    archive_from_day: Optional[str] = None  # YYYY-MM-DD
    archive_to_day: Optional[str] = None    # YYYY-MM-DD


@dataclass
class MonthStats:
    """Statistics for a month."""
    month: int  # YYYYMM format (202510)
    total_count: int
    min_id: int
    max_id: int
    sum_ids: int
    gaps_count: int  # negative = gaps, positive = duplicates


@dataclass
class DayStats:
    """Statistics for a day."""
    day: str  # YYYY-MM-DD format
    total_count: int
    min_id: int
    max_id: int
    gaps_count: int


@dataclass
class TrustPoint:
    """Trust point - last known good position."""
    last_valid_id: int
    last_valid_day: str       # YYYY-MM-DD - last fully valid day
    recovery_start_day: str   # YYYY-MM-DD - day to start recovery from (delete & re-download)
    source: str               # Description of how trust point was found


@dataclass
class IntegrityResult:
    """Result of integrity check."""
    status: IntegrityStatus
    symbol: str
    trust_point: Optional[TrustPoint] = None
    error_message: Optional[str] = None
    monthly_stats: Optional[List[MonthStats]] = None
    daily_stats: Optional[List[DayStats]] = None
    check_duration_seconds: float = 0.0


class IntegrityChecker:
    """
    Checks data integrity for trading symbols.

    Uses hierarchical analysis:
    1. Monthly statistics (~3 sec) - find problematic month
    2. Daily statistics (~0.4 sec) - find exact trust point

    Trust point = last position where data is guaranteed valid.
    Recovery starts from trust point day (delete day & re-download).
    """

    def __init__(self, clickhouse_client):
        """
        Initialize IntegrityChecker.

        Args:
            clickhouse_client: ClickHouse client with execute() method
        """
        self.clickhouse = clickhouse_client
        logger.info("IntegrityChecker initialized")

    # ========== MAIN PUBLIC METHODS ==========

    async def find_trust_point(self, symbol: str) -> IntegrityResult:
        """
        Find the trust point for a symbol using month/day analysis.

        Algorithm:
        1. Get monthly stats (~3 sec)
        2. Check transitions between months
        3. Find month requiring daily analysis
        4. Get daily stats for that month (~0.4 sec)
        5. Check transitions between days
        6. Return result with trust point or valid status

        Args:
            symbol: Trading symbol

        Returns:
            IntegrityResult with status and trust_point if needed
        """
        start_time = time.time()
        logger.info(f"{symbol}: Starting integrity check")

        try:
            # Step 1: Get monthly stats
            monthly_stats = await self.get_monthly_stats(symbol)

            if not monthly_stats:
                duration = time.time() - start_time
                logger.warning(f"{symbol}: No data found ({duration:.2f}s)")
                return IntegrityResult(
                    status=IntegrityStatus.NO_DATA,
                    symbol=symbol,
                    error_message="No data found for symbol",
                    check_duration_seconds=duration
                )

            logger.debug(f"{symbol}: Found {len(monthly_stats)} months of data")

            # Step 2: Find month requiring daily analysis
            month_to_analyze, analysis_reason = self._find_month_for_daily_analysis(monthly_stats)

            if month_to_analyze is None:
                # All months valid including transitions - check last month's days
                month_to_analyze = monthly_stats[-1].month
                analysis_reason = "Checking last month for completeness"

            logger.debug(f"{symbol}: Analyzing month {month_to_analyze} - {analysis_reason}")

            # Step 3: Get daily stats for the month
            daily_stats = await self.get_daily_stats(symbol, month_to_analyze)

            if not daily_stats:
                duration = time.time() - start_time
                logger.error(f"{symbol}: No daily data for month {month_to_analyze}")
                return IntegrityResult(
                    status=IntegrityStatus.ERROR,
                    symbol=symbol,
                    error_message=f"No daily data for month {month_to_analyze} despite monthly data existing",
                    monthly_stats=monthly_stats,
                    check_duration_seconds=duration
                )

            logger.debug(f"{symbol}: Found {len(daily_stats)} days in month {month_to_analyze}")

            # Step 4: Find trust point from daily analysis
            trust_point = self._find_trust_point_from_days(
                daily_stats, monthly_stats, month_to_analyze
            )

            duration = time.time() - start_time

            if trust_point:
                logger.info(f"{symbol}: Trust point found - {trust_point.source} ({duration:.2f}s)")
                return IntegrityResult(
                    status=IntegrityStatus.NEEDS_RECOVERY,
                    symbol=symbol,
                    trust_point=trust_point,
                    monthly_stats=monthly_stats,
                    daily_stats=daily_stats,
                    check_duration_seconds=duration
                )

            # All checks passed
            logger.info(f"{symbol}: Data is fully valid ({duration:.2f}s)")
            return IntegrityResult(
                status=IntegrityStatus.VALID,
                symbol=symbol,
                monthly_stats=monthly_stats,
                daily_stats=daily_stats,
                check_duration_seconds=duration
            )

        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"{symbol}: Integrity check failed - {e}")
            return IntegrityResult(
                status=IntegrityStatus.ERROR,
                symbol=symbol,
                error_message=str(e),
                check_duration_seconds=duration
            )

    def determine_strategy(self, result: IntegrityResult) -> Optional[RecoveryStrategy]:
        """
        Determine recovery strategy based on integrity result.

        Args:
            result: IntegrityResult from find_trust_point()

        Returns:
            RecoveryStrategy or None if no recovery needed
        """
        if result.status == IntegrityStatus.VALID:
            return None

        if result.status == IntegrityStatus.ERROR:
            logger.warning(f"{result.symbol}: Cannot determine strategy due to error")
            return None

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        if result.status == IntegrityStatus.NO_DATA:
            # No data - need full archive recovery from 30 days ago
            start_day = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

            return RecoveryStrategy(
                delete_from_day=start_day,
                needs_archive=True,
                archive_from_day=start_day,
                archive_to_day=yesterday
            )

        if result.status == IntegrityStatus.NEEDS_RECOVERY and result.trust_point:
            recovery_day = result.trust_point.recovery_start_day

            # Convert int YYYYMMDD to string YYYY-MM-DD if needed
            if isinstance(recovery_day, int):
                recovery_day_str = f"{recovery_day // 10000}-{(recovery_day % 10000) // 100:02d}-{recovery_day % 100:02d}"
            else:
                recovery_day_str = str(recovery_day)

            # Need archive if recovery starts before today
            needs_archive = recovery_day_str < today

            return RecoveryStrategy(
                delete_from_day=recovery_day_str,
                needs_archive=needs_archive,
                archive_from_day=recovery_day_str if needs_archive else None,
                archive_to_day=yesterday if needs_archive else None
            )

        return None

    # ========== STATISTICS QUERIES ==========

    async def get_monthly_stats(self, symbol: str) -> List[MonthStats]:
        """
        Get integrity statistics grouped by month.

        Args:
            symbol: Trading symbol

        Returns:
            List of MonthStats ordered by month
        """
        query = f"""
            SELECT 
                toYYYYMM(timestamp) as month,
                count() as total_count,
                min(aggregate_id) as min_id,
                max(aggregate_id) as max_id,
                sum(aggregate_id) as sum_ids,
                max(aggregate_id) - min(aggregate_id) + 1 - count() as gaps_count
            FROM trades_local
            WHERE symbol = '{symbol}'
            GROUP BY month
            ORDER BY month
        """

        try:
            result = await self.clickhouse.fetch(query)

            if not result:
                return []

            return [
                MonthStats(
                    month=row[0],
                    total_count=row[1],
                    min_id=row[2],
                    max_id=row[3],
                    sum_ids=row[4],
                    gaps_count=row[5]
                )
                for row in result
            ]

        except Exception as e:
            logger.error(f"{symbol}: get_monthly_stats failed - {e}")
            raise

    async def get_daily_stats(self, symbol: str, month: int) -> List[DayStats]:
        """
        Get integrity statistics grouped by day for a specific month.

        Args:
            symbol: Trading symbol
            month: Month in YYYYMM format

        Returns:
            List of DayStats ordered by day
        """
        query = f"""
            SELECT 
                formatDateTime(timestamp, '%Y-%m-%d') as day,
                count() as total_count,
                min(aggregate_id) as min_id,
                max(aggregate_id) as max_id,
                max(aggregate_id) - min(aggregate_id) + 1 - count() as gaps_count
            FROM trades_local
            WHERE symbol = '{symbol}'
              AND toYYYYMM(timestamp) = {month}
            GROUP BY day
            ORDER BY day
        """

        try:
            result = await self.clickhouse.fetch(query)

            if not result:
                return []

            return [
                DayStats(
                    day=row[0],
                    total_count=row[1],
                    min_id=row[2],
                    max_id=row[3],
                    gaps_count=row[4]
                )
                for row in result
            ]

        except Exception as e:
            logger.error(f"{symbol}: get_daily_stats failed - {e}")
            raise

    # ========== ANALYSIS LOGIC ==========

    def _find_month_for_daily_analysis(
        self,
        monthly_stats: List[MonthStats]
    ) -> tuple[Optional[int], str]:
        """
        Find which month needs daily analysis.

        Logic:
        - If month has gaps_count != 0 → analyze this month
        - If next month is missing → analyze current month
        - If transition to next is invalid → analyze current month
        - If transition valid but next has gaps → analyze next month

        Args:
            monthly_stats: List of monthly statistics

        Returns:
            Tuple of (month_to_analyze, reason) or (None, "") if all valid
        """
        for i, current in enumerate(monthly_stats):
            is_last_month = (i == len(monthly_stats) - 1)

            # Check if current month has internal gaps/duplicates
            if current.gaps_count != 0:
                return (current.month, f"Month has gaps_count={current.gaps_count}")

            if not is_last_month:
                next_month = monthly_stats[i + 1]
                expected_next_min = current.max_id + 1

                # Check transition to next month
                if next_month.min_id != expected_next_min:
                    gap_size = next_month.min_id - current.max_id - 1
                    return (current.month, f"Gap of {gap_size} between months {current.month} and {next_month.month}")

                # Transition valid, but check if next month has problems
                if next_month.gaps_count != 0:
                    return (next_month.month, f"Next month {next_month.month} has gaps_count={next_month.gaps_count}")

        # All months and transitions are valid
        return (None, "")

    def _find_trust_point_from_days(
            self,
            daily_stats: List[DayStats],
            monthly_stats: List[MonthStats],
            analyzed_month: int
    ) -> Optional[TrustPoint]:
        # Find previous month's data
        prev_month_stats: Optional[MonthStats] = None
        for ms in monthly_stats:
            if ms.month < analyzed_month:
                prev_month_stats = ms

        prev_day: Optional[DayStats] = None

        for i, current in enumerate(daily_stats):
            is_first_day = (i == 0)

            # === Check transition from previous period ===
            if is_first_day:
                # First day - check against previous month (if exists)
                if prev_month_stats is not None:
                    expected_min = prev_month_stats.max_id + 1
                    if current.min_id != expected_min:
                        gap_size = current.min_id - prev_month_stats.max_id - 1
                        return TrustPoint(
                            last_valid_id=prev_month_stats.max_id,
                            last_valid_day=self._month_to_last_day(prev_month_stats.month),
                            recovery_start_day=self._month_to_last_day(prev_month_stats.month),
                            source=f"Gap of {gap_size} between month {prev_month_stats.month} and day {current.day}"
                        )
            else:
                # Not first day - check transition from previous day
                expected_min = prev_day.max_id + 1
                if current.min_id != expected_min:
                    gap_size = current.min_id - prev_day.max_id - 1
                    return TrustPoint(
                        last_valid_id=prev_day.max_id,
                        last_valid_day=prev_day.day,
                        recovery_start_day=prev_day.day,
                        source=f"Gap of {gap_size} between days {prev_day.day} and {current.day}"
                    )

            # === Check current day's internal integrity ===
            if current.gaps_count != 0:
                if prev_day:
                    return TrustPoint(
                        last_valid_id=prev_day.max_id,
                        last_valid_day=prev_day.day,
                        recovery_start_day=prev_day.day,
                        source=f"Day {current.day} has gaps_count={current.gaps_count}"
                    )
                elif prev_month_stats:
                    last_day = self._month_to_last_day(prev_month_stats.month)
                    return TrustPoint(
                        last_valid_id=prev_month_stats.max_id,
                        last_valid_day=last_day,
                        recovery_start_day=last_day,
                        source=f"First day {current.day} has gaps_count={current.gaps_count}"
                    )
                else:
                    return TrustPoint(
                        last_valid_id=0,
                        last_valid_day=current.day,
                        recovery_start_day=current.day,
                        source=f"First day {current.day} has gaps_count={current.gaps_count}, no previous data"
                    )

            # === IMPORTANT: Update prev_day at END of each iteration ===
            prev_day = current

        # All days within month are valid
        # Check if there's a gap AFTER this month
        if prev_day:
            next_month_stats = None
            for ms in monthly_stats:
                if ms.month > analyzed_month:
                    next_month_stats = ms
                    break

            if next_month_stats:
                expected_next_min = prev_day.max_id + 1
                if next_month_stats.min_id != expected_next_min:
                    gap_size = next_month_stats.min_id - prev_day.max_id - 1
                    return TrustPoint(
                        last_valid_id=prev_day.max_id,
                        last_valid_day=prev_day.day,
                        recovery_start_day=prev_day.day,
                        source=f"Gap of {gap_size} after day {prev_day.day} to month {next_month_stats.month}"
                    )

        return None

    @staticmethod
    def _month_to_last_day(month: int) -> str:
        """
        Convert YYYYMM to last day of month string YYYY-MM-DD.

        Args:
            month: Month in YYYYMM format

        Returns:
            Last day of month in YYYY-MM-DD format
        """
        year = month // 100
        month_num = month % 100

        # Days in each month
        if month_num in [1, 3, 5, 7, 8, 10, 12]:
            last_day = 31
        elif month_num in [4, 6, 9, 11]:
            last_day = 30
        else:  # February
            is_leap = (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)
            last_day = 29 if is_leap else 28

        return f"{year}-{month_num:02d}-{last_day:02d}"

    # ========== LEGACY METHODS (for backward compatibility) ==========

    async def check(self, symbol: str) -> Optional[Problem]:
        """
        Legacy integrity check method.

        Use find_trust_point() for new code.
        """
        result = await self.find_trust_point(symbol)

        if result.status == IntegrityStatus.VALID:
            return None

        if result.status == IntegrityStatus.NO_DATA:
            return Problem(
                type=ProblemType.NO_DATA,
                symbol=symbol,
                description="No data found"
            )

        if result.status == IntegrityStatus.NEEDS_RECOVERY and result.trust_point:
            return Problem(
                type=ProblemType.GAP,
                symbol=symbol,
                start_id=result.trust_point.last_valid_id,
                description=result.trust_point.source
            )

        if result.status == IntegrityStatus.ERROR:
            return Problem(
                type=ProblemType.GAP,
                symbol=symbol,
                description=f"Check failed: {result.error_message}"
            )

        return None

    async def quick_check(self, symbol: str) -> Optional[Problem]:
        """Legacy quick check - same as check()."""
        return await self.check(symbol)

    async def get_stats(self, symbol: str) -> dict:
        """Get basic statistics for a symbol."""
        query = f"""
            SELECT 
                count() as total_count,
                min(aggregate_id) as min_id,
                max(aggregate_id) as max_id,
                min(timestamp) as first_ts,
                max(timestamp) as last_ts
            FROM trades_local
            WHERE symbol = '{symbol}'
        """

        try:
            result = await self.clickhouse.fetch(query)

            if result and result[0][0] > 0:
                return {
                    'total_count': result[0][0],
                    'min_id': result[0][1],
                    'max_id': result[0][2],
                    'first_timestamp': result[0][3],
                    'last_timestamp': result[0][4],
                    'expected_count': result[0][2] - result[0][1] + 1,
                    'gap_count': result[0][2] - result[0][1] + 1 - result[0][0]
                }

            return {
                'total_count': 0,
                'min_id': None,
                'max_id': None,
                'first_timestamp': None,
                'last_timestamp': None,
                'expected_count': 0,
                'gap_count': 0
            }

        except Exception as e:
            logger.error(f"{symbol}: get_stats failed - {e}")
            raise
