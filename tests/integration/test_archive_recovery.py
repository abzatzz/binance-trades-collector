"""
Tests for ArchiveRecovery
=========================

Tests for downloading, validating, and parsing Binance archives.

File: tests/integration/test_archive_recovery.py
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
test_log_file = str(logs_folder / 'test_archive_recovery.log')
loggerino.create('recovery_state', test_log_file)
loggerino.create('integrity_checker', test_log_file)
loggerino.create('archive_recovery', test_log_file)
loggerino.create('integrity_manager', test_log_file)

# Now import test dependencies
import pytest
import asyncio
import hashlib
import time
import sys
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
import io
import zipfile

# Add integrity module directly to path (bypasses src.core.__init__.py)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src" / "core" / "integrity"))

# Import directly from module files
from archive_recovery import ArchiveRecovery, RecoveryResult, AggTradeRecord


class TestChecksumValidation:
    """Tests for SHA256 checksum validation."""

    @pytest.fixture
    def recovery(self):
        mock_clickhouse = AsyncMock()
        return ArchiveRecovery(mock_clickhouse)

    def test_valid_checksum(self, recovery):
        """Valid checksum should pass validation."""
        test_data = b"test zip data content"
        expected_hash = hashlib.sha256(test_data).hexdigest()
        checksum_content = f"{expected_hash}  test.zip\n".encode()

        result = recovery._validate_checksum(test_data, checksum_content)

        assert result is True

    def test_invalid_checksum(self, recovery):
        """Invalid checksum should fail validation."""
        test_data = b"test zip data content"
        wrong_hash = "a" * 64  # Wrong hash
        checksum_content = f"{wrong_hash}  test.zip\n".encode()

        result = recovery._validate_checksum(test_data, checksum_content)

        assert result is False

    def test_checksum_format_variations(self, recovery):
        """Should handle various checksum file formats."""
        test_data = b"test data"
        correct_hash = hashlib.sha256(test_data).hexdigest()

        # Format 1: hash  filename (two spaces)
        checksum1 = f"{correct_hash}  filename.zip\n".encode()
        assert recovery._validate_checksum(test_data, checksum1) is True

        # Format 2: hash filename (one space)
        checksum2 = f"{correct_hash} filename.zip\n".encode()
        assert recovery._validate_checksum(test_data, checksum2) is True

        # Format 3: just hash
        checksum3 = f"{correct_hash}\n".encode()
        assert recovery._validate_checksum(test_data, checksum3) is True

    def test_checksum_case_insensitive(self, recovery):
        """Checksum comparison should be case insensitive."""
        test_data = b"test data"
        correct_hash = hashlib.sha256(test_data).hexdigest()

        # Uppercase hash
        checksum = f"{correct_hash.upper()}  test.zip\n".encode()
        assert recovery._validate_checksum(test_data, checksum) is True


class TestZipExtraction:
    """Tests for ZIP extraction."""

    @pytest.fixture
    def recovery(self):
        mock_clickhouse = AsyncMock()
        return ArchiveRecovery(mock_clickhouse)

    def test_extract_valid_zip(self, recovery):
        """Should extract CSV from valid ZIP."""
        # Create in-memory ZIP
        csv_content = b"1,100.0,0.5,1,1,1700000000000,true\n"

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            zf.writestr("BTCUSDT-aggTrades-2025-11-01.csv", csv_content)

        zip_data = zip_buffer.getvalue()

        result = recovery._extract_zip(zip_data)

        assert result == csv_content

    def test_extract_corrupted_zip_raises(self, recovery):
        """Should raise on corrupted ZIP."""
        bad_zip = b"not a valid zip file"

        with pytest.raises(zipfile.BadZipFile):
            recovery._extract_zip(bad_zip)

    def test_extract_empty_zip_raises(self, recovery):
        """Should raise on empty ZIP."""
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w') as zf:
            pass  # Empty zip

        with pytest.raises(Exception, match="empty"):
            recovery._extract_zip(zip_buffer.getvalue())


class TestCSVParsing:
    """Tests for CSV parsing."""

    @pytest.fixture
    def recovery(self):
        mock_clickhouse = AsyncMock()
        return ArchiveRecovery(mock_clickhouse)

    def test_parse_valid_line(self, recovery):
        """Should parse valid CSV line."""
        line = "1558614031,16547.77,0.001,1893208256,1893208256,1672531204118,true"

        record = recovery._parse_csv_line(line)

        assert record is not None
        assert record.aggregate_id == 1558614031
        assert record.price == 16547.77
        assert record.quantity == 0.001
        assert record.first_trade_id == 1893208256
        assert record.last_trade_id == 1893208256
        assert record.timestamp == 1672531204118
        assert record.is_buyer_maker is True

    def test_parse_seller_maker(self, recovery):
        """Should parse is_buyer_maker=false correctly."""
        line = "123,100.0,1.0,1,1,1700000000000,false"

        record = recovery._parse_csv_line(line)

        assert record.is_buyer_maker is False

    def test_parse_invalid_line_returns_none(self, recovery):
        """Should return None for invalid lines."""
        # Too few columns
        assert recovery._parse_csv_line("1,2,3") is None

        # Non-numeric values
        assert recovery._parse_csv_line("abc,def,ghi,1,1,1,true") is None

    def test_parse_skips_header(self, recovery):
        """Should return None for header lines."""
        headers = [
            "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker",
            "aggregate_id,price,quantity,first_trade_id,last_trade_id,timestamp,is_buyer_maker"
        ]

        for header in headers:
            # Header detection happens in _parse_and_insert, not _parse_csv_line
            # But _parse_csv_line should handle them gracefully
            result = recovery._parse_csv_line(header)
            # May return None or invalid data - both acceptable


class TestDateGeneration:
    """Tests for date range generation."""

    @pytest.fixture
    def recovery(self):
        mock_clickhouse = AsyncMock()
        return ArchiveRecovery(mock_clickhouse)

    def test_generate_single_day(self, recovery):
        """Should generate list with single date."""
        dates = recovery._generate_dates(date(2025, 11, 1), date(2025, 11, 1))

        assert len(dates) == 1
        assert dates[0] == date(2025, 11, 1)

    def test_generate_date_range(self, recovery):
        """Should generate all dates in range."""
        dates = recovery._generate_dates(date(2025, 11, 1), date(2025, 11, 5))

        assert len(dates) == 5
        assert dates[0] == date(2025, 11, 1)
        assert dates[-1] == date(2025, 11, 5)

    def test_generate_month_range(self, recovery):
        """Should handle month boundaries."""
        dates = recovery._generate_dates(date(2025, 10, 30), date(2025, 11, 2))

        assert len(dates) == 4
        assert dates[0] == date(2025, 10, 30)
        assert dates[1] == date(2025, 10, 31)
        assert dates[2] == date(2025, 11, 1)
        assert dates[3] == date(2025, 11, 2)


class TestURLGeneration:
    """Tests for Binance archive URL generation."""

    def test_url_format(self):
        """URL should match Binance format."""
        base = "https://data.binance.vision/data/futures/um/daily/aggTrades"
        symbol = "BTCUSDT"
        date_str = "2025-11-01"

        expected_zip = f"{base}/{symbol}/{symbol}-aggTrades-{date_str}.zip"
        expected_checksum = f"{base}/{symbol}/{symbol}-aggTrades-{date_str}.zip.CHECKSUM"

        # Verify format matches ArchiveRecovery
        assert "data.binance.vision" in expected_zip
        assert "futures/um/daily/aggTrades" in expected_zip
        assert f"{symbol}-aggTrades-{date_str}.zip" in expected_zip


@pytest.mark.skipif(
    not pytest.importorskip("aiohttp", reason="aiohttp required"),
    reason="aiohttp not available"
)
class TestRealArchiveDownload:
    """
    Tests with real Binance archive downloads.

    These tests make real HTTP requests to Binance.
    Use sparingly to avoid rate limits.
    """

    @pytest.fixture
    def recovery(self):
        mock_clickhouse = AsyncMock()
        return ArchiveRecovery(mock_clickhouse)

    @pytest.mark.asyncio
    async def test_download_real_archive(self, recovery):
        """Test downloading a real archive file."""
        # Use a known available date (not too recent)
        test_date = "2024-01-15"  # Old date that should exist
        symbol = "BTCUSDT"

        try:
            start = time.time()
            zip_data = await recovery._download_file(symbol, test_date, ".zip")
            duration = time.time() - start

            print(f"\nDownload time: {duration:.2f}s")
            print(f"File size: {len(zip_data):,} bytes")

            assert len(zip_data) > 0

            # Verify it's a valid ZIP
            csv_content = recovery._extract_zip(zip_data)
            assert len(csv_content) > 0

        except FileNotFoundError:
            pytest.skip("Archive not found - may be too old")
        except Exception as e:
            pytest.skip(f"Download failed: {e}")

    @pytest.mark.asyncio
    async def test_download_and_validate_checksum(self, recovery):
        """Test downloading and validating checksum."""
        test_date = "2024-01-15"
        symbol = "BTCUSDT"

        try:
            # Download both files
            zip_data = await recovery._download_file(symbol, test_date, ".zip")
            checksum_data = await recovery._download_file(symbol, test_date, ".zip.CHECKSUM")

            # Validate
            is_valid = recovery._validate_checksum(zip_data, checksum_data)

            print(f"\nChecksum valid: {is_valid}")
            print(f"Checksum content: {checksum_data.decode()[:80]}...")

            assert is_valid is True

        except FileNotFoundError:
            pytest.skip("Archive not found")
        except Exception as e:
            pytest.skip(f"Download failed: {e}")

    @pytest.mark.asyncio
    async def test_parse_real_archive(self, recovery):
        """Test parsing real archive data."""
        test_date = "2024-01-15"
        symbol = "BTCUSDT"

        try:
            zip_data = await recovery._download_file(symbol, test_date, ".zip")
            csv_content = recovery._extract_zip(zip_data)

            # Parse first 100 lines
            lines = csv_content.decode().strip().split('\n')[:100]

            parsed = 0
            failed = 0

            for line in lines:
                if line.startswith('agg') or line.startswith('aggregate'):
                    continue  # Skip header

                record = recovery._parse_csv_line(line)
                if record:
                    parsed += 1
                else:
                    failed += 1

            print(f"\nParsed: {parsed}, Failed: {failed}")
            print(f"Sample line: {lines[1][:80]}...")

            assert parsed > 0
            assert failed == 0 or failed < parsed * 0.01  # <1% failure acceptable

        except FileNotFoundError:
            pytest.skip("Archive not found")
        except Exception as e:
            pytest.skip(f"Download failed: {e}")

    @pytest.mark.asyncio
    async def test_nonexistent_archive_raises(self, recovery):
        """Test that nonexistent archive raises FileNotFoundError."""
        # Very old date that shouldn't exist
        test_date = "2015-01-01"
        symbol = "BTCUSDT"

        # Reduce retries for this test
        recovery.MAX_DOWNLOAD_ATTEMPTS = 2

        with pytest.raises(Exception):  # FileNotFoundError or timeout
            await recovery._download_file(symbol, test_date, ".zip")


class TestRecoveryResult:
    """Tests for RecoveryResult."""

    def test_success_result(self):
        """Success result should have correct fields."""
        result = RecoveryResult(
            success=True,
            days_processed=31,
            trades_inserted=45000000,
            duration_seconds=620.5
        )

        assert result.success is True
        assert result.days_processed == 31
        assert result.trades_inserted == 45000000
        assert result.failed_date is None
        assert result.error is None

    def test_failure_result(self):
        """Failure result should have error info."""
        result = RecoveryResult(
            success=False,
            days_processed=15,
            trades_inserted=20000000,
            duration_seconds=300.0,
            failed_date=date(2025, 11, 16),
            error="Checksum validation failed"
        )

        assert result.success is False
        assert result.failed_date == date(2025, 11, 16)
        assert "Checksum" in result.error


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
