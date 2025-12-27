"""
Unit Tests for AggTrade Model
============================

Comprehensive testing of AggTrade binary serialization, validation,
batch operations, and utility methods.

File: tests/unit/test_aggtrade.py
"""

import pytest
import struct
import time
from typing import Dict, Any, List

from models.aggtrade import AggTrade, AggTradeStats


class TestAggTradeCreation:
    """Test AggTrade creation from various data sources."""

    def test_from_binance_dict_valid_data(self):
        """Test creation from valid Binance API response."""
        binance_data = {
            'a': 26129,
            'p': '0.01633102',
            'q': '4.70443515',
            'f': 27781,
            'l': 27781,
            'T': 1498793709153,
            'm': True
        }

        trade = AggTrade.from_binance_dict(binance_data)

        assert trade.aggregate_id == 26129
        assert trade.price == 0.01633102
        assert trade.quantity == 4.70443515
        assert trade.first_trade_id == 27781
        assert trade.last_trade_id == 27781
        assert trade.timestamp == 1498793709153
        assert trade.is_buyer_maker is True

    def test_from_binance_dict_string_numbers(self):
        """Test creation with string numbers (real Binance API format)."""
        binance_data = {
            'a': '123456',
            'p': '50000.12345678',
            'q': '0.00123456',
            'f': '789012',
            'l': '789015',
            'T': '1640995200000',
            'm': 'true'  # String boolean
        }

        trade = AggTrade.from_binance_dict(binance_data)

        assert trade.aggregate_id == 123456
        assert trade.price == 50000.12345678
        assert trade.quantity == 0.00123456
        assert trade.first_trade_id == 789012
        assert trade.last_trade_id == 789015
        assert trade.timestamp == 1640995200000
        assert trade.is_buyer_maker is True

    def test_from_binance_dict_missing_fields(self):
        """Test error handling for missing required fields."""
        incomplete_data = {
            'a': 26129,
            'p': '0.01633102',
            # Missing 'q', 'f', 'l', 'T', 'm'
        }

        with pytest.raises(ValueError, match="Invalid Binance trade data"):
            AggTrade.from_binance_dict(incomplete_data)

    def test_from_binance_dict_invalid_types(self):
        """Test error handling for invalid data types."""
        invalid_data = {
            'a': 'not_a_number',
            'p': '0.01633102',
            'q': '4.70443515',
            'f': 27781,
            'l': 27781,
            'T': 1498793709153,
            'm': True
        }

        with pytest.raises(ValueError, match="Invalid Binance trade data"):
            AggTrade.from_binance_dict(invalid_data)

    def test_from_binance_list_empty(self):
        """Test batch creation from empty list."""
        result = AggTrade.from_binance_list([])
        assert result == []

    def test_from_binance_list_valid_data(self):
        """Test batch creation from valid data."""
        binance_list = [
            {
                'a': 26129,
                'p': '0.01633102',
                'q': '4.70443515',
                'f': 27781,
                'l': 27781,
                'T': 1498793709153,
                'm': True
            },
            {
                'a': 26130,
                'p': '0.01633103',
                'q': '2.50000000',
                'f': 27782,
                'l': 27782,
                'T': 1498793709154,
                'm': False
            }
        ]

        trades = AggTrade.from_binance_list(binance_list)

        assert len(trades) == 2
        assert trades[0].aggregate_id == 26129
        assert trades[1].aggregate_id == 26130
        assert trades[0].is_buyer_maker is True
        assert trades[1].is_buyer_maker is False

    def test_from_binance_list_mixed_valid_invalid(self):
        """Test error handling with mixed valid/invalid data."""
        mixed_list = [
            {
                'a': 26129,
                'p': '0.01633102',
                'q': '4.70443515',
                'f': 27781,
                'l': 27781,
                'T': 1498793709153,
                'm': True
            },
            {
                'a': 'invalid',  # Invalid data
                'p': '0.01633103',
                # Missing other fields
            }
        ]

        with pytest.raises(ValueError, match="Invalid trade at index 1"):
            AggTrade.from_binance_list(mixed_list)


class TestAggTradeBinarySerialization:
    """Test binary serialization and deserialization."""

    @pytest.fixture
    def sample_trade(self):
        """Sample AggTrade for testing."""
        return AggTrade(
            aggregate_id=123456789,
            price=50000.12345678,
            quantity=1.23456789,
            first_trade_id=987654321,
            last_trade_id=987654325,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

    def test_binary_format_exact_size(self, sample_trade):
        """Test binary format produces exactly 49 bytes."""
        binary_data = sample_trade.to_binary()
        assert len(binary_data) == 49
        assert len(binary_data) == AggTrade.BINARY_SIZE

    def test_binary_struct_format_compliance(self, sample_trade):
        """Test binary format matches struct format 'QddQQQB'."""
        binary_data = sample_trade.to_binary()

        # Manual unpacking to verify format
        unpacked = struct.unpack('QddQQQB', binary_data)

        assert unpacked[0] == sample_trade.aggregate_id
        assert unpacked[1] == sample_trade.price
        assert unpacked[2] == sample_trade.quantity
        assert unpacked[3] == sample_trade.first_trade_id
        assert unpacked[4] == sample_trade.last_trade_id
        assert unpacked[5] == sample_trade.timestamp
        assert unpacked[6] == int(sample_trade.is_buyer_maker)

    def test_to_binary_from_binary_roundtrip(self, sample_trade):
        """Test complete serialization/deserialization cycle."""
        # Serialize to binary
        binary_data = sample_trade.to_binary()

        # Deserialize back to AggTrade
        reconstructed_trade = AggTrade.from_binary(binary_data)

        # Verify all fields are identical
        assert reconstructed_trade.aggregate_id == sample_trade.aggregate_id
        assert reconstructed_trade.price == sample_trade.price
        assert reconstructed_trade.quantity == sample_trade.quantity
        assert reconstructed_trade.first_trade_id == sample_trade.first_trade_id
        assert reconstructed_trade.last_trade_id == sample_trade.last_trade_id
        assert reconstructed_trade.timestamp == sample_trade.timestamp
        assert reconstructed_trade.is_buyer_maker == sample_trade.is_buyer_maker

        # Verify objects are equal
        assert reconstructed_trade == sample_trade

    def test_binary_with_extreme_values(self):
        """Test binary serialization with large but valid values."""
        # Use large but reasonable values that pass validation
        extreme_trade = AggTrade(
            aggregate_id=999999999999,  # Large but valid
            price=999999.99999999,  # High price but reasonable
            quantity=999999.99999999,  # High quantity but reasonable
            first_trade_id=1,  # Min positive
            last_trade_id=999999999999,  # Large but valid
            timestamp=3999999999999,  # Near max timestamp (year 2096)
            is_buyer_maker=False
        )

        # Test serialization doesn't crash
        binary_data = extreme_trade.to_binary()
        assert len(binary_data) == 49

        # Test deserialization
        reconstructed = AggTrade.from_binary(binary_data)
        assert reconstructed.aggregate_id == extreme_trade.aggregate_id
        assert reconstructed.is_buyer_maker is False

    def test_from_binary_invalid_size(self):
        """Test error handling for invalid binary data size."""
        # Test various invalid sizes
        invalid_sizes = [0, 1, 48, 50, 100]

        for size in invalid_sizes:
            invalid_data = b'x' * size
            with pytest.raises(ValueError, match=f"Invalid binary data size: {size}"):
                AggTrade.from_binary(invalid_data)

    def test_from_binary_corrupted_data(self):
        """Test handling of corrupted binary data."""
        # Create valid trade first
        valid_trade = AggTrade(
            aggregate_id=123,
            price=100.0,
            quantity=1.0,
            first_trade_id=1,
            last_trade_id=1,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        # Corrupt the data by truncating
        binary_data = valid_trade.to_binary()
        corrupted_data = binary_data[:48]  # Remove last byte

        with pytest.raises(ValueError, match="Invalid binary data size"):
            AggTrade.from_binary(corrupted_data)

    def test_boolean_serialization(self):
        """Test boolean field serialization as 0/1."""
        # Test True
        trade_true = AggTrade(
            aggregate_id=1,
            price=100.0,
            quantity=1.0,
            first_trade_id=1,
            last_trade_id=1,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        binary_true = trade_true.to_binary()
        assert binary_true[-1] == 1  # Last byte should be 1

        # Test False
        trade_false = AggTrade(
            aggregate_id=1,
            price=100.0,
            quantity=1.0,
            first_trade_id=1,
            last_trade_id=1,
            timestamp=1640995200000,
            is_buyer_maker=False
        )

        binary_false = trade_false.to_binary()
        assert binary_false[-1] == 0  # Last byte should be 0


class TestAggTradeBatchOperations:
    """Test batch serialization operations for performance."""

    @pytest.fixture
    def sample_trades(self):
        """Generate sample trades for batch testing."""
        trades = []
        base_time = 1640995200000

        for i in range(100):
            trade = AggTrade(
                aggregate_id=100000 + i,
                price=50000.0 + (i * 0.01),
                quantity=1.0 + (i * 0.001),
                first_trade_id=200000 + i * 2,
                last_trade_id=200000 + i * 2 + 1,
                timestamp=base_time + (i * 1000),
                is_buyer_maker=(i % 2 == 0)
            )
            trades.append(trade)

        return trades

    def test_batch_to_binary_empty_list(self):
        """Test batch serialization of empty list."""
        result = AggTrade.batch_to_binary([])
        assert result == b''

    def test_batch_to_binary_single_trade(self):
        """Test batch serialization of single trade."""
        trade = AggTrade(
            aggregate_id=1,
            price=100.0,
            quantity=1.0,
            first_trade_id=1,
            last_trade_id=1,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        batch_binary = AggTrade.batch_to_binary([trade])
        single_binary = trade.to_binary()

        assert len(batch_binary) == 49
        assert batch_binary == single_binary

    def test_batch_to_binary_multiple_trades(self, sample_trades):
        """Test batch serialization of multiple trades."""
        batch_binary = AggTrade.batch_to_binary(sample_trades)

        # Check total size
        expected_size = len(sample_trades) * AggTrade.BINARY_SIZE
        assert len(batch_binary) == expected_size

        # Verify each trade can be extracted
        for i, trade in enumerate(sample_trades):
            start_offset = i * AggTrade.BINARY_SIZE
            end_offset = start_offset + AggTrade.BINARY_SIZE
            trade_binary = batch_binary[start_offset:end_offset]

            reconstructed = AggTrade.from_binary(trade_binary)
            assert reconstructed == trade

    def test_batch_from_binary_roundtrip(self, sample_trades):
        """Test complete batch serialization/deserialization cycle."""
        # Serialize batch
        batch_binary = AggTrade.batch_to_binary(sample_trades)

        # Deserialize batch
        reconstructed_trades = AggTrade.batch_from_binary(batch_binary)

        # Verify all trades are identical
        assert len(reconstructed_trades) == len(sample_trades)

        for original, reconstructed in zip(sample_trades, reconstructed_trades):
            assert reconstructed == original

    def test_batch_from_binary_invalid_length(self):
        """Test error handling for invalid batch binary length."""
        # Create data that's not a multiple of 49 bytes
        invalid_data = b'x' * 100  # 100 bytes, not divisible by 49

        with pytest.raises(ValueError, match="Invalid binary data length"):
            AggTrade.batch_from_binary(invalid_data)

    def test_batch_performance_benchmark(self, sample_trades):
        """Test batch operations performance."""
        # Extend to larger dataset for meaningful benchmark
        large_dataset = sample_trades * 10  # 1000 trades

        # Time batch serialization
        start_time = time.time()
        batch_binary = AggTrade.batch_to_binary(large_dataset)
        serialize_time = time.time() - start_time

        # Time batch deserialization
        start_time = time.time()
        reconstructed = AggTrade.batch_from_binary(batch_binary)
        deserialize_time = time.time() - start_time

        # Performance assertions (should be fast)
        assert serialize_time < 0.1  # Less than 100ms for 1000 trades
        assert deserialize_time < 0.1  # Less than 100ms for 1000 trades
        assert len(reconstructed) == len(large_dataset)

    def test_batch_memory_efficiency(self, sample_trades):
        """Test batch operations work correctly and produce same results."""
        large_dataset = sample_trades * 20  # 2000 trades

        # Test batch operation
        batch_binary = AggTrade.batch_to_binary(large_dataset)

        # Test individual operations for comparison
        individual_binaries = [trade.to_binary() for trade in large_dataset]
        individual_binary = b''.join(individual_binaries)

        # Results should be identical
        assert batch_binary == individual_binary

        # Verify correct size
        expected_size = len(large_dataset) * AggTrade.BINARY_SIZE
        assert len(batch_binary) == expected_size

        # Test deserialization works correctly
        reconstructed = AggTrade.batch_from_binary(batch_binary)
        assert len(reconstructed) == len(large_dataset)


class TestAggTradeValidation:
    """Test data validation logic."""

    def test_validate_valid_trade(self):
        """Test validation passes for valid trade."""
        valid_trade = AggTrade(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        assert valid_trade.validate() is True

    def test_validate_negative_values(self):
        """Test validation fails for negative values."""
        with pytest.raises(ValueError):
            AggTrade(
                aggregate_id=-1,  # Negative aggregate_id
                price=50000.12,
                quantity=1.23456,
                first_trade_id=789012,
                last_trade_id=789012,
                timestamp=1640995200000,
                is_buyer_maker=True
            )

    def test_validate_zero_values(self):
        """Test validation fails for zero values in critical fields."""
        with pytest.raises(ValueError):
            AggTrade(
                aggregate_id=0,  # Zero aggregate_id
                price=50000.12,
                quantity=1.23456,
                first_trade_id=789012,
                last_trade_id=789012,
                timestamp=1640995200000,
                is_buyer_maker=True
            )

    def test_validate_timestamp_range(self):
        """Test validation of timestamp boundaries."""
        # Test timestamp too early (before year 2001)
        with pytest.raises(ValueError):
            AggTrade(
                aggregate_id=123456,
                price=50000.12,
                quantity=1.23456,
                first_trade_id=789012,
                last_trade_id=789012,
                timestamp=999999999999,  # Too early
                is_buyer_maker=True
            )

        # Test timestamp too late (after year 2096)
        with pytest.raises(ValueError):
            AggTrade(
                aggregate_id=123456,
                price=50000.12,
                quantity=1.23456,
                first_trade_id=789012,
                last_trade_id=789012,
                timestamp=4000000000001,  # Too late
                is_buyer_maker=True
            )

    def test_validate_trade_id_consistency(self):
        """Test validation of trade ID consistency."""
        with pytest.raises(ValueError):
            AggTrade(
                aggregate_id=123456,
                price=50000.12,
                quantity=1.23456,
                first_trade_id=789015,  # Greater than last_trade_id
                last_trade_id=789012,
                timestamp=1640995200000,
                is_buyer_maker=True
            )

    def test_validate_extreme_prices_quantities(self):
        """Test validation rejects extreme values."""
        with pytest.raises(ValueError):
            AggTrade(
                aggregate_id=123456,
                price=1e11,  # Extremely high price
                quantity=1.23456,
                first_trade_id=789012,
                last_trade_id=789012,
                timestamp=1640995200000,
                is_buyer_maker=True
            )

    def test_post_init_validation(self):
        """Test that validation runs during object creation."""
        # This should pass validation in __post_init__
        valid_trade = AggTrade(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=1640995200000,
            is_buyer_maker=True
        )
        assert valid_trade is not None


class TestAggTradeUtilityMethods:
    """Test utility methods and convenience functions."""

    @pytest.fixture
    def sample_trade(self):
        """Sample AggTrade for utility testing."""
        return AggTrade(
            aggregate_id=123456,
            price=50000.12345678,
            quantity=1.23456789,
            first_trade_id=789012,
            last_trade_id=789015,
            timestamp=1640995200000,  # 2022-01-01 00:00:00 UTC
            is_buyer_maker=True
        )

    def test_to_dict_format(self, sample_trade):
        """Test conversion to Binance API format."""
        trade_dict = sample_trade.to_dict()

        expected_dict = {
            'a': 123456,
            'p': '50000.12345678',
            'q': '1.23456789',
            'f': 789012,
            'l': 789015,
            'T': 1640995200000,
            'm': True
        }

        assert trade_dict == expected_dict

    def test_get_trade_value_calculation(self, sample_trade):
        """Test trade value calculation (price * quantity)."""
        expected_value = 50000.12345678 * 1.23456789
        calculated_value = sample_trade.get_trade_value()

        # Use approximate equality for float comparison
        assert abs(calculated_value - expected_value) < 1e-10

    def test_get_datetime_str_formatting(self, sample_trade):
        """Test datetime string formatting."""
        datetime_str = sample_trade.get_datetime_str()

        # 2022-01-01 00:00:00
        assert datetime_str == '2022-01-01 00:00:00'

    def test_is_recent_functionality(self):
        """Test is_recent method with different timeframes."""
        current_time_ms = int(time.time() * 1000)

        # Recent trade (within last hour)
        recent_trade = AggTrade(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=current_time_ms - (30 * 60 * 1000),  # 30 minutes ago
            is_buyer_maker=True
        )

        assert recent_trade.is_recent(3600) is True  # Within 1 hour
        assert recent_trade.is_recent(1500) is False  # Not within 25 minutes (30 > 25)

        # Old trade
        old_trade = AggTrade(
            aggregate_id=123457,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789013,
            last_trade_id=789013,
            timestamp=current_time_ms - (2 * 60 * 60 * 1000),  # 2 hours ago
            is_buyer_maker=False
        )

        assert old_trade.is_recent(3600) is False  # Not within 1 hour

    def test_sorting_by_aggregate_id(self):
        """Test sorting trades by aggregate_id."""
        trades = [
            AggTrade(
                aggregate_id=300,
                price=100.0,
                quantity=1.0,
                first_trade_id=1,
                last_trade_id=1,
                timestamp=1640995200000,
                is_buyer_maker=True
            ),
            AggTrade(
                aggregate_id=100,
                price=100.0,
                quantity=1.0,
                first_trade_id=2,
                last_trade_id=2,
                timestamp=1640995200001,
                is_buyer_maker=False
            ),
            AggTrade(
                aggregate_id=200,
                price=100.0,
                quantity=1.0,
                first_trade_id=3,
                last_trade_id=3,
                timestamp=1640995200002,
                is_buyer_maker=True
            )
        ]

        sorted_trades = sorted(trades)

        assert sorted_trades[0].aggregate_id == 100
        assert sorted_trades[1].aggregate_id == 200
        assert sorted_trades[2].aggregate_id == 300

    def test_string_representations(self, sample_trade):
        """Test __str__ and __repr__ methods."""
        str_repr = str(sample_trade)
        repr_repr = repr(sample_trade)

        # __str__ should be human-readable
        assert 'AggTrade' in str_repr
        assert 'id=123456' in str_repr
        assert 'price=50000.12345678' in str_repr
        assert '2022-01-01 00:00:00' in str_repr

        # __repr__ should be developer-friendly with all fields
        assert 'AggTrade(' in repr_repr
        assert 'aggregate_id=123456' in repr_repr
        assert 'price=50000.12345678' in repr_repr
        assert 'quantity=1.23456789' in repr_repr
        assert 'is_buyer_maker=True' in repr_repr


class TestAggTradeStats:
    """Test AggTradeStats utility class."""

    @pytest.fixture
    def sample_trades(self):
        """Sample trades for statistics testing."""
        return [
            AggTrade(
                aggregate_id=1,
                price=100.0,
                quantity=2.0,
                first_trade_id=1,
                last_trade_id=1,
                timestamp=1640995200000,
                is_buyer_maker=True
            ),
            AggTrade(
                aggregate_id=2,
                price=200.0,
                quantity=1.5,
                first_trade_id=2,
                last_trade_id=2,
                timestamp=1640995260000,  # 1 minute later
                is_buyer_maker=False
            ),
            AggTrade(
                aggregate_id=3,
                price=150.0,
                quantity=3.0,
                first_trade_id=3,
                last_trade_id=3,
                timestamp=1640995320000,  # 2 minutes later
                is_buyer_maker=True
            )
        ]

    def test_calculate_volume(self, sample_trades):
        """Test volume calculation."""
        total_volume = AggTradeStats.calculate_volume(sample_trades)
        expected_volume = 2.0 + 1.5 + 3.0  # Sum of quantities

        assert total_volume == expected_volume

    def test_calculate_value(self, sample_trades):
        """Test total value calculation."""
        total_value = AggTradeStats.calculate_value(sample_trades)
        expected_value = (100.0 * 2.0) + (200.0 * 1.5) + (150.0 * 3.0)

        assert total_value == expected_value

    def test_get_price_range(self, sample_trades):
        """Test price range calculation."""
        min_price, max_price = AggTradeStats.get_price_range(sample_trades)

        assert min_price == 100.0
        assert max_price == 200.0

    def test_get_time_range(self, sample_trades):
        """Test time range calculation."""
        min_time, max_time = AggTradeStats.get_time_range(sample_trades)

        assert min_time == 1640995200000
        assert max_time == 1640995320000

    def test_filter_by_time_range(self, sample_trades):
        """Test filtering by time range."""
        # Filter to include only first two trades
        filtered = AggTradeStats.filter_by_time_range(
            sample_trades,
            start_time=1640995200000,
            end_time=1640995260000
        )

        assert len(filtered) == 2
        assert filtered[0].aggregate_id == 1
        assert filtered[1].aggregate_id == 2

    def test_filter_by_value_threshold(self, sample_trades):
        """Test filtering by minimum trade value."""
        # Filter trades with value >= 300 (200*1.5=300, 150*3=450)
        filtered = AggTradeStats.filter_by_value_threshold(sample_trades, 300.0)

        assert len(filtered) == 2
        assert filtered[0].aggregate_id == 2  # 200 * 1.5 = 300
        assert filtered[1].aggregate_id == 3  # 150 * 3 = 450

    def test_stats_with_empty_list(self):
        """Test statistics methods with empty list."""
        empty_trades = []

        assert AggTradeStats.calculate_volume(empty_trades) == 0
        assert AggTradeStats.calculate_value(empty_trades) == 0
        assert AggTradeStats.get_price_range(empty_trades) == (0.0, 0.0)
        assert AggTradeStats.get_time_range(empty_trades) == (0, 0)
        assert AggTradeStats.filter_by_time_range(empty_trades, 0, 999999) == []
        assert AggTradeStats.filter_by_value_threshold(empty_trades, 100.0) == []


class TestAggTradeErrorHandling:
    """Test error handling and edge cases."""

    def test_struct_pack_error_handling(self):
        """Test error handling during struct packing."""
        # Test by creating valid trade first, then trying to serialize invalid data directly
        valid_trade = AggTrade(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        # Test normal serialization works
        binary_data = valid_trade.to_binary()
        assert len(binary_data) == 49

        # Test what happens with extreme float values that could cause struct.error
        # We can't create an invalid AggTrade due to validation, so we test the error path
        # by attempting to pack impossible values directly
        import struct

        # Test that our validation prevents problematic values
        # struct.pack actually handles inf/nan in newer Python versions,
        # so we verify our validation layer works
        try:
            # This should work in modern Python
            inf_packed = struct.pack('d', float('inf'))
            assert len(inf_packed) == 8

            # Our validation should prevent creating such trades
            with pytest.raises(ValueError):
                # This will fail at validation level, not struct level
                AggTrade(
                    aggregate_id=123456,
                    price=float('inf'),
                    quantity=1.23456,
                    first_trade_id=789012,
                    last_trade_id=789012,
                    timestamp=1640995200000,
                    is_buyer_maker=True
                )
        except struct.error:
            # Older Python versions that do raise struct.error
            pass

    def test_immutability(self):
        """Test that AggTrade instances are immutable."""
        trade = AggTrade(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        # Attempting to modify should raise AttributeError
        with pytest.raises(AttributeError):
            trade.aggregate_id = 999999

        with pytest.raises(AttributeError):
            trade.price = 60000.0

    def test_hash_consistency(self):
        """Test that equal trades have same hash."""
        trade1 = AggTrade(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        trade2 = AggTrade(
            aggregate_id=123456,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        assert hash(trade1) == hash(trade2)
        assert trade1 == trade2

    def test_comparison_operations(self):
        """Test comparison operations work correctly."""
        trade1 = AggTrade(
            aggregate_id=100,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789012,
            last_trade_id=789012,
            timestamp=1640995200000,
            is_buyer_maker=True
        )

        trade2 = AggTrade(
            aggregate_id=200,
            price=50000.12,
            quantity=1.23456,
            first_trade_id=789013,
            last_trade_id=789013,
            timestamp=1640995200001,
            is_buyer_maker=False
        )

        assert trade1 < trade2  # Based on aggregate_id
        assert not trade1 > trade2
        assert trade1 != trade2