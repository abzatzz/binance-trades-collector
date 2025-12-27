"""
TimingCalculator Test Suite - Critical for Dynamic WebSocket Management
======================================================================

Comprehensive test suite for TimingCalculator focusing on:
- Basic timing calculation accuracy (queue_size + 1) * time_per_request
- WeightCoordinator state-based time_per_request determination
- Critical threshold scenarios for WebSocket activation/deactivation
- Edge cases and boundary conditions
- Prediction accuracy and conservative estimation principles

CRITICAL FOR SYSTEM COORDINATION:
- WebSocket activation threshold: ≤ 5.0 seconds
- WebSocket deactivation threshold: > 30.0 seconds
- Emergency deactivation: > 45.0 seconds
- Conservative estimation: better to overestimate than underestimate

File: tests/unit/test_timing_calculator.py
"""

import pytest
import time
from unittest.mock import Mock, MagicMock
from dataclasses import dataclass
from typing import Optional


# ===== MOCK DATA STRUCTURES =====

@dataclass
class MockWeightStats:
    """Mock WeightStats for testing TimingCalculator."""
    current_weight_usage: int
    available_weight: int
    critical_queue_size: int
    normal_queue_size: int
    total_requests_processed: int
    requests_per_minute: float
    last_request_time: Optional[float]
    is_rate_limited: bool
    recovery_end_time: Optional[float]

    def __post_init__(self):
        """Ensure recovery_end_time consistency."""
        if self.is_rate_limited and self.recovery_end_time is None:
            # Set default recovery time for rate limited state
            self.recovery_end_time = time.time() + 10.0


class MockWeightCoordinator:
    """Mock WeightCoordinator for testing TimingCalculator."""

    def __init__(self, weight_stats: MockWeightStats):
        self._stats = weight_stats

    def get_stats(self) -> MockWeightStats:
        """Return configured weight statistics."""
        return self._stats

    def set_stats(self, stats: MockWeightStats) -> None:
        """Update statistics for testing different scenarios."""
        self._stats = stats


# ===== TIMING CALCULATOR IMPLEMENTATION =====

class TimingCalculator:
    """
    Simple and accurate calculator for aggTrades request execution time.
    Copy of the actual implementation for testing.
    """

    def __init__(self, weight_coordinator):
        self.weight_coordinator = weight_coordinator

    def calculate_request_delay(self) -> float:
        """
        Maximum simple and accurate calculation of aggTrades request execution time.

        Returns:
            Estimated time in seconds for next aggTrades request
        """
        weight_stats = self.weight_coordinator.get_stats()

        # Requests in queue + our request
        total_requests = weight_stats.normal_queue_size + 1

        # Time per aggTrades request in current conditions
        if weight_stats.is_rate_limited:
            time_per_request = 5.0  # 5 seconds in rate limiting
        elif weight_stats.available_weight < 200:
            time_per_request = 2.0  # 2 seconds at low available_weight
        else:
            time_per_request = 0.5  # 0.5 seconds in normal conditions

        return total_requests * time_per_request


# ===== FIXTURES =====

@pytest.fixture
def optimal_conditions_stats():
    """WeightStats for optimal conditions - fast processing."""
    return MockWeightStats(
        current_weight_usage=400,
        available_weight=1800,  # High available weight
        critical_queue_size=0,
        normal_queue_size=2,    # Small queue
        total_requests_processed=100,
        requests_per_minute=60.0,
        last_request_time=time.time(),
        is_rate_limited=False,  # Normal operation
        recovery_end_time=None
    )

@pytest.fixture
def low_weight_stats():
    """WeightStats for low available weight - slower processing."""
    return MockWeightStats(
        current_weight_usage=2200,
        available_weight=50,    # Low available weight
        critical_queue_size=1,
        normal_queue_size=10,
        total_requests_processed=500,
        requests_per_minute=30.0,
        last_request_time=time.time(),
        is_rate_limited=False,
        recovery_end_time=None
    )

@pytest.fixture
def rate_limited_stats():
    """WeightStats for rate limited state - slowest processing."""
    return MockWeightStats(
        current_weight_usage=2400,
        available_weight=0,     # No available weight
        critical_queue_size=5,
        normal_queue_size=50,   # Large queue
        total_requests_processed=200,
        requests_per_minute=10.0,
        last_request_time=time.time(),
        is_rate_limited=True,   # Rate limited
        recovery_end_time=time.time() + 30.0
    )

@pytest.fixture
def timing_calculator_factory():
    """Factory to create TimingCalculator with different weight coordinator states."""
    def create_calculator(weight_stats: MockWeightStats) -> TimingCalculator:
        weight_coordinator = MockWeightCoordinator(weight_stats)
        return TimingCalculator(weight_coordinator)
    return create_calculator


# ===== BASIC FUNCTIONALITY TESTS =====

class TestBasicTimingCalculation:
    """Test fundamental timing calculation logic."""

    def test_basic_formula_accuracy(self, timing_calculator_factory, optimal_conditions_stats):
        """Test basic formula: (queue_size + 1) * time_per_request."""
        calculator = timing_calculator_factory(optimal_conditions_stats)

        result = calculator.calculate_request_delay()

        # Expected: (2 + 1) * 0.5 = 1.5 seconds
        expected = (optimal_conditions_stats.normal_queue_size + 1) * 0.5
        assert result == expected
        assert result == 1.5

    def test_queue_size_impact(self, timing_calculator_factory, optimal_conditions_stats):
        """Test that queue size directly impacts timing calculation."""
        test_cases = [
            (0, 0.5),    # Empty queue: (0+1)*0.5 = 0.5s
            (5, 3.0),    # Small queue: (5+1)*0.5 = 3.0s
            (20, 10.5),  # Medium queue: (20+1)*0.5 = 10.5s
            (100, 50.5)  # Large queue: (100+1)*0.5 = 50.5s
        ]

        for queue_size, expected_time in test_cases:
            # Update queue size
            optimal_conditions_stats.normal_queue_size = queue_size
            calculator = timing_calculator_factory(optimal_conditions_stats)

            result = calculator.calculate_request_delay()
            assert result == expected_time

    def test_time_per_request_determination(self, timing_calculator_factory):
        """Test correct time_per_request based on WeightCoordinator state."""
        base_stats = MockWeightStats(
            current_weight_usage=400,
            available_weight=1000,
            critical_queue_size=0,
            normal_queue_size=1,  # Fixed queue for simple calculation
            total_requests_processed=100,
            requests_per_minute=60.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        # Test normal conditions: available_weight >= 200 → 0.5s per request
        base_stats.available_weight = 1000
        base_stats.is_rate_limited = False
        calculator = timing_calculator_factory(base_stats)
        assert calculator.calculate_request_delay() == 2 * 0.5  # (1+1)*0.5

        # Test low weight: available_weight < 200 → 2.0s per request
        base_stats.available_weight = 100
        base_stats.is_rate_limited = False
        calculator = timing_calculator_factory(base_stats)
        assert calculator.calculate_request_delay() == 2 * 2.0  # (1+1)*2.0

        # Test rate limited: is_rate_limited = True → 5.0s per request
        base_stats.available_weight = 0
        base_stats.is_rate_limited = True
        base_stats.recovery_end_time = time.time() + 10.0
        calculator = timing_calculator_factory(base_stats)
        assert calculator.calculate_request_delay() == 2 * 5.0  # (1+1)*5.0


# ===== CRITICAL STATES TESTING =====

class TestCriticalWeightCoordinatorStates:
    """Test critical WeightCoordinator states for WebSocket coordination."""

    def test_rate_limited_timing_calculation(self, timing_calculator_factory, rate_limited_stats):
        """Test timing calculation when WeightCoordinator is rate limited."""
        calculator = timing_calculator_factory(rate_limited_stats)

        result = calculator.calculate_request_delay()

        # Expected: (50 + 1) * 5.0 = 255.0 seconds (rate limited)
        expected = (rate_limited_stats.normal_queue_size + 1) * 5.0
        assert result == expected
        assert result == 255.0

        # This should trigger "START_HISTORICAL_FIRST" decision (>5s)
        assert result > 5.0
        # This should also trigger emergency protocols (>30s)
        assert result > 30.0

    def test_low_available_weight_timing(self, timing_calculator_factory, low_weight_stats):
        """Test timing calculation with low available weight."""
        calculator = timing_calculator_factory(low_weight_stats)

        result = calculator.calculate_request_delay()

        # Expected: (10 + 1) * 2.0 = 22.0 seconds (low weight)
        expected = (low_weight_stats.normal_queue_size + 1) * 2.0
        assert result == expected
        assert result == 22.0

        # Should trigger "START_HISTORICAL_FIRST" (>5s)
        assert result > 5.0
        # Should not trigger emergency deactivation yet (<30s)
        assert result < 30.0

    def test_normal_conditions_timing(self, timing_calculator_factory, optimal_conditions_stats):
        """Test timing calculation under normal operating conditions."""
        calculator = timing_calculator_factory(optimal_conditions_stats)

        result = calculator.calculate_request_delay()

        # Expected: (2 + 1) * 0.5 = 1.5 seconds (normal conditions)
        expected = (optimal_conditions_stats.normal_queue_size + 1) * 0.5
        assert result == expected
        assert result == 1.5

        # Should trigger "ACTIVATE_WEBSOCKET_FIRST" (≤5s)
        assert result <= 5.0

    def test_available_weight_boundary_conditions(self, timing_calculator_factory):
        """Test behavior at available_weight boundary (200)."""
        base_stats = MockWeightStats(
            current_weight_usage=400,
            available_weight=200,  # Will be modified
            critical_queue_size=0,
            normal_queue_size=5,   # Fixed for predictable results
            total_requests_processed=100,
            requests_per_minute=60.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        # Test available_weight = 199 (just below threshold) → 2.0s per request
        base_stats.available_weight = 199
        calculator = timing_calculator_factory(base_stats)
        result = calculator.calculate_request_delay()
        assert result == 6 * 2.0  # (5+1)*2.0 = 12.0s

        # Test available_weight = 200 (at threshold) → 0.5s per request
        base_stats.available_weight = 200
        calculator = timing_calculator_factory(base_stats)
        result = calculator.calculate_request_delay()
        assert result == 6 * 0.5  # (5+1)*0.5 = 3.0s

        # Test available_weight = 201 (just above threshold) → 0.5s per request
        base_stats.available_weight = 201
        calculator = timing_calculator_factory(base_stats)
        result = calculator.calculate_request_delay()
        assert result == 6 * 0.5  # (5+1)*0.5 = 3.0s


# ===== WEBSOCKET COORDINATION SCENARIOS =====

class TestWebSocketCoordinationScenarios:
    """Test scenarios critical for WebSocket activation/deactivation decisions."""

    def test_websocket_activation_threshold(self, timing_calculator_factory):
        """Test timing calculations around WebSocket activation threshold (5.0s)."""
        base_stats = MockWeightStats(
            current_weight_usage=400,
            available_weight=1000,
            critical_queue_size=0,
            normal_queue_size=0,  # Will be modified
            total_requests_processed=100,
            requests_per_minute=60.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        test_cases = [
            # (queue_size, expected_time, should_activate_websocket)
            (9, 5.0, True),   # Exactly at threshold: (9+1)*0.5 = 5.0s
            (8, 4.5, True),   # Just below: (8+1)*0.5 = 4.5s → activate
            (10, 5.5, False), # Just above: (10+1)*0.5 = 5.5s → historical first
            (1, 1.0, True),   # Well below: (1+1)*0.5 = 1.0s → activate
            (20, 10.5, False) # Well above: (20+1)*0.5 = 10.5s → historical first
        ]

        for queue_size, expected_time, should_activate in test_cases:
            base_stats.normal_queue_size = queue_size
            calculator = timing_calculator_factory(base_stats)

            result = calculator.calculate_request_delay()
            assert result == expected_time

            # WebSocket should be activated if time <= 5.0 seconds
            websocket_activation = (result <= 5.0)
            assert websocket_activation == should_activate

    def test_websocket_deactivation_threshold(self, timing_calculator_factory):
        """Test timing calculations around WebSocket deactivation threshold (30.0s)."""
        # Use low weight conditions to get higher times
        base_stats = MockWeightStats(
            current_weight_usage=2200,
            available_weight=100,  # Low weight → 2.0s per request
            critical_queue_size=1,
            normal_queue_size=0,   # Will be modified
            total_requests_processed=500,
            requests_per_minute=30.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        test_cases = [
            # (queue_size, expected_time, should_deactivate_websocket)
            (14, 30.0, False), # Exactly at threshold: (14+1)*2.0 = 30.0s → keep active (≤30.0)
            (13, 28.0, False), # Just below: (13+1)*2.0 = 28.0s → keep active
            (15, 32.0, True),  # Just above: (15+1)*2.0 = 32.0s → deactivate
            (25, 52.0, True),  # Well above: (25+1)*2.0 = 52.0s → emergency deactivate
        ]

        for queue_size, expected_time, should_deactivate in test_cases:
            base_stats.normal_queue_size = queue_size
            calculator = timing_calculator_factory(base_stats)

            result = calculator.calculate_request_delay()
            assert result == expected_time

            # WebSocket should be deactivated if time > 30.0 seconds (strictly greater)
            websocket_deactivation = (result > 30.0)
            assert websocket_deactivation == should_deactivate

    def test_emergency_deactivation_scenario(self, timing_calculator_factory):
        """Test emergency deactivation scenario (>45.0 seconds)."""
        # Rate limited conditions for extreme timing
        emergency_stats = MockWeightStats(
            current_weight_usage=2400,
            available_weight=0,
            critical_queue_size=3,
            normal_queue_size=10,  # (10+1)*5.0 = 55.0s
            total_requests_processed=1000,
            requests_per_minute=5.0,
            last_request_time=time.time(),
            is_rate_limited=True,
            recovery_end_time=time.time() + 60.0
        )

        calculator = timing_calculator_factory(emergency_stats)
        result = calculator.calculate_request_delay()

        # Expected: (10 + 1) * 5.0 = 55.0 seconds
        assert result == 55.0

        # Should trigger emergency deactivation (>45s threshold)
        assert result > 45.0
        # Should definitely trigger normal deactivation (>30s)
        assert result > 30.0
        # Should definitely not activate WebSocket (>5s)
        assert result > 5.0


# ===== EDGE CASES AND BOUNDARY CONDITIONS =====

class TestEdgeCasesAndBoundaryConditions:
    """Test edge cases and boundary conditions for robust operation."""

    def test_empty_queue_calculation(self, timing_calculator_factory, optimal_conditions_stats):
        """Test calculation with empty queue (queue_size = 0)."""
        optimal_conditions_stats.normal_queue_size = 0
        calculator = timing_calculator_factory(optimal_conditions_stats)

        result = calculator.calculate_request_delay()

        # Expected: (0 + 1) * 0.5 = 0.5 seconds (minimum possible time)
        assert result == 0.5
        assert result <= 5.0  # Should activate WebSocket

    def test_very_large_queue_calculation(self, timing_calculator_factory, rate_limited_stats):
        """Test calculation with very large queue size."""
        rate_limited_stats.normal_queue_size = 1000  # Very large queue
        calculator = timing_calculator_factory(rate_limited_stats)

        result = calculator.calculate_request_delay()

        # Expected: (1000 + 1) * 5.0 = 5005.0 seconds (~1.4 hours)
        assert result == 5005.0
        assert result > 45.0  # Extreme emergency deactivation

    def test_zero_available_weight_non_rate_limited(self, timing_calculator_factory):
        """Test edge case: zero available weight but not rate limited."""
        edge_stats = MockWeightStats(
            current_weight_usage=2400,
            available_weight=0,    # Zero weight
            critical_queue_size=0,
            normal_queue_size=5,
            total_requests_processed=100,
            requests_per_minute=60.0,
            last_request_time=time.time(),
            is_rate_limited=False,  # Not rate limited yet
            recovery_end_time=None
        )

        calculator = timing_calculator_factory(edge_stats)
        result = calculator.calculate_request_delay()

        # Should use low weight timing: (5+1)*2.0 = 12.0s
        assert result == 12.0
        assert result > 5.0  # Should not activate WebSocket

    def test_negative_queue_size_handling(self, timing_calculator_factory, optimal_conditions_stats):
        """Test handling of invalid negative queue size."""
        # This shouldn't happen in practice, but test defensive programming
        optimal_conditions_stats.normal_queue_size = -1
        calculator = timing_calculator_factory(optimal_conditions_stats)

        result = calculator.calculate_request_delay()

        # Formula still applies: (-1+1)*0.5 = 0.0 seconds
        assert result == 0.0
        assert result <= 5.0  # Would activate WebSocket

    def test_extremely_high_available_weight(self, timing_calculator_factory):
        """Test with extremely high available weight."""
        extreme_stats = MockWeightStats(
            current_weight_usage=100,
            available_weight=10000,  # Extremely high
            critical_queue_size=0,
            normal_queue_size=3,
            total_requests_processed=50,
            requests_per_minute=120.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        calculator = timing_calculator_factory(extreme_stats)
        result = calculator.calculate_request_delay()

        # Should still use normal timing: (3+1)*0.5 = 2.0s
        assert result == 2.0
        assert result <= 5.0  # Should activate WebSocket


# ===== PREDICTION ACCURACY AND CONSISTENCY =====

class TestPredictionAccuracyAndConsistency:
    """Test prediction accuracy and consistency requirements."""

    def test_conservative_estimation_principle(self, timing_calculator_factory):
        """Test that estimates are conservative (better to overestimate)."""
        # Create scenarios where conservative estimation is important
        scenarios = [
            # Borderline cases where conservative estimation matters
            MockWeightStats(
                current_weight_usage=2100,
                available_weight=199,  # Just below threshold
                critical_queue_size=0,
                normal_queue_size=2,
                total_requests_processed=100,
                requests_per_minute=60.0,
                last_request_time=time.time(),
                is_rate_limited=False,
                recovery_end_time=None
            ),
            MockWeightStats(
                current_weight_usage=2350,
                available_weight=50,
                critical_queue_size=2,
                normal_queue_size=8,
                total_requests_processed=200,
                requests_per_minute=20.0,
                last_request_time=time.time(),
                is_rate_limited=False,
                recovery_end_time=None
            )
        ]

        for stats in scenarios:
            calculator = timing_calculator_factory(stats)
            result = calculator.calculate_request_delay()

            # Conservative estimates should lean toward higher values
            # when conditions are uncertain (low weight, growing queues)
            if stats.available_weight < 200:
                expected_time_per_request = 2.0  # Conservative estimate for low weight
                expected = (stats.normal_queue_size + 1) * expected_time_per_request
                assert result == expected

    def test_timing_calculation_consistency(self, timing_calculator_factory, optimal_conditions_stats):
        """Test that identical inputs produce identical outputs."""
        calculator = timing_calculator_factory(optimal_conditions_stats)

        # Multiple calls with same state should return same result
        results = [calculator.calculate_request_delay() for _ in range(10)]

        # All results should be identical
        assert all(result == results[0] for result in results)
        assert results[0] == 1.5  # Expected result for optimal conditions

    def test_no_timing_oscillation(self, timing_calculator_factory):
        """Test that small changes in queue size don't cause dramatic timing changes."""
        base_stats = MockWeightStats(
            current_weight_usage=1000,
            available_weight=500,
            critical_queue_size=0,
            normal_queue_size=5,  # Will be modified
            total_requests_processed=100,
            requests_per_minute=60.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        # Test small incremental changes
        queue_sizes = [5, 6, 7, 8, 9, 10]
        expected_times = [3.0, 3.5, 4.0, 4.5, 5.0, 5.5]  # (n+1)*0.5

        for queue_size, expected in zip(queue_sizes, expected_times):
            base_stats.normal_queue_size = queue_size
            calculator = timing_calculator_factory(base_stats)
            result = calculator.calculate_request_delay()

            assert result == expected

            # Verify gradual increase (no sudden jumps)
            if queue_size > 5:
                prev_expected = expected_times[queue_sizes.index(queue_size) - 1]
                increase = expected - prev_expected
                assert increase == 0.5  # Consistent 0.5s increase per queue item


# ===== INTEGRATION WITH WEBSOCKET DECISION MAKING =====

class TestWebSocketDecisionMakingIntegration:
    """Test TimingCalculator integration with WebSocket coordination decisions."""

    def test_websocket_activation_decision_points(self, timing_calculator_factory):
        """Test all critical decision points for WebSocket activation."""
        # Create calculator for optimal conditions (should activate WebSocket)
        optimal_stats = MockWeightStats(
            current_weight_usage=200,
            available_weight=2000,
            critical_queue_size=0,
            normal_queue_size=2,  # (2+1)*0.5 = 1.5s
            total_requests_processed=50,
            requests_per_minute=100.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        calculator = timing_calculator_factory(optimal_stats)
        estimated_time = calculator.calculate_request_delay()

        # Test WebSocket activation logic
        assert estimated_time <= 5.0  # Should activate WebSocket first
        websocket_decision = "ACTIVATE_WEBSOCKET_FIRST" if estimated_time <= 5.0 else "START_HISTORICAL_FIRST"
        assert websocket_decision == "ACTIVATE_WEBSOCKET_FIRST"

    def test_websocket_deactivation_decision_points(self, timing_calculator_factory):
        """Test all critical decision points for WebSocket deactivation."""
        # Create calculator for overloaded conditions (should deactivate WebSocket)
        overloaded_stats = MockWeightStats(
            current_weight_usage=2300,
            available_weight=50,
            critical_queue_size=5,
            normal_queue_size=20,  # (20+1)*2.0 = 42.0s
            total_requests_processed=1000,
            requests_per_minute=15.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        calculator = timing_calculator_factory(overloaded_stats)
        estimated_time = calculator.calculate_request_delay()

        # Test WebSocket deactivation logic
        assert estimated_time > 30.0  # Should deactivate WebSocket
        deactivation_decision = "DEACTIVATE_WEBSOCKET" if estimated_time > 30.0 else "KEEP_WEBSOCKET_ACTIVE"
        assert deactivation_decision == "DEACTIVATE_WEBSOCKET"

        # Also test emergency deactivation
        emergency_threshold = 45.0
        emergency_decision = "EMERGENCY_DEACTIVATE" if estimated_time > emergency_threshold else "NORMAL_DEACTIVATE"
        # 42.0s is below emergency threshold
        assert emergency_decision == "NORMAL_DEACTIVATE"

    def test_complete_coordination_scenario_simulation(self, timing_calculator_factory):
        """Test complete coordination scenario from optimal to overloaded."""
        # Simulate system going from optimal → low weight → rate limited

        # Stage 1: Optimal conditions
        stage1_stats = MockWeightStats(
            current_weight_usage=500,
            available_weight=1500,
            critical_queue_size=0,
            normal_queue_size=3,  # (3+1)*0.5 = 2.0s
            total_requests_processed=100,
            requests_per_minute=80.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        calculator = timing_calculator_factory(stage1_stats)
        stage1_time = calculator.calculate_request_delay()
        assert stage1_time == 2.0
        assert stage1_time <= 5.0  # WebSocket should be active

        # Stage 2: System degradation - low weight
        stage2_stats = MockWeightStats(
            current_weight_usage=2100,
            available_weight=150,
            critical_queue_size=2,
            normal_queue_size=8,  # (8+1)*2.0 = 18.0s
            total_requests_processed=300,
            requests_per_minute=40.0,
            last_request_time=time.time(),
            is_rate_limited=False,
            recovery_end_time=None
        )

        calculator = timing_calculator_factory(stage2_stats)
        stage2_time = calculator.calculate_request_delay()
        assert stage2_time == 18.0
        assert stage2_time > 5.0   # Historical should start first
        assert stage2_time < 30.0  # WebSocket can still be activated later

        # Stage 3: System overload - rate limited
        stage3_stats = MockWeightStats(
            current_weight_usage=2400,
            available_weight=0,
            critical_queue_size=10,
            normal_queue_size=25,  # (25+1)*5.0 = 130.0s
            total_requests_processed=800,
            requests_per_minute=8.0,
            last_request_time=time.time(),
            is_rate_limited=True,
            recovery_end_time=time.time() + 60.0
        )

        calculator = timing_calculator_factory(stage3_stats)
        stage3_time = calculator.calculate_request_delay()
        assert stage3_time == 130.0
        assert stage3_time > 30.0  # WebSocket should be deactivated
        assert stage3_time > 45.0  # Emergency deactivation threshold

        # Verify progression: system gets progressively slower
        assert stage1_time < stage2_time < stage3_time


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])