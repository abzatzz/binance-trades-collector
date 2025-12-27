"""
Benchmark: Timestamp Conversion Performance
===========================================

Compares different methods of timestamp conversion for ClickHouse inserts.
Tests with realistic trade volumes (300K+ trades).

Run: python benchmark_timestamp.py
"""

import time
from datetime import datetime, timezone
from typing import List, Any


def generate_mock_trades(count: int) -> List[dict]:
    """Generate mock trade data."""
    base_timestamp = int(time.time() * 1000)
    trades = []
    for i in range(count):
        trades.append({
            'symbol': 'BTCUSDT',
            'aggregate_id': 1000000 + i,
            'price': 50000.0 + (i % 100),
            'quantity': 0.1 + (i % 10) * 0.01,
            'first_trade_id': 2000000 + i,
            'last_trade_id': 2000000 + i,
            'timestamp': base_timestamp + i,  # milliseconds
            'is_buyer_maker': i % 2
        })
    return trades


def method_1_current(trades: List[dict]) -> List[List[Any]]:
    """
    Current method: Python datetime conversion for each trade.
    Used in GlobalTradesUpdater._process_all_buffers()
    """
    result = []
    for trade in trades:
        dt_str = datetime.fromtimestamp(
            trade['timestamp'] / 1000,
            tz=timezone.utc
        ).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        result.append([
            trade['symbol'],
            trade['aggregate_id'],
            trade['price'],
            trade['quantity'],
            trade['first_trade_id'],
            trade['last_trade_id'],
            dt_str,
            trade['is_buyer_maker']
        ])
    return result


def method_2_raw_timestamp(trades: List[dict]) -> List[List[Any]]:
    """
    Optimized: Pass raw millisecond timestamp.
    ClickHouse can accept int for DateTime64(3).
    """
    result = []
    for trade in trades:
        result.append([
            trade['symbol'],
            trade['aggregate_id'],
            trade['price'],
            trade['quantity'],
            trade['first_trade_id'],
            trade['last_trade_id'],
            trade['timestamp'],  # Raw milliseconds
            trade['is_buyer_maker']
        ])
    return result


def method_3_batch_convert(trades: List[dict]) -> List[List[Any]]:
    """
    Batch conversion: Pre-calculate divisor, minimal operations.
    """
    result = []
    utc = timezone.utc

    for trade in trades:
        ts = trade['timestamp']
        # Simplified conversion
        dt = datetime.fromtimestamp(ts / 1000, tz=utc)
        dt_str = f"{dt.year:04d}-{dt.month:02d}-{dt.day:02d} {dt.hour:02d}:{dt.minute:02d}:{dt.second:02d}.{ts % 1000:03d}"

        result.append([
            trade['symbol'],
            trade['aggregate_id'],
            trade['price'],
            trade['quantity'],
            trade['first_trade_id'],
            trade['last_trade_id'],
            dt_str,
            trade['is_buyer_maker']
        ])
    return result


def method_4_manual_conversion(trades: List[dict]) -> List[List[Any]]:
    """
    Manual timestamp to string without datetime objects.
    Uses integer math only.
    """
    result = []

    for trade in trades:
        ts_ms = trade['timestamp']
        ts_sec = ts_ms // 1000
        ms_part = ts_ms % 1000

        # Calculate date/time components from Unix timestamp
        # Days since 1970-01-01
        days = ts_sec // 86400
        remaining = ts_sec % 86400
        hours = remaining // 3600
        remaining = remaining % 3600
        minutes = remaining // 60
        seconds = remaining % 60

        # Calculate year, month, day (simplified - no leap second handling)
        year = 1970
        while True:
            days_in_year = 366 if (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0)) else 365
            if days < days_in_year:
                break
            days -= days_in_year
            year += 1

        # Month calculation
        is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
        month_days = [31, 29 if is_leap else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
        month = 1
        for md in month_days:
            if days < md:
                break
            days -= md
            month += 1
        day = days + 1

        dt_str = f"{year:04d}-{month:02d}-{day:02d} {hours:02d}:{minutes:02d}:{seconds:02d}.{ms_part:03d}"

        result.append([
            trade['symbol'],
            trade['aggregate_id'],
            trade['price'],
            trade['quantity'],
            trade['first_trade_id'],
            trade['last_trade_id'],
            dt_str,
            trade['is_buyer_maker']
        ])
    return result


def run_benchmark(name: str, func, trades: List[dict], iterations: int = 5) -> float:
    """Run benchmark and return average time."""
    times = []

    # Warmup
    func(trades[:1000])

    for i in range(iterations):
        start = time.perf_counter()
        result = func(trades)
        elapsed = time.perf_counter() - start
        times.append(elapsed)

    avg_time = sum(times) / len(times)
    min_time = min(times)
    max_time = max(times)

    print(f"{name}:")
    print(f"  Average: {avg_time * 1000:.2f}ms")
    print(f"  Min:     {min_time * 1000:.2f}ms")
    print(f"  Max:     {max_time * 1000:.2f}ms")
    print(f"  Trades/sec: {len(trades) / avg_time:,.0f}")
    print()

    return avg_time


def main():
    print("=" * 60)
    print("Timestamp Conversion Benchmark")
    print("=" * 60)
    print()

    # Test different volumes
    for trade_count in [10_000, 50_000, 100_000, 300_000]:
        print(f"Testing with {trade_count:,} trades")
        print("-" * 40)

        trades = generate_mock_trades(trade_count)

        results = {}

        # Method 1: Current implementation
        results['current'] = run_benchmark(
            "Method 1 (Current - datetime.strftime)",
            method_1_current,
            trades
        )

        # Method 2: Raw timestamp
        results['raw'] = run_benchmark(
            "Method 2 (Raw timestamp - no conversion)",
            method_2_raw_timestamp,
            trades
        )

        # Method 3: Batch convert with f-string
        results['batch'] = run_benchmark(
            "Method 3 (F-string formatting)",
            method_3_batch_convert,
            trades
        )

        # Method 4: Pure integer math (no datetime)
        if trade_count <= 100_000:  # Skip for large counts - too slow
            results['manual'] = run_benchmark(
                "Method 4 (Manual integer math)",
                method_4_manual_conversion,
                trades
            )

        # Summary
        print("Summary:")
        baseline = results['current']
        for name, time_val in results.items():
            speedup = baseline / time_val
            print(f"  {name}: {speedup:.2f}x {'faster' if speedup > 1 else 'slower'}")

        print()
        print("=" * 60)
        print()


if __name__ == "__main__":
    main()