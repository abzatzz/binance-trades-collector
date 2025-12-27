"""
Pytest Configuration and Fixtures
=================================

Central pytest configuration with path setup and common fixtures.

File: tests/conftest.py
"""

import sys
import os
from pathlib import Path

# Add src directory to Python path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

import pytest
import time
from typing import Dict, Any, List


@pytest.fixture
def sample_binance_trade_data():
    """Sample Binance API trade data for testing."""
    return {
        'a': 26129,
        'p': '0.01633102',
        'q': '4.70443515',
        'f': 27781,
        'l': 27781,
        'T': 1498793709153,
        'm': True
    }


@pytest.fixture
def sample_binance_trade_list():
    """Sample list of Binance API trade data."""
    base_time = 1640995200000
    return [
        {
            'a': 100000 + i,
            'p': f'{50000.0 + (i * 0.01):.8f}',
            'q': f'{1.0 + (i * 0.001):.8f}',
            'f': 200000 + i * 2,
            'l': 200000 + i * 2 + 1,
            'T': base_time + (i * 1000),
            'm': i % 2 == 0
        }
        for i in range(10)
    ]


@pytest.fixture
def current_timestamp_ms():
    """Current timestamp in milliseconds."""
    return int(time.time() * 1000)


@pytest.fixture
def temp_storage_dir(tmp_path):
    """Temporary directory for storage testing."""
    storage_dir = tmp_path / "test_storage"
    storage_dir.mkdir()
    return storage_dir


@pytest.fixture
def mock_env_vars():
    """Mock environment variables for configuration testing."""
    return {
        'STORAGE_DATA_DIR': './test_data',
        'STORAGE_RETENTION_DAYS': '30',
        'BINANCE_API_KEY': 'test_api_key',
        'BINANCE_SECRET_KEY': 'test_secret_key',
        'PROCESSING_HISTORICAL_DAYS_BACK': '7',
        'API_REST_PORT': '8080'
    }


# Test markers
def pytest_configure(config):
    """Configure pytest markers."""
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "system: mark test as system test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance test"
    )
    config.addinivalue_line(
        "markers", "api: mark test as API test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


# Skip markers for different environments
def pytest_collection_modifyitems(config, items):
    """Modify test collection based on environment."""
    if config.getoption("--no-slow"):
        skip_slow = pytest.mark.skip(reason="--no-slow option given")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--no-slow", action="store_true", default=False, help="skip slow tests"
    )
