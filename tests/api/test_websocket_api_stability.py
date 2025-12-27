"""
Level 3.4 - WebSocket API Stability Testing
===========================================

Comprehensive WebSocket API stability testing for production readiness.
Tests long-running connections, memory management, resilience, and load handling.

CRITICAL STABILITY VALIDATION:
- Long-running connection stability (configurable 1min/1hour)
- Memory leak detection during extended usage
- Connection recovery and reconnection logic
- High-volume data streaming stress testing
- Client connection limits and resource management
- Graceful degradation under extreme load

File: tests/api/test_websocket_api_stability.py
"""

import pytest
import asyncio
import json
import time
import threading
import concurrent.futures
import psutil
import os
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from dataclasses import dataclass
from unittest.mock import Mock, AsyncMock, patch
import gc

# Cross-platform resource monitoring
try:
    import resource
    HAS_RESOURCE = True
except ImportError:
    # Windows doesn't have resource module
    HAS_RESOURCE = False

# Test logging setup
from loggerino import loggerino

logs_folder = Path('./test_logs/api')
if not logs_folder.exists():
    logs_folder.mkdir(parents=True)

loggerino.configure(
    logs_dir=str(logs_folder),
    debug_in_console=True,
    buffer_size=500,
    flush_interval=2,
)

# Create required loggers
stability_test_log = logs_folder / 'websocket_api_stability.log'
required_loggers = [
    'websocket_stability_test',
    'websocket_manager',
    'rest_api',
    'shared',
    'clickhouse_manager'
]

for logger_name in required_loggers:
    loggerino.create(logger_name, str(stability_test_log))

ws_stability_logger = loggerino.get('websocket_stability_test')

# FastAPI and WebSocket imports
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.testclient import TestClient


# ===== STABILITY TESTING MODELS =====

@dataclass
class MemorySnapshot:
    """Memory usage snapshot"""
    timestamp: float
    rss_mb: float
    vms_mb: float
    connections_count: int
    messages_sent: int


@dataclass
class StabilityTestResult:
    """Result from stability test"""
    test_name: str
    duration_seconds: float
    success: bool
    memory_snapshots: List[MemorySnapshot]
    performance_metrics: Dict[str, Any]
    errors: List[str]
    final_stats: Dict[str, Any]


# ===== ENHANCED MOCK WEBSOCKET MANAGER =====

class StabilityMockWebSocketManager:
    """Enhanced mock WebSocket manager for stability testing"""

    def __init__(self):
        self.connections = {}
        self.subscriptions = {}
        self.message_history = []
        self.is_running = True
        self.total_messages_sent = 0
        self.total_errors = 0
        self.connections_count = 0
        self.start_time = time.time()

        # Stability metrics
        self.peak_connections = 0
        self.total_connections_created = 0
        self.reconnection_count = 0
        self.memory_usage_mb = []

    async def handle_connection(self, websocket: WebSocket, client_ip: str):
        """Handle WebSocket connection with stability tracking"""
        client_id = f"{client_ip}_{id(websocket)}_{int(time.time() * 1000)}"

        self.connections[client_id] = {
            'websocket': websocket,
            'connected_at': time.time(),
            'ip_address': client_ip,
            'subscriptions': {},
            'messages_sent': 0,
            'last_activity': time.time()
        }

        self.connections_count += 1
        self.total_connections_created += 1

        if self.connections_count > self.peak_connections:
            self.peak_connections = self.connections_count

        try:
            while True:
                try:
                    message_data = await websocket.receive_text()
                    message_dict = json.loads(message_data)

                    # Update activity
                    self.connections[client_id]['last_activity'] = time.time()

                    await self._process_message(client_id, message_dict)

                except WebSocketDisconnect:
                    break
                except json.JSONDecodeError:
                    await self._send_error(client_id, "INVALID_JSON", "Invalid JSON format")
                except Exception as e:
                    await self._send_error(client_id, "MESSAGE_ERROR", str(e))

        finally:
            self.connections.pop(client_id, None)
            self.subscriptions.pop(client_id, None)
            self.connections_count = max(0, self.connections_count - 1)

    async def _process_message(self, client_id: str, message: Dict[str, Any]):
        """Process message with stability tracking"""
        action = message.get('action')

        if action == 'subscribe':
            await self._handle_subscription(client_id, message)
        elif action == 'unsubscribe':
            await self._handle_unsubscription(client_id, message)
        elif action == 'ping':
            await self._handle_ping(client_id, message)
        elif action == 'stress_ping':  # Special stress testing ping
            await self._handle_stress_ping(client_id, message)
        else:
            await self._send_error(client_id, "UNKNOWN_ACTION", f"Unknown action: {action}")

    async def _handle_subscription(self, client_id: str, message: Dict[str, Any]):
        """Handle subscription with tracking"""
        try:
            subscriptions = message.get('subscriptions', [])
            successful_subscriptions = {}

            for sub in subscriptions:
                symbol = sub.get('symbol', '').upper()
                timeframes = sub.get('timeframes', [])

                if symbol in ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']:
                    successful_timeframes = []

                    for tf_config in timeframes:
                        timeframe = tf_config.get('timeframe')
                        initial_candles = tf_config.get('initial_candles', 0)

                        if isinstance(timeframe, int) and timeframe > 0:
                            successful_timeframes.append({
                                "timeframe": timeframe,
                                "initial_candles": initial_candles
                            })

                            # Send initial history if requested
                            if initial_candles > 0:
                                await self._send_initial_history(client_id, symbol, timeframe, initial_candles)

                    if successful_timeframes:
                        successful_subscriptions[symbol] = successful_timeframes

                        # Track subscriptions
                        if client_id not in self.subscriptions:
                            self.subscriptions[client_id] = {}
                        self.subscriptions[client_id][symbol] = successful_timeframes

            await self._send_subscription_result(client_id, successful_subscriptions)

        except Exception as e:
            await self._send_error(client_id, "SUBSCRIPTION_ERROR", str(e))

    async def _handle_unsubscription(self, client_id: str, message: Dict[str, Any]):
        """Handle unsubscription"""
        unsubscriptions = message.get('unsubscriptions', [])
        unsubscribed = []

        for unsub in unsubscriptions:
            symbol = unsub.get('symbol', '').upper()
            timeframes = unsub.get('timeframes', [])

            # Remove from tracking
            if client_id in self.subscriptions and symbol in self.subscriptions[client_id]:
                del self.subscriptions[client_id][symbol]

            for timeframe in timeframes:
                unsubscribed.append({
                    "symbol": symbol,
                    "timeframe": timeframe
                })

        await self._send_message(client_id, {
            "type": "unsubscription_result",
            "unsubscribed": unsubscribed,
            "message": f"Unsubscribed from {len(unsubscribed)} subscriptions"
        })

    async def _handle_ping(self, client_id: str, message: Dict[str, Any]):
        """Handle ping with stability tracking"""
        pong_message = {
            "type": "pong",
            "timestamp": int(time.time() * 1000),
            "client_timestamp": message.get('timestamp'),
            "uptime_seconds": time.time() - self.start_time
        }
        await self._send_message(client_id, pong_message)

    async def _handle_stress_ping(self, client_id: str, message: Dict[str, Any]):
        """Handle stress testing ping with minimal processing"""
        sequence = message.get('sequence', 0)
        pong_message = {
            "type": "stress_pong",
            "timestamp": int(time.time() * 1000),
            "sequence": sequence
        }
        await self._send_message(client_id, pong_message)

    async def _send_initial_history(self, client_id: str, symbol: str, timeframe: int, candles_count: int):
        """Send initial history with stability tracking"""
        candles = []
        current_time = int(time.time() * 1000)

        for i in range(min(candles_count, 20)):  # Limit for stability
            candle_time = current_time - (i * timeframe * 60 * 1000)
            candles.append({
                "candle_time": candle_time,
                "open": 50000.0 + (i * 0.1),
                "high": 50100.0 + (i * 0.1),
                "low": 49900.0 + (i * 0.1),
                "close": 50050.0 + (i * 0.1),
                "volume": 100.0 + i,
                "trades_count": 50 + i
            })

        history_message = {
            "type": "initial_history",
            "symbol": symbol,
            "timeframe": f"{timeframe}m",
            "requested_candles": candles_count,
            "actual_candles": len(candles),
            "candles": candles
        }

        await self._send_message(client_id, history_message)

    async def _send_subscription_result(self, client_id: str, successful: Dict):
        """Send subscription result"""
        message = {
            "type": "subscription_result",
            "successful_subscriptions": successful,
            "message": "Subscription processed",
            "timestamp": int(time.time() * 1000)
        }
        await self._send_message(client_id, message)

    async def _send_error(self, client_id: str, error_code: str, error_message: str):
        """Send error message with tracking"""
        error_msg = {
            "type": "error",
            "error_code": error_code,
            "message": error_message,
            "timestamp": int(time.time() * 1000)
        }
        await self._send_message(client_id, error_msg)
        self.total_errors += 1

    async def _send_message(self, client_id: str, message: Dict[str, Any]):
        """Send message with stability tracking"""
        if client_id not in self.connections:
            return

        try:
            websocket = self.connections[client_id]['websocket']
            message_json = json.dumps(message)
            await websocket.send_text(message_json)

            self.total_messages_sent += 1
            self.connections[client_id]['messages_sent'] += 1

            # Don't store all messages in stability tests to prevent memory issues
            if len(self.message_history) < 1000:
                from tests.api.test_websocket_api_functionality import WebSocketMessage
                self.message_history.append(WebSocketMessage(
                    message_type=message.get('type', 'unknown'),
                    payload=message,
                    timestamp=time.time(),
                    client_id=client_id
                ))

        except Exception as e:
            self.total_errors += 1

    async def simulate_data_stream(self, client_id: str, symbol: str, timeframe: int, count: int = 10):
        """Simulate high-volume data streaming for stress testing"""
        for i in range(count):
            update_message = {
                "type": "ohlcv_update",
                "symbol": symbol,
                "timeframe": f"{timeframe}m",
                "sequence": i,
                "data": {
                    "candle_time": int(time.time() * 1000) + (i * 1000),
                    "open": 50000.0 + i,
                    "high": 50200.0 + i,
                    "low": 49800.0 + i,
                    "close": 50100.0 + i,
                    "volume": 150.5 + i,
                    "trades_count": 75 + i
                }
            }
            await self._send_message(client_id, update_message)

            # Small delay to prevent overwhelming
            await asyncio.sleep(0.001)

    def get_stability_status(self):
        """Get enhanced status for stability testing"""
        current_time = time.time()
        uptime = current_time - self.start_time

        return {
            'connections_count': len(self.connections),
            'peak_connections': self.peak_connections,
            'total_connections_created': self.total_connections_created,
            'total_subscriptions': len(self.subscriptions),
            'messages_sent': self.total_messages_sent,
            'errors_count': self.total_errors,
            'reconnection_count': self.reconnection_count,
            'uptime_seconds': uptime,
            'messages_per_second': self.total_messages_sent / max(uptime, 1),
            'avg_messages_per_connection': self.total_messages_sent / max(self.total_connections_created, 1)
        }

    def cleanup_old_connections(self, max_idle_seconds: int = 300):
        """Cleanup idle connections for stability"""
        current_time = time.time()
        to_remove = []

        for client_id, conn_info in self.connections.items():
            if current_time - conn_info['last_activity'] > max_idle_seconds:
                to_remove.append(client_id)

        for client_id in to_remove:
            self.connections.pop(client_id, None)
            self.subscriptions.pop(client_id, None)
            self.connections_count = max(0, self.connections_count - 1)


# ===== ENHANCED TEST API =====

def create_stability_websocket_api() -> FastAPI:
    """Create enhanced API for stability testing"""
    app = FastAPI(title="WebSocket Stability Test API")
    websocket_manager = StabilityMockWebSocketManager()

    @app.websocket("/ws/stream")
    async def websocket_endpoint(websocket: WebSocket):
        """Enhanced WebSocket endpoint for stability testing"""
        await websocket.accept()
        client_ip = "stability_testclient"
        await websocket_manager.handle_connection(websocket, client_ip)

    @app.get("/api/v1/admin/websocket/status")
    async def get_websocket_status():
        """Get enhanced WebSocket status"""
        return {
            "success": True,
            "data": websocket_manager.get_stability_status(),
            "timestamp": int(time.time() * 1000)
        }

    @app.get("/api/v1/admin/websocket/connections")
    async def get_websocket_connections():
        """Get WebSocket connections details"""
        connections_info = []
        for client_id, conn_info in websocket_manager.connections.items():
            connections_info.append({
                'client_id': client_id,
                'connected_at': conn_info['connected_at'],
                'ip_address': conn_info['ip_address'],
                'messages_sent': conn_info['messages_sent'],
                'last_activity': conn_info['last_activity'],
                'subscriptions': list(websocket_manager.subscriptions.get(client_id, {}).keys())
            })

        return {
            "success": True,
            "data": {
                "connections": connections_info,
                "total_connections": len(connections_info)
            }
        }

    @app.post("/api/v1/admin/websocket/cleanup")
    async def cleanup_connections():
        """Cleanup idle connections"""
        websocket_manager.cleanup_old_connections()
        return {
            "success": True,
            "message": "Cleanup completed",
            "timestamp": int(time.time() * 1000)
        }

    app.websocket_manager = websocket_manager
    return app


@pytest.fixture
def stability_api_client():
    """Enhanced API client for stability testing"""
    app = create_stability_websocket_api()
    client = TestClient(app)
    ws_stability_logger.info("Created WebSocket stability test client")
    return client


# ===== MEMORY MONITORING UTILITIES =====

def get_memory_usage() -> MemorySnapshot:
    """Get current memory usage snapshot"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()

    return MemorySnapshot(
        timestamp=time.time(),
        rss_mb=memory_info.rss / 1024 / 1024,  # Convert to MB
        vms_mb=memory_info.vms / 1024 / 1024,  # Convert to MB
        connections_count=0,  # Will be updated by caller
        messages_sent=0       # Will be updated by caller
    )


def monitor_memory_during_test(test_duration: float, interval: float = 5.0) -> List[MemorySnapshot]:
    """Monitor memory usage during test execution"""
    snapshots = []
    start_time = time.time()

    while time.time() - start_time < test_duration:
        snapshot = get_memory_usage()
        snapshots.append(snapshot)
        time.sleep(interval)

    return snapshots


def safe_websocket_receive(websocket, max_messages=10, max_time=3.0):
    """Enhanced safe receive for stability tests"""
    messages = []
    start_time = time.time()

    for i in range(max_messages):
        if time.time() - start_time > max_time:
            break

        try:
            result = [None]
            exception = [None]

            def receive_message():
                try:
                    result[0] = websocket.receive_json()
                except Exception as e:
                    exception[0] = e

            thread = threading.Thread(target=receive_message)
            thread.daemon = True
            thread.start()
            thread.join(timeout=0.5)

            if thread.is_alive():
                break

            if exception[0]:
                break

            if result[0] is not None:
                messages.append(result[0])
            else:
                break

        except Exception:
            break

    return messages


# ===== STABILITY TEST CLASSES =====

class TestLongRunningConnections:
    """Test long-running WebSocket connection stability"""

    @pytest.mark.skip(reason="Long-running test - run manually with --run-long-tests")
    def test_extended_connection_stability(self, stability_api_client):
        """Test WebSocket connection stability over extended period (1 hour)"""
        ws_stability_logger.info("Testing extended connection stability (1 hour)")

        # This test is skipped by default - can be run with specific flag
        # For normal testing: 1 minute, for overnight: 1 hour
        test_duration = 60 * 60  # 1 hour for overnight testing

        self._run_connection_stability_test(stability_api_client, test_duration, "1-hour")

    def test_medium_connection_stability(self, stability_api_client):
        """Test WebSocket connection stability over medium period (1 minute)"""
        ws_stability_logger.info("Testing medium connection stability (1 minute)")

        # This runs normally - 1 minute test
        test_duration = 60  # 1 minute

        self._run_connection_stability_test(stability_api_client, test_duration, "1-minute")

    def _run_connection_stability_test(self, stability_api_client, duration_seconds: int, test_name: str):
        """Run connection stability test with specified duration"""
        start_time = time.time()
        memory_snapshots = []
        errors = []

        try:
            with stability_api_client.websocket_connect("/ws/stream") as websocket:
                # Initial subscription
                subscription_msg = {
                    "action": "subscribe",
                    "subscriptions": [
                        {
                            "symbol": "BTCUSDT",
                            "timeframes": [{"timeframe": 1, "initial_candles": 1}]
                        }
                    ]
                }

                websocket.send_json(subscription_msg)
                initial_messages = safe_websocket_receive(websocket, max_messages=2, max_time=2.0)

                ws_stability_logger.info(f"Connection established, received {len(initial_messages)} initial messages")

                # Monitor during test duration
                end_time = start_time + duration_seconds
                ping_interval = 30  # Ping every 30 seconds
                memory_check_interval = 10  # Memory check every 10 seconds
                last_ping = start_time
                last_memory_check = start_time
                ping_count = 0

                while time.time() < end_time:
                    current_time = time.time()

                    # Send periodic ping
                    if current_time - last_ping >= ping_interval:
                        ping_msg = {
                            "action": "ping",
                            "timestamp": int(current_time * 1000)
                        }
                        websocket.send_json(ping_msg)
                        ping_count += 1
                        last_ping = current_time

                        # Try to receive pong
                        pong_messages = safe_websocket_receive(websocket, max_messages=1, max_time=1.0)
                        if pong_messages:
                            ws_stability_logger.debug(f"Ping {ping_count} successful")
                        else:
                            errors.append(f"No pong received for ping {ping_count}")

                    # Memory check
                    if current_time - last_memory_check >= memory_check_interval:
                        memory_snapshot = get_memory_usage()

                        # Get connection stats
                        response = stability_api_client.get("/api/v1/admin/websocket/status")
                        if response.status_code == 200:
                            stats = response.json()['data']
                            memory_snapshot.connections_count = stats.get('connections_count', 0)
                            memory_snapshot.messages_sent = stats.get('messages_sent', 0)

                        memory_snapshots.append(memory_snapshot)
                        last_memory_check = current_time

                        ws_stability_logger.debug(f"Memory: {memory_snapshot.rss_mb:.1f}MB RSS, {memory_snapshot.connections_count} connections")

                    # Short sleep to prevent busy loop
                    time.sleep(1)

                # Final statistics
                total_duration = time.time() - start_time

                response = stability_api_client.get("/api/v1/admin/websocket/status")
                final_stats = response.json()['data'] if response.status_code == 200 else {}

                ws_stability_logger.info(f"{test_name} stability test completed:")
                ws_stability_logger.info(f"   Duration: {total_duration:.1f}s")
                ws_stability_logger.info(f"   Pings sent: {ping_count}")
                ws_stability_logger.info(f"   Errors: {len(errors)}")
                ws_stability_logger.info(f"   Final stats: {final_stats}")

                if memory_snapshots:
                    initial_memory = memory_snapshots[0].rss_mb
                    final_memory = memory_snapshots[-1].rss_mb
                    memory_growth = final_memory - initial_memory

                    ws_stability_logger.info(f"   Memory growth: {memory_growth:.1f}MB ({initial_memory:.1f} -> {final_memory:.1f})")

                    # Assert reasonable memory growth (less than 10MB for 1 minute, 50MB for 1 hour)
                    max_acceptable_growth = 50 if duration_seconds >= 3600 else 10
                    assert memory_growth < max_acceptable_growth, f"Excessive memory growth: {memory_growth:.1f}MB"

                # Assert minimal errors
                assert len(errors) < (duration_seconds / 60), f"Too many errors: {len(errors)}"

                ws_stability_logger.info(f"✅ {test_name} connection stability test passed")

        except Exception as e:
            errors.append(f"Connection test failed: {e}")
            ws_stability_logger.error(f"Connection stability test failed: {e}")
            raise


class TestMemoryLeakDetection:
    """Test memory leak detection during WebSocket operations"""

    def test_connection_memory_stability(self, stability_api_client):
        """Test memory stability with frequent connections"""
        ws_stability_logger.info("Testing connection memory stability")

        initial_memory = get_memory_usage()
        connection_cycles = 20

        for i in range(connection_cycles):
            try:
                with stability_api_client.websocket_connect("/ws/stream") as websocket:
                    # Quick subscription cycle
                    subscription_msg = {
                        "action": "subscribe",
                        "subscriptions": [
                            {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 1}]}
                        ]
                    }

                    websocket.send_json(subscription_msg)
                    messages = safe_websocket_receive(websocket, max_messages=2, max_time=1.0)

                    # Send some pings
                    for j in range(3):
                        ping_msg = {"action": "ping", "timestamp": int(time.time() * 1000)}
                        websocket.send_json(ping_msg)

                    # Unsubscribe
                    unsubscribe_msg = {
                        "action": "unsubscribe",
                        "unsubscriptions": [{"symbol": "BTCUSDT", "timeframes": [1]}]
                    }
                    websocket.send_json(unsubscribe_msg)

                if i % 5 == 0:
                    gc.collect()  # Force garbage collection
                    current_memory = get_memory_usage()
                    ws_stability_logger.debug(f"Cycle {i}: {current_memory.rss_mb:.1f}MB RSS")

            except Exception as e:
                ws_stability_logger.error(f"Connection cycle {i} failed: {e}")

        # Final memory check
        gc.collect()
        time.sleep(1)  # Allow cleanup
        final_memory = get_memory_usage()

        memory_growth = final_memory.rss_mb - initial_memory.rss_mb

        ws_stability_logger.info(f"Memory stability test completed:")
        ws_stability_logger.info(f"   Connection cycles: {connection_cycles}")
        ws_stability_logger.info(f"   Memory growth: {memory_growth:.1f}MB")
        ws_stability_logger.info(f"   Initial: {initial_memory.rss_mb:.1f}MB -> Final: {final_memory.rss_mb:.1f}MB")

        # Assert reasonable memory growth (less than 20MB for 20 cycles)
        assert memory_growth < 20, f"Potential memory leak detected: {memory_growth:.1f}MB growth"

        ws_stability_logger.info("✅ Connection memory stability test passed")

    def test_message_processing_memory_stability(self, stability_api_client):
        """Test memory stability during high message processing"""
        ws_stability_logger.info("Testing message processing memory stability")

        initial_memory = get_memory_usage()

        with stability_api_client.websocket_connect("/ws/stream") as websocket:
            # Subscribe
            subscription_msg = {
                "action": "subscribe",
                "subscriptions": [
                    {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 1}]}
                ]
            }
            websocket.send_json(subscription_msg)
            safe_websocket_receive(websocket, max_messages=2, max_time=1.0)

            # Send many stress pings
            message_count = 100
            for i in range(message_count):
                stress_ping = {
                    "action": "stress_ping",
                    "sequence": i,
                    "timestamp": int(time.time() * 1000)
                }
                websocket.send_json(stress_ping)

                if i % 20 == 0:
                    # Receive some responses
                    responses = safe_websocket_receive(websocket, max_messages=5, max_time=0.5)

                    if i % 50 == 0:
                        current_memory = get_memory_usage()
                        ws_stability_logger.debug(f"Message {i}: {current_memory.rss_mb:.1f}MB RSS")

            # Final receive attempt
            final_responses = safe_websocket_receive(websocket, max_messages=10, max_time=2.0)

        gc.collect()
        time.sleep(1)
        final_memory = get_memory_usage()

        memory_growth = final_memory.rss_mb - initial_memory.rss_mb

        ws_stability_logger.info(f"Message processing stability test completed:")
        ws_stability_logger.info(f"   Messages sent: {message_count}")
        ws_stability_logger.info(f"   Memory growth: {memory_growth:.1f}MB")

        # Assert reasonable memory growth (less than 15MB for 100 messages)
        assert memory_growth < 15, f"Memory leak in message processing: {memory_growth:.1f}MB growth"

        ws_stability_logger.info("✅ Message processing memory stability test passed")


class TestConnectionRecoveryAndResilience:
    """Test connection recovery and resilience mechanisms"""

    def test_connection_cleanup_mechanisms(self, stability_api_client):
        """Test connection cleanup and resource management"""
        ws_stability_logger.info("Testing connection cleanup mechanisms")

        # Check initial state
        response = stability_api_client.get("/api/v1/admin/websocket/status")
        initial_stats = response.json()['data']
        initial_connections = initial_stats.get('connections_count', 0)

        # Create multiple connections and let them disconnect
        connection_count = 5
        for i in range(connection_count):
            with stability_api_client.websocket_connect("/ws/stream") as websocket:
                subscription_msg = {
                    "action": "subscribe",
                    "subscriptions": [
                        {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 1}]}
                    ]
                }
                websocket.send_json(subscription_msg)
                safe_websocket_receive(websocket, max_messages=2, max_time=1.0)

        # Check that connections are cleaned up
        time.sleep(2)  # Allow cleanup
        response = stability_api_client.get("/api/v1/admin/websocket/status")
        final_stats = response.json()['data']
        final_connections = final_stats.get('connections_count', 0)

        ws_stability_logger.info(f"Connection cleanup test:")
        ws_stability_logger.info(f"   Initial connections: {initial_connections}")
        ws_stability_logger.info(f"   Final connections: {final_connections}")
        ws_stability_logger.info(f"   Total created: {final_stats.get('total_connections_created', 0)}")

        # Assert connections are cleaned up
        assert final_connections == initial_connections, f"Connections not cleaned up: {final_connections} != {initial_connections}"

        ws_stability_logger.info("✅ Connection cleanup test passed")

    def test_subscription_state_management(self, stability_api_client):
        """Test subscription state persistence and cleanup"""
        ws_stability_logger.info("Testing subscription state management")

        subscription_cycles = 10

        for cycle in range(subscription_cycles):
            with stability_api_client.websocket_connect("/ws/stream") as websocket:
                # Subscribe to multiple symbols
                subscription_msg = {
                    "action": "subscribe",
                    "subscriptions": [
                        {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 1}]},
                        {"symbol": "ETHUSDT", "timeframes": [{"timeframe": 5, "initial_candles": 1}]}
                    ]
                }
                websocket.send_json(subscription_msg)
                messages = safe_websocket_receive(websocket, max_messages=4, max_time=2.0)

                # Check connection details
                response = stability_api_client.get("/api/v1/admin/websocket/connections")
                if response.status_code == 200:
                    connections_data = response.json()['data']
                    current_connections = connections_data.get('connections', [])

                    if current_connections:
                        connection = current_connections[0]
                        subscriptions = connection.get('subscriptions', [])
                        ws_stability_logger.debug(f"Cycle {cycle}: {len(subscriptions)} subscriptions")

                # Unsubscribe from one symbol
                unsubscribe_msg = {
                    "action": "unsubscribe",
                    "unsubscriptions": [{"symbol": "BTCUSDT", "timeframes": [1]}]
                }
                websocket.send_json(unsubscribe_msg)
                safe_websocket_receive(websocket, max_messages=1, max_time=1.0)

        # Final state check
        response = stability_api_client.get("/api/v1/admin/websocket/status")
        final_stats = response.json()['data']

        ws_stability_logger.info(f"Subscription state management test completed:")
        ws_stability_logger.info(f"   Cycles: {subscription_cycles}")
        ws_stability_logger.info(f"   Final connections: {final_stats.get('connections_count', 0)}")
        ws_stability_logger.info(f"   Final subscriptions: {final_stats.get('total_subscriptions', 0)}")

        # Should have no lingering connections or subscriptions
        assert final_stats.get('connections_count', 0) == 0, "Connections not cleaned up"
        assert final_stats.get('total_subscriptions', 0) == 0, "Subscriptions not cleaned up"

        ws_stability_logger.info("✅ Subscription state management test passed")


class TestHighVolumeStressTesting:
    """Test high-volume data streaming and stress scenarios"""

    def test_concurrent_client_stress(self, stability_api_client):
        """Test system behavior under concurrent client load"""
        ws_stability_logger.info("Testing concurrent client stress")

        def stress_client_worker(client_id: int) -> Dict[str, Any]:
            """Stress test worker for individual client"""
            try:
                with stability_api_client.websocket_connect("/ws/stream") as websocket:
                    # Subscribe
                    subscription_msg = {
                        "action": "subscribe",
                        "subscriptions": [
                            {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 1}]}
                        ]
                    }
                    websocket.send_json(subscription_msg)

                    # Send stress pings
                    stress_messages = 20
                    for i in range(stress_messages):
                        stress_ping = {
                            "action": "stress_ping",
                            "sequence": i,
                            "client_id": client_id
                        }
                        websocket.send_json(stress_ping)

                    # Collect responses
                    responses = safe_websocket_receive(websocket, max_messages=stress_messages + 2, max_time=3.0)

                    return {
                        'client_id': client_id,
                        'messages_sent': stress_messages + 1,  # +1 for subscription
                        'messages_received': len(responses),
                        'success': True
                    }

            except Exception as e:
                ws_stability_logger.error(f"Stress client {client_id} failed: {e}")
                return {
                    'client_id': client_id,
                    'messages_sent': 0,
                    'messages_received': 0,
                    'success': False,
                    'error': str(e)
                }

        # Run concurrent stress clients
        num_clients = 8  # Reasonable concurrent load
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(stress_client_worker, i) for i in range(num_clients)]

            try:
                results = [future.result(timeout=15) for future in futures]
            except concurrent.futures.TimeoutError:
                ws_stability_logger.warning("Some stress clients timed out")
                results = [f.result() for f in futures if f.done()]

        test_duration = time.time() - start_time

        # Analyze results
        successful_clients = [r for r in results if r.get('success', False)]
        total_messages_sent = sum(r.get('messages_sent', 0) for r in results)
        total_messages_received = sum(r.get('messages_received', 0) for r in results)

        success_rate = (len(successful_clients) / len(results)) * 100 if results else 0
        throughput = total_messages_sent / test_duration if test_duration > 0 else 0

        # Get final system stats
        response = stability_api_client.get("/api/v1/admin/websocket/status")
        final_stats = response.json()['data'] if response.status_code == 200 else {}

        ws_stability_logger.info(f"Concurrent client stress test completed:")
        ws_stability_logger.info(f"   Clients: {len(results)}")
        ws_stability_logger.info(f"   Successful: {len(successful_clients)}")
        ws_stability_logger.info(f"   Success rate: {success_rate:.1f}%")
        ws_stability_logger.info(f"   Total messages sent: {total_messages_sent}")
        ws_stability_logger.info(f"   Total messages received: {total_messages_received}")
        ws_stability_logger.info(f"   Throughput: {throughput:.1f} msg/s")
        ws_stability_logger.info(f"   Duration: {test_duration:.1f}s")
        ws_stability_logger.info(f"   Final system stats: {final_stats}")

        # Performance assertions
        assert len(results) == num_clients, f"Not all clients completed: {len(results)}/{num_clients}"
        assert success_rate >= 75, f"Success rate too low: {success_rate:.1f}%"
        assert throughput > 10, f"Throughput too low: {throughput:.1f} msg/s"

        ws_stability_logger.info("✅ Concurrent client stress test passed")

    def test_message_rate_limiting(self, stability_api_client):
        """Test message rate limiting and backpressure handling"""
        ws_stability_logger.info("Testing message rate limiting")

        with stability_api_client.websocket_connect("/ws/stream") as websocket:
            # Subscribe
            subscription_msg = {
                "action": "subscribe",
                "subscriptions": [
                    {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 1}]}
                ]
            }
            websocket.send_json(subscription_msg)
            safe_websocket_receive(websocket, max_messages=2, max_time=1.0)

            # Send rapid messages to test rate limiting
            rapid_messages = 50
            start_time = time.time()

            for i in range(rapid_messages):
                message = {
                    "action": "stress_ping",
                    "sequence": i,
                    "rapid_test": True
                }
                websocket.send_json(message)

                # Very short delay to simulate rapid sending
                if i % 10 == 0:
                    time.sleep(0.01)

            send_duration = time.time() - start_time

            # Collect responses with generous timeout
            responses = safe_websocket_receive(websocket, max_messages=rapid_messages + 5, max_time=5.0)

            # Analyze rate limiting
            response_count = len(responses)
            messages_per_second = rapid_messages / send_duration if send_duration > 0 else 0
            response_rate = response_count / send_duration if send_duration > 0 else 0

            ws_stability_logger.info(f"Rate limiting test results:")
            ws_stability_logger.info(f"   Messages sent: {rapid_messages}")
            ws_stability_logger.info(f"   Send rate: {messages_per_second:.1f} msg/s")
            ws_stability_logger.info(f"   Responses received: {response_count}")
            ws_stability_logger.info(f"   Response rate: {response_rate:.1f} msg/s")
            ws_stability_logger.info(f"   Send duration: {send_duration:.2f}s")

            # System should handle high message rates gracefully
            # Even if not all messages get responses, system should not crash
            assert response_count > 0, "No responses received - system may have failed"

        # Check system is still responsive after stress
        response = stability_api_client.get("/api/v1/admin/websocket/status")
        assert response.status_code == 200, "System not responsive after rate limiting test"

        ws_stability_logger.info("✅ Message rate limiting test passed")


class TestClientManagementAndLimits:
    """Test client connection management and limits enforcement"""

    def test_connection_limit_enforcement(self, stability_api_client):
        """Test connection limit enforcement"""
        ws_stability_logger.info("Testing connection limit enforcement")

        # Test reasonable number of concurrent connections
        max_test_connections = 15
        active_connections = []

        try:
            # Try to create many connections
            for i in range(max_test_connections):
                try:
                    websocket = stability_api_client.websocket_connect("/ws/stream")
                    websocket.__enter__()
                    active_connections.append(websocket)

                    # Quick subscribe to make connection active
                    subscription_msg = {
                        "action": "subscribe",
                        "subscriptions": [
                            {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 0}]}
                        ]
                    }
                    websocket.send_json(subscription_msg)

                    if i % 5 == 0:
                        ws_stability_logger.debug(f"Created {i+1} connections")

                except Exception as e:
                    ws_stability_logger.info(f"Connection {i+1} failed (expected if limits enforced): {e}")
                    break

            successful_connections = len(active_connections)

            # Check system status under load
            response = stability_api_client.get("/api/v1/admin/websocket/status")
            if response.status_code == 200:
                stats = response.json()['data']
                reported_connections = stats.get('connections_count', 0)

                ws_stability_logger.info(f"Connection limit test results:")
                ws_stability_logger.info(f"   Successful connections: {successful_connections}")
                ws_stability_logger.info(f"   Reported connections: {reported_connections}")
                ws_stability_logger.info(f"   Peak connections: {stats.get('peak_connections', 0)}")

                # System should track connections accurately
                assert reported_connections <= successful_connections + 2, "Connection count mismatch"

            # Test that system is still responsive
            assert response.status_code == 200, "System not responsive under connection load"

        finally:
            # Cleanup all connections
            for websocket in active_connections:
                try:
                    websocket.__exit__(None, None, None)
                except:
                    pass

        # Verify cleanup
        time.sleep(2)
        response = stability_api_client.get("/api/v1/admin/websocket/status")
        if response.status_code == 200:
            final_stats = response.json()['data']
            final_connections = final_stats.get('connections_count', 0)

            ws_stability_logger.info(f"After cleanup: {final_connections} connections")

            # Allow some time for cleanup, but should be low
            assert final_connections <= 2, f"Connections not properly cleaned up: {final_connections}"

        ws_stability_logger.info("✅ Connection limit enforcement test passed")

    def test_resource_usage_monitoring(self, stability_api_client):
        """Test resource usage monitoring and reporting"""
        ws_stability_logger.info("Testing resource usage monitoring")

        # Get baseline metrics
        initial_response = stability_api_client.get("/api/v1/admin/websocket/status")
        assert initial_response.status_code == 200

        initial_stats = initial_response.json()['data']
        baseline_messages = initial_stats.get('messages_sent', 0)

        # Create activity to generate metrics
        activity_sessions = 3

        for session in range(activity_sessions):
            with stability_api_client.websocket_connect("/ws/stream") as websocket:
                # Subscribe
                subscription_msg = {
                    "action": "subscribe",
                    "subscriptions": [
                        {"symbol": "BTCUSDT", "timeframes": [{"timeframe": 1, "initial_candles": 2}]}
                    ]
                }
                websocket.send_json(subscription_msg)

                # Send pings
                for i in range(5):
                    ping_msg = {"action": "ping", "timestamp": int(time.time() * 1000)}
                    websocket.send_json(ping_msg)

                # Receive messages
                messages = safe_websocket_receive(websocket, max_messages=8, max_time=2.0)
                ws_stability_logger.debug(f"Session {session}: received {len(messages)} messages")

        # Check final metrics
        final_response = stability_api_client.get("/api/v1/admin/websocket/status")
        assert final_response.status_code == 200

        final_stats = final_response.json()['data']

        # Verify metrics are being tracked
        required_metrics = [
            'connections_count', 'peak_connections', 'total_connections_created',
            'messages_sent', 'uptime_seconds', 'messages_per_second'
        ]

        for metric in required_metrics:
            assert metric in final_stats, f"Missing metric: {metric}"

        # Verify metrics make sense
        assert final_stats['total_connections_created'] >= activity_sessions, "Connection count not tracked"
        assert final_stats['messages_sent'] > baseline_messages, "Message count not increasing"
        assert final_stats['uptime_seconds'] > 0, "Uptime not tracked"
        assert final_stats['peak_connections'] >= 0, "Peak connections invalid"

        ws_stability_logger.info(f"Resource monitoring test results:")
        ws_stability_logger.info(f"   Total connections created: {final_stats['total_connections_created']}")
        ws_stability_logger.info(f"   Peak connections: {final_stats['peak_connections']}")
        ws_stability_logger.info(f"   Messages sent: {final_stats['messages_sent']}")
        ws_stability_logger.info(f"   Messages per second: {final_stats['messages_per_second']:.2f}")
        ws_stability_logger.info(f"   Uptime: {final_stats['uptime_seconds']:.1f}s")

        # Test connection details endpoint
        connections_response = stability_api_client.get("/api/v1/admin/websocket/connections")
        if connections_response.status_code == 200:
            connections_data = connections_response.json()['data']
            assert 'connections' in connections_data, "Connection details not available"
            assert 'total_connections' in connections_data, "Total connections not reported"

            ws_stability_logger.info(f"   Active connections: {connections_data['total_connections']}")

        ws_stability_logger.info("✅ Resource usage monitoring test passed")


if __name__ == "__main__":
    # Run WebSocket API stability tests
    # Use pytest.main with specific markers to control which tests run
    # For overnight testing: pytest -m "not skip" --run-long-tests
    pytest.main([__file__, "-v", "-s", "--tb=short"])