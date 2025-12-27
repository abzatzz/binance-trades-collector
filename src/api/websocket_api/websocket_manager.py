"""
Central WebSocket Manager
========================

Manages all WebSocket connections, subscriptions, and real-time OHLCV streaming.

File: src/api/websocket_api/websocket_manager.py
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Dict, Set, List, Optional, Any, Tuple, TYPE_CHECKING

from ...core.websocket_updater import WebSocketMode
from ...models import ProcessorState

if TYPE_CHECKING:
    from ...models.config import Config

from fastapi import WebSocket, WebSocketDisconnect
from loggerino import loggerino

from .websocket_models import (
    ServerMessage, SubscriptionRequest, UnsubscriptionRequest, PingRequest,
    InitialHistoryMessage, OHLCVUpdateMessage,
    UnsubscriptionResultMessage, PongMessage, HeartbeatMessage,
    CandleData, ClientConnection,
    create_error_message, create_subscription_success_message, create_subscription_partial_message,
    create_timeframe_string, validate_timeframe, validate_symbol_format, DataStatus
)

logger = loggerino.get('websocket_manager')


class CentralWebSocketManager:
    """
    Central WebSocket manager for handling all connections and subscriptions.

    Features:
    - Multiple client connections with IP-based access control
    - Dynamic subscription management with custom timeframes
    - Automatic MV creation and lifecycle management
    - Real-time OHLCV streaming with 1-second intervals
    - Historical data delivery on subscription
    - Reference counting for materialized views cleanup
    """

    def __init__(self, shared, config: 'Config'):
        """
        Initialize Central WebSocket Manager.

        Args:
            shared: Shared resource manager instance
            config: Main configuration object with unified APIConfig
        """
        self.shared = shared
        self.config = config

        # Extract WebSocket and security configurations from unified APIConfig
        self.websocket_config = config.api.get_websocket_config()
        self.security_config = config.api.get_security_config()

        # Connection management
        self.connections: Dict[str, ClientConnection] = {}  # client_id -> ClientConnection
        self.subscriptions: Dict[str, Dict[str, Set[int]]] = {}  # client_id -> {symbol -> timeframes}

        # Single shared ClickHouse manager
        self.shared_clickhouse_manager = shared.clickhouse_manager

        # Active subscriptions tracking (what data we need to fetch)
        self.active_subscriptions: Dict[str, Set[int]] = {}  # symbol -> unique_timeframes

        # MV reference counting for cleanup
        self.mv_references: Dict[Tuple[str, int], Set[str]] = {}  # (symbol, timeframe) -> {client_ids}

        # Lock for MV operations to prevent race conditions
        self._mv_locks: Dict[Tuple[str, int], asyncio.Lock] = {}

        # Track MVs currently being populated
        self._populating_mvs: Set[Tuple[str, int]] = set()

        # Pending historical requests
        self.pending_history_requests: Dict[str, List[Tuple[str, int, int]]] = {}  # symbol -> [(client_id, timeframe, initial_candles)]

        # Background tasks
        self._update_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None

        # Track disconnected clients pending cleanup
        self._disconnected_clients: Set[str] = set()

        # Statistics
        self.total_messages_sent = 0
        self.total_errors = 0
        self.start_time = time.time()

        logger.info("CentralWebSocketManager initialized")

    async def handle_connection(self, websocket: WebSocket, client_ip: str):
        """
        Handle new WebSocket connection.

        Args:
            websocket: FastAPI WebSocket instance
            client_ip: Client IP address
        """
        client_id = f"{client_ip}_{id(websocket)}"

        # Create client connection record
        connection = ClientConnection(
            client_id=client_id,
            websocket=websocket,
            subscriptions={},
            connected_at=time.time(),
            ip_address=client_ip
        )

        self.connections[client_id] = connection
        self.subscriptions[client_id] = {}

        logger.info(f"WebSocket connection established: {client_id}")

        try:
            # Start background tasks if this is the first connection
            if len(self.connections) == 1:
                await self._start_background_tasks()

            # Handle client messages
            await self._handle_client_messages(websocket, client_id)

        except WebSocketDisconnect:
            logger.info(f"WebSocket disconnected: {client_id}")
        except Exception as e:
            logger.error(f"WebSocket error for {client_id}: {e}")
            await self._send_error_to_client(client_id, "CONNECTION_ERROR", str(e))
        finally:
            # Clean up client connection
            await self._cleanup_client(client_id)

    async def _handle_client_messages(self, websocket: WebSocket, client_id: str):
        """Handle incoming messages from client."""
        while True:
            try:
                # Receive message with timeout
                message_data = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=self.websocket_config.connection_timeout_seconds
                )

                # Parse and validate message
                try:
                    message_dict = json.loads(message_data)
                    await self._process_client_message(client_id, message_dict)

                except json.JSONDecodeError:
                    await self._send_error_to_client(client_id, "INVALID_JSON", "Invalid JSON format")
                except Exception as e:
                    await self._send_error_to_client(client_id, "MESSAGE_ERROR", str(e))

            except asyncio.TimeoutError:
                await self._send_error_to_client(client_id, "TIMEOUT", "Connection timeout")
                break
            except WebSocketDisconnect:
                break

    async def _process_client_message(self, client_id: str, message_dict: Dict[str, Any]):
        """Process and route client message based on action type."""
        action = message_dict.get('action')

        if action == 'subscribe':
            await self._handle_subscription(client_id, message_dict)
        elif action == 'unsubscribe':
            await self._handle_unsubscription(client_id, message_dict)
        elif action == 'ping':
            await self._handle_ping(client_id, message_dict)
        else:
            await self._send_error_to_client(client_id, "UNKNOWN_ACTION", f"Unknown action: {action}")

    async def _handle_subscription(self, client_id: str, subscription_data: Dict[str, Any]):
        """Handle client subscription request with historical data delivery."""
        logger.info(f"[SUB] Start processing subscription from {client_id}")
        try:
            # Validate subscription request
            request = SubscriptionRequest(**subscription_data)
        except Exception as e:
            await self._send_error_to_client(client_id, "INVALID_SUBSCRIPTION", f"Invalid subscription format: {e}")
            return

        successful_subscriptions = {}
        failed_subscriptions = []

        for symbol_sub in request.subscriptions:
            symbol = symbol_sub.symbol
            timeframe_configs = symbol_sub.timeframes

            # Validate symbol format
            if not validate_symbol_format(symbol):
                failed_subscriptions.append({
                    "symbol": symbol,
                    "reason": "Invalid symbol format"
                })
                continue

            successful_timeframes = []
            failed_timeframes = []

            for tf_config in timeframe_configs:
                timeframe_minutes = tf_config.timeframe
                initial_candles = tf_config.initial_candles

                # Validate timeframe
                if not validate_timeframe(timeframe_minutes):
                    failed_timeframes.append({
                        "timeframe": timeframe_minutes,
                        "initial_candles": initial_candles,
                        "reason": f"Invalid timeframe: {timeframe_minutes}"
                    })
                    continue

                try:
                    # Send initial historical data if requested
                    if initial_candles > 0:
                        await self._send_initial_historical_data(
                            client_id, symbol, timeframe_minutes, initial_candles
                        )

                    # Add to active subscriptions
                    await self._add_to_active_subscriptions(client_id, symbol, timeframe_minutes)

                    successful_timeframes.append({
                        "timeframe": timeframe_minutes,
                        "initial_candles": initial_candles
                    })

                except Exception as e:
                    logger.error(f"Failed to subscribe {client_id} to {symbol}:{timeframe_minutes}m: {e}")
                    failed_timeframes.append({
                        "timeframe": timeframe_minutes,
                        "initial_candles": initial_candles,
                        "reason": str(e)
                    })

            # Record results
            if successful_timeframes:
                successful_subscriptions[symbol] = successful_timeframes
            if failed_timeframes:
                failed_subscriptions.append({
                    "symbol": symbol,
                    "failed_timeframes": failed_timeframes
                })

        # Send subscription result
        logger.info(f"[SUB] Sending result: {len(successful_subscriptions)} success, {len(failed_subscriptions)} failed")
        await self._send_subscription_result(client_id, successful_subscriptions, failed_subscriptions)
        logger.info(f"[SUB] Result sent to {client_id}")

    async def _send_empty_initial_history(
            self, client_id: str, symbol: str, timeframe_minutes: int,
            requested_candles: int, data_status: DataStatus
    ):
        """Send empty initial history with status information."""
        message = InitialHistoryMessage(
            symbol=symbol,
            timeframe=create_timeframe_string(timeframe_minutes),
            requested_candles=requested_candles,
            actual_candles=0,
            candles=[],
            data_status=data_status
        )

        await self._send_to_client(client_id, message)
        logger.info(f"Sent empty initial history for {symbol}:{timeframe_minutes}m (status: {data_status.data_quality})")

    async def _send_initial_historical_data(
            self, client_id: str, symbol: str, timeframe_minutes: int, initial_candles: int
    ):
        """Send initial historical data to client."""
        logger.info(f"[HIST] Starting for {symbol}:{timeframe_minutes}m, {initial_candles} candles")
        try:
            try:
                logger.info(f"[HIST] Calling _get_ticker_status...")  # <-- ДОБАВЬ ЭТО
                data_status = await self._get_ticker_status(symbol)
                logger.info(f"[HIST] Status: {data_status.data_quality}")
            except Exception as e:
                logger.error(f"[HIST] _get_ticker_status FAILED: {e}", exc_info=True)  # <-- И ЭТО
                raise

            logger.info(f"[HIST] Calling _ensure_mv_exists...")  # <-- ДОБАВЬ
            mv_exists = await self._ensure_mv_exists(symbol, timeframe_minutes)
            logger.info(f"[HIST] MV exists: {mv_exists}")  # <-- ДОБАВЬ

            if not mv_exists:
                logger.error(f"Failed to ensure MV exists for {symbol}:{timeframe_minutes}m")
                await self._send_empty_initial_history(client_id, symbol, timeframe_minutes,
                                                       initial_candles, data_status)
                return

            # Если данные загружаются - зарегистрировать pending request
            if data_status.data_quality in ["loading", "partial"]:
                logger.info(f"Data loading for {symbol}, registering pending history request")

                # Добавить в очередь ожидания
                if symbol not in self.pending_history_requests:
                    self.pending_history_requests[symbol] = []
                self.pending_history_requests[symbol].append((client_id, timeframe_minutes, initial_candles))

                # Отправить пустую историю со статусом loading
                await self._send_empty_initial_history(client_id, symbol, timeframe_minutes,
                                                       initial_candles, data_status)
                return

            # Данные готовы - отправить историю
            await self._fetch_and_send_history(client_id, symbol, timeframe_minutes, initial_candles, data_status)

        except Exception as e:
            logger.error(f"Failed to send initial historical data: {e}")

    async def _handle_unsubscription(self, client_id: str, unsubscription_data: Dict[str, Any]):
        """Handle client unsubscription request."""
        try:
            request = UnsubscriptionRequest(**unsubscription_data)
        except Exception as e:
            await self._send_error_to_client(client_id, "INVALID_UNSUBSCRIPTION", f"Invalid unsubscription format: {e}")
            return

        unsubscribed = []

        for unsub in request.unsubscriptions:
            symbol = unsub.get('symbol', '').upper()
            timeframes = unsub.get('timeframes', [])

            for timeframe_minutes in timeframes:
                try:
                    # Remove from client subscriptions
                    if (client_id in self.subscriptions and
                            symbol in self.subscriptions[client_id]):
                        self.subscriptions[client_id][symbol].discard(timeframe_minutes)

                        # Clean up empty symbol subscriptions
                        if not self.subscriptions[client_id][symbol]:
                            del self.subscriptions[client_id][symbol]

                    # Remove MV reference and cleanup if needed
                    await self._remove_mv_reference(client_id, symbol, timeframe_minutes)

                    unsubscribed.append({
                        "symbol": symbol,
                        "timeframe": timeframe_minutes
                    })

                except Exception as e:
                    logger.error(f"Failed to unsubscribe {client_id} from {symbol}:{timeframe_minutes}m: {e}")

        # Send unsubscription result
        message = UnsubscriptionResultMessage(
            unsubscribed=unsubscribed,
            message=f"Unsubscribed from {len(unsubscribed)} subscriptions"
        )
        await self._send_to_client(client_id, message)

    async def _handle_ping(self, client_id: str, ping_data: Dict[str, Any]):
        """Handle ping request from client."""
        try:
            request = PingRequest(**ping_data)

            # Update last ping time
            if client_id in self.connections:
                self.connections[client_id].last_ping = time.time()

            # Send pong response
            pong = PongMessage(
                timestamp=int(time.time() * 1000),
                client_timestamp=request.timestamp
            )
            await self._send_to_client(client_id, pong)

        except Exception as e:
            await self._send_error_to_client(client_id, "PING_ERROR", str(e))

    async def _start_background_tasks(self):
        """Start background tasks for updates and heartbeat."""
        if not self._update_task or self._update_task.done():
            self._update_task = asyncio.create_task(self._global_periodic_updates())
            logger.info("Started periodic updates task")

        if self.websocket_config.enable_heartbeat and (not self._heartbeat_task or self._heartbeat_task.done()):
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            logger.info("Started heartbeat task")

    async def _global_periodic_updates(self):
        """Optimized batch updates - один запрос для всех подписок."""
        while self.connections:
            try:
                queries_needed = set()
                for symbol, timeframes in self.active_subscriptions.items():
                    for timeframe_minutes in timeframes:
                        queries_needed.add((symbol, timeframe_minutes))

                if not queries_needed:
                    await asyncio.sleep(self.websocket_config.update_interval_seconds)
                    continue

                candles_data = await self.shared_clickhouse_manager.batch_get_current_candles_realtime(
                    queries_needed
                )

                # Check for completed historical loading
                await self._check_pending_history_requests()

                # Broadcast results
                for (symbol, timeframe_minutes) in queries_needed:
                    candle_data = candles_data.get((symbol, timeframe_minutes))

                    await self._broadcast_symbol_timeframe_update(
                        symbol, timeframe_minutes, candle_data
                    )

                await asyncio.sleep(self.websocket_config.update_interval_seconds)

            except Exception as e:
                logger.error(f"Error in global periodic updates: {e}", exc_info=True)
                await asyncio.sleep(self.websocket_config.update_interval_seconds)

        logger.info("Global periodic updates stopped - no active connections")

    async def _broadcast_symbol_timeframe_update(self, symbol: str, timeframe_minutes: int, candle_data: Dict[str, Any]):
        """Broadcast OHLCV update с информацией о статусе данных."""
        # Get data status
        data_status = await self._get_ticker_status(symbol)

        # Check if MV is still populating
        mv_key = (symbol, timeframe_minutes)
        if mv_key in self._populating_mvs:
            candle = None
            data_status = DataStatus(
                historical_loading=False,
                gap_recovery=False,
                data_quality="populating",
                last_complete_timestamp=None
            )
        elif data_status.data_quality in ["loading", "partial"]:
            candle = None
        else:
            candle = CandleData(**candle_data) if candle_data else None

        # Create update message
        message = OHLCVUpdateMessage(
            symbol=symbol,
            timeframe=create_timeframe_string(timeframe_minutes),
            data=candle,
            data_status=data_status
        )

        # Find and send to subscribed clients (copy keys to avoid modification during iteration)
        for client_id in list(self.subscriptions.keys()):
            if client_id in self._disconnected_clients:
                continue
            client_subscriptions = self.subscriptions.get(client_id, {})
            if symbol in client_subscriptions and timeframe_minutes in client_subscriptions[symbol]:
                await self._send_to_client(client_id, message)

    async def _heartbeat_loop(self):
        """Send periodic heartbeat messages to all connected clients."""
        while self.connections:
            try:
                heartbeat = HeartbeatMessage(
                    timestamp=int(time.time() * 1000),
                    connections_count=len(self.connections)
                )

                # Broadcast heartbeat to all clients
                for client_id in list(self.connections.keys()):
                    await self._send_to_client(client_id, heartbeat)

                await asyncio.sleep(self.websocket_config.heartbeat_interval)

            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(self.websocket_config.heartbeat_interval)

    # ===== UTILITY METHODS =====
    async def _get_ticker_status(self, symbol: str) -> DataStatus:
        """Получить текущий статус данных для тикера."""
        try:
            logger.debug(f"[STATUS] Getting processor for {symbol}")
            processor = self.shared.get_ticker_processor(symbol)
            logger.debug(f"[STATUS] Processor: {processor}")
            if not processor:
                return DataStatus(
                    historical_loading=True,
                    gap_recovery=False,
                    data_quality="loading",
                    last_complete_timestamp=None
                )

            processor_state = processor.state

            ws_mode = processor.websocket_updater.get_current_mode() if processor.websocket_updater else None

            if processor_state == ProcessorState.DOWNLOADING_HISTORICAL:
                return DataStatus(
                    historical_loading=True,
                    gap_recovery=False,
                    data_quality="loading",
                    last_complete_timestamp=processor.last_aggregate_id
                )
            elif ws_mode == WebSocketMode.BUFFERING:
                return DataStatus(
                    historical_loading=False,
                    gap_recovery=True,
                    data_quality="partial",
                    last_complete_timestamp=processor.last_aggregate_id
                )
            elif processor_state == ProcessorState.RUNNING:
                return DataStatus(
                    historical_loading=False,
                    gap_recovery=False,
                    data_quality="complete",
                    last_complete_timestamp=None
                )
            else:
                return DataStatus(
                    historical_loading=True,
                    gap_recovery=False,
                    data_quality="loading",
                    last_complete_timestamp=None
                )

        except Exception as e:
            logger.error(f"Failed to get ticker status for {symbol}: {e}")
            return DataStatus(
                historical_loading=True,
                gap_recovery=False,
                data_quality="unknown",
                last_complete_timestamp=None
            )

    def _get_mv_lock(self, symbol: str, timeframe_minutes: int) -> asyncio.Lock:
        key = (symbol, timeframe_minutes)
        if key not in self._mv_locks:
            self._mv_locks[key] = asyncio.Lock()
        return self._mv_locks[key]

    async def _ensure_mv_exists(self, symbol: str, timeframe_minutes: int) -> bool:
        """
        MV creation disabled - OHLCV queries use projections on trades_local.
        MVs were causing 30x INSERT slowdown (2123 MVs = synchronous updates).
        """
        return True

    async def _populate_mv_background(self, symbol: str, timeframe_minutes: int):
        """Populate MV with historical data in background."""
        mv_key = (symbol, timeframe_minutes)
        try:
            logger.info(f"[MV] Background populate started for {symbol}:{timeframe_minutes}m")
            await self.shared_clickhouse_manager.populate_mv_historical(symbol, timeframe_minutes)
            logger.info(f"[MV] Background populate completed for {symbol}:{timeframe_minutes}m")
        except Exception as e:
            logger.error(f"[MV] Background populate failed for {symbol}:{timeframe_minutes}m: {e}")
        finally:
            # Remove from populating set
            self._populating_mvs.discard(mv_key)

            # Send pending history to subscribed clients
            await self._send_pending_mv_history(symbol, timeframe_minutes)

    async def _send_pending_mv_history(self, symbol: str, timeframe_minutes: int):
        """Send history to clients waiting for MV populate."""
        # Find clients subscribed to this symbol:timeframe
        for client_id, client_subs in list(self.subscriptions.items()):
            if symbol in client_subs and timeframe_minutes in client_subs[symbol]:
                if client_id in self.connections:
                    try:
                        data_status = await self._get_ticker_status(symbol)
                        # Get initial_candles from pending or use default
                        initial_candles = 100  # Default, or track per-client
                        await self._fetch_and_send_history(
                            client_id, symbol, timeframe_minutes, initial_candles, data_status
                        )
                    except Exception as e:
                        logger.error(f"Failed to send pending history to {client_id}: {e}")

    async def _fetch_and_send_history(
            self, client_id: str, symbol: str, timeframe_minutes: int,
            initial_candles: int, data_status: DataStatus
    ):
        """Fetch and send historical candles."""
        historical_klines = await self.shared_clickhouse_manager.get_historical_klines_fast(
            symbol=symbol,
            timeframe_minutes=timeframe_minutes,
            limit=initial_candles
        )

        if historical_klines:
            candles = []
            for kline in historical_klines:
                candle = CandleData(
                    candle_time=kline['timestamp'],
                    open=kline['open'],
                    high=kline['high'],
                    low=kline['low'],
                    close=kline['close'],
                    volume=kline['volume'],
                    trades_count=kline['trades_count'],
                    taker_buy_volume=kline.get('taker_buy_volume'),
                    taker_sell_volume=kline.get('taker_sell_volume')
                )
                candles.append(candle)

            message = InitialHistoryMessage(
                symbol=symbol,
                timeframe=create_timeframe_string(timeframe_minutes),
                requested_candles=initial_candles,
                actual_candles=len(candles),
                candles=candles,
                data_status=data_status
            )

            await self._send_to_client(client_id, message)
            logger.info(f"Sent {len(candles)} initial candles to {client_id}")
        else:
            await self._send_empty_initial_history(client_id, symbol, timeframe_minutes,
                                                   initial_candles, data_status)

    async def _check_pending_history_requests(self):
        """Check if any pending history requests can now be fulfilled."""
        if not self.pending_history_requests:
            return

        symbols_to_remove = []

        for symbol in list(self.pending_history_requests.keys()):
            data_status = await self._get_ticker_status(symbol)

            if data_status.data_quality == "complete":
                requests = self.pending_history_requests[symbol]
                logger.info(f"Historical data ready for {symbol}, sending to {len(requests)} pending clients")

                for client_id, timeframe_minutes, initial_candles in requests:
                    if client_id in self.connections:
                        try:
                            await self._fetch_and_send_history(
                                client_id, symbol, timeframe_minutes, initial_candles, data_status
                            )
                        except Exception as e:
                            logger.error(f"Failed to send pending history to {client_id}: {e}")

                symbols_to_remove.append(symbol)

        for symbol in symbols_to_remove:
            del self.pending_history_requests[symbol]

    async def _add_to_active_subscriptions(self, client_id: str, symbol: str, timeframe_minutes: int):
        """Add client to active subscriptions with reference counting."""
        # Update client subscriptions
        if client_id not in self.subscriptions:
            self.subscriptions[client_id] = {}
        if symbol not in self.subscriptions[client_id]:
            self.subscriptions[client_id][symbol] = set()

        self.subscriptions[client_id][symbol].add(timeframe_minutes)

        # Update global active subscriptions
        if symbol not in self.active_subscriptions:
            self.active_subscriptions[symbol] = set()
        self.active_subscriptions[symbol].add(timeframe_minutes)

        # Update MV reference counting
        mv_key = (symbol, timeframe_minutes)
        if mv_key not in self.mv_references:
            self.mv_references[mv_key] = set()
        self.mv_references[mv_key].add(client_id)

        logger.debug(f"Added subscription: {client_id} -> {symbol}:{timeframe_minutes}m (refs: {len(self.mv_references[mv_key])})")

    async def _remove_mv_reference(self, client_id: str, symbol: str, timeframe_minutes: int):
        """Remove MV reference and cleanup if no longer used."""
        mv_key = (symbol, timeframe_minutes)

        # Remove reference
        if mv_key in self.mv_references:
            self.mv_references[mv_key].discard(client_id)

            # If no more references, delete MV and cleanup
            if not self.mv_references[mv_key]:
                await self._delete_unused_mv(symbol, timeframe_minutes)
                del self.mv_references[mv_key]

                # Remove from active subscriptions
                if symbol in self.active_subscriptions:
                    self.active_subscriptions[symbol].discard(timeframe_minutes)
                    if not self.active_subscriptions[symbol]:
                        del self.active_subscriptions[symbol]

                logger.info(f"Cleaned up unused MV: {symbol}:{timeframe_minutes}m")

    async def _delete_unused_mv(self, symbol: str, timeframe_minutes: int):
        """Delete unused materialized view."""
        try:
            success = await self.shared_clickhouse_manager.drop_custom_mv(symbol, timeframe_minutes)
            if success:
                logger.info(f"Deleted unused MV: ohlcv_{timeframe_minutes}_{symbol.lower()}_mv")
        except Exception as e:
            logger.error(f"Failed to delete MV {symbol}:{timeframe_minutes}m: {e}")

    async def _cleanup_client(self, client_id: str):
        """Full cleanup of client connection and subscriptions."""
        try:
            # Remove all client subscriptions and MV references
            if client_id in self.subscriptions:
                client_subscriptions = self.subscriptions[client_id]

                for symbol, timeframes in client_subscriptions.items():
                    for timeframe_minutes in timeframes:
                        await self._remove_mv_reference(client_id, symbol, timeframe_minutes)

                del self.subscriptions[client_id]

            # Remove from pending history requests
            for symbol, requests in list(self.pending_history_requests.items()):
                self.pending_history_requests[symbol] = [
                    (cid, tf, ic) for cid, tf, ic in requests if cid != client_id
                ]
                # Remove empty lists
                if not self.pending_history_requests[symbol]:
                    del self.pending_history_requests[symbol]

            # Remove connection record
            self.connections.pop(client_id, None)

            # Stop background tasks if no more connections
            if not self.connections:
                if self._update_task and not self._update_task.done():
                    self._update_task.cancel()
                    logger.info("Stopped periodic updates task")

                if self._heartbeat_task and not self._heartbeat_task.done():
                    self._heartbeat_task.cancel()
                    logger.info("Stopped heartbeat task")

            logger.info(f"Cleaned up client: {client_id}")

        except Exception as e:
            logger.error(f"Error cleaning up client {client_id}: {e}")

    async def _send_to_client(self, client_id: str, message: ServerMessage):
        """Send message to specific client."""
        # Skip if client is already marked for cleanup
        if client_id in self._disconnected_clients:
            return

        if client_id not in self.connections:
            return

        connection = self.connections[client_id]
        websocket = connection.websocket

        # Check WebSocket state before sending
        try:
            # FastAPI WebSocket has both client_state and application_state
            client_state = getattr(websocket, 'client_state', None)
            app_state = getattr(websocket, 'application_state', None)

            if client_state and client_state.name != "CONNECTED":
                await self._mark_client_disconnected(client_id)
                return
            if app_state and app_state.name != "CONNECTED":
                await self._mark_client_disconnected(client_id)
                return
        except Exception:
            pass

        try:
            message_dict = message.dict() if hasattr(message, 'dict') else message.__dict__
            await websocket.send_text(json.dumps(message_dict))
            self.total_messages_sent += 1

        except Exception as e:
            self.total_errors += 1
            # Mark client as disconnected to prevent further send attempts
            await self._mark_client_disconnected(client_id)

    async def _mark_client_disconnected(self, client_id: str):
        """Mark client as disconnected and schedule cleanup."""
        if client_id in self._disconnected_clients:
            return  # Already marked

        self._disconnected_clients.add(client_id)
        logger.debug(f"Marked client {client_id} as disconnected")

        # Schedule async cleanup (don't block current operation)
        asyncio.create_task(self._deferred_cleanup(client_id))

    async def _deferred_cleanup(self, client_id: str):
        """Deferred cleanup to avoid modifying collections during iteration."""
        await asyncio.sleep(0.1)  # Small delay to let current iteration complete
        try:
            await self._cleanup_client(client_id)
        finally:
            self._disconnected_clients.discard(client_id)

    async def _send_error_to_client(self, client_id: str, error_code: str, message: str):
        """Send error message to client."""
        error_msg = create_error_message(error_code, message)
        await self._send_to_client(client_id, error_msg)

    async def _send_subscription_result(self, client_id: str, successful: Dict, failed: List):
        """Send subscription result to client."""
        if failed:
            message = create_subscription_partial_message(successful, failed)
        else:
            message = create_subscription_success_message(successful)

        await self._send_to_client(client_id, message)

    # ===== MANAGEMENT METHODS =====

    async def cleanup(self):
        """Cleanup all resources and connections."""
        logger.info("Starting WebSocket manager cleanup...")

        # Cancel background tasks
        if self._update_task and not self._update_task.done():
            self._update_task.cancel()
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()

        # Close all WebSocket connections
        for client_id, connection in list(self.connections.items()):
            try:
                await connection.websocket.close()
            except:
                pass

        # Cleanup not needed - shared manager handled by main system
        pass

        # Clear all data
        self.connections.clear()
        self.subscriptions.clear()
        self.active_subscriptions.clear()
        self.mv_references.clear()
        self._disconnected_clients.clear()

        logger.info("WebSocket manager cleanup completed")

    def get_status(self) -> Dict[str, Any]:
        """Get current WebSocket manager status."""
        return {
            'connections_count': len(self.connections),
            'active_symbols': len(self.active_subscriptions),
            'total_subscriptions': sum(len(timeframes) for timeframes in self.active_subscriptions.values()),
            'mv_references_count': len(self.mv_references),
            'shared_clickhouse_manager_connected': self.shared_clickhouse_manager.is_connected if self.shared_clickhouse_manager else False,
            'messages_sent': self.total_messages_sent,
            'errors_count': self.total_errors,
            'uptime_seconds': time.time() - self.start_time,
            'update_task_running': self._update_task and not self._update_task.done(),
            'heartbeat_task_running': self._heartbeat_task and not self._heartbeat_task.done()
        }

    def get_connections_info(self) -> Dict[str, Any]:
        """Get detailed information about active connections."""
        connections_info = []

        for client_id, connection in self.connections.items():
            client_subscriptions = self.subscriptions.get(client_id, {})

            info = {
                'client_id': client_id,
                'ip_address': connection.ip_address,
                'connected_at': connection.connected_at,
                'connection_duration': time.time() - connection.connected_at,
                'last_ping': connection.last_ping,
                'subscriptions': {
                    symbol: list(timeframes)
                    for symbol, timeframes in client_subscriptions.items()
                },
                'total_subscriptions': sum(len(timeframes) for timeframes in client_subscriptions.values())
            }
            connections_info.append(info)

        return {
            'connections': connections_info,
            'total_connections': len(connections_info),
            'active_symbols': list(self.active_subscriptions.keys()),
            'mv_references': {
                f"{symbol}:{tf}m": len(clients)
                for (symbol, tf), clients in self.mv_references.items()
            }
        }
