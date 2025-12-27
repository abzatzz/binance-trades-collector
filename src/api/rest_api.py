"""
FastAPI REST API Server for Data Provider
==========================================

Main FastAPI application with all routes and middleware.
Provides access to trading data and system monitoring with WebSocket streaming.

File: src/api/rest_api.py
"""

import asyncio
import time
import uvicorn
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, status, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from loggerino import loggerino
from ..models.config import Config
from ..core.shared import Shared
from .models import APIResponse, TradeResponse, TickerInfo, SystemInfo, HealthResponse, OHLCVCandle

import ipaddress
from pathlib import Path

logger = loggerino.get('rest_api')


class IPWhitelistMiddleware(BaseHTTPMiddleware):
    """IP whitelist middleware with subnet support. Reads from file on each request."""

    def __init__(self, app, whitelist_file: str = "ip_whitelist.txt"):
        super().__init__(app)
        self.whitelist_file = Path(whitelist_file)
        logger.info(f"IPWhitelistMiddleware initialized, file: {self.whitelist_file}")

    def _load_whitelist(self) -> tuple:
        """Load whitelist from file. Returns (ips_set, networks_list)."""
        ips = set()
        networks = []

        if not self.whitelist_file.exists():
            logger.warning(f"Whitelist file not found: {self.whitelist_file}")
            return ips, networks

        try:
            with open(self.whitelist_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue

                    try:
                        if '/' in line:
                            # It's a network (CIDR notation)
                            networks.append(ipaddress.ip_network(line, strict=False))
                        else:
                            # It's a single IP
                            ips.add(line)
                    except ValueError as e:
                        logger.warning(f"Invalid IP/network in whitelist: {line} - {e}")
        except Exception as e:
            logger.error(f"Failed to read whitelist file: {e}")

        return ips, networks

    def _is_ip_allowed(self, client_ip: str) -> bool:
        """Check if IP is in whitelist."""
        ips, networks = self._load_whitelist()

        # Check exact IP match
        if client_ip in ips:
            return True

        # Check network match
        try:
            ip_obj = ipaddress.ip_address(client_ip)
            for network in networks:
                if ip_obj in network:
                    return True
        except ValueError:
            logger.warning(f"Invalid client IP: {client_ip}")
            return False

        return False

    async def dispatch(self, request, call_next):
        client_ip = request.client.host if request.client else "unknown"

        # Skip check for health endpoint
        if request.url.path in ["/health", "/api/v1/monitoring/health"]:
            return await call_next(request)

        if not self._is_ip_allowed(client_ip):
            logger.warning(f"Access denied for IP: {client_ip}")
            raise HTTPException(status_code=403, detail="Access denied")

        response = await call_next(request)
        return response


class RestAPIServer:
    """
    REST API server for Data Provider system.

    Provides endpoints for:
    - Trading data access
    - System monitoring
    - Ticker management
    - Administrative operations
    - WebSocket streaming
    """

    def __init__(self, shared: Shared, config):
        """
        Initialize REST API server.

        Args:
            shared: Shared resource manager
            config: Main configuration object
        """
        self.shared = shared
        self.config = config.api
        self.full_config = config

        self.central_websocket_manager = None

        # App and server instances
        self.app: Optional[FastAPI] = None
        self.server: Optional[uvicorn.Server] = None
        self.server_task: Optional[asyncio.Task] = None

        # Server state
        self.server_start_time = time.time()
        self.is_server_running: bool = False

        # Statistics
        self.requests_count = 0
        self.errors_count = 0

        logger.info("REST API server initialized")

    def _is_ip_allowed(self, client_ip: str) -> bool:
        """Check if IP is in whitelist file."""
        whitelist_file = Path("ip_whitelist.txt")

        if not whitelist_file.exists():
            logger.warning(f"Whitelist file not found: {whitelist_file}")
            return False

        ips = set()
        networks = []

        try:
            with open(whitelist_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue

                    try:
                        if '/' in line:
                            networks.append(ipaddress.ip_network(line, strict=False))
                        else:
                            ips.add(line)
                    except ValueError:
                        pass
        except Exception as e:
            logger.error(f"Failed to read whitelist: {e}")
            return False

        # Check exact IP
        if client_ip in ips:
            return True

        # Check networks
        try:
            ip_obj = ipaddress.ip_address(client_ip)
            for network in networks:
                if ip_obj in network:
                    return True
        except ValueError:
            return False

        return False

    def create_app(self) -> FastAPI:
        """
        Create FastAPI application with all routes and middleware.

        Returns:
            Configured FastAPI app
        """

        @asynccontextmanager
        async def lifespan(app: FastAPI):
            """Handle app startup and shutdown."""
            logger.info("FastAPI application starting up...")

            # Initialize WebSocket manager - ИСПРАВЛЕНО
            if self.config.websocket_enabled:
                from .websocket_api.websocket_manager import CentralWebSocketManager
                self.central_websocket_manager = CentralWebSocketManager(
                    self.shared,
                    self.full_config
                )
                logger.info("WebSocket manager initialized")

            yield

            # Cleanup WebSocket manager
            if self.central_websocket_manager:
                await self.central_websocket_manager.cleanup()
                logger.info("WebSocket manager cleaned up")

            logger.info("FastAPI application shutting down...")

        # Create FastAPI app
        app = FastAPI(
            title="Data Provider REST & WebSocket API",
            description="High-performance Binance AggTrades data provider with WebSocket streaming",
            version="2.0.0",
            docs_url="/docs",
            redoc_url="/redoc",
            lifespan=lifespan
        )

        # Add IP Whitelist middleware (for both REST and WebSocket)
        app.add_middleware(IPWhitelistMiddleware, whitelist_file="ip_whitelist.txt")

        # Add CORS middleware
        if self.config.cors_enabled:
            app.add_middleware(
                CORSMiddleware,
                allow_origins=self.config.cors_origins,
                allow_credentials=True,
                allow_methods=self.config.cors_methods,
                allow_headers=["*"],
            )

        # Add request logging middleware
        @app.middleware("http")
        async def log_requests(request, call_next):
            start_time = time.time()

            try:
                response = await call_next(request)
                self.requests_count += 1

                duration = time.time() - start_time
                logger.debug(f"Request: {request.method} {request.url.path} - "
                             f"Status: {response.status_code} - Duration: {duration:.3f}s")

                return response

            except Exception as e:
                self.errors_count += 1
                logger.error(f"Request error: {request.method} {request.url.path} - Error: {e}")
                raise

        # Register routes
        self._register_data_routes(app)
        self._register_monitoring_routes(app)
        self._register_ticker_routes(app)
        self._register_admin_routes(app)
        self._register_websocket_routes(app)

        self.app = app
        return app

    async def start_server(self) -> bool:
        """
        Start REST API server with uvicorn.

        Returns:
            True if started successfully
        """
        if self.is_server_running:
            logger.warning("REST API server already running")
            return True

        try:
            # Create FastAPI app if not exists
            if not self.app:
                self.app = self.create_app()

            # Create uvicorn server config
            uvicorn_config = uvicorn.Config(
                app=self.app,
                host=self.config.host,
                port=self.config.port,
                log_level="info",
                access_log=False,  # We handle logging ourselves
                loop="asyncio"
            )

            # Create server instance
            self.server = uvicorn.Server(uvicorn_config)

            # Start server in background task with exception handler
            self.server_task = asyncio.create_task(
                self._run_server_with_exception_handling(),
                name="rest_api_server"
            )

            # Wait for server to actually start listening
            await asyncio.sleep(2)

            # Log successful start
            self.is_server_running = True
            logger.info(f"REST API server started on {self.config.host}:{self.config.port}")
            return True

        except Exception as e:
            logger.error(f"Failed to start REST API server: {e}")
            self.is_server_running = False
            return False

    async def _run_server_with_exception_handling(self):
        """
        Run uvicorn server with exception handling and logging.

        This wrapper catches and logs any exceptions that occur during server startup or runtime.
        """
        try:
            logger.info(f"Starting uvicorn server on {self.config.host}:{self.config.port}")
            await self.server.serve()
            logger.info("Uvicorn server stopped normally")
        except Exception as e:
            logger.error(f"❌ REST API server failed with exception: {e}", exc_info=True)
            self.is_server_running = False
            raise

    async def stop_server(self) -> None:
        """Stop REST API server gracefully."""
        if not self.is_server_running:
            logger.warning("REST API server not running")
            return

        try:
            logger.info("Stopping REST API server...")

            # Signal server to stop
            if self.server:
                self.server.should_exit = True

            # Cancel server task
            if self.server_task and not self.server_task.done():
                self.server_task.cancel()
                try:
                    await asyncio.wait_for(self.server_task, timeout=10.0)
                except asyncio.TimeoutError:
                    logger.warning("Server stop timeout, forcing shutdown")
                except asyncio.CancelledError:
                    pass

            self.is_server_running = False
            self.server = None
            self.server_task = None

            logger.info("REST API server stopped")

        except Exception as e:
            logger.error(f"Error stopping REST API server: {e}")

    def is_running(self) -> bool:
        """
        Check if REST API server is running.

        Returns:
            True if server is running
        """
        return self.is_server_running and (
                self.server_task is not None and not self.server_task.done()
        )

    def _register_data_routes(self, app: FastAPI):
        """Register data access routes."""

        @app.get("/api/v1/data/trades/{symbol}",
                 response_model=APIResponse[List[TradeResponse]],
                 tags=["data"],
                 summary="Get trades for symbol")
        async def get_trades(
                symbol: str,
                start_time: Optional[int] = Query(None, description="Start timestamp (ms)"),
                end_time: Optional[int] = Query(None, description="End timestamp (ms)"),
                limit: Optional[int] = Query(1000, ge=1, le=10000, description="Max trades to return"),
                from_id: Optional[int] = Query(None, description="Start from aggregate_id")
        ):
            """Get trades for specific symbol within time range."""
            symbol = symbol.upper()

            # Validate symbol exists
            if not self.shared.is_ticker_active(symbol):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Symbol {symbol} not found or not active"
                )

            try:
                # Get processor for this symbol
                processor = self.shared.get_ticker_processor(symbol)
                if not processor:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Data storage not available for {symbol}"
                    )

                # Set default time range if not provided
                if end_time is None:
                    end_time = int(time.time() * 1000)
                if start_time is None:
                    start_time = end_time - (24 * 60 * 60 * 1000)  # Last 24 hours

                # Read trades from storage
                trades = await processor.trades_writer.read_trades(symbol, start_time, end_time)

                # Filter by from_id if provided
                if from_id is not None:
                    trades = [trade for trade in trades if trade.aggregate_id >= from_id]

                # Apply limit
                if limit and len(trades) > limit:
                    trades = trades[:limit]

                # Convert to response format
                trade_responses = [
                    TradeResponse(
                        aggregate_id=trade.aggregate_id,
                        price=trade.price,
                        quantity=trade.quantity,
                        first_trade_id=trade.first_trade_id,
                        last_trade_id=trade.last_trade_id,
                        timestamp=trade.timestamp,
                        is_buyer_maker=trade.is_buyer_maker
                    ) for trade in trades
                ]

                return APIResponse(
                    success=True,
                    data=trade_responses,
                    message=f"Retrieved {len(trade_responses)} trades for {symbol}",
                    timestamp=int(time.time() * 1000),
                )

            except Exception as e:
                logger.error(f"Error retrieving trades for {symbol}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve trades: {str(e)}"
                )

        @app.get("/api/v1/data/trades/{symbol}/latest",
                 response_model=APIResponse[List[TradeResponse]],
                 tags=["data"],
                 summary="Get latest trades for symbol")
        async def get_latest_trades(
                symbol: str,
                limit: int = Query(100, ge=1, le=1000, description="Number of latest trades")
        ):
            """Get latest trades for symbol."""
            symbol = symbol.upper()

            # Validate symbol exists
            if not self.shared.is_ticker_active(symbol):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Symbol {symbol} not found or not active"
                )

            try:
                processor = self.shared.get_ticker_processor(symbol)
                if not processor:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"Data storage not available for {symbol}"
                    )

                # Get last hour of data and take latest trades
                end_time = int(time.time() * 1000)
                start_time = end_time - (60 * 60 * 1000)  # Last hour

                trades = await processor.trades_writer.read_trades(symbol, start_time, end_time)

                # Take latest trades (already sorted by aggregate_id)
                latest_trades = trades[-limit:] if len(trades) > limit else trades

                # Convert to response format
                trade_responses = [
                    TradeResponse(
                        aggregate_id=trade.aggregate_id,
                        price=trade.price,
                        quantity=trade.quantity,
                        first_trade_id=trade.first_trade_id,
                        last_trade_id=trade.last_trade_id,
                        timestamp=trade.timestamp,
                        is_buyer_maker=trade.is_buyer_maker
                    ) for trade in latest_trades
                ]

                return APIResponse(
                    success=True,
                    data=trade_responses,
                    message=f"Retrieved {len(trade_responses)} latest trades for {symbol}",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving latest trades for {symbol}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve latest trades: {str(e)}"
                )

        @app.get("/api/v1/data/ohlcv/{symbol}",
                 response_model=APIResponse[List[OHLCVCandle]],
                 tags=["data"],
                 summary="Get OHLCV candles for symbol")
        async def get_ohlcv(
                symbol: str,
                timeframe: int = Query(..., ge=1, description="Timeframe in minutes"),
                start_time: Optional[int] = Query(None, description="Start timestamp (ms)"),
                end_time: Optional[int] = Query(None, description="End timestamp (ms)"),
                limit: Optional[int] = Query(1000, ge=1, le=30000, description="Max candles")
        ):
            """
            Get OHLCV candles for symbol.

            Either provide start_time + end_time, OR just limit (returns last N candles).
            Uses existing MV if available, otherwise calculates on-the-fly from trades.
            """
            symbol = symbol.upper()

            if not self.shared.is_ticker_active(symbol):
                return APIResponse(
                    success=False,
                    data=[],
                    message=f"Symbol {symbol} not found",
                    timestamp=int(time.time() * 1000),
                    error="SymbolNotFound"
                )

            # Validate params logic
            if (start_time is None) != (end_time is None):
                return APIResponse(
                    success=False,
                    data=[],
                    message="Both start_time and end_time must be provided together",
                    timestamp=int(time.time() * 1000),
                    error="InvalidParameters"
                )

            try:
                clickhouse_manager = self.shared.clickhouse_manager
                if not clickhouse_manager:
                    return APIResponse(
                        success=False,
                        data=[],
                        message="Storage not available",
                        timestamp=int(time.time() * 1000),
                        error="StorageUnavailable"
                    )

                # Calculate time range
                if start_time is not None and end_time is not None:
                    # User provided exact range
                    calc_start = start_time
                    calc_end = end_time
                else:
                    # Calculate range from limit
                    current_time_ms = int(time.time() * 1000)
                    interval_ms = timeframe * 60 * 1000
                    calc_start = current_time_ms - (limit * interval_ms)
                    calc_end = current_time_ms

                # Get candles using get_ohlcv_range (which checks for MV internally)
                klines = await clickhouse_manager.get_ohlcv_range(
                    symbol=symbol,
                    timeframe_minutes=timeframe,
                    start_time=calc_start,
                    end_time=calc_end
                )

                # Apply limit if using limit-based query
                if start_time is None and end_time is None and len(klines) > limit:
                    klines = klines[-limit:]

                # Convert to response format
                candles = [
                    OHLCVCandle(
                        candle_time=k['timestamp'],
                        open=k['open'],
                        high=k['high'],
                        low=k['low'],
                        close=k['close'],
                        volume=k['volume'],
                        trades_count=k['trades_count']
                    ) for k in klines
                ]

                return APIResponse(
                    success=True,
                    data=candles,
                    message=f"Retrieved {len(candles)} candles for {symbol} {timeframe}m",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving OHLCV for {symbol}: {e}")
                return APIResponse(
                    success=False,
                    data=[],
                    message=f"Failed to retrieve OHLCV data: {str(e)}",
                    timestamp=int(time.time() * 1000),
                    error="InternalError"
                )

    def _register_monitoring_routes(self, app: FastAPI):
        """Register monitoring routes."""

        @app.get("/api/v1/monitoring/health",
                 response_model=HealthResponse,
                 tags=["monitoring"],
                 summary="System health check")
        async def health_check(detailed: bool = False):
            """
            Get system health status.

            Args:
                detailed: Include detailed processor information (default: False)
            """
            try:
                health = await self.shared.health_check(detailed=detailed)

                clean_health = self._clean_for_serialization(health)

                # Map degraded status correctly
                status = health.get('overall_status', 'unknown')
                if status == 'degraded':
                    status = 'degraded'  # Keep as is
                elif status == 'healthy':
                    status = 'healthy'
                else:
                    status = 'unhealthy'

                return HealthResponse(
                    status=status,
                    components=clean_health,
                    timestamp=int(time.time() * 1000),
                    uptime_seconds=time.time() - self.server_start_time
                )

            except Exception as e:
                logger.error(f"Health check error: {e}")
                return HealthResponse(
                    status="error",
                    components={"error": str(e)},
                    timestamp=int(time.time() * 1000),
                    uptime_seconds=time.time() - self.server_start_time
                )

        @app.get("/api/v1/monitoring/system/info",
                 response_model=APIResponse[SystemInfo],
                 tags=["monitoring"],
                 summary="System information")
        async def system_info():
            """Get detailed system information."""
            try:
                info = self.shared.get_system_info()

                clean_info = self._clean_for_serialization(info)

                system_info = SystemInfo(
                    initialized=bool(clean_info.get('initialized', False)),
                    total_ticker_tasks=int(clean_info.get('total_ticker_tasks', 0)),
                    active_tickers=int(clean_info.get('active_tickers', 0)),
                    binance_client_connected=bool(clean_info.get('binance_client_connected', False)),
                    weight_coordinator_status=str(clean_info.get('weight_coordinator_status', 'unknown')),
                    ticker_synchronizer_status=str(clean_info.get('ticker_synchronizer_status', 'unknown')),
                    environment=str(clean_info.get('environment', 'unknown')),
                    api_requests_count=self.requests_count,
                    api_errors_count=self.errors_count,
                    server_uptime_seconds=time.time() - self.server_start_time
                )

                return APIResponse(
                    success=True,
                    data=system_info,
                    message="System information retrieved successfully",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"System info error: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve system info: {str(e)}"
                )

        @app.get("/api/v1/monitoring/server",
                 response_model=APIResponse[Dict[str, Any]],
                 tags=["monitoring"],
                 summary="Server status")
        async def server_status():
            """Get REST API server status."""
            try:
                stats = self.get_server_stats()
                clean_stats = self._clean_for_serialization(stats)

                return APIResponse(
                    success=True,
                    data=clean_stats,
                    message="Server status retrieved successfully",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Server status error: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve server status: {str(e)}"
                )

    def _register_ticker_routes(self, app: FastAPI):
        """Register ticker management routes."""

        @app.get("/api/v1/tickers/",
                 response_model=APIResponse[List[str]],
                 tags=["tickers"],
                 summary="Get all active tickers")
        async def get_tickers():
            """Get list of all active ticker symbols."""
            try:
                active_symbols = self.shared.get_active_ticker_symbols()

                return APIResponse(
                    success=True,
                    data=active_symbols,
                    message=f"Retrieved {len(active_symbols)} active tickers",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving tickers: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve tickers: {str(e)}"
                )

        @app.get("/api/v1/tickers/running",
                 response_model=APIResponse[Dict[str, Any]],
                 tags=["tickers"],
                 summary="Get running tickers")
        async def get_running_tickers():
            """Get list of tickers that are currently in RUNNING state."""
            try:
                running_tickers = []
                stopped_tickers = []

                for symbol, task in self.shared.tickers.items():
                    if task.done():
                        stopped_tickers.append(symbol)
                    else:
                        processor = getattr(task, 'processor', None)
                        if processor and processor._state.is_active():
                            running_tickers.append(symbol)
                        else:
                            stopped_tickers.append(symbol)

                return APIResponse(
                    success=True,
                    data={
                        "running_count": len(running_tickers),
                        "stopped_count": len(stopped_tickers),
                        "total_count": len(self.shared.tickers),
                        "running": sorted(running_tickers),
                        "stopped": sorted(stopped_tickers)
                    },
                    message=f"{len(running_tickers)} running, {len(stopped_tickers)} stopped",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving running tickers: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve running tickers: {str(e)}"
                )

        @app.get("/api/v1/tickers/{symbol}",
                 response_model=APIResponse[TickerInfo],
                 tags=["tickers"],
                 summary="Get ticker information")
        async def get_ticker_info(symbol: str):
            """Get detailed information for specific ticker."""
            symbol = symbol.upper()

            if not self.shared.is_ticker_active(symbol):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Symbol {symbol} not found or not active"
                )

            try:
                ticker_model = self.shared.get_ticker_model(symbol)
                processor_stats = self.shared.get_ticker_processor_stats(symbol)
                clean_processor_stats = self._clean_for_serialization(processor_stats)

                if not ticker_model:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Ticker model not found for {symbol}"
                    )

                ticker_info = TickerInfo(
                    symbol=ticker_model.symbol,
                    base_asset=ticker_model.base_asset,
                    quote_asset=ticker_model.quote_asset,
                    status=ticker_model.status.name,
                    is_tradeable=ticker_model.is_tradeable(),
                    onboard_date=ticker_model.onboard_date,
                    price_precision=ticker_model.price_precision,
                    quantity_precision=ticker_model.quantity_precision,
                    processor_stats=clean_processor_stats
                )

                return APIResponse(
                    success=True,
                    data=ticker_info,
                    message=f"Ticker information for {symbol}",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving ticker info for {symbol}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve ticker info: {str(e)}"
                )

        @app.get("/api/v1/tickers/{symbol}/status",
                 response_model=APIResponse[Dict[str, Any]],
                 tags=["tickers"],
                 summary="Get ticker processing status")
        async def get_ticker_status(symbol: str):
            """Get comprehensive processing status for ticker."""
            symbol = symbol.upper()

            if not self.shared.is_ticker_active(symbol):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Symbol {symbol} not found or not active"
                )

            try:
                comprehensive_status = self.shared.get_ticker_comprehensive_status(symbol)

                if not comprehensive_status:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Status not available for {symbol}"
                    )

                clean_status = self._clean_for_serialization(comprehensive_status)

                return APIResponse(
                    success=True,
                    data=clean_status,
                    message=f"Comprehensive status for {symbol}",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving ticker status for {symbol}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve ticker status: {str(e)}"
                )

        @app.post("/api/v1/tickers/{symbol}/pause",
                  response_model=APIResponse[bool],
                  tags=["tickers"],
                  summary="Pause ticker processing")
        async def pause_ticker(symbol: str):
            """Pause processing for specific ticker."""
            symbol = symbol.upper()

            if not self.shared.is_ticker_active(symbol):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Symbol {symbol} not found or not active"
                )

            try:
                success = self.shared.pause_ticker(symbol)

                return APIResponse(
                    success=success,
                    data=success,
                    message=f"Ticker {symbol} {'paused' if success else 'failed to pause'}",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error pausing ticker {symbol}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to pause ticker: {str(e)}"
                )

        @app.post("/api/v1/tickers/{symbol}/resume",
                  response_model=APIResponse[bool],
                  tags=["tickers"],
                  summary="Resume ticker processing")
        async def resume_ticker(symbol: str):
            """Resume processing for specific ticker."""
            symbol = symbol.upper()

            if not self.shared.is_ticker_active(symbol):
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Symbol {symbol} not found or not active"
                )

            try:
                success = self.shared.resume_ticker(symbol)

                return APIResponse(
                    success=success,
                    data=success,
                    message=f"Ticker {symbol} {'resumed' if success else 'failed to resume'}",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error resuming ticker {symbol}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to resume ticker: {str(e)}"
                )

    def _register_admin_routes(self, app: FastAPI):
        """Register administrative routes."""

        @app.get("/api/v1/admin/coordinators/weight",
                 response_model=APIResponse[Dict[str, Any]],
                 tags=["admin"],
                 summary="WeightCoordinator statistics")
        async def get_weight_stats():
            """Get WeightCoordinator statistics."""
            try:
                stats = self.shared.get_weight_coordinator_stats()

                if not stats:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="WeightCoordinator not available"
                    )

                clean_stats = self._clean_for_serialization(stats)

                return APIResponse(
                    success=True,
                    data=clean_stats,
                    message="WeightCoordinator statistics retrieved",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving weight coordinator stats: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve weight coordinator stats: {str(e)}"
                )

        @app.get("/api/v1/admin/coordinators/synchronizer",
                 response_model=APIResponse[Dict[str, Any]],
                 tags=["admin"],
                 summary="TickerSynchronizer statistics")
        async def get_synchronizer_stats():
            """Get TickerSynchronizer statistics."""
            try:
                stats = self.shared.get_ticker_synchronizer_stats()

                if not stats:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="TickerSynchronizer not available"
                    )

                clean_stats = self._clean_for_serialization(stats)
                return APIResponse(
                    success=True,
                    data=clean_stats,
                    message="TickerSynchronizer statistics retrieved",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving synchronizer stats: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve synchronizer stats: {str(e)}"
                )

        @app.post("/api/v1/admin/system/sync",
                  response_model=APIResponse[bool],
                  tags=["admin"],
                  summary="Force system synchronization")
        async def force_sync():
            """Force immediate synchronization with Binance."""
            try:
                if not self.shared.ticker_synchronizer:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="TickerSynchronizer not available"
                    )

                success = await self.shared.ticker_synchronizer.force_sync()

                return APIResponse(
                    success=success,
                    data=success,
                    message=f"Force sync {'completed' if success else 'failed'}",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error during force sync: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to force sync: {str(e)}"
                )

        @app.get("/api/v1/admin/websocket/status",
                 response_model=APIResponse[Dict[str, Any]],
                 tags=["admin"],
                 summary="WebSocket manager status")
        async def get_websocket_status():
            """Get WebSocket manager status and statistics."""
            try:
                if not self.central_websocket_manager:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="WebSocket manager not available"
                    )

                stats = self.central_websocket_manager.get_status()
                clean_stats = self._clean_for_serialization(stats)

                return APIResponse(
                    success=True,
                    data=clean_stats,
                    message="WebSocket status retrieved successfully",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving WebSocket status: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve WebSocket status: {str(e)}"
                )

        @app.get("/api/v1/admin/websocket/connections",
                 response_model=APIResponse[Dict[str, Any]],
                 tags=["admin"],
                 summary="Active WebSocket connections")
        async def get_websocket_connections():
            """Get information about active WebSocket connections."""
            try:
                if not self.central_websocket_manager:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail="WebSocket manager not available"
                    )

                connections_info = self.central_websocket_manager.get_connections_info()
                clean_connections = self._clean_for_serialization(connections_info)

                return APIResponse(
                    success=True,
                    data=clean_connections,
                    message="WebSocket connections info retrieved successfully",
                    timestamp=int(time.time() * 1000)
                )

            except Exception as e:
                logger.error(f"Error retrieving WebSocket connections: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to retrieve WebSocket connections: {str(e)}"
                )

        @app.get("/api/v1/tickers/{symbol}/mvs",
                 response_model=APIResponse[List[Dict[str, Any]]],
                 tags=["tickers"],
                 summary="List materialized views for ticker")
        async def list_ticker_mvs(symbol: str):
            """List all materialized views for specific ticker."""
            symbol = symbol.upper()

            try:
                processor = self.shared.get_ticker_processor(symbol)
                if not processor:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Ticker processor not found for {symbol}"
                    )

                # Если это ClickHouseManager, получаем список MV
                clickhouse_manager = self.shared.clickhouse_manager
                if clickhouse_manager and hasattr(clickhouse_manager, 'list_ticker_mvs'):
                    mvs = await clickhouse_manager.list_ticker_mvs(symbol)

                    return APIResponse(
                        success=True,
                        data=mvs,
                        message=f"Retrieved {len(mvs)} materialized views for {symbol}",
                        timestamp=int(time.time() * 1000)
                    )
                else:
                    # BinaryFileManager не поддерживает MV
                    return APIResponse(
                        success=True,
                        data=[],
                        message=f"No materialized views available for {symbol}",
                        timestamp=int(time.time() * 1000)
                    )

            except Exception as e:
                logger.error(f"Error listing MVs for {symbol}: {e}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to list materialized views: {str(e)}"
                )

    def _register_websocket_routes(self, app: FastAPI):
        """Register WebSocket routes."""

        @app.websocket("/ws/stream")
        async def websocket_stream(websocket: WebSocket):
            """WebSocket endpoint for real-time OHLCV streaming."""

            # IP validation for WebSocket
            client_ip = websocket.client.host
            if not self._is_ip_allowed(client_ip):
                logger.warning(f"WebSocket access denied for IP: {client_ip}")
                await websocket.close(code=1008, reason="Access denied")
                return

            if not self.central_websocket_manager:
                await websocket.close(code=1011, reason="WebSocket service not available")
                return

            await websocket.accept()
            await self.central_websocket_manager.handle_connection(websocket, client_ip)

    def _clean_for_serialization(self, data: Any) -> Any:
        """Remove non-serializable objects from data."""
        if isinstance(data, dict):
            return {
                key: self._clean_for_serialization(value)
                for key, value in data.items()
                if not callable(value) and 'method' not in str(type(value))
            }
        elif isinstance(data, (list, tuple)):
            return [self._clean_for_serialization(item) for item in data]
        else:
            return data if not callable(data) else str(data)

    def get_server_stats(self) -> Dict[str, Any]:
        """Get REST API server statistics."""
        return {
            'requests_count': self.requests_count,
            'errors_count': self.errors_count,
            'uptime_seconds': time.time() - self.server_start_time,
            'host': self.config.host,
            'port': self.config.port,
            'is_running': self.is_running(),
            'server_active': self.server is not None
        }


def create_app(shared: Shared, config: Config) -> tuple[FastAPI, RestAPIServer]:
    """
    Factory function to create REST API application and server instance.

    Args:
        shared: Shared resource manager
        config: Main configuration object

    Returns:
        Tuple of (FastAPI app, RestAPIServer instance)
    """
    server = RestAPIServer(shared, config)
    app = server.create_app()
    return app, server
