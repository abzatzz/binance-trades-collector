"""
WeightCoordinator - Binance API Rate Limiting with Priority Queues

Manages all Binance API requests with dual-queue system:
- Critical queue: exchangeInfo requests (highest priority)
- Normal queue: aggTrades requests (FIFO processing)

Features:
- Weight tracking with sliding window (2400/minute limit)
- Dual-queue priority system with FIFO within each queue
- Automatic rate limiting and recovery mechanisms
- Comprehensive statistics and health monitoring
- Transparent integration with existing components
"""

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Tuple, Optional, Any, Deque

from loggerino import loggerino

logger = loggerino.get('weight_coordinator')


@dataclass
class WeightRequest:
    """Individual API request with metadata"""
    endpoint: str
    expected_weight: int
    kwargs: Dict[str, Any]
    future: asyncio.Future
    created_at: float
    retry_count: int = 0


@dataclass
class WeightStats:
    """Weight coordinator statistics"""
    current_weight_usage: int
    available_weight: int
    critical_queue_size: int
    normal_queue_size: int
    total_requests_processed: int
    requests_per_minute: float
    last_request_time: Optional[float]
    is_rate_limited: bool
    recovery_end_time: Optional[float]


class WeightCoordinator:
    """
    Centralized coordinator for Binance API rate limiting with priority queues.

    Architecture:
    - Critical queue: exchangeInfo requests (always processed first)
    - Normal queue: aggTrades requests (FIFO when no critical requests)
    - Weight tracking: Sliding window of 60 seconds for 2400 weight/minute limit
    - Recovery mechanism: Automatic handling of rate limit responses
    """
    from typing import TYPE_CHECKING
    if TYPE_CHECKING:
        from ..models.config import Config

    def __init__(self, binance_client, config: 'Config'):
        self.binance_client = binance_client
        self.config = config

        # Dual queue system
        self.critical_queue = asyncio.Queue()  # exchangeInfo only
        self.normal_queue = asyncio.Queue()  # aggTrades only

        # Weight tracking with sliding window
        self.weight_history: Deque[Tuple[float, int]] = deque()  # (timestamp, weight)
        self.max_weight_per_minute = config.binance.max_weight_per_minute
        self.weight_buffer = config.binance.weight_buffer

        # Statistics and monitoring
        self.total_requests_processed = 0
        self.last_request_time: Optional[float] = None

        # Rate limiting and recovery
        self.is_rate_limited = False
        self.recovery_end_time: Optional[float] = None
        self.rate_limit_count = 0

        # Background processing task
        self.processing_task: Optional[asyncio.Task] = None
        self.is_running = False

        logger.info("WeightCoordinator initialized with dual-queue priority system")

    async def start(self):
        """Start the background queue processing task"""
        if self.is_running:
            return

        self.is_running = True
        self.processing_task = asyncio.create_task(self._process_queues())
        logger.info("WeightCoordinator processing started")

    async def shutdown(self):
        """Graceful shutdown of the weight coordinator"""
        if not self.is_running:
            # Even if not running, still cancel pending requests
            await self._cancel_pending_requests()
            return

        self.is_running = False

        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass

        # Cancel any pending requests
        await self._cancel_pending_requests()
        logger.info("WeightCoordinator shutdown completed")

    async def request(self, endpoint: str, expected_weight: int, **kwargs) -> dict:
        """
        Main interface for API requests with automatic rate limiting.

        Args:
            endpoint: API endpoint name ('exchangeInfo' or 'aggTrades')
            expected_weight: Expected weight consumption for this request
            **kwargs: Additional parameters for the API call

        Returns:
            API response as dictionary

        Raises:
            RuntimeError: If coordinator is not running
            ValueError: If endpoint is not supported
        """
        if not self.is_running:
            raise RuntimeError("WeightCoordinator is not running. Call start() first.")

        if endpoint not in ["exchangeInfo", "aggTrades"]:
            raise ValueError(f"Unsupported endpoint: {endpoint}")

        # Create request item with future for async result
        future = asyncio.Future()
        request_item = WeightRequest(
            endpoint=endpoint,
            expected_weight=expected_weight,
            kwargs=kwargs,
            future=future,
            created_at=time.time()
        )

        # Route to appropriate queue based on endpoint
        if endpoint == "exchangeInfo":
            await self.critical_queue.put(request_item)
            logger.debug("Added exchangeInfo request to critical queue")
        elif endpoint == "aggTrades":
            await self.normal_queue.put(request_item)
            logger.debug(f"Added aggTrades request to normal queue for {kwargs.get('symbol', 'unknown')}")

        # Wait for processing result
        return await future

    async def _process_queues(self):
        """
        Background task that processes both queues with priority logic.

        Processing order:
        1. Always check critical_queue first (exchangeInfo)
        2. If critical_queue is empty, process normal_queue (aggTrades)
        3. Respect weight limits and handle rate limiting
        """
        logger.info("Queue processing started")

        while self.is_running:
            try:
                # Check if we're in recovery mode
                if self.is_rate_limited and self.recovery_end_time is not None:
                    if time.time() < self.recovery_end_time:
                        await asyncio.sleep(1.0)
                        continue
                    else:
                        self.is_rate_limited = False
                        self.recovery_end_time = None
                        logger.info("Rate limit recovery completed")

                request_item = None
                queue_type = None

                # Priority 1: Check critical queue (exchangeInfo)
                try:
                    request_item = self.critical_queue.get_nowait()
                    queue_type = "critical"
                    logger.debug("Processing CRITICAL request: exchangeInfo")
                except asyncio.QueueEmpty:
                    pass

                # Priority 2: Check normal queue (aggTrades) if critical is empty
                if request_item is None:
                    try:
                        request_item = self.normal_queue.get_nowait()
                        queue_type = "normal"
                        symbol = request_item.kwargs.get('symbol', 'unknown')
                        logger.debug(f"Processing NORMAL request: aggTrades for {symbol}")
                    except asyncio.QueueEmpty:
                        pass

                # If both queues are empty, wait and continue
                if request_item is None:
                    await asyncio.sleep(0.1)
                    continue

                # Wait for weight availability
                await self._wait_for_weight_availability(request_item.expected_weight)

                # Execute the request
                try:
                    response = await self._execute_request(request_item)
                    self._update_weight_tracking(request_item.expected_weight)
                    self.total_requests_processed += 1
                    self.last_request_time = time.time()

                    # Mark task as done for the appropriate queue
                    if queue_type == "critical":
                        self.critical_queue.task_done()
                    elif queue_type == "normal":
                        self.normal_queue.task_done()

                    # Return result to waiting coroutine
                    request_item.future.set_result(response)

                    # Log successful processing
                    elapsed = time.time() - request_item.created_at
                    logger.debug(f"Request processed successfully in {elapsed:.2f}s")

                except Exception as e:
                    # Handle errors and return to waiting coroutine
                    logger.error(f"Request processing failed: {e}")

                    if queue_type == "critical":
                        self.critical_queue.task_done()
                    elif queue_type == "normal":
                        self.normal_queue.task_done()

                    # Check if it's a rate limit error
                    if self._is_rate_limit_error(e):
                        await self._handle_rate_limit_error(e)

                        # Add retry logic with limit
                        request_item.retry_count += 1
                        if request_item.retry_count < 3:
                            # Put back in appropriate queue for retry
                            if queue_type == "critical":
                                await self.critical_queue.put(request_item)
                            elif queue_type == "normal":
                                await self.normal_queue.put(request_item)
                            continue
                        else:
                            logger.warning(f"Max retries exceeded for {request_item.endpoint}")

                    request_item.future.set_exception(e)

            except Exception as e:
                logger.error(f"Unexpected error in queue processing: {e}")
                await asyncio.sleep(1.0)

        logger.info("Queue processing stopped")

    async def _wait_for_weight_availability(self, required_weight: int):
        """
        Wait until sufficient weight is available for the request.

        Args:
            required_weight: Weight required for the upcoming request
        """
        max_wait_iterations = 300  # Maximum 5 minutes wait
        iterations = 0

        while iterations < max_wait_iterations:
            self._cleanup_old_weight_records()
            current_weight = self._get_current_weight_usage()
            available_weight = self.max_weight_per_minute - current_weight - self.weight_buffer

            if required_weight <= available_weight:
                return

            # Log weight status occasionally
            if iterations % 60 == 0:  # Every minute
                logger.debug(f"Waiting for weight availability: {current_weight}/{self.max_weight_per_minute} used")

            await asyncio.sleep(1.0)
            iterations += 1

        logger.warning(f"Maximum wait time exceeded for weight availability")

    def _cleanup_old_weight_records(self):
        """Remove weight records older than 1 minute from tracking"""
        cutoff_time = time.time() - 60
        while self.weight_history and self.weight_history[0][0] < cutoff_time:
            self.weight_history.popleft()

    def _get_current_weight_usage(self) -> int:
        """Calculate current weight usage in the sliding window"""
        self._cleanup_old_weight_records()
        return sum(weight for _, weight in self.weight_history)

    def _update_weight_tracking(self, weight_used: int):
        """Add weight usage to tracking history"""
        current_time = time.time()
        self.weight_history.append((current_time, weight_used))

        # Keep history reasonable size (max ~3600 entries for 1 request per second)
        if len(self.weight_history) > 5000:
            # Remove oldest 1000 entries
            for _ in range(1000):
                if self.weight_history:
                    self.weight_history.popleft()

    async def _execute_request(self, request_item: WeightRequest) -> dict:
        """
        Execute the actual API request through binance_client.

        Args:
            request_item: Request details and parameters

        Returns:
            API response as dictionary
        """
        endpoint = request_item.endpoint
        kwargs = request_item.kwargs

        start_time = time.time()

        try:
            if endpoint == "exchangeInfo":
                response = await self.binance_client.futures_exchange_info()
                logger.debug("Executed exchangeInfo request")

            elif endpoint == "aggTrades":
                response = await self.binance_client.futures_aggregate_trades(**kwargs)
                symbol = kwargs.get('symbol', 'unknown')
                logger.debug(f"Executed aggTrades request for {symbol}")

            else:
                raise ValueError(f"Unsupported endpoint: {endpoint}")

            # Log execution time
            execution_time = time.time() - start_time
            logger.debug(f"Request executed in {execution_time:.3f}s")

            return response

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Request failed after {execution_time:.3f}s: {e}")
            raise

    def _is_rate_limit_error(self, error: Exception) -> bool:
        """Check if error indicates rate limiting"""
        error_str = str(error).lower()
        return any(phrase in error_str for phrase in [
            "429", "rate limit", "too many requests", "weight exceeded"
        ])

    async def _handle_rate_limit_error(self, error: Exception):
        """Handle rate limiting by entering recovery mode"""
        self.rate_limit_count += 1
        recovery_delay = max(5, min(60, 10 * self.rate_limit_count))  # минимум 5 сек

        self.is_rate_limited = True
        self.recovery_end_time = time.time() + recovery_delay

        logger.warning(f"Rate limit hit (#{self.rate_limit_count}), "
                       f"entering recovery mode for {recovery_delay}s: {error}")

    async def _cancel_pending_requests(self):
        """Cancel all pending requests during shutdown"""
        cancelled_count = 0

        # Cancel critical queue requests
        while not self.critical_queue.empty():
            try:
                request_item = self.critical_queue.get_nowait()
                if not request_item.future.cancelled() and not request_item.future.done():
                    request_item.future.cancel()
                self.critical_queue.task_done()
                cancelled_count += 1
            except asyncio.QueueEmpty:
                break

        # Cancel normal queue requests
        while not self.normal_queue.empty():
            try:
                request_item = self.normal_queue.get_nowait()
                if not request_item.future.cancelled() and not request_item.future.done():
                    request_item.future.cancel()
                self.normal_queue.task_done()
                cancelled_count += 1
            except asyncio.QueueEmpty:
                break

        if cancelled_count > 0:
            logger.info(f"Cancelled {cancelled_count} pending requests during shutdown")

    # Public monitoring and statistics methods

    def get_stats(self) -> WeightStats:
        """Get current weight coordinator statistics"""
        return WeightStats(
            current_weight_usage=self._get_current_weight_usage(),
            available_weight=self.max_weight_per_minute - self._get_current_weight_usage() - self.weight_buffer,
            critical_queue_size=self.critical_queue.qsize(),
            normal_queue_size=self.normal_queue.qsize(),
            total_requests_processed=self.total_requests_processed,
            requests_per_minute=self._calculate_requests_per_minute(),
            last_request_time=self.last_request_time,
            is_rate_limited=self.is_rate_limited,
            recovery_end_time=self.recovery_end_time
        )

    def _calculate_requests_per_minute(self) -> float:
        """Calculate current requests per minute rate"""
        if not self.weight_history:
            return 0.0

        # Count requests in last minute
        cutoff_time = time.time() - 60
        recent_requests = sum(1 for timestamp, _ in self.weight_history if timestamp > cutoff_time)

        return float(recent_requests)

    def is_healthy(self) -> bool:
        """Check if weight coordinator is operating normally"""
        stats = self.get_stats()

        # Healthy means coordinator is processing requests without critical issues
        # Large queues and low available weight are normal under load
        return (
                not stats.is_rate_limited and  # Not blocked by Binance
                stats.critical_queue_size < 50  # Critical queue not severely backed up
        )

    def get_queue_info(self) -> Dict[str, int]:
        """Get current queue sizes"""
        return {
            "critical_queue": self.critical_queue.qsize(),
            "normal_queue": self.normal_queue.qsize(),
            "total_pending": self.critical_queue.qsize() + self.normal_queue.qsize()
        }
