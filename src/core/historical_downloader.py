"""
HistoricalDownloader - Binance Historical AggTrade Data Downloader
================================================================

Downloads historical trade data from Binance API with comprehensive error handling
and WebSocket coordination. Includes robust Binance API error management.

File: src/core/historical_downloader.py
"""

import asyncio
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, timezone

from loggerino import loggerino
from binance import AsyncClient

from ..models.ticker_model import TickerModel
from ..models.aggtrade import AggTrade
from ..models.config import ProcessingConfig
from ..models.binance_errors import BinanceClientError, BinanceAPIError, is_recoverable_error, get_retry_delay
from .trades_interfaces import TradesWriter
from .telegram_notifier import get_notifier, AlertLevel
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .weight_coordinator import WeightCoordinator

_default_logger = loggerino.get('historical_downloader')


@dataclass
class DownloadProgress:
    """Progress tracking for historical downloads"""
    total_trades_downloaded: int
    api_requests_made: int
    bytes_written: int
    current_timestamp: int
    start_timestamp: int
    websocket_activated: bool
    download_rate_per_minute: float
    estimated_completion_time: Optional[int]
    last_aggregate_id: Optional[int]


@dataclass
class HistoricalDownloadResult:
    """Result of historical download operation"""
    success: bool
    trades_loaded: int
    time_range_start: int
    time_range_end: int
    api_requests_made: int
    errors_count: int
    binance_errors_count: int
    recoverable_errors_count: int
    download_duration_seconds: float
    gaps_filled: int
    bytes_written: int
    websocket_handoff_completed: bool
    last_aggregate_id: Optional[int]


@dataclass
class BatchValidationResult:
    """Result of batch validation"""
    is_valid: bool
    error_message: Optional[str] = None
    expected_first_id: Optional[int] = None
    actual_first_id: Optional[int] = None
    gap_size: int = 0


class HistoricalDownloader:
    """
    Historical trade data downloader with comprehensive Binance error handling.

    Responsibilities:
    - Download historical trades from Binance API with proper error classification
    - Handle all types of Binance API errors with appropriate recovery strategies
    - Coordinate WebSocket activation at merge_threshold_hours
    - Save data through BinaryFileManager with error recovery
    - Provide detailed progress monitoring and error statistics
    """

    def __init__(
            self,
            ticker_model: TickerModel,
            binance_client: AsyncClient,
            weight_coordinator: 'WeightCoordinator',
            trades_writer: TradesWriter,
            config: ProcessingConfig,
            logger=None
    ):
        """
        Initialize HistoricalDownloader.

        Args:
            ticker_model: TickerModel with ticker metadata
            binance_client: Binance AsyncClient for API calls
            weight_coordinator: WeightCoordinator for rate limiting
            trades_writer: TradesWriter interface for data operations
            config: ProcessingConfig with download settings
        """
        # Validation settings
        self.MAX_ALLOWED_GAP = 100  # Maximum allowed gap between expected and received ID
        self.MAX_VALIDATION_RETRIES = 3  # Number of retries on validation failure
        self.VALIDATION_RETRY_DELAY = 2  # Seconds to wait between retries

        self.ticker_model = ticker_model
        self.binance_client = binance_client
        self.weight_coordinator = weight_coordinator
        self.trades_writer = trades_writer  # НОВЫЙ ИНТЕРФЕЙС
        self.config = config
        # Use provided logger or fallback to default
        self.logger = logger if logger else _default_logger

        # Download state
        self.is_running = False
        self.should_stop = False
        self.download_start_time: Optional[float] = None

        # Statistics
        self.total_trades_downloaded = 0
        self.api_requests_made = 0
        self.bytes_written = 0
        self.errors_count = 0
        self.binance_errors_count = 0
        self.recoverable_errors_count = 0
        self.last_aggregate_id: Optional[int] = None

        # Error handling
        self.consecutive_errors = 0
        self.max_consecutive_errors = 10
        self.last_error_time: Optional[float] = None

        self.logger.info(f"HistoricalDownloader initialized for {ticker_model.symbol}")

    # ===== MAIN DOWNLOAD METHODS =====

    async def download_historical_data(self) -> HistoricalDownloadResult:
        """
        Main method to download historical trade data with comprehensive error handling.

        Returns:
            HistoricalDownloadResult with download statistics
        """
        self.logger.info(f"Starting historical data download for {self.ticker_model.symbol}")

        self.download_start_time = time.time()
        self.is_running = True
        start_timestamp = None
        end_timestamp = None

        try:
            # Step 1: Determine download strategy and start point
            strategy, start_point, from_id = await self._determine_request_strategy()
            start_timestamp = start_point

            self.logger.info(f"Download strategy for {self.ticker_model.symbol}: {strategy}, "
                             f"start_point: {start_point}, from_id: {from_id}")

            # Step 2: Execute download loop
            end_timestamp = await self._execute_download_loop(strategy, start_point, from_id)

            # Step 4: Calculate final statistics
            download_duration = time.time() - self.download_start_time

            return HistoricalDownloadResult(
                success=True,
                trades_loaded=self.total_trades_downloaded,
                time_range_start=start_timestamp,
                time_range_end=end_timestamp,
                api_requests_made=self.api_requests_made,
                errors_count=self.errors_count,
                binance_errors_count=self.binance_errors_count,
                recoverable_errors_count=self.recoverable_errors_count,
                download_duration_seconds=download_duration,
                gaps_filled=await self._detect_data_gaps(),
                bytes_written=self.bytes_written,
                websocket_handoff_completed=False,
                last_aggregate_id=self.last_aggregate_id
            )

        except Exception as e:
            self._update_error_statistics(e)
            download_duration = time.time() - (self.download_start_time or time.time())
            self.logger.error(f"Historical download failed for {self.ticker_model.symbol}: {e}", exc_info=True)

            return HistoricalDownloadResult(
                success=False,
                trades_loaded=self.total_trades_downloaded,
                time_range_start=start_timestamp or 0,
                time_range_end=end_timestamp or 0,
                api_requests_made=self.api_requests_made,
                errors_count=self.errors_count,
                binance_errors_count=self.binance_errors_count,
                recoverable_errors_count=self.recoverable_errors_count,
                download_duration_seconds=download_duration,
                gaps_filled=0,
                bytes_written=self.bytes_written,
                websocket_handoff_completed=False,
                last_aggregate_id=self.last_aggregate_id
            )

        finally:
            self.is_running = False

    async def _detect_data_gaps(self) -> int:
        """Detect and count gaps in historical data."""
        try:
            # Проверить есть ли данные за последние 24 часа
            current_time = int(time.time() * 1000)
            day_ago = current_time - (24 * 60 * 60 * 1000)

            try:
                recent_trades = await self.trades_writer.read_trades(
                    self.ticker_model.symbol, day_ago, current_time
                )
                return 0 if recent_trades else 1
            except Exception:
                return 0

        except Exception as e:
            self.logger.warning(f"Gap detection failed for {self.ticker_model.symbol}: {e}")
            return 0

    async def _execute_download_loop(
            self,
            strategy: str,
            start_point: int,
            from_id: Optional[int],
    ) -> int:
        """
        Execute main download loop with comprehensive error handling and validation.

        Args:
            strategy: 'first_load' or 'continue_load'
            start_point: Starting timestamp
            from_id: Starting aggregate_id (for continue_load)

        Returns:
            End timestamp of last downloaded trade
        """

        is_first_request = True
        current_from_id = from_id
        last_trade_timestamp = start_point
        self.consecutive_errors = 0
        validation_retry_count = 0

        self.logger.info(f"Starting download loop for {self.ticker_model.symbol}, "
                         f"strategy={strategy}, from_id={current_from_id}")

        while not self.should_stop and self.consecutive_errors < self.max_consecutive_errors:
            try:
                # Build API request parameters
                request_params = self._build_request_params(current_from_id)

                # Execute API request with comprehensive error handling
                response = await self._execute_api_request_with_retry(request_params)
                self.api_requests_made += 1
                self.consecutive_errors = 0  # Reset error counter on success

                # Process response
                if not response:
                    self.logger.warning(f"Empty response for {self.ticker_model.symbol}, stopping download")
                    break

                trades = AggTrade.from_binance_list(response)
                batch_size = len(trades)

                self.logger.debug(f"Downloaded {batch_size} trades for {self.ticker_model.symbol}")

                if batch_size == 0:
                    self.logger.info(f"No more trades available for {self.ticker_model.symbol}")
                    break

                # CRITICAL: Validate batch before saving
                validation = self._validate_batch(trades, current_from_id, is_first_request, strategy)

                if not validation.is_valid:
                    validation_retry_count += 1
                    self.logger.error(
                        f"Batch validation failed for {self.ticker_model.symbol}: {validation.error_message} "
                        f"(retry {validation_retry_count}/{self.MAX_VALIDATION_RETRIES})"
                    )

                    if validation_retry_count >= self.MAX_VALIDATION_RETRIES:
                        # Send Telegram alert
                        await self._send_gap_alert(validation)
                        raise ValueError(f"Batch validation failed after {self.MAX_VALIDATION_RETRIES} retries: "
                                         f"{validation.error_message}")

                    # Wait and retry
                    await asyncio.sleep(self.VALIDATION_RETRY_DELAY)
                    continue

                # Reset retry counter on successful validation
                validation_retry_count = 0

                # Save trades to storage
                await self._save_trades_batch(trades)

                # Update progress tracking
                if trades:
                    last_trade_timestamp = trades[-1].timestamp
                    current_from_id = trades[-1].aggregate_id + 1
                    self.last_aggregate_id = trades[-1].aggregate_id

                    last_trade_datetime = datetime.fromtimestamp(last_trade_timestamp / 1000, tz=timezone.utc)
                    last_trade_str = last_trade_datetime.strftime('%Y-%m-%d %H:%M:%S UTC')

                    self.logger.debug(f"Downloaded batch for {self.ticker_model.symbol}: {batch_size} trades, "
                                      f"last trade: ID {self.last_aggregate_id} at {last_trade_str}")

                # Update statistics
                self.total_trades_downloaded += batch_size

                # Log progress periodically
                if self.api_requests_made % 50 == 0:
                    progress = self._calculate_progress()
                    self.logger.info(f"Download progress for {self.ticker_model.symbol}: "
                                     f"{progress.total_trades_downloaded} trades, "
                                     f"{progress.api_requests_made} requests, "
                                     f"{progress.download_rate_per_minute:.0f} trades/min")

                # Check if we should continue (Binance returns < 1000 when no more data)
                if batch_size < 1000:
                    self.logger.info(f"Download completed for {self.ticker_model.symbol} "
                                     f"(received {batch_size} < 1000 trades)")
                    break

                is_first_request = False

            except BinanceClientError as e:
                self._handle_binance_error(e)

                if not e.is_recoverable():
                    self.logger.error(f"Non-recoverable Binance error for {self.ticker_model.symbol}: {e}")
                    raise

                # Wait for retry delay
                retry_delay = e.get_retry_delay()
                self.logger.info(f"Waiting {retry_delay}s before retry for {self.ticker_model.symbol}")
                await asyncio.sleep(retry_delay)

            except ValueError as e:
                # Validation errors - don't retry, propagate up
                self.logger.error(f"Validation error for {self.ticker_model.symbol}: {e}")
                raise

            except Exception as e:
                self.consecutive_errors += 1
                self._update_error_statistics(e)
                self.logger.error(f"Download batch error for {self.ticker_model.symbol} "
                                  f"(attempt {self.consecutive_errors}): {e}")

                # Exponential backoff on errors
                await asyncio.sleep(min(2 ** self.consecutive_errors, 60))

        return last_trade_timestamp

    def _validate_batch(
            self,
            trades: List[AggTrade],
            expected_from_id: Optional[int],
            is_first_request: bool,
            strategy: str
    ) -> BatchValidationResult:
        """
        Validate downloaded batch for data integrity.
        """
        if not trades:
            return BatchValidationResult(is_valid=True)

        actual_first_id = trades[0].aggregate_id

        # Skip ID validation for the very first request when fromId=1 (new ticker)
        # Binance returns nearest available ID, not necessarily ID=1
        skip_id_validation = (is_first_request and expected_from_id == 1)

        if not skip_id_validation and expected_from_id is not None:
            if actual_first_id != expected_from_id:
                gap_size = actual_first_id - expected_from_id
                return BatchValidationResult(
                    is_valid=False,
                    error_message=f"ID mismatch: expected {expected_from_id}, got {actual_first_id} "
                                  f"(gap: {gap_size} trades)",
                    expected_first_id=expected_from_id,
                    actual_first_id=actual_first_id,
                    gap_size=gap_size
                )

        # Validate continuity within batch
        for i in range(1, len(trades)):
            expected_id = trades[i - 1].aggregate_id + 1
            actual_id = trades[i].aggregate_id

            if actual_id != expected_id:
                return BatchValidationResult(
                    is_valid=False,
                    error_message=f"Gap within batch: expected {expected_id}, got {actual_id}",
                    expected_first_id=expected_id,
                    actual_first_id=actual_id,
                    gap_size=actual_id - expected_id
                )

        return BatchValidationResult(is_valid=True)

    async def _send_gap_alert(self, validation: BatchValidationResult) -> None:
        """
        Send Telegram alert about detected gap.

        Args:
            validation: BatchValidationResult with gap details
        """
        notifier = get_notifier()
        if notifier is None:
            self.logger.warning("Telegram notifier not configured, skipping alert")
            return

        try:
            await notifier.send_gap_alert(
                symbol=self.ticker_model.symbol,
                expected_id=validation.expected_first_id or 0,
                received_id=validation.actual_first_id or 0,
                gap_size=validation.gap_size
            )
        except Exception as e:
            self.logger.error(f"Failed to send Telegram alert: {e}")

    # ===== API REQUEST HANDLING WITH ERROR MANAGEMENT =====

    async def _execute_api_request_with_retry(self, request_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Execute API request with comprehensive retry logic and error handling.

        Args:
            request_params: API request parameters

        Returns:
            List of trade dictionaries from Binance API

        Raises:
            BinanceClientError: For Binance-specific errors
            Exception: For other critical errors
        """
        max_retries = 3
        base_delay = 1.0

        for attempt in range(max_retries + 1):
            try:
                self.logger.debug(f"API request for {self.ticker_model.symbol} (attempt {attempt + 1}): {request_params}")

                # Use WeightCoordinator for rate limiting (20 weight per request)
                response = await self.weight_coordinator.request(
                    endpoint="aggTrades",
                    expected_weight=20,
                    **request_params
                )

                # Check for API error responses
                if isinstance(response, dict) and 'code' in response and 'msg' in response:
                    raise BinanceAPIError(response, endpoint="aggTrades")

                return response if response else []

            except BinanceClientError:
                # Re-raise Binance errors to be handled by caller
                raise

            except Exception as e:
                if attempt < max_retries:
                    # Check if this might be a recoverable error
                    if is_recoverable_error(e):
                        retry_delay = get_retry_delay(e) if hasattr(e, 'get_retry_delay') else base_delay * (2 ** attempt)
                        self.logger.warning(f"API request failed for {self.ticker_model.symbol}, "
                                            f"retrying in {retry_delay}s (attempt {attempt + 1}): {e}")
                        await asyncio.sleep(retry_delay)
                        continue

                # Non-recoverable error or max retries exceeded
                self.logger.error(f"API request failed for {self.ticker_model.symbol} after {attempt + 1} attempts: {e}")
                raise

    def _handle_binance_error(self, error: BinanceClientError) -> None:
        """
        Handle Binance-specific errors with appropriate logging and statistics.

        Args:
            error: BinanceClientError instance
        """
        self._update_error_statistics(error)

        # Log error with context
        error_context = error.get_error_context()
        self.logger.error(f"Binance API error for {self.ticker_model.symbol}: {error}")
        self.logger.debug(f"Error context: {error_context}")

        # Handle specific error types
        if error.is_rate_limit_error():
            self.logger.warning(f"Rate limit error for {self.ticker_model.symbol}, will retry after delay")

        elif error.is_authentication_error():
            self.logger.error(f"Authentication error for {self.ticker_model.symbol}: {error}")

        elif error.is_server_error():
            self.logger.warning(f"Server error for {self.ticker_model.symbol}, will retry: {error}")

        # Update last error time for monitoring
        self.last_error_time = time.time()

    def _update_error_statistics(self, error: Exception) -> None:
        """Update error statistics based on error type."""
        self.errors_count += 1

        if isinstance(error, BinanceClientError):
            self.binance_errors_count += 1
            if error.is_recoverable():
                self.recoverable_errors_count += 1

    # ===== STRATEGY AND REQUEST BUILDING =====

    async def _determine_request_strategy(self) -> Tuple[str, int, Optional[int]]:
        """Determine download strategy based on existing data."""
        try:
            # Попытаться получить последний aggregate_id
            last_aggregate_id = await self._get_last_aggregate_id()
            from_id = last_aggregate_id + 1

            self.logger.info(f"Continue load for {self.ticker_model.symbol}, "
                             f"last_aggregate_id: {last_aggregate_id}, from_id: {from_id}")
            return 'continue_load', from_id, from_id

        except (ValueError, Exception):
            # Нет данных - начинаем с fromId=1, Binance вернёт ближайший доступный
            self.logger.info(f"First load for {self.ticker_model.symbol}, starting from fromId=1")
            return 'continue_load', 1, 1

    def _build_request_params(
            self,
            from_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Build API request parameters.

        Args:
            from_id: Starting aggregate_id

        Returns:
            Dictionary with API request parameters
        """
        base_params = {
            'symbol': self.ticker_model.symbol,
            'limit': 1000,
            'fromId': from_id
        }

        return base_params

    # ===== DATA MANAGEMENT =====
    async def _get_last_aggregate_id(self) -> int:
        try:
            result = await self.trades_writer.get_last_aggregate_id(self.ticker_model.symbol)
            if result is not None and result > 0:
                self.logger.debug(f"Last aggregate_id for {self.ticker_model.symbol}: {result}")
                return result
            else:
                self.logger.info(f"No existing data found for {self.ticker_model.symbol}, will start fresh download")
                raise ValueError("No data found for symbol")
        except ValueError:
            raise
        except Exception as e:
            self.logger.error(f"Failed to get last aggregate_id for {self.ticker_model.symbol}: {e}")
            raise ValueError(f"Cannot determine last aggregate_id: {e}")

    async def _save_trades_batch(self, trades: List[AggTrade]) -> None:
        """
        Save batch of trades to storage with error handling.

        Args:
            trades: List of AggTrade instances to save
        """
        if not trades:
            return

        try:
            # Validate trades if enabled
            if self.config.validate_trade_data:
                trades = self._validate_trades(trades)

            # Save through TradesWriter interface
            trades_written = await self.trades_writer.write_trades_batch(self.ticker_model.symbol, trades)
            bytes_written = trades_written * 49  # AggTrade.BINARY_SIZE
            self.bytes_written += bytes_written

            self.logger.debug(f"Saved {len(trades)} trades for {self.ticker_model.symbol}, "
                              f"~{bytes_written} bytes written")

        except Exception as e:
            self.logger.error(f"Failed to save trades batch for {self.ticker_model.symbol}: {e}")
            raise

    def _validate_trades(self, trades: List[AggTrade]) -> List[AggTrade]:
        """
        Validate trade data and filter out invalid trades.

        Args:
            trades: List of trades to validate

        Returns:
            List of valid trades
        """
        valid_trades = []
        invalid_count = 0

        for trade in trades:
            if trade.validate():
                valid_trades.append(trade)
            else:
                invalid_count += 1
                if not self.config.skip_invalid_trades:
                    raise ValueError(f"Invalid trade data: {trade}")

        # Check invalid trades threshold
        if trades and invalid_count > 0:
            invalid_percent = (invalid_count / len(trades)) * 100
            if invalid_percent > self.config.max_invalid_trades_percent:
                raise ValueError(f"Too many invalid trades: {invalid_percent:.1f}% > {self.config.max_invalid_trades_percent}%")

            if invalid_count > 0:
                self.logger.warning(f"Filtered {invalid_count} invalid trades for {self.ticker_model.symbol} "
                                    f"({invalid_percent:.1f}%)")

        return valid_trades

    # ===== PROGRESS AND MONITORING =====

    def _calculate_progress(self) -> DownloadProgress:
        """
        Calculate current download progress.

        Returns:
            DownloadProgress with current statistics
        """
        current_time = time.time()
        elapsed_time = current_time - (self.download_start_time or current_time)

        # Calculate download rate
        download_rate = 0.0
        if elapsed_time > 0:
            download_rate = (self.total_trades_downloaded / elapsed_time) * 60  # per minute

        # Estimate completion (rough calculation)
        estimated_completion = None
        if download_rate > 0 and self.last_aggregate_id:
            # Грубая оценка: если загружаем меньше 1000 trades, скоро завершим
            if download_rate < 60000:  # Меньше 60k trades/minute = скоро конец
                estimated_completion = int(current_time + 300)  # +5 минут

        return DownloadProgress(
            total_trades_downloaded=self.total_trades_downloaded,
            api_requests_made=self.api_requests_made,
            bytes_written=self.bytes_written,
            current_timestamp=int(time.time() * 1000),
            start_timestamp=int((self.download_start_time or time.time()) * 1000),
            websocket_activated=False,
            download_rate_per_minute=download_rate,
            estimated_completion_time=estimated_completion,
            last_aggregate_id=self.last_aggregate_id
        )

    def get_progress(self) -> DownloadProgress:
        """Get current progress for external monitoring."""
        return self._calculate_progress()

    def is_download_running(self) -> bool:
        """Check if download is currently running."""
        return self.is_running

    def get_error_statistics(self) -> Dict[str, int]:
        """Get comprehensive error statistics."""
        return {
            'total_errors': self.errors_count,
            'binance_errors': self.binance_errors_count,
            'recoverable_errors': self.recoverable_errors_count,
            'consecutive_errors': self.consecutive_errors,
            'max_consecutive_errors': self.max_consecutive_errors,
            'last_error_time': int(self.last_error_time) if self.last_error_time else None
        }

    # ===== CONTROL METHODS =====

    async def pause_download(self) -> bool:
        """
        Pause the download process.

        Returns:
            True if paused successfully
        """
        if self.is_running:
            self.should_stop = True
            self.logger.info(f"Download pause requested for {self.ticker_model.symbol}")
            return True
        return False

    async def resume_download(self) -> bool:
        """
        Resume paused download (restarts from last position).

        Returns:
            True if resumed successfully
        """
        if not self.is_running and self.should_stop:
            self.should_stop = False
            self.logger.info(f"Download resume requested for {self.ticker_model.symbol}")
            return True
        return False

    async def recover_from_gap(self, gap_start_id: int) -> HistoricalDownloadResult:
        """
        Enhanced gap recovery with detailed logging and verification.

        Args:
            gap_start_id: Starting aggregate_id for gap recovery

        Returns:
            HistoricalDownloadResult with recovery statistics
        """
        self.logger.info(f"Starting enhanced gap recovery for {self.ticker_model.symbol} from ID {gap_start_id}")

        # Сохранить старые статистики
        old_stats = (self.total_trades_downloaded, self.api_requests_made, self.bytes_written)

        # Сбросить статистики для gap recovery
        self.total_trades_downloaded = 0
        self.api_requests_made = 0
        self.bytes_written = 0
        self.download_start_time = time.time()

        try:
            # Верифицировать gap_start_id с ClickHouse
            last_confirmed_id = await self.trades_writer.get_last_aggregate_id(self.ticker_model.symbol)
            if last_confirmed_id is not None and gap_start_id <= last_confirmed_id:
                self.logger.warning(f"Gap recovery start ID {gap_start_id} is <= last confirmed ID {last_confirmed_id}, "
                                    f"adjusting to {last_confirmed_id + 1}")
                gap_start_id = last_confirmed_id + 1

            self.logger.info(f"Verified gap recovery for {self.ticker_model.symbol}: "
                             f"last_confirmed_id={last_confirmed_id}, recovery_start_id={gap_start_id}")

            # Выполнить download от gap start
            end_timestamp = await self._execute_download_loop('continue_load', gap_start_id, gap_start_id)

            download_duration = time.time() - self.download_start_time

            result = HistoricalDownloadResult(
                success=True,
                trades_loaded=self.total_trades_downloaded,
                time_range_start=gap_start_id,
                time_range_end=end_timestamp,
                api_requests_made=self.api_requests_made,
                errors_count=self.errors_count,
                binance_errors_count=self.binance_errors_count,
                recoverable_errors_count=self.recoverable_errors_count,
                download_duration_seconds=download_duration,
                gaps_filled=1,
                bytes_written=self.bytes_written,
                websocket_handoff_completed=False,
                last_aggregate_id=self.last_aggregate_id
            )

            self.logger.info(f"Gap recovery completed for {self.ticker_model.symbol}: "
                             f"{result.trades_loaded} trades, {result.api_requests_made} requests, "
                             f"{result.download_duration_seconds:.1f}s")

            return result

        except Exception as e:
            self.logger.error(f"Gap recovery failed for {self.ticker_model.symbol}: {e}", exc_info=True)

            return HistoricalDownloadResult(
                success=False,
                trades_loaded=self.total_trades_downloaded,
                time_range_start=gap_start_id,
                time_range_end=0,
                api_requests_made=self.api_requests_made,
                errors_count=self.errors_count + 1,
                binance_errors_count=self.binance_errors_count,
                recoverable_errors_count=self.recoverable_errors_count,
                download_duration_seconds=time.time() - self.download_start_time,
                gaps_filled=0,
                bytes_written=self.bytes_written,
                websocket_handoff_completed=False,
                last_aggregate_id=self.last_aggregate_id
            )
        finally:
            # Восстановить старые статистики
            self.total_trades_downloaded += old_stats[0]
            self.api_requests_made += old_stats[1]
            self.bytes_written += old_stats[2]

    def __str__(self) -> str:
        """Human-readable string representation."""
        status = 'running' if self.is_running else 'idle'
        return f"HistoricalDownloader({self.ticker_model.symbol}, {status})"

    def __repr__(self) -> str:
        """Developer-friendly representation."""
        return (f"HistoricalDownloader(symbol='{self.ticker_model.symbol}', "
                f"running={self.is_running}, trades_downloaded={self.total_trades_downloaded}, "
                f"binance_errors={self.binance_errors_count})")
