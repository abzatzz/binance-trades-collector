"""
TelegramNotifier - Critical Alerts via Telegram
================================================

Sends critical system alerts to Telegram group/channel.
Used for data integrity issues, gap detection failures, and system errors.

File: src/core/telegram_notifier.py
"""

import asyncio
from typing import Optional
from enum import Enum

import aiohttp
from loggerino import loggerino

logger = loggerino.get('telegram_notifier')


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "ℹ️"
    WARNING = "⚠️"
    ERROR = "🔴"
    CRITICAL = "🚨"


class TelegramNotifier:
    """
    Async Telegram notification sender for critical alerts.

    Usage:
        notifier = TelegramNotifier(bot_token, chat_id)
        await notifier.send_alert("Gap detected!", AlertLevel.CRITICAL)
    """

    def __init__(self, bot_token: str, chat_id: str, enabled: bool = True):
        """
        Initialize TelegramNotifier.

        Args:
            bot_token: Telegram bot token from @BotFather
            chat_id: Target chat/group/channel ID
            enabled: Whether notifications are enabled
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = enabled
        self.api_url = f"https://api.telegram.org/bot{bot_token}"

        if enabled and bot_token and chat_id:
            logger.info("TelegramNotifier initialized")
        else:
            logger.warning("TelegramNotifier disabled or not configured")

    async def send_message(self, message: str, parse_mode: str = 'HTML') -> bool:
        """
        Send message to Telegram.

        Args:
            message: Message text (supports HTML formatting)
            parse_mode: 'HTML' or 'Markdown'

        Returns:
            True if sent successfully
        """
        if not self.enabled or not self.bot_token or not self.chat_id:
            logger.debug("Telegram notification skipped (disabled or not configured)")
            return False

        try:
            url = f"{self.api_url}/sendMessage"
            payload = {
                'chat_id': self.chat_id,
                'text': message,
                'parse_mode': parse_mode,
                'disable_web_page_preview': True
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    if response.status == 200:
                        logger.info("Telegram notification sent successfully")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Telegram API error: {response.status} - {error_text}")
                        return False

        except asyncio.TimeoutError:
            logger.error("Telegram notification timeout")
            return False
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False

    async def send_alert(
            self,
            message: str,
            level: AlertLevel = AlertLevel.ERROR,
            symbol: Optional[str] = None,
            title: Optional[str] = None
    ) -> bool:
        """
        Send formatted alert message.

        Args:
            message: Alert message
            level: Alert severity level
            symbol: Optional ticker symbol for context
            title: Optional custom title (overrides default)

        Returns:
            True if sent successfully
        """
        # Format message with level icon and optional symbol
        if title:
            header = f"{level.value} <b>{title}</b>"
        else:
            header = f"{level.value} <b>DataProvider Alert</b>"
        if symbol:
            header += f" [{symbol}]"

        formatted_message = f"{header}\n\n{message}"

        return await self.send_message(formatted_message)

    async def send_gap_alert(
            self,
            symbol: str,
            expected_id: int,
            received_id: int,
            gap_size: int
    ) -> bool:
        """
        Send gap detection alert.

        Args:
            symbol: Ticker symbol
            expected_id: Expected aggregate_id
            received_id: Actually received aggregate_id
            gap_size: Number of missing trades
        """
        message = (
            f"<b>Data Gap Detected!</b>\n\n"
            f"Symbol: <code>{symbol}</code>\n"
            f"Expected ID: <code>{expected_id}</code>\n"
            f"Received ID: <code>{received_id}</code>\n"
            f"Gap size: <code>{gap_size:,}</code> trades\n\n"
            f"⚠️ Historical download paused. Manual intervention may be required."
        )

        return await self.send_alert(message, AlertLevel.CRITICAL, symbol)

    async def send_recovery_failed_alert(
            self,
            symbol: str,
            error: str,
            retry_count: int
    ) -> bool:
        """
        Send recovery failure alert.

        Args:
            symbol: Ticker symbol
            error: Error description
            retry_count: Number of retries attempted
        """
        message = (
            f"<b>Recovery Failed!</b>\n\n"
            f"Symbol: <code>{symbol}</code>\n"
            f"Error: {error}\n"
            f"Retries: {retry_count}\n\n"
            f"🛑 Ticker stopped to prevent data corruption."
        )

        return await self.send_alert(message, AlertLevel.CRITICAL, symbol)

    async def send_ticker_stopped_alert(
            self,
            symbol: str,
            reason: str
    ) -> bool:
        """
        Send ticker stopped alert.

        Args:
            symbol: Ticker symbol
            reason: Reason for stopping
        """
        message = (
            f"<b>Ticker Stopped</b>\n\n"
            f"Symbol: <code>{symbol}</code>\n"
            f"Reason: {reason}"
        )

        return await self.send_alert(message, AlertLevel.ERROR, symbol)

    async def send_system_alert(self, message: str, level: AlertLevel = AlertLevel.ERROR) -> bool:
        """
        Send system-wide alert (not ticker-specific).

        Args:
            message: Alert message
            level: Alert severity
        """
        return await self.send_alert(message, level)


# Singleton instance (initialized from config)
_notifier: Optional[TelegramNotifier] = None


def init_notifier(bot_token: str, chat_id: str, enabled: bool = True) -> TelegramNotifier:
    """
    Initialize global notifier instance.

    Args:
        bot_token: Telegram bot token
        chat_id: Target chat ID
        enabled: Whether notifications are enabled

    Returns:
        TelegramNotifier instance
    """
    global _notifier
    _notifier = TelegramNotifier(bot_token, chat_id, enabled)
    return _notifier


def get_notifier() -> Optional[TelegramNotifier]:
    """
    Get global notifier instance.

    Returns:
        TelegramNotifier instance or None if not initialized
    """
    return _notifier


async def send_alert(message: str, level: AlertLevel = AlertLevel.ERROR, symbol: Optional[str] = None, title: Optional[str] = None) -> bool:
    """
    Convenience function to send alert via global notifier.

    Args:
        message: Alert message
        level: Alert severity
        symbol: Optional ticker symbol
        title: Optional custom title

    Returns:
        True if sent successfully, False if notifier not initialized or send failed
    """
    if _notifier is None:
        logger.warning("TelegramNotifier not initialized, alert not sent")
        return False
    return await _notifier.send_alert(message, level, symbol, title)