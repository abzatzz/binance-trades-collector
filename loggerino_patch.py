"""
Patch for loggerino to fix 'Too many open files' issue.

This patch modifies the _build_logger method to reuse shared handlers
instead of creating new file handlers for each logger.

Problem: When creating 700+ loggers, each one opens a separate file descriptor
for problem.log, exceeding the system limit (typically 1024).

Solution: Create handlers once and reuse them across all loggers.

Usage:
    Import this module BEFORE importing loggerino:

    import loggerino_patch  # Apply patch first
    from loggerino import loggerino
"""
import logging
import os
from typing import Optional

# Store shared handlers globally
_shared_problem_handler = None
_shared_console_handler = None
_file_log_level = logging.DEBUG


def patched_build_logger(
        self,
        name: str,
        log_file: Optional[str],
        level: int,
        buffer_size: Optional[int] = None,
        propagate: bool = True,
        console_log: bool = True
):
    """
    Patched version of _build_logger that reuses shared handlers.

    This prevents opening hundreds of file descriptors for the same files.
    """
    global _shared_problem_handler, _shared_console_handler

    # Import here to avoid circular dependencies
    from loggerino.core import EnhancedLogger
    from loggerino.formatters import get_default_formatter, get_color_formatter
    from loggerino.buffered import BufferedFileHandler

    # Get or create logger
    logger = logging.getLogger(name)

    # Ensure it's an EnhancedLogger
    logger = logger if isinstance(logger, EnhancedLogger) else EnhancedLogger(name)

    # Set the global minimum level to allow all custom debug levels
    logger.setLevel(1)

    # Track existing handler types
    existing_handlers = {type(h): h for h in logger.handlers}

    # === CONSOLE HANDLER (SHARED) ===
    if logging.StreamHandler not in existing_handlers and console_log:
        # Create shared console handler once
        if _shared_console_handler is None:
            _shared_console_handler = logging.StreamHandler()
            _shared_console_handler.setFormatter(get_color_formatter(name))
            _shared_console_handler.setLevel(1 if self._debug_in_console else logging.INFO)

        # Reuse the shared handler
        logger.addHandler(_shared_console_handler)

    # === FILE HANDLER (PER-FILE, NOT SHARED) ===
    if log_file:
        # Ensure directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Create file if it doesn't exist
        if not os.path.exists(log_file):
            with open(log_file, 'w'):
                pass

        # Check if we already have a handler for this specific file
        file_handler = None
        for h in logger.handlers:
            if isinstance(h, BufferedFileHandler) and getattr(h, 'baseFilename', None) == log_file:
                file_handler = h
                break

        # Create new handler only if needed
        if not file_handler:
            file_handler = BufferedFileHandler(
                log_file,
                buffering=buffer_size or self._buffer_size
            )
            file_handler.setFormatter(get_default_formatter())
            file_handler.setLevel(_file_log_level)
            self._buffered_handlers.append(file_handler)
            logger.addHandler(file_handler)

    # === PROBLEM LOG HANDLER (SHARED) ===
    if self._problem_log:
        # Create shared problem handler once
        if _shared_problem_handler is None:
            # Ensure directory exists
            problem_dir = os.path.dirname(self._problem_log)
            if problem_dir:
                os.makedirs(problem_dir, exist_ok=True)

            _shared_problem_handler = logging.FileHandler(
                self._problem_log,
                encoding='utf-8'
            )
            _shared_problem_handler.setFormatter(get_default_formatter())
            _shared_problem_handler.setLevel(logging.WARNING)

        # Add shared handler only if not already present
        if _shared_problem_handler not in logger.handlers:
            logger.addHandler(_shared_problem_handler)

    # Add notifiers to this logger
    for notifier in self._notifiers.values():
        self._add_notifier_to_logger(logger, notifier)

    # Ensure logger propagates correctly
    logger.propagate = propagate

    return logger


def set_file_log_level(level_name: str):
    """
    Set log level for file handlers.

    Args:
        level_name: 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'

    Note: Call this BEFORE creating loggers with loggerino.create()
    """
    global _file_log_level
    _file_log_level = getattr(logging, level_name.upper(), logging.DEBUG)


def apply_patch():
    """Apply the patch to loggerino."""
    try:
        from loggerino.core import Loggerino

        # Replace the _build_logger method
        Loggerino._build_logger = patched_build_logger

        print("✅ Loggerino patch applied successfully")
        print("   - Shared console handler enabled")
        print("   - Shared problem.log handler enabled")
        print("   - File descriptor leak fixed")

        return True

    except ImportError as e:
        print(f"❌ Failed to apply loggerino patch: {e}")
        print("   Make sure loggerino is installed: pip install loggerino")
        return False
    except Exception as e:
        print(f"❌ Unexpected error applying patch: {e}")
        return False


# Auto-apply patch on import
if apply_patch():
    # Verify patch was applied
    from loggerino.core import Loggerino

    if Loggerino._build_logger != patched_build_logger:
        print("⚠️ WARNING: Patch verification failed!")
