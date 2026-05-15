#!/usr/bin/env python3
"""
Shared logging configuration for tw-stock-hunter core modules.

Phase 21: Centralized logging setup so modules use logger.info/debug/warning/error
instead of bare print() statements. This enables:
- Structured log output with timestamps and module names
- Log level control via --verbose flag
- Log file capture for debugging production issues
- Quiet mode for automated/cron runs
"""

import logging
import sys
from pathlib import Path

# Default format: timestamp [module] level: message
DEFAULT_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"
SHORT_FORMAT = "%(levelname)s: %(message)s"

_initialized = False


def setup_logging(verbose=False, log_file=None, quiet=False):
    """Configure logging for the entire application.

    Args:
        verbose: If True, set root logger to DEBUG; otherwise INFO.
        log_file: Optional path to a log file for persistent logging.
        quiet: If True, suppress console output below WARNING level.

    Call this once at application startup (e.g., in run_pipeline.py
    or CLI entry points). Individual modules should use:

        import logging
        logger = logging.getLogger(__name__)

    and then logger.info(), logger.debug(), etc.
    """
    global _initialized
    if _initialized:
        return
    _initialized = True

    root = logging.getLogger()

    # Determine log level
    if verbose:
        root.setLevel(logging.DEBUG)
    else:
        root.setLevel(logging.INFO)

    # Console handler
    if not quiet:
        console = logging.StreamHandler(sys.stderr)
        console.setFormatter(logging.Formatter(DEFAULT_FORMAT))
        root.addHandler(console)
    else:
        # In quiet mode, only show warnings and above
        console = logging.StreamHandler(sys.stderr)
        console.setLevel(logging.WARNING)
        console.setFormatter(logging.Formatter(SHORT_FORMAT))
        root.addHandler(console)

    # File handler (optional)
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(str(log_path), encoding="utf-8")
        file_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))
        file_handler.setLevel(logging.DEBUG)  # Always log everything to file
        root.addHandler(file_handler)

    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("yfinance").setLevel(logging.WARNING)


def get_logger(name):
    """Get a logger with the given module name.

    Convenience function — equivalent to logging.getLogger(__name__).
    """
    return logging.getLogger(name)
