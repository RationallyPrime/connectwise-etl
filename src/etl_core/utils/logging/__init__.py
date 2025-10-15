"""ETL logging system with context awareness.

Adapted from found-family logging system.
"""

from .context import (
    clear_log_context,
    critical,
    debug,
    error,
    get_log_context,
    info,
    set_log_context,
    update_log_context,
    warning,
)
from .setup import get_logger, setup_logging

__all__ = [
    # Setup
    "setup_logging",
    "get_logger",
    # Context management
    "get_log_context",
    "set_log_context",
    "update_log_context",
    "clear_log_context",
    # Logging functions
    "debug",
    "info",
    "warning",
    "error",
    "critical",
]
