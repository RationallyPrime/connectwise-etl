"""Context-aware logging using ContextVar for request/batch scoping.

Adapted from found-family logging system.
"""

from contextvars import ContextVar
from typing import Any

_log_context: ContextVar[dict[str, Any]] = ContextVar("log_context", default={})


def get_log_context() -> dict[str, Any]:
    """Retrieve current logging context."""
    return _log_context.get().copy()


def set_log_context(context: dict[str, Any]) -> None:
    """Set entire logging context."""
    _log_context.set(context.copy())


def update_log_context(key: str, value: Any) -> None:
    """Update a single key in context."""
    context = _log_context.get().copy()
    context[key] = value
    _log_context.set(context)


def clear_log_context() -> None:
    """Reset context to empty."""
    _log_context.set({})


def log_with_context(logger: Any, level: str, message: str, **extra: Any) -> None:
    """Log message with current context merged in."""
    context = get_log_context()
    log_method = getattr(logger, level)
    log_method(message, **context, **extra)


# Convenience functions for common log levels
def get_logger() -> Any:
    """Get structured logger (imported here to avoid circular deps)."""
    import structlog

    return structlog.get_logger()


def debug(message: str, **extra: Any) -> None:
    """Log debug message with context."""
    log_with_context(get_logger(), "debug", message, **extra)


def info(message: str, **extra: Any) -> None:
    """Log info message with context."""
    log_with_context(get_logger(), "info", message, **extra)


def warning(message: str, **extra: Any) -> None:
    """Log warning message with context."""
    log_with_context(get_logger(), "warning", message, **extra)


def error(message: str, **extra: Any) -> None:
    """Log error message with context."""
    log_with_context(get_logger(), "error", message, **extra)


def critical(message: str, **extra: Any) -> None:
    """Log critical message with context."""
    log_with_context(get_logger(), "critical", message, **extra)
