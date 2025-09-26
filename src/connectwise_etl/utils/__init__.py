"""Utilities for unified ETL framework."""

import structlog

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.ExceptionRenderer(),
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)


# Export commonly used functions
def get_logger(name: str):
    """Get a structured logger for the given module name."""
    return structlog.get_logger(name)


# Re-export commonly used items from submodules
from .base import ErrorCode, ErrorLevel
from .decorators import with_etl_error_handling
from .exceptions import ETLConfigError, ETLInfrastructureError, ETLProcessingError

__all__ = [
    "ETLConfigError",
    "ETLInfrastructureError",
    "ETLProcessingError",
    "ErrorCode",
    "ErrorLevel",
    "get_logger",
    "with_etl_error_handling",
]
