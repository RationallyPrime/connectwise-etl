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
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.dev.ConsoleRenderer()
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
from .decorators import with_etl_error_handling
from .exceptions import ETLConfigError, ETLProcessingError, ETLInfrastructureError
from .base import ErrorCode, ErrorLevel

__all__ = [
    "get_logger",
    "with_etl_error_handling",
    "ETLConfigError",
    "ETLProcessingError", 
    "ETLInfrastructureError",
    "ErrorCode",
    "ErrorLevel",
]
