"""Logging setup with Logfire and Structlog.

Adapted from found-family logging system for ETL use cases.
"""

import logging

import logfire
import structlog
from structlog.typing import FilteringBoundLogger


def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with Logfire and structlog.

    Args:
        log_level: Minimum log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    """
    # Configure logfire
    logfire.configure()

    # Map string level to logging constant
    level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }
    log_level_int = level_map.get(log_level.upper(), logging.INFO)

    # Configure structlog with logfire integration
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,  # Merge context variables
            structlog.processors.add_log_level,  # Add log level
            structlog.processors.TimeStamper(fmt="iso"),  # ISO timestamps
            structlog.processors.CallsiteParameterAdder(  # Add caller info
                [
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            ),
            structlog.processors.StackInfoRenderer(),  # Stack traces for exceptions
            logfire.LogfireProcessor(),  # Logfire integration
            structlog.dev.ConsoleRenderer(colors=True),  # Pretty console output
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level_int),
        context_class=dict,
        logger_factory=logfire.LogfireLoggingFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str | None = None) -> FilteringBoundLogger:
    """Get a configured logger instance.

    Args:
        name: Optional logger name (typically __name__).

    Returns:
        Configured structlog logger.
    """
    return structlog.get_logger(name)
