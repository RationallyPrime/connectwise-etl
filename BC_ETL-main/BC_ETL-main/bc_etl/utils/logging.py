# bc_etl/utils/logging.py
import functools
import logging
import sys
from collections.abc import Callable
from typing import Any, TypeVar

# Standard Python logger setup
_logger = logging.getLogger("bc_etl")
_handler = logging.StreamHandler(sys.stdout)
_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
_handler.setFormatter(_formatter)
_logger.addHandler(_handler)

# Try to import logfire for structured logging
_HAVE_LOGFIRE = False
_USE_LOGFIRE = False
try:
    import logfire

    _HAVE_LOGFIRE = True
except ImportError:
    pass

_INITIALIZED = False


def configure(level: str = "INFO", use_logfire: bool = True) -> None:
    """
    Configure logging for the bc_etl package.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        use_logfire: Whether to use logfire if available
    """
    global _INITIALIZED, _USE_LOGFIRE

    # Set log level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {level}")
    _logger.setLevel(numeric_level)

    # Configure logfire if available and requested
    _USE_LOGFIRE = use_logfire and _HAVE_LOGFIRE
    if _USE_LOGFIRE:
        try:
            logfire.configure()
        except Exception as e:
            _logger.warning(f"Failed to configure logfire: {e}. Falling back to standard logging.")
            _USE_LOGFIRE = False

    _INITIALIZED = True


def _ensure_initialized() -> None:
    """Ensure logging is initialized with defaults if not already done."""
    global _INITIALIZED
    if not _INITIALIZED:
        configure()


def _log(level: int, msg_template: str, **kwargs: Any) -> None:
    """
    Internal logging function that handles both standard logging and logfire.

    Args:
        level: Logging level
        msg_template: Log message template
        **kwargs: Additional context to include in the log
    """
    _ensure_initialized()

    if _USE_LOGFIRE:
        # Use logfire for structured logging
        log_func = getattr(logfire, logging.getLevelName(level).lower(), None)
        if log_func:
            log_func(msg_template, **kwargs)
        else:
            # Fallback if level doesn't have a direct logfire method
            logfire.log(level=level, msg_template=msg_template, **kwargs)
    else:
        # Use standard logging
        if kwargs:
            context_str = ", ".join(f"{k}={v!r}" for k, v in kwargs.items())
            _logger.log(level, f"{msg_template} [{context_str}]")
        else:
            _logger.log(level, msg_template)


def debug(msg: str, **kwargs: Any) -> None:
    """Log a debug message with optional structured context."""
    _log(logging.DEBUG, msg, **kwargs)


def info(msg: str, **kwargs: Any) -> None:
    """Log an info message with optional structured context."""
    _log(logging.INFO, msg, **kwargs)


def warning(msg: str, **kwargs: Any) -> None:
    """Log a warning message with optional structured context."""
    _log(logging.WARNING, msg, **kwargs)


def error(msg: str, **kwargs: Any) -> None:
    """Log an error message with optional structured context."""
    _log(logging.ERROR, msg, **kwargs)


def critical(msg: str, **kwargs: Any) -> None:
    """Log a critical message with optional structured context."""
    _log(logging.CRITICAL, msg, **kwargs)


# Type variable for the decorated function's return type
T = TypeVar("T")


def span(name: str, **kwargs: Any) -> Any:
    """
    Create a logging span for a code block or function.

    Can be used as a context manager:
        with logging.span("operation_name", param=value):
            # code here

    Or as a decorator:
        @logging.span("operation_name", param=value)
        def my_function():
            # function code here

    Args:
        name: Name of the span
        **kwargs: Additional context to include in span logs

    Returns:
        A context manager/decorator that logs entry and exit
    """
    _ensure_initialized()

    class SimpleSpan:
        def __init__(self, span_name: str, **span_kwargs: Any) -> None:
            self.span_name = span_name
            self.span_kwargs = span_kwargs

        def __enter__(self) -> "SimpleSpan":
            debug(f"Entering span {self.span_name}", **self.span_kwargs)
            return self

        def __exit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: Any,
        ) -> None:
            if exc_type:
                error(
                    f"Exiting span {self.span_name} with error: {exc_val}",
                    error_type=exc_type.__name__,
                    **self.span_kwargs,
                )
            else:
                debug(f"Exiting span {self.span_name}", **self.span_kwargs)

        def __call__(self, func: Callable[..., T]) -> Callable[..., T]:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                with self:
                    return func(*args, **kwargs)

            return wrapper

    return SimpleSpan(name, **kwargs)


def instrument_spark_job(spark: Any, app_name: str) -> None:
    """
    Set up instrumentation for a Spark job.

    Args:
        spark: SparkSession to instrument
        app_name: Name of the Spark application
    """
    _ensure_initialized()

    # Add basic context
    info(
        f"Instrumenting Spark job: {app_name}",
        spark_version=spark.version,
        spark_conf=dict(spark.conf.getAll()),
    )

    if _USE_LOGFIRE:
        try:
            # Attempt to register logfire's Spark listener if available
            if hasattr(logfire, "instrument_spark"):
                logfire.instrument_spark(spark)
                info("Logfire Spark instrumentation enabled")
            else:
                info("Logfire Spark instrumentation not available in this version")
        except Exception as e:
            warning("Failed to instrument Spark with Logfire", error=str(e))
