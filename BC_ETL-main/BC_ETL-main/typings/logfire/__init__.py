"""Type stubs for logfire package."""

from typing import Any, TypeVar

T = TypeVar("T")


def setup(
    app_name: str | None = None,
    api_key: str | None = None,
    endpoint: str | None = None,
    environment: str | None = None,
    service_name: str | None = None,
    service_version: str | None = None,
    **kwargs: Any,
) -> None:
    """Set up logfire with the given configuration."""
    ...


def info(message: str, **kwargs: Any) -> None:
    """Log an info message."""
    ...


def debug(message: str, **kwargs: Any) -> None:
    """Log a debug message."""
    ...


def warning(message: str, **kwargs: Any) -> None:
    """Log a warning message."""
    ...


def error(message: str, **kwargs: Any) -> None:
    """Log an error message."""
    ...


def critical(message: str, **kwargs: Any) -> None:
    """Log a critical message."""
    ...


def span(name: str, **kwargs: Any) -> "Span":
    """Create a new span."""
    ...


class Span:
    """Type stub for Span class."""

    def __enter__(self) -> "Span":
        """Enter the span context."""
        ...

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the span context."""
        ...

    def add_event(self, name: str, **kwargs: Any) -> None:
        """Add an event to the span."""
        ...

    def set_attribute(self, key: str, value: Any) -> None:
        """Set an attribute on the span."""
        ...


def instrument_spark(spark: Any) -> None:
    """Instrument a Spark session with logfire."""
    ...
