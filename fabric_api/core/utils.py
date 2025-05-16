#!/usr/bin/env python
"""
General utility functions for ConnectWise data processing.
Pure helper functions that are reused across client, extract and transform layers.
All helpers are deliberately side-effect-free so they can be unit-tested in isolation.
"""

from datetime import date, datetime
from typing import Any


def get_nested_value(
    data: dict[str, Any] | list[Any] | None, dotted_path: str, default: Any = None
) -> Any:
    """
    Safely retrieve nested value using a dotted path.

    Args:
        data: Dictionary or list to traverse
        dotted_path: Path to value (e.g., "member.identifier")
        default: Value to return if path doesn't exist

    Returns:
        Value at path or default if not found
    """
    if data is None:
        return default

    current_value: Any = data
    for segment in dotted_path.split(sep="."):
        if isinstance(current_value, dict):
            current_value = current_value.get(segment, default)
        elif isinstance(current_value, list):
            try:
                index: int = int(segment)
                current_value = current_value[index]
            except (ValueError, IndexError):
                return default
        else:
            return default
    return current_value


def create_batch_identifier(timestamp: datetime | None = None) -> str:
    """
    Return UTC timestamp formatted as YYYYMMDD-HHMMSS.

    Args:
        timestamp: Optional timestamp (default is current time)

    Returns:
        Formatted timestamp string
    """
    ts: datetime = timestamp or datetime.utcnow()
    return ts.strftime("%Y%m%d-%H%M%S")


def get_first_day_next_month(reference: date | None = None) -> str:
    """
    Return ISO string for the first day of the month following reference.

    Args:
        reference: Optional reference date (default is today)

    Returns:
        ISO formatted date string
    """
    ref: date = reference or date.today()
    year: int = ref.year + (1 if ref.month == 12 else 0)
    month: int = 1 if ref.month == 12 else ref.month + 1
    return date(year, month, day=1).isoformat()
