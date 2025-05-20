"""Utility functions for the BC pipeline."""

from datetime import datetime

from pyspark.sql import DataFrame


def log_info(message: str) -> None:
    """Log an info message with timestamp."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: {message}")


def log_error(message: str) -> None:
    """Log an error message with timestamp."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: {message}")


def log_warning(message: str) -> None:
    """Log a warning message with timestamp."""
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: {message}")


def log_dataframe_info(df: DataFrame, description: str) -> None:
    """Log dataframe information."""
    count = df.count()
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] DataFrame {description}: {count} rows, {len(df.columns)} columns")