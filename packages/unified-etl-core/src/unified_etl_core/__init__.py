"""Unified ETL Core - The foundation for all ETL operations."""

from .extract.base import BaseExtractor
from .pipeline import Pipeline

__version__ = "1.0.0"

__all__ = [
    "BaseExtractor",
    "Pipeline",
]
