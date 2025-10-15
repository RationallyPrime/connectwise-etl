"""ETL error handling system.

Adapted from found-family with ETL-specific error codes and details.
"""

from .base import (
    APIErrorDetails,
    ApplicationError,
    EntityErrorDetails,
    ErrorCode,
    ErrorDetails,
    ErrorLevel,
    SparkErrorDetails,
    ValidationErrorDetails,
)
from .types import (
    BronzeProcessingError,
    ETLConfigError,
    FetchError,
    GoldProcessingError,
    MergeError,
    SilverProcessingError,
    SparkError,
    ValidationError,
)

__all__ = [
    # Base classes
    "ApplicationError",
    "ErrorCode",
    "ErrorLevel",
    # Error details
    "ErrorDetails",
    "ValidationErrorDetails",
    "EntityErrorDetails",
    "APIErrorDetails",
    "SparkErrorDetails",
    # Specific errors
    "ETLConfigError",
    "FetchError",
    "ValidationError",
    "BronzeProcessingError",
    "SilverProcessingError",
    "MergeError",
    "GoldProcessingError",
    "SparkError",
]
