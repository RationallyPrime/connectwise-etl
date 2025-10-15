"""Domain-specific error types for ETL operations."""

from typing import Any

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


class ETLConfigError(ApplicationError):
    """Configuration-related error."""

    def __init__(self, message: str, details: ErrorDetails | dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.CONFIG_INVALID,
            level=ErrorLevel.ERROR,
            details=details,
        )


class FetchError(ApplicationError):
    """Data fetching error."""

    def __init__(self, message: str, details: APIErrorDetails | dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.FETCH_ERROR,
            level=ErrorLevel.ERROR,
            details=details,
        )


class ValidationError(ApplicationError):
    """Data validation error."""

    def __init__(
        self, message: str, details: ValidationErrorDetails | dict[str, Any] | None = None
    ):
        super().__init__(
            message=message,
            code=ErrorCode.VALIDATION_FAILED,
            level=ErrorLevel.WARNING,
            details=details,
        )


class BronzeProcessingError(ApplicationError):
    """Bronze layer processing error."""

    def __init__(self, message: str, details: EntityErrorDetails | dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.BRONZE_EXTRACT_FAILED,
            level=ErrorLevel.ERROR,
            details=details,
        )


class SilverProcessingError(ApplicationError):
    """Silver layer processing error."""

    def __init__(self, message: str, details: EntityErrorDetails | dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.SILVER_TRANSFORM_FAILED,
            level=ErrorLevel.ERROR,
            details=details,
        )


class MergeError(ApplicationError):
    """Merge operation error."""

    def __init__(self, message: str, details: SparkErrorDetails | dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.MERGE_ERROR,
            level=ErrorLevel.ERROR,
            details=details,
        )


class GoldProcessingError(ApplicationError):
    """Gold layer processing error."""

    def __init__(self, message: str, details: EntityErrorDetails | dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.GOLD_DIMENSION_FAILED,
            level=ErrorLevel.ERROR,
            details=details,
        )


class SparkError(ApplicationError):
    """Spark infrastructure error."""

    def __init__(self, message: str, details: SparkErrorDetails | dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.SPARK_SESSION_FAILED,
            level=ErrorLevel.CRITICAL,
            details=details,
        )
