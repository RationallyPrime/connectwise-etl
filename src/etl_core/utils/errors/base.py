"""Base error classes and enums for ETL framework.

Adapted from found-family error handling system with ETL-specific error codes.
"""

import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Self

from pydantic import BaseModel, Field, field_serializer


class ErrorLevel(str, Enum):
    """Error severity levels."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    def to_logging_level(self) -> int:
        """Convert ErrorLevel to logging level."""
        return {
            ErrorLevel.DEBUG: logging.DEBUG,
            ErrorLevel.INFO: logging.INFO,
            ErrorLevel.WARNING: logging.WARNING,
            ErrorLevel.ERROR: logging.ERROR,
            ErrorLevel.CRITICAL: logging.CRITICAL,
        }[self]


class ErrorCode(str, Enum):
    """ETL-specific error codes with domain-based ranges.

    Ranges:
    - 1xxx: General/Configuration errors
    - 2xxx: API/Authentication errors
    - 3xxx: Bronze layer errors
    - 4xxx: Silver layer errors
    - 5xxx: Gold layer errors
    - 6xxx: Infrastructure errors
    """

    # Configuration & Validation (1xxx)
    CONFIG_MISSING = "1001"
    CONFIG_INVALID = "1002"
    VALIDATION_FAILED = "1003"
    SCHEMA_MISMATCH = "1004"

    # API & Source System Errors (2xxx)
    API_AUTH_FAILED = "2001"
    API_RATE_LIMITED = "2002"
    API_RESPONSE_INVALID = "2003"
    API_FIELD_MISSING = "2004"
    FETCH_ERROR = "2005"

    # Bronze Layer (3xxx)
    BRONZE_EXTRACT_FAILED = "3001"
    BRONZE_VALIDATION_FAILED = "3002"
    BRONZE_WRITE_FAILED = "3003"

    # Silver Layer (4xxx)
    SILVER_TRANSFORM_FAILED = "4001"
    SILVER_TYPE_CONVERSION = "4002"
    SILVER_FLATTEN_FAILED = "4003"
    SILVER_SCD_FAILED = "4004"
    MERGE_ERROR = "4005"

    # Gold Layer (5xxx)
    GOLD_DIMENSION_FAILED = "5001"
    GOLD_FACT_FAILED = "5002"
    GOLD_SURROGATE_KEY = "5003"
    GOLD_AGGREGATION = "5004"
    PROCESSING_ERROR = "5005"

    # Infrastructure (6xxx)
    SPARK_SESSION_FAILED = "6001"
    STORAGE_ACCESS_FAILED = "6002"
    MEMORY_EXCEEDED = "6003"


class ErrorDetails(BaseModel):
    """Base model for structured error details."""

    source: str = Field(description="Component or module where the error occurred")
    operation: str = Field(description="Operation being performed when the error occurred")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="When the error occurred"
    )

    @field_serializer("timestamp")
    def serialize_timestamp(self, timestamp: datetime) -> str:
        """Ensure timestamp is serialized consistently."""
        return timestamp.isoformat()


class ValidationErrorDetails(ErrorDetails):
    """Details for validation-related errors."""

    field: str | None = Field(None, description="Field that failed validation")
    actual_value: Any = Field(None, description="Value that failed validation")
    expected_type: str | None = Field(None, description="Expected type or format")
    constraint: str | None = Field(None, description="Constraint that was violated")


class EntityErrorDetails(ErrorDetails):
    """Details for entity-specific errors."""

    entity_name: str = Field(description="Entity being processed")
    layer: str | None = Field(None, description="Medallion layer (bronze/silver/gold)")
    table_name: str | None = Field(None, description="Target table name")
    batch_id: str | None = Field(None, description="ETL batch ID")


class APIErrorDetails(ErrorDetails):
    """Details for API-related errors."""

    endpoint: str | None = Field(None, description="API endpoint that was called")
    status_code: int | None = Field(None, description="HTTP status code")
    request_id: str | None = Field(None, description="Request ID for tracing")
    latency_ms: float | None = Field(None, description="Response time in milliseconds")


class SparkErrorDetails(ErrorDetails):
    """Details for Spark-related errors."""

    query_type: str | None = Field(None, description="Type of operation (read/write/transform)")
    table: str | None = Field(None, description="Table being accessed")
    partition: str | None = Field(None, description="Partition specification")
    executor_info: str | None = Field(None, description="Executor/task information")


class ApplicationError(Exception):
    """Base class for all application errors."""

    def __init__(
        self,
        message: str,
        code: ErrorCode,
        level: ErrorLevel = ErrorLevel.ERROR,
        details: ErrorDetails | dict[str, Any] | None = None,
    ):
        self.message = message
        self.code = code
        self.level = level

        # Convert dict to ErrorDetails if needed
        if details is None:
            self.details = ErrorDetails(source="unknown", operation="unknown")
        elif isinstance(details, dict):
            # Extract source and operation from dict if available
            source = details.pop("source", "unknown")
            operation = details.pop("operation", "unknown")
            self.details = ErrorDetails(source=source, operation=operation, **details)
        else:
            self.details = details

        super().__init__(message)

    def __str__(self) -> str:
        return f"Error {self.code}: {self.message}"

    @classmethod
    def with_details(cls, message: str, details: ErrorDetails, **kwargs: Any) -> Self:
        """Create an error with specific details model."""
        return cls(message=message, details=details, **kwargs)
