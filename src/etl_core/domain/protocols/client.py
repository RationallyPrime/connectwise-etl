"""Protocols for data extraction and validation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Iterator, Protocol, runtime_checkable

from .types import TConfigDict, TLoadMode, TSparkModelClass

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class ValidationResult:
    """Result of data validation and DataFrame creation."""

    dataframe: "DataFrame"
    valid_count: int
    invalid_count: int
    invalid_sample: "DataFrame | None" = None  # Optional sample of rejected records


@runtime_checkable
class DataFetcherProtocol(Protocol):
    """
    Protocol for fetching raw data from a source system (API, database, files).

    This interface handles ONLY raw data extraction. Validation and DataFrame
    creation are handled separately by DataValidatorProtocol.

    Responsibilities:
    - Authenticate with data source
    - Determine source location from config
    - Handle incremental logic (lookback windows, watermarks)
    - Fetch raw records (pagination, API calls, file reading)
    """

    def fetch_raw(
        self,
        entity_name: str,
        mode: TLoadMode,
        config: TConfigDict,
        **kwargs: Any,
    ) -> Iterator[dict[str, Any]]:
        """
        Fetch raw records from the data source.

        Args:
            entity_name: Name of the entity to fetch.
            mode: Load mode (full, incremental, append).
            config: Entity-specific configuration (e.g., endpoint, keys).
            **kwargs: Additional runtime parameters (e.g., lookback_days).

        Returns:
            Iterator of raw dictionaries (unvalidated).

        Raises:
            FetchError: If data extraction fails.
        """
        ...

    def test_connection(self) -> bool:
        """
        Verify connectivity and authentication with the data source.

        Returns:
            True if connection successful, False otherwise.
        """
        ...

    def close(self) -> None:
        """
        Clean up resources (connections, file handles, etc.).

        Called after extraction completes. Default no-op if not needed.
        """
        ...


@runtime_checkable
class DataValidatorProtocol(Protocol):
    """
    Protocol for validating raw data and creating Spark DataFrames.

    This interface handles Pydantic validation and SparkDantic DataFrame creation.
    Typically implemented once in core and reused across all integrations.

    Responsibilities:
    - Validate each record using Pydantic model
    - Log validation errors with context
    - Optionally collect invalid records for review
    - Create Spark DataFrame using SparkDantic schema
    """

    def validate_and_create_dataframe(
        self,
        raw_data: Iterator[dict[str, Any]],
        model_class: TSparkModelClass,
        spark: "SparkSession",
        entity_name: str,
    ) -> ValidationResult:
        """
        Validate raw data using Pydantic and create Spark DataFrame.

        Args:
            raw_data: Iterator of raw dictionaries from the data source.
            model_class: SparkDantic model class for validation and schema.
            spark: Active Spark session.
            entity_name: Entity name for logging context.

        Returns:
            ValidationResult with DataFrame, counts, and optional invalid sample.

        Raises:
            ValidationError: If critical validation failure occurs.
        """
        ...
