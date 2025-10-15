"""Protocols for medallion layer processors."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .types import BronzeResult, GoldResult, SilverResult

if TYPE_CHECKING:
    from etl_core.config import RuntimeContext


@runtime_checkable
class BronzeProcessorProtocol(Protocol):
    """
    Protocol for bronze layer processing (extraction and validation).

    Responsibilities:
    - Fetch raw data from source using DataFetcherProtocol
    - Validate records using DataValidatorProtocol
    - Add ETL metadata (batch_id, etl_timestamp)
    - Write to bronze tables
    - Handle incremental vs full load logic
    """

    def process(self, context: "RuntimeContext") -> BronzeResult:
        """
        Execute bronze layer extraction and validation.

        Args:
            context: Runtime context including Spark session, mode, and configuration.

        Returns:
            Structured result with metrics and table information.

        Raises:
            FetchError: If data extraction fails.
            ValidationError: If critical validation failure occurs.
        """
        ...


@runtime_checkable
class SilverProcessorProtocol(Protocol):
    """
    Protocol for silver layer processing (transformation and cleansing).

    Responsibilities:
    - Read data from bronze tables
    - Apply business transformations (flatten structs, type conversion)
    - Add processing metadata (_etl_processed_at, _etl_batch_id)
    - Merge with existing silver tables (SCD1/SCD2)
    - Handle schema evolution
    """

    def process(self, context: "RuntimeContext") -> SilverResult:
        """
        Execute silver layer transformations.

        Args:
            context: Runtime context including Spark session, mode, and configuration.

        Returns:
            Structured result with merge statistics and table information.

        Raises:
            MergeError: If merge operation fails.
        """
        ...


@runtime_checkable
class GoldProcessorProtocol(Protocol):
    """
    Protocol for gold layer processing (dimensional modeling).

    Responsibilities:
    - Create dimension tables from YAML schemas
    - Create fact tables with business logic
    - Add surrogate keys via window functions
    - Apply integration-specific transformations (e.g., Icelandic agreement types)
    - Generate star schema for BI consumption
    """

    def process(
        self,
        context: "RuntimeContext",
        dimension_schema_path: Path | None = None,
        fact_schema_path: Path | None = None,
    ) -> GoldResult:
        """
        Execute gold layer dimensional modeling.

        Args:
            context: Runtime context including Spark session and configuration.
            dimension_schema_path: Path to YAML dimension definitions.
                If None, uses convention: {lakehouse}/Files/schemas/dimensions.yaml
            fact_schema_path: Path to YAML fact definitions (optional).
                If None, uses convention: {lakehouse}/Files/schemas/facts.yaml

        Returns:
            Structured result with dimension and fact table information.

        Raises:
            ProcessingError: If gold layer creation fails.
        """
        ...
