"""Common type aliases and result types for the ETL framework."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Literal, TypeAlias

# Use TYPE_CHECKING to keep Spark types for static analysis without runtime dependency
if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    try:
        from sparkdantic import SparkModel
    except ImportError:
        # Fallback for type checking when sparkdantic not installed
        class SparkModel:  # type: ignore
            ...

else:
    # At runtime, use Any when Spark isn't available (e.g., during type checking in CI)
    DataFrame = Any
    SparkSession = Any

    class SparkModel:  # type: ignore
        ...


# ============ Type Aliases ============

TSparkModelClass: TypeAlias = type[SparkModel]
TModelRegistryDict: TypeAlias = dict[str, TSparkModelClass]
TDataFrameRegistryDict: TypeAlias = dict[str, "DataFrame"]
TConfigDict: TypeAlias = dict[str, Any]

# Literal types for pipeline control flow
TLayer: TypeAlias = Literal["bronze", "silver", "gold"]
TLoadMode: TypeAlias = Literal["full", "incremental", "append"]
"""
Load modes:
- full: Complete reload, truncate and rebuild
- incremental: Delta load using watermarks/timestamps
- append: Write without merge (no deduplication)
"""

TSCDType: TypeAlias = Literal["scd1", "scd2"]

# Function signature for table naming strategy
TTableNameStrategy: TypeAlias = Callable[[TLayer, str], str]


# ============ Result Types ============


@dataclass(frozen=True)
class MergeResult:
    """Result of a merge operation."""

    inserted: int
    updated: int
    deleted: int
    source_rows: int


@dataclass(frozen=True)
class BronzeResult:
    """Result of bronze layer processing."""

    entities_processed: dict[str, int]  # entity_name -> record count
    tables_written: list[str]  # Fully qualified table names
    validation_errors: int  # Number of records that failed validation
    duration_seconds: float
    batch_id: str
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class SilverResult:
    """Result of silver layer processing."""

    entities_processed: dict[str, int]  # entity_name -> record count
    tables_written: list[str]  # Fully qualified table names
    records_merged: int  # Total records merged across all entities
    records_inserted: int  # New records inserted
    records_updated: int  # Existing records updated
    duration_seconds: float
    batch_id: str
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class GoldResult:
    """Result of gold layer processing."""

    dimensions_created: list[str]  # Dimension table names
    facts_created: list[str]  # Fact table names
    total_dimension_records: int
    total_fact_records: int
    duration_seconds: float
    batch_id: str
    warnings: list[str] = field(default_factory=list)
