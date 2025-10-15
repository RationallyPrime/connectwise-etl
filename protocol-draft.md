This is the first draft of the abstract protocols and core utilities for the redesigned ETL framework, focusing on abstraction and dependency injection. This structure is designed to live within a core package (e.g., etl_core).

Package Structure Overview

etl_core/

├── config.py            # Concrete configuration models (Pydantic)

├── protocols/

│   ├── types.py         # Type aliases and common types

│   ├── registry.py      # ModelRegistryProtocol

│   ├── client.py        # DataSourceClientProtocol

│   ├── processor.py     # LayerProcessorProtocol and aliases

│   └── plugin.py        # IntegrationPluginProtocol

└── utils/

    └── incremental.py   # Concrete IncrementalHandler utility

etl_core/protocols/types.py

Common type aliases and definitions used across the framework.

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Literal, TypeAlias

# Placeholder types for environments where Spark/SparkDantic might not be installed during static analysis
try:
    from pyspark.sql import DataFrame, SparkSession
    from sparkdantic import SparkModel
except ImportError:
    DataFrame = Any
    SparkSession = Any
    SparkModel = Any

# Type Aliases for clarity and consistency
TSparkModelClass: TypeAlias = type[SparkModel]
TModelRegistryDict: TypeAlias = dict[str, TSparkModelClass]
TDataFrameRegistryDict: TypeAlias = dict[str, DataFrame]
TConfigDict: TypeAlias = dict[str, Any]

# Literal types for pipeline control flow
TLayer: TypeAlias = Literal["bronze", "silver", "gold"]
TLoadMode: TypeAlias = Literal["full", "incremental", "append"]
TSCDType: TypeAlias = Literal["scd1", "scd2"]

# Function signature for table naming strategy
TTableNameStrategy: TypeAlias = Callable[[TLayer, str], str]


# ============ Structured Result Types ============

@dataclass(frozen=True)
class BronzeResult:
    """Result of bronze layer processing."""
    entities_processed: dict[str, int]  # entity_name -> record count
    tables_written: list[str]  # Fully qualified table names
    validation_errors: int  # Number of records that failed validation
    duration_seconds: float
    batch_id: str


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


@dataclass(frozen=True)
class GoldResult:
    """Result of gold layer processing."""
    dimensions_created: list[str]  # Dimension table names
    facts_created: list[str]  # Fact table names
    total_dimension_records: int
    total_fact_records: int
    duration_seconds: float
    batch_id: str

etl_core/config.py

Concrete Pydantic models for shared configuration structures. These are provided by the core.

from __future__ import annotations

from datetime import datetime
from pydantic import BaseModel, Field

from etl_core.protocols.types import SparkSession, TConfigDict, TLayer, TLoadMode


class LakehouseConfig(BaseModel):
    """Configuration for the target Lakehouse structure."""
    catalog: str = Field(..., description="Target catalog name")
    bronze_schema: str = Field("bronze", description="Schema name for the Bronze layer")
    silver_schema: str = Field("silver", description="Schema name for the Silver layer")
    gold_schema: str = Field("gold", description="Schema name for the Gold layer")

    def get_table_name(self, layer: TLayer, entity_name: str) -> str:
        """Construct a fully qualified table name (catalog.schema.table)."""
        schema_map = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
        }
        schema = schema_map[layer]
        # Format for Spark SQL
        return f"{self.catalog}.{schema}.{entity_name}"


class RuntimeContext(BaseModel):
    """Runtime context passed to processors during execution."""
    spark: SparkSession = Field(..., description="Active Spark Session")
    lakehouse: LakehouseConfig = Field(..., description="Lakehouse configuration")
    mode: TLoadMode = Field("incremental", description="Processing mode")
    batch_id: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"),
        description="Unique ID for the ETL run"
    )
    entities: list[str] | None = Field(None, description="Specific entities to process (None for all)")
    extra_args: TConfigDict = Field(default_factory=dict, description="Additional runtime arguments")

    class Config:
        # Allow arbitrary types like SparkSession
        arbitrary_types_allowed = True
        etl_core/protocols/registry.py

Protocol for accessing integration-specific models and configurations.

from __future__ import annotations

from typing import Protocol, runtime_checkable

from etl_core.protocols.types import TSparkModelClass, TConfigDict


@runtime_checkable
class ModelRegistryProtocol(Protocol):
    """
    Protocol for accessing integration-specific models and entity configurations.
    This allows the core framework to look up models and settings dynamically.
    """

    def get_model(self, entity_name: str) -> TSparkModelClass:
        """
        Retrieve the SparkDantic model class for a given entity.

        Raises:
            KeyError: If the entity name is not found.
        """
        ...

    def list_entities(self) -> list[str]:
        """
        List all available entity names managed by this integration.
        """
        ...

    def get_entity_config(self, entity_name: str) -> TConfigDict:
        """
        Retrieve specific configuration for an entity (e.g., endpoints, keys, transformations).
        """
        ...


etl_core/protocols/client.py

Protocols for extracting and validating data from source systems.

from __future__ import annotations

from typing import Protocol, runtime_checkable, Any, Iterator

from etl_core.protocols.types import DataFrame, SparkSession, TSparkModelClass, TConfigDict, TLoadMode


@runtime_checkable
class DataFetcherProtocol(Protocol):
    """
    Protocol for fetching raw data from a source system (API, database, files).

    This interface handles ONLY the raw data extraction. Validation and DataFrame
    creation are handled separately by the DataValidatorProtocol.
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

        This method is responsible for:
        1. Determining the source location (using config).
        2. Handling incremental logic (if mode='incremental', using kwargs like lookback_days).
        3. Fetching the data (pagination, API calls, or file reading).

        Args:
            entity_name: The name of the entity.
            mode: The load mode.
            config: Entity-specific configuration (e.g., endpoint, keys).
            **kwargs: Additional runtime parameters (e.g., lookback_days).

        Returns:
            Iterator of raw dictionaries (unvalidated).
        """
        ...

    def test_connection(self) -> bool:
        """
        Verify connectivity and authentication with the data source.
        """
        ...


@runtime_checkable
class DataValidatorProtocol(Protocol):
    """
    Protocol for validating raw data and creating Spark DataFrames.

    This interface handles Pydantic validation and SparkDantic DataFrame creation.
    Typically implemented once in core and reused across all integrations.
    """

    def validate_and_create_dataframe(
        self,
        raw_data: Iterator[dict[str, Any]],
        model_class: TSparkModelClass,
        spark: SparkSession,
        entity_name: str,
    ) -> DataFrame:
        """
        Validate raw data using Pydantic and create Spark DataFrame.

        This method:
        1. Validates each record using the Pydantic model.
        2. Logs validation errors.
        3. Creates a Spark DataFrame using the SparkDantic schema.

        Args:
            raw_data: Iterator of raw dictionaries from the data source.
            model_class: The SparkDantic model class for validation and schema.
            spark: Active Spark session.
            entity_name: Entity name for logging.

        Returns:
            A Spark DataFrame with validated data.
        """
        ...


etl_core/protocols/processor.py

Protocols for the Medallion layer processing stages.

from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable

from etl_core.config import RuntimeContext
from etl_core.protocols.types import BronzeResult, SilverResult, GoldResult


@runtime_checkable
class BronzeProcessorProtocol(Protocol):
    """
    Protocol for bronze layer processing (extraction and validation).
    """

    def process(self, context: RuntimeContext) -> BronzeResult:
        """
        Execute bronze layer extraction and validation.

        Args:
            context: Runtime context including Spark session, mode, and configuration.

        Returns:
            Structured result with metrics and table information.
        """
        ...


@runtime_checkable
class SilverProcessorProtocol(Protocol):
    """
    Protocol for silver layer processing (transformation and cleansing).
    """

    def process(self, context: RuntimeContext) -> SilverResult:
        """
        Execute silver layer transformations.

        Args:
            context: Runtime context including Spark session, mode, and configuration.

        Returns:
            Structured result with merge statistics and table information.
        """
        ...


@runtime_checkable
class GoldProcessorProtocol(Protocol):
    """
    Protocol for gold layer processing (dimensional modeling).
    """

    def process(
        self,
        context: RuntimeContext,
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
        """
        ...


etl_core/protocols/plugin.py

The main contract defining an integration plugin.

from __future__ import annotations

from typing import Protocol, runtime_checkable

from etl_core.protocols.client import DataFetcherProtocol, DataValidatorProtocol
from etl_core.protocols.processor import (
    BronzeProcessorProtocol,
    SilverProcessorProtocol,
    GoldProcessorProtocol,
)
from etl_core.protocols.registry import ModelRegistryProtocol
# Assuming IncrementalHandler is a concrete class in etl_core.utils.incremental
from etl_core.utils.incremental import IncrementalHandler


@runtime_checkable
class IntegrationPluginProtocol(Protocol):
    """
    The main protocol defining an integration plugin (e.g., ConnectWise, Jira).
    It provides the concrete implementations required by the core orchestrator.

    Each integration gets its own lakehouse, so table naming collisions are not a concern.
    """

    @property
    def name(self) -> str:
        """The unique identifier for the integration (e.g., 'connectwise', 'jira')."""
        ...

    def initialize_registry(self) -> ModelRegistryProtocol:
        """Initialize and return the model registry."""
        ...

    def initialize_fetcher(self) -> DataFetcherProtocol:
        """Initialize and return the data fetcher (integration-specific)."""
        ...

    def initialize_processors(
        self,
        fetcher: DataFetcherProtocol,
        validator: DataValidatorProtocol,
        registry: ModelRegistryProtocol,
        incremental_handler: IncrementalHandler,
    ) -> tuple[BronzeProcessorProtocol, SilverProcessorProtocol, GoldProcessorProtocol]:
        """
        Initialize and return the processors for this integration.

        Args:
            fetcher: The initialized data fetcher (integration-specific).
            validator: The data validator (provided by core, typically reused).
            registry: The initialized model registry.
            incremental_handler: The core incremental handler utility.

        Returns:
            Tuple of (bronze_processor, silver_processor, gold_processor).
        """
        ...
        
etl_core/utils/incremental.py

A concrete utility class provided by the core to handle common incremental logic. This is injected into the processors.

from __future__ import annotations

import structlog
from datetime import datetime
from typing import Literal

from pyspark.sql import DataFrame, SparkSession, Row
from etl_core.protocols.types import TSCDType

logger = structlog.get_logger(__name__)

class IncrementalHandler:
    """
    Handles generic incremental processing logic using Spark SQL MERGE.
    This is a concrete utility class provided by the core framework.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_timestamp_col(self, table_name: str) -> str:
        """Determine the ETL timestamp column based on convention."""
        # Assuming convention: 'bronze' uses 'etl_timestamp', others use '_etl_processed_at'
        # This relies on the table name including the schema (e.g., lakehouse.bronze.table)
        parts = table_name.split('.')
        schema = parts[1] if len(parts) > 1 else 'unknown'
        return "etl_timestamp" if schema == 'bronze' else "_etl_processed_at"

    def get_last_etl_timestamp(self, table_name: str) -> datetime | None:
        """Get the maximum ETL timestamp from a target table."""
        try:
            if not self.spark.catalog.tableExists(table_name):
                return None

            timestamp_col = self._get_timestamp_col(table_name)

            result: list[Row] = self.spark.sql(f"""
                SELECT MAX({timestamp_col}) as last_refresh
                FROM {table_name}
            """).collect()

            return result[0]['last_refresh'] if result and result[0]['last_refresh'] else None
        except Exception as e:
            logger.warning("Could not get last timestamp", table=table_name, error=str(e))
            return None

    def get_changed_records(
        self,
        source_table: str,
        target_table: str,
    ) -> DataFrame:
        """Get records from the source that changed since the last load into the target."""
        since_timestamp = self.get_last_etl_timestamp(target_table)

        if since_timestamp:
            logger.info("Filtering changed records", source=source_table, since=since_timestamp.isoformat())
            timestamp_col = self._get_timestamp_col(source_table)

            return self.spark.sql(f"""
                SELECT * FROM {source_table}
                WHERE {timestamp_col} > '{since_timestamp.isoformat()}'
            """)
        else:
            logger.info("No previous timestamp found, performing full load from source", source=source_table)
            return self.spark.table(source_table)

    def merge(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_keys: list[str],
        scd_type: TSCDType = "scd1",
    ) -> int:
        """
        Merge data using the specified SCD strategy via MERGE INTO.
        """
        if scd_type == "scd1":
            return self._merge_scd1(source_df, target_table, merge_keys)
        elif scd_type == "scd2":
            # SCD2 implementation omitted for brevity, but would go here
            raise NotImplementedError("SCD Type 2 merge is not yet implemented.")
        else:
            raise ValueError(f"Unsupported SCD type: {scd_type}")

    def _merge_scd1(
        self,
        source_df: DataFrame,
        target_table: str,
        merge_keys: list[str],
        deduplicate_source: bool = True,
    ) -> int:
        """Helper for SCD Type 1 (overwrite)."""
        if not merge_keys:
            raise ValueError("merge_keys must be provided.")

        # Deduplicate source if requested
        if deduplicate_source:
            source_count = source_df.count()
            source_df = source_df.dropDuplicates(merge_keys)
            deduped_count = source_df.count()
            if deduped_count < source_count:
                logger.info("Deduplicated source records", removed=source_count - deduped_count)

        # Create temp view for merge
        temp_view = f"temp_merge_source_{abs(hash(target_table))}"
        source_df.createOrReplaceTempView(temp_view)

        # Build merge conditions
        merge_conditions = " AND ".join(
            [f"target.{key} = source.{key}" for key in merge_keys]
        )

        # Execute MERGE
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_conditions}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """

        logger.debug("Executing MERGE SQL", sql=merge_sql)
        self.spark.sql(merge_sql)

        # Clean up
        self.spark.catalog.dropTempView(temp_view)

        return source_df.count()