"""Core configuration models using Pydantic v2."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable

from pydantic import BaseModel, ConfigDict, Field

from etl_core.domain.protocols.types import TConfigDict, TLayer, TLoadMode

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class LakehouseConfig(BaseModel):
    """Configuration for the target Lakehouse structure.

    Supports both Unity Catalog (3-part names) and legacy 2-part naming.
    """

    catalog: str | None = Field(
        None, description="Target catalog name (None for legacy 2-part names)"
    )
    bronze_schema: str = Field("bronze", description="Schema name for the Bronze layer")
    silver_schema: str = Field("silver", description="Schema name for the Silver layer")
    gold_schema: str = Field("gold", description="Schema name for the Gold layer")

    def get_table_name(self, layer: TLayer, entity_name: str) -> str:
        """
        Construct a fully qualified table name.

        Supports:
        - Unity Catalog: catalog.schema.table (3-part)
        - Legacy: schema.table (2-part)

        Args:
            layer: Medallion layer (bronze, silver, gold).
            entity_name: Name of the entity/table.

        Returns:
            Fully qualified table name.

        Example:
            >>> config = LakehouseConfig(catalog="main", bronze_schema="bronze")
            >>> config.get_table_name("bronze", "agreement")
            'main.bronze.agreement'

            >>> config = LakehouseConfig(catalog=None, bronze_schema="bronze")
            >>> config.get_table_name("bronze", "agreement")
            'bronze.agreement'
        """
        schema_map = {
            "bronze": self.bronze_schema,
            "silver": self.silver_schema,
            "gold": self.gold_schema,
        }
        schema = schema_map[layer]

        if self.catalog:
            return f"{self.catalog}.{schema}.{entity_name}"  # 3-part Unity Catalog
        return f"{schema}.{entity_name}"  # 2-part legacy


class RuntimeContext(BaseModel):
    """Runtime context passed to processors during execution.

    This is the primary container for all runtime state and configuration.
    Passed to every processor's process() method.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    spark: "SparkSession" = Field(..., description="Active Spark Session")
    lakehouse: LakehouseConfig = Field(..., description="Lakehouse configuration")
    mode: TLoadMode = Field("incremental", description="Processing mode")
    batch_id: str = Field(
        default_factory=lambda: datetime.now().strftime("%Y%m%d_%H%M%S"),
        description="Unique ID for the ETL run",
    )
    entities: list[str] | None = Field(
        None, description="Specific entities to process (None for all)"
    )
    extra_args: TConfigDict = Field(
        default_factory=dict, description="Additional runtime arguments"
    )
    table_name_strategy: Callable[[TLayer, str], str] | None = Field(
        None, description="Optional custom table naming function"
    )

    def table_name(self, layer: TLayer, entity: str) -> str:
        """
        Get fully qualified table name using strategy.

        Allows custom naming strategies while defaulting to lakehouse config.

        Args:
            layer: Medallion layer.
            entity: Entity name.

        Returns:
            Fully qualified table name.
        """
        strategy = self.table_name_strategy or self.lakehouse.get_table_name
        return strategy(layer, entity)
