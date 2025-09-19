"""Fact table configuration models for Gold layer processing. NO OPTIONAL FIELDS."""

from pydantic import BaseModel, ConfigDict, Field

from ..utils.base import ErrorCode
from ..utils.exceptions import ETLConfigError


class DimensionMapping(BaseModel):
    """Mapping configuration for joining dimensions. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)

    fact_column: str = Field(description="Column in fact table")
    dimension_table: str = Field(description="Dimension table name")
    dimension_key_column: str = Field(description="Key column in dimension")
    surrogate_key_column: str = Field(description="Surrogate key column name")


class CalculatedColumn(BaseModel):
    """Configuration for calculated columns. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)

    name: str = Field(description="Column name")
    expression: str = Field(description="Spark SQL expression")
    data_type: str = Field(description="Result data type")


class FactConfig(BaseModel):
    """Configuration for creating fact tables. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)

    # Fact identification - REQUIRED
    name: str = Field(description="Fact table name")
    source: str = Field(description="Source integration")
    source_entities: list[str] = Field(description="Source entity names")

    # Key configuration - REQUIRED
    business_keys: list[str] = Field(description="Business key columns")
    surrogate_keys: list[str] = Field(description="Surrogate key columns to generate")

    # Dimension configuration - REQUIRED
    dimension_mappings: list[DimensionMapping] = Field(description="Dimension join configs")

    # Column configuration - REQUIRED
    measure_columns: list[str] = Field(description="Measure columns")
    dimension_columns: list[str] = Field(description="Dimension reference columns")
    calculated_columns: list[CalculatedColumn] = Field(description="Calculated columns")

    # Processing configuration - REQUIRED
    date_column: str = Field(description="Primary date column for partitioning")
    add_entity_type: bool = Field(description="Add entity type column for multi-entity facts")
    entity_type_column: str = Field(description="Name of entity type column")

    # Audit configuration - REQUIRED
    add_audit_columns: bool = Field(description="Add ETL audit columns")

    def validate_config(self) -> None:
        """Validate fact configuration. FAIL FAST on issues."""
        # Validate required fields
        if not self.name:
            raise ETLConfigError(
                "Fact table name is required",
                code=ErrorCode.CONFIG_MISSING
            )

        if not self.source:
            raise ETLConfigError(
                "Source integration is required",
                code=ErrorCode.CONFIG_MISSING
            )

        if not self.source_entities:
            raise ETLConfigError(
                "At least one source entity is required",
                code=ErrorCode.CONFIG_MISSING,
                details={"fact": self.name}
            )

        if not self.business_keys:
            raise ETLConfigError(
                "At least one business key is required",
                code=ErrorCode.CONFIG_MISSING,
                details={"fact": self.name}
            )

        if not self.date_column:
            raise ETLConfigError(
                "Date column is required for fact tables",
                code=ErrorCode.CONFIG_MISSING,
                details={"fact": self.name}
            )

        # Validate multi-entity configuration
        if self.add_entity_type and not self.entity_type_column:
            raise ETLConfigError(
                "Entity type column name required when add_entity_type is True",
                code=ErrorCode.CONFIG_INVALID,
                details={"fact": self.name, "add_entity_type": True}
            )

        # Validate dimension mappings
        for mapping in self.dimension_mappings:
            if not mapping.fact_column:
                raise ETLConfigError(
                    "Fact column is required in dimension mapping",
                    code=ErrorCode.CONFIG_INVALID,
                    details={"fact": self.name, "dimension": mapping.dimension_table}
                )
