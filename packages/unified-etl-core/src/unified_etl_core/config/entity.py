"""Entity configuration models for Silver layer processing. NO OPTIONAL FIELDS."""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from unified_etl_core.utils.base import ErrorCode
from unified_etl_core.utils.exceptions import ETLConfigError


class DataType(str, Enum):
    """Supported data types for column mappings."""
    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    DOUBLE = "double"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    JSON = "json"


class ColumnMapping(BaseModel):
    """Column mapping configuration. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)
    
    source_column: str = Field(description="Source column name")
    target_column: str = Field(description="Target column name")
    target_type: DataType = Field(description="Target data type")
    transformation: str = Field(description="Transformation expression (Spark SQL)")


class SCDConfig(BaseModel):
    """Slowly Changing Dimension configuration. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)
    
    type: Literal[1, 2] = Field(description="SCD Type (1=overwrite, 2=historize)")
    business_keys: list[str] = Field(description="Business key columns")
    timestamp_column: str = Field(description="Timestamp column for versioning")
    
    def validate_business_keys(self) -> None:
        """Validate business keys are provided. FAIL FAST."""
        if not self.business_keys:
            raise ETLConfigError(
                "SCD configuration requires at least one business key",
                code=ErrorCode.CONFIG_INVALID,
                details={"scd_type": self.type}
            )


class EntityConfig(BaseModel):
    """Configuration for processing an entity through Silver layer. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)
    
    # Entity identification - REQUIRED
    name: str = Field(description="Entity name")
    source: str = Field(description="Source integration name")
    model_class_name: str = Field(description="Pydantic model class name")
    
    # Processing configuration - REQUIRED
    flatten_nested: bool = Field(description="Whether to flatten nested structures")
    flatten_max_depth: int = Field(description="Maximum depth for flattening", ge=0)
    preserve_columns: list[str] = Field(description="Columns to preserve as-is")
    
    # Column configurations - REQUIRED
    column_mappings: dict[str, ColumnMapping] = Field(description="Column type mappings")
    json_columns: list[str] = Field(description="Columns containing JSON to parse")
    business_keys: list[str] = Field(description="Business key columns")
    
    # SCD configuration - REQUIRED (even if not using SCD)
    scd: SCDConfig | None = Field(description="SCD configuration if applicable")
    
    # Audit configuration - REQUIRED
    add_audit_columns: bool = Field(description="Add ETL audit columns")
    strip_null_columns: bool = Field(description="Remove columns with all nulls")
    
    @field_validator('flatten_max_depth')
    @classmethod
    def validate_flatten_max_depth(cls, v) -> int:
        """Ensure flatten_max_depth is always an integer."""
        if isinstance(v, str):
            try:
                return int(v)
            except ValueError:
                raise ValueError(f"flatten_max_depth must be an integer, got string: {v}")
        if not isinstance(v, int):
            raise ValueError(f"flatten_max_depth must be an integer, got {type(v).__name__}: {v}")
        return v
    
    def validate_config(self) -> None:
        """Validate entity configuration. FAIL FAST on issues."""
        # Validate required fields
        if not self.name:
            raise ETLConfigError(
                "Entity name is required",
                code=ErrorCode.CONFIG_MISSING
            )
        
        if not self.source:
            raise ETLConfigError(
                "Source integration is required",
                code=ErrorCode.CONFIG_MISSING
            )
        
        if not self.model_class_name:
            raise ETLConfigError(
                "Model class name is required",
                code=ErrorCode.CONFIG_MISSING
            )
        
        if not self.business_keys:
            raise ETLConfigError(
                "At least one business key is required",
                code=ErrorCode.CONFIG_MISSING,
                details={"entity": self.name}
            )
        
        # Validate SCD if configured
        if self.scd:
            self.scd.validate_business_keys()
            
            # Ensure SCD business keys are subset of entity business keys
            for key in self.scd.business_keys:
                if key not in self.business_keys:
                    raise ETLConfigError(
                        f"SCD business key '{key}' not in entity business keys",
                        code=ErrorCode.CONFIG_INVALID,
                        details={
                            "entity": self.name,
                            "scd_keys": self.scd.business_keys,
                            "entity_keys": self.business_keys
                        }
                    )