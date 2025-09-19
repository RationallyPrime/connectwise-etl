"""Core configuration models for the ETL framework. NO BACKWARDS COMPATIBILITY."""

from enum import Enum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ..utils.base import ErrorCode
from ..utils.exceptions import ETLConfigError


class TableNamingConvention(str, Enum):
    """Table naming patterns."""
    UNDERSCORE = "underscore"  # bronze_cw_agreement
    CAMELCASE = "camelcase"    # bronzeCwAgreement


class LayerConfig(BaseModel):
    """Configuration for a medallion layer. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)

    catalog: str = Field(description="Catalog name (e.g., Lakehouse)")
    schema_name: str = Field(description="Schema name (bronze, silver, gold)", alias="schema")
    prefix: str = Field(description="Table prefix (bronze_, silver_, gold_)")
    naming_convention: TableNamingConvention = Field(description="Naming convention for tables")


class IntegrationConfig(BaseModel):
    """Configuration for an integration. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)

    name: str = Field(description="Integration name (connectwise)")
    abbreviation: str = Field(description="Short code (cw)")
    base_url: str = Field(description="Base URL for API access")
    enabled: bool = Field(description="Whether integration is enabled")


class SparkConfig(BaseModel):
    """Spark session configuration. ALL FIELDS REQUIRED."""
    model_config = ConfigDict(frozen=True)

    app_name: str = Field(description="Spark application name")
    session_type: Literal["fabric", "local", "databricks"] = Field(description="Session type")
    config_overrides: dict[str, str] = Field(description="Additional Spark config")


class ETLConfig(BaseModel):
    """Root configuration for entire ETL system. ZERO OPTIONAL FIELDS."""
    model_config = ConfigDict(frozen=True)

    # Layer configurations - ALL REQUIRED
    bronze: LayerConfig = Field(description="Bronze layer configuration")
    silver: LayerConfig = Field(description="Silver layer configuration")
    gold: LayerConfig = Field(description="Gold layer configuration")

    # Integration configurations - ALL REQUIRED
    integrations: dict[str, IntegrationConfig] = Field(description="Integration configurations")

    # Spark configuration - REQUIRED
    spark: SparkConfig = Field(description="Spark session configuration")

    # Global settings - ALL REQUIRED
    fail_on_error: bool = Field(description="Fail fast on errors")
    audit_columns: bool = Field(description="Add ETL audit columns")

    def get_table_name(
        self,
        layer: Literal["bronze", "silver", "gold"],
        integration: str,
        entity: str,
        table_type: Literal["table", "fact", "dim"] = "table"
    ) -> str:
        """Generate fully qualified table name. FAIL FAST on missing config."""
        # Get layer config - REQUIRED
        layer_config = getattr(self, layer)

        # Get integration config - FAIL FAST if missing
        if integration not in self.integrations:
            raise ETLConfigError(
                f"Integration '{integration}' not configured",
                code=ErrorCode.CONFIG_MISSING,
                details={"integration": integration, "available": list(self.integrations.keys())}
            )

        integration_config = self.integrations[integration]

        # Validate entity name - REQUIRED
        if not entity:
            raise ETLConfigError(
                "Entity name is required for table name generation",
                code=ErrorCode.CONFIG_MISSING,
                details={"layer": layer, "integration": integration}
            )

        # Build table name components
        if table_type == "dim":
            base_name = f"dim_{entity}"
        elif table_type == "fact":
            base_name = f"fact_{entity}"
        else:
            # Build base name with proper handling of empty prefix/abbreviation
            parts = []
            if layer_config.prefix:
                parts.append(layer_config.prefix)
            if integration_config.abbreviation:
                parts.append(integration_config.abbreviation)
            parts.append(entity)
            base_name = "_".join(parts)

        # Return fully qualified name
        return f"{layer_config.catalog}.{layer_config.schema_name}.{base_name}"

    def get_layer_path(
        self,
        layer: Literal["bronze", "silver", "gold"],
        integration: str,
        entity: str
    ) -> str:
        """Get file system path for a table. FAIL FAST on missing config."""
        table_name = self.get_table_name(layer, integration, entity)
        # Remove catalog prefix for path
        schema_table = table_name.split(".", 1)[1]
        return f"/lakehouse/default/Tables/{schema_table.replace('.', '/')}"
