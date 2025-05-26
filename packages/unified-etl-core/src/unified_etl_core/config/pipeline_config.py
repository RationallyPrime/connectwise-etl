"""Pipeline configuration models and loader."""

import os
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings


class AuthConfig(BaseModel):
    """Authentication configuration."""

    type: str
    credentials: dict[str, str]


class SourceConfig(BaseModel):
    """Source system configuration."""

    base_url: str
    auth: AuthConfig
    retry_max_attempts: int = 3
    retry_wait_exponential: float = 1.0
    timeout: int = 30


class FieldSelectionConfig(BaseModel):
    """Field selection configuration."""

    mode: str = "pydantic"  # pydantic or explicit
    include_nested: bool = True
    fields: list[str] | None = None  # For explicit mode


class TransformationConfig(BaseModel):
    """Single transformation configuration."""

    type: str  # flatten, scd, surrogate_key, dimension, etc.
    params: dict[str, Any] = Field(default_factory=dict)

    @field_validator("type")
    def validate_type(cls, v: str) -> str:
        """Validate transformation type."""
        valid_types = {
            "flatten",
            "scd",
            "surrogate_key",
            "dimension",
            "hierarchy",
            "calculate",
        }
        if v not in valid_types:
            raise ValueError(f"Invalid transformation type: {v}")
        return v


class EntityConfig(BaseModel):
    """Entity pipeline configuration."""

    source: str
    endpoint: str
    bronze_table: str
    silver_table: str
    gold_tables: list[str] = Field(default_factory=list)
    field_selection: FieldSelectionConfig = Field(default_factory=FieldSelectionConfig)
    transformations: dict[str, list[TransformationConfig]] = Field(default_factory=dict)
    partition_by: str | None = None
    z_order_by: list[str] | None = None
    incremental_key: str | None = None


class PipelineConfig(BaseSettings):
    """Main pipeline configuration."""

    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    entities: dict[str, EntityConfig] = Field(default_factory=dict)

    # Pipeline settings
    bronze_path: str = "/lakehouse/default/Tables/bronze"
    silver_path: str = "/lakehouse/default/Tables/silver"
    gold_path: str = "/lakehouse/default/Tables/gold"

    # Performance settings
    max_parallel_entities: int = 5
    batch_size: int = 1000

    # Monitoring
    enable_metrics: bool = True
    metrics_table: str = "pipeline_metrics"

    class Config:
        env_prefix = "PIPELINE_"
        env_nested_delimiter = "__"


def load_pipeline_config(config_path: str) -> PipelineConfig:
    """Load pipeline configuration from YAML file.

    Args:
        config_path: Path to YAML configuration file

    Returns:
        Loaded and validated configuration
    """
    # Expand environment variables in path
    config_path = os.path.expanduser(config_path)

    with open(config_path) as f:
        raw_config = yaml.safe_load(f)

    # Expand environment variables in config values
    def expand_env_vars(obj: Any) -> Any:
        """Recursively expand environment variables."""
        if isinstance(obj, str):
            return os.path.expandvars(obj)
        elif isinstance(obj, dict):
            return {k: expand_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [expand_env_vars(item) for item in obj]
        return obj

    expanded_config = expand_env_vars(raw_config)

    # Parse transformations properly
    if "entities" in expanded_config:
        for entity_name, entity_config in expanded_config["entities"].items():
            if "transformations" in entity_config:
                for layer, transforms in entity_config["transformations"].items():
                    parsed_transforms = []
                    for transform in transforms:
                        if isinstance(transform, dict):
                            # Extract type and params
                            transform_type = transform.pop("type", None)
                            if transform_type:
                                parsed_transforms.append(
                                    {"type": transform_type, "params": transform}
                                )
                        else:
                            parsed_transforms.append(transform)
                    entity_config["transformations"][layer] = parsed_transforms

    return PipelineConfig(**expanded_config)
