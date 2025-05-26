"""Configuration management for unified ETL framework."""

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class SourceConfig(BaseModel):
    """Configuration for a data source."""

    base_url: str
    auth_type: str = "api_key"
    credentials: dict[str, str] = Field(default_factory=dict)
    retry_count: int = 3
    timeout: int = 30


class EntityConfig(BaseModel):
    """Configuration for an entity/table."""

    source: str
    endpoint: str
    bronze_table: str
    silver_table: str
    gold_tables: list[str] = Field(default_factory=list)
    field_selection_mode: str = "pydantic"
    include_nested: bool = True
    transformations: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)


class PipelineConfig(BaseSettings):
    """Main pipeline configuration."""

    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    entities: dict[str, EntityConfig] = Field(default_factory=dict)
    lakehouse_root: str = "/lakehouse/default/Tables"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"

    @classmethod
    def from_yaml(cls, path: str | Path) -> "PipelineConfig":
        """Load configuration from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)

        # Expand environment variables
        data = _expand_env_vars(data)

        return cls(**data)


def _expand_env_vars(data: Any) -> Any:
    """Recursively expand environment variables in configuration."""
    if isinstance(data, str) and data.startswith("${") and data.endswith("}"):
        env_var = data[2:-1]
        return os.getenv(env_var, data)
    elif isinstance(data, dict):
        return {k: _expand_env_vars(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [_expand_env_vars(item) for item in data]
    return data


def load_pipeline_config(path: str | None = None) -> PipelineConfig:
    """
    Load pipeline configuration from file or environment.
    
    Args:
        path: Optional path to YAML config file
        
    Returns:
        Loaded configuration
    """
    if path and Path(path).exists():
        return PipelineConfig.from_yaml(path)

    # Try default locations
    for default_path in ["config/pipeline.yaml", "pipeline.yaml", ".pipeline.yaml"]:
        if Path(default_path).exists():
            return PipelineConfig.from_yaml(default_path)

    # Fall back to environment/defaults
    return PipelineConfig()
