"""Configuration models for the pipeline."""
from __future__ import annotations

from pydantic import BaseModel, Field


class AuthConfig(BaseModel):
    """Authentication configuration."""
    type: str
    credentials: dict[str, str]


class SourceConfig(BaseModel):
    """Source system configuration."""
    base_url: str
    auth: AuthConfig


class EntityConfig(BaseModel):
    """Entity pipeline configuration."""
    source: str
    endpoint: str
    bronze_table: str
    silver_table: str
    gold_tables: list[str] = Field(default_factory=list)
    incremental_key: str | None = None


class PipelineConfig(BaseModel):
    """Main pipeline configuration."""
    sources: dict[str, SourceConfig]
    entities: dict[str, EntityConfig]
    
    # Table paths
    bronze_path: str = "/lakehouse/default/Tables/bronze"
    silver_path: str = "/lakehouse/default/Tables/silver"
    gold_path: str = "/lakehouse/default/Tables/gold"