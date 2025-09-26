"""Table schema models for YAML-driven configuration."""

from sparkdantic import SparkModel


class ColumnSchema(SparkModel):
    """Single column definition."""

    name: str
    type: str  # "string", "integer", "bigint", "decimal", etc.
    description: str | None = None
    nullable: bool = True


class TableSchema(SparkModel):
    """Universal schema for any table (dimension or fact)."""

    table_name: str
    primary_key: str
    natural_key: str | None = None
    source_table: str | None = None
    columns: list[ColumnSchema]
