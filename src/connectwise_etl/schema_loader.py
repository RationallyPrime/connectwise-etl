"""Load YAML schemas into TableSchema models."""

from pathlib import Path

from yaml import safe_load

from .table_schema import ColumnSchema, TableSchema


class SchemaLoader:
    """Load YAML schemas into TableSchema instances."""

    def __init__(self, schema_dir: Path | str):
        self.schema_dir = Path(schema_dir)

    def load_dimensions(self) -> dict[str, TableSchema]:
        """Load dimension schemas from YAML."""
        yaml_path = self.schema_dir / "connectwise-dimensions.yaml"

        with open(yaml_path) as f:
            data = safe_load(f)

        dimensions = {}
        for dim_name, dim_spec in data["dimensions"].items():
            # Parse columns
            columns = [ColumnSchema(**col) for col in dim_spec["columns"]]

            # Create TableSchema instance
            schema = TableSchema(
                table_name=dim_spec["table_name"],
                primary_key=dim_spec["primary_key"],
                natural_key=dim_spec.get("natural_key"),
                source_table=dim_spec.get("source_table"),
                columns=columns,
            )

            dimensions[dim_name] = schema

        return dimensions

    def load_facts(self) -> dict[str, TableSchema]:
        """Load fact schemas from YAML."""
        yaml_path = self.schema_dir / "connectwise-facts.yaml"

        with open(yaml_path) as f:
            data = safe_load(f)

        facts = {}
        for fact_name, fact_spec in data["facts"].items():
            # Parse columns
            columns = [ColumnSchema(**col) for col in fact_spec["columns"]]

            # Create TableSchema instance
            schema = TableSchema(
                table_name=fact_spec["table_name"],
                primary_key=fact_spec["primary_key"],
                natural_key=fact_spec.get("natural_key"),
                source_table=fact_spec.get("source_table"),
                columns=columns,
            )

            facts[fact_name] = schema

        return facts
