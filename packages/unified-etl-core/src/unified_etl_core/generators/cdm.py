"""CDM (Common Data Model) schema model generator."""
import json
import logging
import tempfile
from pathlib import Path
from typing import Any

from unified_etl_core.generators.base import ModelGenerator

logger = logging.getLogger(__name__)


class CDMConverter:
    """Convert CDM manifests to JSON Schema format."""

    def __init__(self, config: dict[str, Any] | None = None):
        """Initialize converter with optional config."""
        self.config = config or {}
        self.type_mapping = self.config.get("cdm", {}).get("type_mapping", {
            "String": "string",
            "Int32": "integer",
            "Int64": "integer",
            "Decimal": "number",
            "Boolean": "boolean",
            "DateTime": "string",  # With format: date-time
            "Guid": "string",  # With format: uuid
        })

    def convert_file(self, cdm_file: Path) -> dict[str, Any]:
        """Convert a single CDM file to JSON Schema."""
        logger.info(f"Converting CDM file: {cdm_file}")

        with open(cdm_file) as f:
            cdm_data = json.load(f)

        # Extract entity information
        entity = self._extract_entity(cdm_data)

        # Convert attributes to properties
        properties = {}
        required = []

        for attr in entity.get("hasAttributes", []):
            field_name, field_def, is_required = self._convert_attribute(attr)
            properties[field_name] = field_def
            if is_required:
                required.append(field_name)

        # Build JSON Schema
        return self._build_schema(entity["entityName"], properties, required)

    def _extract_entity(self, cdm_data: dict) -> dict:
        """Extract entity definition from CDM data."""
        # CDM can have entity at root or in definitions
        if cdm_data.get("definitions"):
            definitions = cdm_data["definitions"]
            # Definitions can be a list or dict
            if isinstance(definitions, list):
                # Get first entity from list
                if definitions:
                    return definitions[0]
                else:
                    raise ValueError("Empty definitions list in CDM data")
            else:
                # Get first entity from dict
                entity_name = next(iter(definitions.keys()))
                return definitions[entity_name]
        elif "entityName" in cdm_data:
            # Entity at root level
            return cdm_data
        else:
            raise ValueError("Could not find entity definition in CDM data")

    def _convert_attribute(self, attr: dict) -> tuple[str, dict, bool]:
        """Convert CDM attribute to JSON Schema property."""
        name = attr.get("name", "")

        # Get data type
        data_type = attr.get("dataFormat", "String")
        json_type = self.type_mapping.get(data_type, "string")

        # Build property definition
        prop_def = {
            "type": json_type,
        }

        # Add description if available
        if "description" in attr:
            prop_def["description"] = attr["description"]

        # Add format hints
        if data_type == "DateTime":
            prop_def["format"] = "date-time"
        elif data_type == "Guid":
            prop_def["format"] = "uuid"

        # Check if nullable
        is_nullable = attr.get("isNullable", True)
        is_required = not is_nullable

        # Handle nullable with union type
        if is_nullable:
            prop_def = {
                "anyOf": [prop_def, {"type": "null"}]
            }

        return name, prop_def, is_required

    def _build_schema(self, entity_name: str, properties: dict, required: list[str]) -> dict:
        """Build complete JSON Schema for entity."""
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": entity_name,
            "type": "object",
            "properties": properties,
        }

        if required:
            schema["required"] = required

        return schema


class CDMGenerator(ModelGenerator):
    """Generate models from CDM manifests."""

    def __init__(self, config_path: Path | None = None):
        """Initialize with converter."""
        super().__init__(config_path)
        self.converter = CDMConverter(self.config)

    def prepare_input(self, source: Path) -> Path:
        """Convert CDM to JSON Schema for datamodel-codegen."""
        if source.is_file():
            # Single CDM file
            return self._convert_single_cdm(source)
        else:
            # Directory of CDM files
            return self._convert_cdm_directory(source)

    def _convert_single_cdm(self, cdm_file: Path) -> Path:
        """Convert single CDM file to JSON Schema."""
        schema = self.converter.convert_file(cdm_file)

        # Write to temp file
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            dir=self.config.get("temp_dir", None)
        ) as f:
            json.dump(schema, f, indent=2)
            return Path(f.name)

    def _convert_cdm_directory(self, cdm_dir: Path) -> Path:
        """Convert directory of CDM files to JSON Schema directory."""
        # Create temp directory
        temp_dir = Path(tempfile.mkdtemp(
            dir=self.config.get("temp_dir", None),
            prefix="cdm_schemas_"
        ))

        # Convert each CDM file
        for cdm_file in cdm_dir.glob("*.cdm.json"):
            logger.info(f"Converting {cdm_file.name}")
            schema = self.converter.convert_file(cdm_file)

            # Write schema file
            schema_file = temp_dir / f"{cdm_file.stem}.schema.json"
            with open(schema_file, "w") as f:
                json.dump(schema, f, indent=2)

        return temp_dir

    def _get_format_args(self) -> list[str]:
        """JSON Schema specific arguments."""
        return [
            "--input-file-type", "jsonschema",
            "--disable-timestamp",
        ]
