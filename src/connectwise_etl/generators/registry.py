"""Central registry for all model generators."""

import json
import logging
from pathlib import Path
from typing import ClassVar

from .base import ModelGenerator
from .cdm import CDMGenerator
from .openapi import OpenAPIGenerator

logger = logging.getLogger(__name__)


class GeneratorRegistry:
    """Central registry for all generators."""

    _generators: ClassVar[dict[str, type[ModelGenerator]]] = {
        "openapi": OpenAPIGenerator,
        "cdm": CDMGenerator,
        # "jsonschema": JSONSchemaGenerator,  # To be implemented
        # "json": RawJSONGenerator,  # To be implemented
    }

    @classmethod
    def register(cls, format: str, generator_class: type[ModelGenerator]) -> None:
        """Register a new generator."""
        cls._generators[format] = generator_class
        logger.info(f"Registered generator for format: {format}")

    @classmethod
    def create(cls, format: str, config_path: Path | None = None) -> ModelGenerator:
        """Create appropriate generator."""
        if format not in cls._generators:
            available = ", ".join(cls._generators.keys())
            raise ValueError(f"Unknown format: {format}. Available: {available}")

        return cls._generators[format](config_path)

    @classmethod
    def detect_format(cls, source: Path) -> str:
        """Auto-detect format from file/directory."""
        if source.is_dir():
            # Check for CDM files
            if any(source.glob("*.cdm.json")):
                return "cdm"

            # Check for OpenAPI files
            for pattern in ["openapi.json", "openapi.yaml", "swagger.json"]:
                if (source / pattern).exists():
                    return "openapi"

            return "json"  # Default for directories

        # Check file extension
        suffix = source.suffix.lower()

        if suffix in [".yaml", ".yml"]:
            # Could be OpenAPI YAML
            return "openapi"

        if suffix == ".json":
            # Need to check content
            try:
                with open(source) as f:
                    # Read first 2KB to detect format
                    content = f.read(2048)

                # Try to parse as JSON
                if content.strip().startswith("{"):
                    # Read enough to get first few keys
                    with open(source) as f:
                        data = json.load(f)

                    # Check for format indicators
                    if "openapi" in data or "swagger" in data:
                        return "openapi"
                    elif "definitions" in data:
                        # Check if it's CDM by looking for hasAttributes
                        definitions = data["definitions"]
                        if isinstance(definitions, list) and definitions:
                            # CDM with list of definitions
                            if "hasAttributes" in definitions[0]:
                                return "cdm"
                        elif isinstance(definitions, dict):
                            # Check first definition
                            first_def = next(iter(definitions.values()), {})
                            if "hasAttributes" in first_def:
                                return "cdm"
                    elif "$schema" in data:
                        return "jsonschema"

            except (json.JSONDecodeError, KeyError):
                pass

        # Default
        return "json"

    @classmethod
    def list_formats(cls) -> list[str]:
        """List all registered formats."""
        return list(cls._generators.keys())
