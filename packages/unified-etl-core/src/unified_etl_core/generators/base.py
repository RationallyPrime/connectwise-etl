"""Base generator with common logic for all model generators."""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Protocol
import subprocess
import json
import logging
import tempfile
import shutil
import tomli

logger = logging.getLogger(__name__)


class SchemaConverter(Protocol):
    """Protocol for schema converters."""
    
    def convert(self, input_data: Any) -> dict[str, Any]:
        """Convert input to JSON Schema format."""
        ...


class ModelGenerator(ABC):
    """Base generator with common logic."""
    
    def __init__(self, config_path: Path | None = None):
        """Initialize generator with optional config."""
        self.config = self._load_config(config_path) if config_path else {}
        self.base_args = [
            "datamodel-codegen",
            "--base-class", self.config.get("base_class", "sparkdantic.SparkModel"),
            "--target-python-version", self.config.get("target_python", "3.11"),
            "--use-standard-collections",
            "--use-union-operator",
            "--strict-nullable",
            "--use-field-description",
            "--field-constraints",
            "--reuse-model",
        ]
        
        # Add config-based args
        if not self.config.get("snake_case_field", True):
            self.base_args.append("--keep-model-order")
            
        if self.config.get("aliased_fields", True):
            self.base_args.append("--use-annotated")
    
    def _load_config(self, config_path: Path) -> dict[str, Any]:
        """Load configuration from TOML file."""
        if not config_path.exists():
            logger.warning(f"Config file not found: {config_path}")
            return {}
            
        with open(config_path, "rb") as f:
            return tomli.load(f)
    
    @abstractmethod
    def prepare_input(self, source: Path) -> Path:
        """Convert source to format datamodel-codegen understands."""
        pass
    
    def generate(self, source: Path, output: Path) -> None:
        """Common generation logic."""
        logger.info(f"Generating models from {source} → {output}")
        
        # Ensure output directory exists
        output.parent.mkdir(parents=True, exist_ok=True)
        
        # Prepare input
        prepared_input = self.prepare_input(source)
        
        # Add format-specific args
        args = self.base_args + self._get_format_args() + [
            "--input", str(prepared_input),
            "--output", str(output)
        ]
        
        logger.debug(f"Running: {' '.join(args)}")
        
        # Run generation
        result = subprocess.run(args, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Generation failed: {result.stderr}")
        
        logger.info("Model generation completed successfully")
        
        # Post-process
        self._post_process(output)
        
        # Validate
        errors = self._validate_output(output)
        if errors:
            raise RuntimeError(f"Validation failed: {', '.join(errors)}")
            
        logger.info("✅ Models validated successfully")
    
    @abstractmethod
    def _get_format_args(self) -> list[str]:
        """Get format-specific arguments."""
        pass
    
    def _post_process(self, output: Path) -> None:
        """Post-process generated models. Override if needed."""
        # Add header comment if configured
        if self.config.get("add_header_comment", True):
            self._add_header(output)
    
    def _add_header(self, output: Path) -> None:
        """Add header comment to generated file."""
        with open(output, "r") as f:
            content = f.read()
            
        header = '''"""
Auto-generated models from schema.
DO NOT EDIT MANUALLY - regenerate using regenerate_models.py
"""

'''
        
        if not content.startswith('"""'):
            with open(output, "w") as f:
                f.write(header + content)
    
    def _validate_output(self, output: Path) -> list[str]:
        """Basic validation of generated models."""
        errors = []
        
        # Check file exists
        if not output.exists():
            errors.append(f"Output file not created: {output}")
            return errors
            
        # Try to compile the Python file
        try:
            with open(output, "r") as f:
                compile(f.read(), str(output), "exec")
        except SyntaxError as e:
            errors.append(f"Syntax error in generated file: {e}")
            
        return errors