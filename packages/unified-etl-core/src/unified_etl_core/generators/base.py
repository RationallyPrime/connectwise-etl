"""Base generator with common logic for all model generators."""

import logging
import subprocess
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Protocol

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
            "--base-class",
            self.config.get("base_class", "sparkdantic.SparkModel"),
            "--target-python-version",
            self.config.get("target_python", "3.11"),
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

    def generate(self, source: Path, output: Path, validate: bool = True) -> None:
        """Common generation logic."""
        logger.info(f"Generating models from {source} → {output}")

        # Ensure output directory exists
        output.parent.mkdir(parents=True, exist_ok=True)

        # Prepare input
        prepared_input = self.prepare_input(source)

        # Add format-specific args
        args = (
            self.base_args
            + self._get_format_args()
            + ["--input", str(prepared_input), "--output", str(output)]
        )

        logger.debug(f"Running: {' '.join(args)}")

        # Run generation
        result = subprocess.run(args, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Generation failed: {result.stderr}")

        logger.info("Model generation completed successfully")

        # Post-process
        self._post_process(output)

        # Validate if requested
        if validate:
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
        """Add header comment to generated file(s)."""
        # Check if output exists
        if not output.exists():
            logger.warning(f"Output {output} does not exist")
            return

        # Handle both file and directory outputs
        if output.is_file():
            files_to_process = [output]
        elif output.is_dir():
            # Add header to all .py files in directory
            files_to_process = list(output.glob("*.py"))
        else:
            logger.warning(f"Output {output} is neither file nor directory")
            return

        header = '''"""
Auto-generated models from schema.
DO NOT EDIT MANUALLY - regenerate using regenerate_models.py
"""

'''

        for file_path in files_to_process:
            try:
                with open(file_path) as f:
                    content = f.read()

                if not content.startswith('"""'):
                    with open(file_path, "w") as f:
                        f.write(header + content)
            except Exception as e:
                logger.warning(f"Failed to add header to {file_path}: {e}")

    def _validate_output(self, output: Path) -> list[str]:
        """Basic validation of generated models."""
        errors = []

        # Check output exists
        if not output.exists():
            errors.append(f"Output not created: {output}")
            return errors

        # Get files to validate
        if output.is_file():
            files_to_validate = [output]
        elif output.is_dir():
            files_to_validate = list(output.glob("*.py"))
            if not files_to_validate:
                errors.append(f"No Python files found in output directory: {output}")
                return errors
        else:
            errors.append(f"Output is neither file nor directory: {output}")
            return errors

        # Try to compile each Python file
        for file_path in files_to_validate:
            try:
                with open(file_path) as f:
                    compile(f.read(), str(file_path), "exec")
            except SyntaxError as e:
                errors.append(f"Syntax error in {file_path}: {e}")
            except Exception as e:
                errors.append(f"Failed to validate {file_path}: {e}")

        return errors
