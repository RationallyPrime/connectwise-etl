"""OpenAPI schema model generator."""
from pathlib import Path
import json
import logging

from unified_etl_core.generators.base import ModelGenerator

logger = logging.getLogger(__name__)


class OpenAPIGenerator(ModelGenerator):
    """Generate models from OpenAPI specifications."""
    
    def prepare_input(self, source: Path) -> Path:
        """OpenAPI files are already in the right format."""
        if not source.exists():
            raise FileNotFoundError(f"OpenAPI schema not found: {source}")
            
        # Validate it's valid JSON/YAML
        try:
            with open(source, "r") as f:
                data = json.load(f)
                
            # Check it's actually OpenAPI
            if "openapi" not in data and "swagger" not in data:
                raise ValueError(f"File doesn't appear to be OpenAPI: {source}")
                
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in OpenAPI schema: {e}")
            
        return source
    
    def _get_format_args(self) -> list[str]:
        """OpenAPI-specific arguments for datamodel-codegen."""
        args = [
            "--input-file-type", "openapi",
            "--openapi-scopes", "schemas",
            # "--collapse-root-models",  # This causes the directory issue
            "--disable-timestamp",
        ]
        
        # Add any OpenAPI-specific config
        openapi_config = self.config.get("openapi", {})
        
        if openapi_config.get("validation", True):
            args.append("--enable-version-header")
            
        if not openapi_config.get("snake_case_field", True):
            # This is actually controlled in base args
            pass
            
        return args
    
    def _post_process(self, output: Path) -> None:
        """Post-process OpenAPI generated models."""
        super()._post_process(output)
        
        # Skip if output is a directory (modular references)
        if output.is_dir():
            logger.debug("Skipping duplicate import cleanup for directory output")
            return
            
        # Fix any known issues with OpenAPI generation
        with open(output, "r") as f:
            content = f.read()
            
        # Remove duplicate imports if any
        lines = content.split("\n")
        seen_imports = set()
        filtered_lines = []
        
        for line in lines:
            if line.strip().startswith(("from ", "import ")):
                if line not in seen_imports:
                    seen_imports.add(line)
                    filtered_lines.append(line)
            else:
                filtered_lines.append(line)
                
        # Write back if we made changes
        new_content = "\n".join(filtered_lines)
        if new_content != content:
            with open(output, "w") as f:
                f.write(new_content)
            logger.info("Cleaned up duplicate imports")