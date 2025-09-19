# CLAUDE.md - Model Generators

This directory contains the model generation framework for creating Pydantic models from various sources.

## Overview

The generators module provides a unified interface for generating SparkDantic-compatible models from:
- **OpenAPI schemas** (ConnectWise PSA)
- **CDM manifests** (Business Central)
- Additional formats can be added via the registry

## Architecture

- **base.py**: Abstract base generator class defining the interface
- **openapi.py**: OpenAPI schema parser and generator
- **cdm.py**: Common Data Model manifest parser and generator
- **registry.py**: Dynamic generator registration system
- **templates/**: Custom templates for specific systems

## Configuration

Model generation is configured via `configs/generation.toml`:

```toml
[datamodel-codegen]
input-file-type = "openapi"
output-file-type = "pydantic_v2"
base-class = "sparkdantic.SparkModel"
snake-case-field = false  # Preserve camelCase - CRITICAL
aliased-fields = true
field-constraints = true
use-schema-description = true
enable-version-header = true
disable-timestamp = true
openapi-scopes = "schemas"

[validation]
strict-nullable = true
use-standard-collections = true
```

## Usage

### Using regenerate_models_v2.py (Recommended)

The script auto-detects input format and handles all configuration:

```bash
# OpenAPI Schema (ConnectWise)
python scripts/regenerate_models_v2.py \
    PSA_OpenAPI_schema.json \
    packages/unified-etl-connectwise/src/unified_etl_connectwise/models/models.py

# CDM Manifest (Business Central) 
python scripts/regenerate_models_v2.py \
    BC_CDM_manifest.json \
    packages/unified-etl-businesscentral/src/unified_etl_businesscentral/models/ \
    --format cdm

# Custom template
python scripts/regenerate_models_v2.py \
    schema.json \
    output/models.py \
    --template-dir templates/custom/
```

### Direct datamodel-codegen (Fallback)

For simple cases or debugging:

```bash
datamodel-codegen \
    --input PSA_OpenAPI_schema.json \
    --output models.py \
    --base-class sparkdantic.SparkModel \
    --snake-case-field false
```

## Key Features

### CamelCase Preservation

**CRITICAL**: The `snake-case-field = false` setting preserves original API field names:
- API field: `agreementId` → Model field: `agreementId` (not `agreement_id`)
- This maintains compatibility with source systems

### SparkDantic Integration

All generated models inherit from `sparkdantic.SparkModel`:
- Automatic Spark schema generation
- Built-in validation
- Seamless DataFrame conversion

### Auto-Detection

The generator automatically detects input format:
- `.json` files containing `"openapi"` → OpenAPI generator
- `.json` files containing `"cdm:traits"` → CDM generator
- Override with `--format` flag if needed

## Adding New Generators

1. Create a new generator class inheriting from `BaseGenerator`:
   ```python
   from .base import BaseGenerator
   
   class MyGenerator(BaseGenerator):
       def can_handle(self, content: dict) -> bool:
           return "my_format" in content
       
       def generate(self, input_path: str, output_path: str, **kwargs) -> None:
           # Implementation
   ```

2. Register in `registry.py`:
   ```python
   registry.register("myformat", MyGenerator)
   ```

3. Use via regenerate_models_v2.py:
   ```bash
   python scripts/regenerate_models_v2.py input.json output.py --format myformat
   ```

## Common Issues

### Wrong Field Names
- **Issue**: Fields become snake_case instead of camelCase
- **Fix**: Ensure `snake-case-field = false` in generation.toml

### Missing Models
- **Issue**: Not all schemas are generated
- **Fix**: Check `openapi-scopes = "schemas"` setting

### Import Errors
- **Issue**: Generated models have incorrect imports
- **Fix**: Use `--base-class sparkdantic.SparkModel` explicitly

## Templates

Custom templates in `templates/` directory:
- `psa/`: ConnectWise-specific customizations
- Add new directories for other systems

Templates override default generation behavior for specific models or fields.