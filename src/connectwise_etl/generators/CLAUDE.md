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

  uv run datamodel-codegen \
      --input PSA_OpenAPI_schema.json \
      --output src/connectwise_etl/models/ \
      --input-file-type openapi \
      --base-class sparkdantic.SparkModel \
      --parent-scoped-naming \
      --reuse-model \
      --keep-model-order \
      --use-annotated \
      --use-subclass-enum \
      --use-standard-collections \
      --force-optional \
      --use-union-operator \
      --openapi-scopes schemas \
      --disable-timestamp

  The key flags that made the difference:
  - --parent-scoped-naming - Creates the nested structure with proper relationships
  - --reuse-model - Prevents duplicate model definitions
  - --strip-default-none - Cleaner output without excessive None defaults