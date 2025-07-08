# CLAUDE.md

Guidance for Claude Code when working with the Unified ETL Framework.

## Overview

ETL framework integrating ConnectWise PSA and Business Central APIs into Microsoft Fabric OneLake using medallion architecture.

## Package Structure

- **[packages/unified-etl-core/](packages/unified-etl-core/CLAUDE.md)**: Generic ETL patterns
- **[packages/unified-etl-connectwise/](packages/unified-etl-connectwise/CLAUDE.md)**: ConnectWise adapter
- **[packages/unified-etl-businesscentral/](packages/unified-etl-businesscentral/CLAUDE.md)**: Business Central adapter

## Architecture

```
Source APIs → Bronze (Validated) → Silver (Transformed) → Gold (Enhanced)
     ↓              ↓                    ↓                   ↓
  Paginated    Pydantic Valid      Spark Schema        Dimensional
  API Calls    Row-by-Row          Distributed         + Surrogate Keys
               Validation           Processing          + Business Logic
```

## Core Principles

1. **Fail-Fast**: ALL parameters required, no defaults
2. **CamelCase**: Preserve source system naming
3. **Model Names**: No underscores ("timeentry" not "time_entry")
4. **Performance**: No collect() after Bronze layer

## Setup

```bash
pip install uv
uv pip install -e .
```

Python ≥3.11

## Model Generation

```bash
# ConnectWise (OpenAPI)
python scripts/regenerate_models_v2.py PSA_OpenAPI_schema.json packages/unified-etl-connectwise/src/unified_etl_connectwise/models/models.py

# Business Central (CDM)
python scripts/regenerate_models_v2.py BC_CDM_manifest.json packages/unified-etl-businesscentral/src/unified_etl_businesscentral/models/ --format cdm
```

## Testing

```bash
pytest
pyright
ruff check .
```

