# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Unified ETL Framework integrates company data from ConnectWise PSA and Microsoft Business Central APIs, loading it directly into Microsoft Fabric OneLake using a proper medallion architecture. The framework handles API limitations with intelligent field selection and provides a generic, configuration-driven approach to ETL processing.

## Current Architecture

After comprehensive consolidation, the project now follows a clean unified structure:

- **unified_etl/**: Single consolidated ETL framework
- **Eliminated duplication**: Removed fabric_api/ and BC_ETL-main/ (180+ files, ~40k lines)
- **Generic processing**: Single fact creator replaces 9 entity-specific implementations
- **CamelCase preservation**: Configured to maintain source system naming conventions

## Medallion Architecture - Separation of Concerns

### Bronze Layer (Validated Raw Data)
**Purpose**: Extract and land validated raw data from source systems
- **Pydantic validation during extraction**: Validate API responses with Pydantic models
- Store validated raw data with original structure
- Preserve original field names and data types
- Add extraction metadata (timestamp, source, batch_id)
- **Key insight**: API data is small enough for row-by-row Pydantic validation

**Files**:
- `unified_etl/extract/` (API clients with Pydantic validation)
- `unified_etl/bronze/writer.py` (Write validated data to bronze tables)

### Silver Layer (Schema Transformation & Standardization)
**Purpose**: Transform and standardize data structure using Spark (no row-by-row processing)
- **Apply SparkDantic schema**: Use model's Spark schema for type casting
- **Flatten nested columns**: Expand complex objects into flat table structure
- **Convert data types**: Cast columns to proper types (string dates → timestamps)
- **Standardize column names**: Apply naming conventions if needed
- **Column mappings**: Rename/reorganize columns per configuration
- **No validation**: Data already validated in Bronze, use distributed Spark operations

**Files**:
- `unified_etl/silver.py` (Schema transformations, flattening, type conversions)
- `unified_etl/pipeline/bronze_silver.py`

### Gold Layer (Business Enhancement)
**Purpose**: Add business intelligence constructs and dimensional modeling
- **Add surrogate keys**: Generate business-meaningful unique identifiers
- **Create date dimension**: Standard calendar table with business periods
- **Build dimension bridge**: Connect facts to multiple dimensions
- **Add dimension tables**: Normalize entities not present in source (geography, categories)
- **Table-specific transforms**: Account hierarchy, organizational structures
- **Calculated columns**: Business metrics and derived values
- **No column removal**: Only addition and enrichment

**Files**:
- `unified_etl/gold.py` (Generic gold transformations)
- `unified_etl/facts.py` (Generic fact table creator)
- `unified_etl/transforms.py` (Business-specific logic)
- `unified_etl/pipeline/silver_gold.py`

## Implementation Status

### ✅ Completed: Consolidation & Framework
- Eliminated code duplication across multiple directories
- Created generic fact table creator
- Updated pyproject.toml for camelCase preservation
- Fixed import paths and dependencies
- Comprehensive documentation and README

### 🔄 Current Phase: Model Regeneration & Layer Refinement
1. **Regenerate Models**: Use updated datamodel-codegen configuration
2. **Layer Specification**: Implement clear medallion architecture boundaries
3. **Field Selection**: Dynamic API field selection from Pydantic models

### 📋 Next Phase: Advanced Features
- Schema evolution handling
- Multi-source integration (Jira, ServiceNow, etc.)
- Performance optimization for large datasets

## Development Setup

### Environment Setup

The project uses uv for dependency management:

```bash
# Install uv if needed
pip install uv

# Install dependencies
uv pip install -e .

# Install optional Azure dependencies
uv pip install -e ".[azure]"
```

### Python Requirements

- Python ≥3.11
- Key dependencies: requests, pandas, pyarrow, pydantic ≥2.11.4, pyspark, sparkdantic

## Model Generation

### Current Configuration (pyproject.toml)
```toml
[tool.datamodel-codegen]
input-file-type = "openapi"
output-file-type = "pydantic_v2"
base-class = "sparkdantic.SparkModel"
snake-case-field = false  # Preserve camelCase
aliased-fields = true
field-constraints = true
```

### Regenerating Models
```bash
# For ConnectWise PSA models
datamodel-codegen --input PSA_OpenAPI_schema.json --output unified_etl/models/models.py

# For Business Central models (when available)
datamodel-codegen --input BC_OpenAPI_schema.json --output unified_etl/models/bc_models.py
```

## Testing

Run tests with pytest:

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/unit/         # Unit tests
pytest tests/test_*.py     # Integration tests

# Run with coverage
pytest --cov=unified_etl
```

## Code Quality Tools

### Type Checking
```bash
pyright
```

### Linting
```bash
ruff check .
ruff format .
```

## Build and Deployment

### Building the Package
```bash
# Using pip build
python -m pip install build
python -m build --wheel
```

### Deploying to Microsoft Fabric

1. Create a Lakehouse in your workspace
2. Add secrets to your workspace Key Vault:
   - `CW_COMPANY`, `CW_PUBLIC_KEY`, `CW_PRIVATE_KEY`, `CW_CLIENTID`
   - `BC_TENANT_ID`, `BC_CLIENT_ID`, `BC_CLIENT_SECRET`
3. Upload the wheel file to your lakehouse
4. Create a Notebook and attach your Lakehouse
5. Install the wheel:
   ```python
   %pip install /lakehouse/Files/dist/unified_etl-0.1.0-py3-none-any.whl
   ```
6. Run the ETL pipeline

## Core Components

### Extract Layer
- **Generic Extractor** (`unified_etl/extract/generic.py`): Unified API client with retry logic
- **Field Selection** (`unified_etl/utils/api_utils.py`): Dynamic field selection from Pydantic models
- **Source Adapters**: ConnectWise PSA, Business Central, extensible for new sources

### Transform Layer
- **Bronze→Silver** (`unified_etl/pipeline/bronze_silver.py`): Validation and standardization
- **Silver→Gold** (`unified_etl/pipeline/silver_gold.py`): Business enhancement
- **Generic Processing** (`unified_etl/gold/generic_fact.py`): Configuration-driven fact creation

### Storage Layer
- **Delta Tables** (`unified_etl/storage/fabric_delta.py`): OneLake integration
- **Schema Management** (`unified_etl/utils/schema_utils.py`): Evolution and compatibility
- **Partitioning Strategy**: Optimized for query performance

## Data Flow

```
Source APIs → Bronze (Validated) → Silver (Transformed) → Gold (Enhanced)
     ↓              ↓                    ↓                   ↓
  Paginated    Pydantic Valid      Spark Schema        Dimensional
  API Calls    Raw Structure       Flattened           + Surrogate Keys
               Original Fields     Type Casted         + Business Logic
                                  Column Mapped        + Calculated Metrics
```

**Key Performance Insight**: 
- Bronze validation happens during API extraction (small, paginated datasets)
- Silver uses distributed Spark operations (no collect(), no row-by-row processing)
- This architecture scales to millions of rows in Silver/Gold layers

## Key Features

- **Medallion Architecture**: Proper separation of concerns across Bronze→Silver→Gold
- **Scalable Validation**: Pydantic validation in Bronze (small API data), Spark transformations in Silver (large datasets)
- **Generic Processing**: Configuration-driven, not entity-specific
- **CamelCase Preservation**: Maintains source system naming conventions
- **SparkDantic Integration**: Automatic Spark schema generation from Pydantic models
- **Schema Evolution**: Graceful handling of API changes
- **Fabric Optimization**: Native OneLake integration with Delta tables
- **Resilient Processing**: Retry logic, error handling, and recovery strategies
- **Performance at Scale**: No collect() or row-by-row processing after Bronze layer

## Running the Pipeline

```python
from unified_etl.main import run_etl_pipeline

# Run complete pipeline
run_etl_pipeline(
    sources=["connectwise", "business_central"],
    layers=["bronze", "silver", "gold"],
    lakehouse_root="/lakehouse/default/Tables/"
)

# Run specific layer
from unified_etl.pipeline.silver_gold import run_silver_to_gold_pipeline
run_silver_to_gold_pipeline(
    entity_configs=["agreement", "invoice", "time_entry"],
    lakehouse_root="/lakehouse/default/Tables/"
)
```

## Optimization Guidelines

1. **CamelCase Fields**: Preserve source system naming with `snake-case-field = false`
2. **SparkModel Inheritance**: All Pydantic models inherit from `sparkdantic.SparkModel`
3. **Generic Processing**: Use configuration-driven approach, avoid entity-specific code
4. **Layer Boundaries**: Strict separation - Bronze (raw), Silver (validated), Gold (enhanced)
5. **Schema Evolution**: Handle API changes gracefully with Pydantic validation
6. **Performance**: Leverage Spark SQL and Delta optimizations for large datasets
7. **Error Handling**: Comprehensive logging and recovery strategies
8. **Field Selection**: Dynamic API calls based on Pydantic model introspection
9. **Partitioning**: Optimize for common query patterns while maintaining schema compatibility
10. **Testing**: Unit tests for transformations, integration tests for full pipeline flows