# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The Unified ETL Framework integrates company data from ConnectWise PSA and Microsoft Business Central APIs, loading it directly into Microsoft Fabric OneLake using a proper medallion architecture. The framework handles API limitations with intelligent field selection and provides a generic, configuration-driven approach to ETL processing.

## Current Architecture

After comprehensive consolidation, the project now follows a clean package-based structure:

- **packages/unified-etl-core/**: Foundation framework optimized for ConnectWise patterns
- **packages/unified-etl-connectwise/**: ConnectWise PSA adapter with business logic
- **packages/unified-etl-businesscentral/**: Business Central adapter with dedicated pipeline
- **Eliminated duplication**: Removed fabric_api/ and BC_ETL-main/ (180+ files, ~40k lines)
- **Fail-fast philosophy**: ALL parameters required, no optional behaviors
- **CamelCase preservation**: Configured to maintain source system naming conventions
- **Separated pipelines**: BC and ConnectWise use different orchestration respecting their data patterns

## Medallion Architecture - Separation of Concerns

### Bronze Layer (Validated Raw Data)

**Purpose**: Extract and land validated raw data from source systems

- **Pydantic validation during extraction**: Validate API responses with Pydantic models
- Store validated raw data with original structure
- Preserve original field names and data types
- Add extraction metadata (timestamp, source, batch_id)
- **Key insight**: API data is small enough for row-by-row Pydantic validation

**Implementation Details**:

- API clients in integration packages (e.g., `unified_etl_connectwise/client.py`)
- Model validation via `model_class.model_validate(item)`
- Structured error records with raw data preserved

### Silver Layer (Schema Transformation & Standardization)

**Purpose**: Transform and standardize data structure using Spark (no row-by-row processing)

- **SKIP validation**: Data already validated in Bronze layer (see `silver.py` line 415)
- **Apply SparkDantic schema**: Models MUST inherit from SparkModel (runtime validated)
- **Flatten nested columns**: CamelCase preserved as `parentFieldChildField`
- **Convert data types**: Pure SparkDantic schema-driven type conversion (NO manual mapping)
- **SCD Type 1/2**: Slowly Changing Dimension with REQUIRED business keys
- **Fail-fast approach**: ALL parameters required, explicit ValueError on missing

**Implementation Details**:

- Assumes Fabric global spark: `sys.modules["__main__"].spark`
- No optional entity_config fields - all required
- Naming conflict resolution with suffix counters
- JSON columns converted to strings for nested data

**Files**:

- `packages/unified-etl-core/src/unified_etl_core/silver.py`

### Gold Layer (Business Enhancement)

**Purpose**: Add business intelligence constructs and dimensional modeling

- **Add surrogate keys**: Window-based dense_rank with required business keys
- **Generic fact creation**: ALL parameters required (no defaults)
- **Entity type tagging**: Add EntityType column for multi-entity facts
- **ETL metadata**: Universal tracking (\_etl_gold_processed_at, \_etl_batch_id)
- **Custom exceptions**: FactTableError, SurrogateKeyError for granular handling

**Implementation Details**:

- Inline implementations to avoid circular imports
- Business-specific logic in integration packages (e.g., `create_time_entry_fact`)
- Model naming convention: "timeentry" not "time_entry" (no underscores)

**Files**:

- `packages/unified-etl-core/src/unified_etl_core/gold.py`
- `packages/unified-etl-core/src/unified_etl_core/facts.py`
- `packages/unified-etl-core/src/unified_etl_core/dimensions.py`
- `packages/unified-etl-core/src/unified_etl_core/date_utils.py`

## Implementation Status

### ✅ Completed: Phase 1 - Consolidation & Framework

- Eliminated code duplication (180+ files, ~40k lines)
- Created generic fact table creator with fail-fast approach
- Updated pyproject.toml for camelCase preservation
- Fixed import paths and modularized packages
- Comprehensive documentation for both packages

### ✅ Completed: Phase 2 - Business Logic & Modularization

- Fixed Tímapottur detection (`r"Tímapottur\s*:?"` regex pattern)
- Modularized transforms.py (1000+ → 350 lines) via aikido refactor:
  - `date_utils.py` → core package
  - `agreement_utils.py` → ConnectWise package
- Created dimension generator in core package
- Fixed regenerate_models_v2.py to handle directory outputs
- Reorganized project structure (/docs, /scripts, /sql)

### 🔄 Current Phase: Data Refresh & Testing

1. **Refresh stale Fabric data** (2 weeks old)
2. **Test all fact creators** with new business logic
3. **Validate $18M cost recovery** via comprehensive time entry facts

### 📋 Next Phase: Advanced Features

- Business Central integration completion
- Schema evolution handling
- Multi-source integration (Jira, ServiceNow)
- Performance optimization for large datasets

## Key Business Logic & Patterns

### ConnectWise Agreement Types (Icelandic)

- **yÞjónusta**: Billable service agreements
- **Tímapottur**: Prepaid hours (special handling - excluded from invoices)
- **Innri verkefni**: Internal projects (non-billable)
- **Rekstrarþjónusta/Alrekstur**: Operations/maintenance
- **Hugbúnaðarþjónusta/Office 365**: Software service agreements

### Critical Implementation Patterns

1. **Fail-Fast Philosophy**: ALL function parameters required, no defaults

   ```python
   # WRONG: def create_fact(df, config=None)
   # RIGHT: def create_fact(df: DataFrame, config: dict[str, Any])
   ```

2. **Model Naming Convention**: No underscores in table names

   ```python
   models = {
       "timeentry": models.TimeEntry,    # NOT "time_entry"
       "expenseentry": models.ExpenseEntry  # NOT "expense_entry"
   }
   ```

3. **Tímapottur Detection**: Must match exact pattern with space/colon

   ```python
   r"Tímapottur\s*:?"  # Matches "Tímapottur :" in data
   ```

4. **Cost Recovery Strategy**: Create comprehensive fact_time_entry including ALL work
   - Captures internal projects (missing $18M)
   - Includes non-billable work
   - Tracks Tímapottur consumption

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

### Current Configuration (configs/generation.toml)

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

### Regenerating Models

```bash
# Using the v2 generator with auto-detection
python scripts/regenerate_models_v2.py \
    PSA_OpenAPI_schema.json \
    packages/unified-etl-connectwise/src/unified_etl_connectwise/models/models.py

# Or with explicit format
python scripts/regenerate_models_v2.py \
    BC_CDM_manifest.json \
    packages/unified-etl-businesscentral/src/unified_etl_businesscentral/models/ \
    --format cdm

# Direct datamodel-codegen (fallback)
datamodel-codegen \
    --input PSA_OpenAPI_schema.json \
    --output models.py \
    --base-class sparkdantic.SparkModel \
    --snake-case-field false
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

### unified-etl-core Package

- **Silver Processing** (`silver.py`): Schema transformation, NO validation (already done in Bronze)
- **Fact Creation** (`facts.py`): Generic fact table creator, ALL parameters required
- **Dimensions** (`dimensions.py`): Generic dimension generator from enum columns
- **Date Utils** (`date_utils.py`): Date dimension and spine generation
- **Gold Utils** (`gold.py`): Surrogate keys, ETL metadata, date dimensions
- **Generators** (`generators/`): Model generation framework (OpenAPI, CDM)

### unified-etl-connectwise Package

- **Client** (`client.py`): Unified API client with field selection & pagination
- **API Utils** (`api_utils.py`): Field generation from Pydantic models
- **Agreement Utils** (`agreement_utils.py`): Icelandic business logic
- **Transforms** (`transforms.py`): Fact creators (time_entry, invoice_line, agreement_period)
- **Config** (`config.py`): Silver layer configurations
- **Models** (`models/models.py`): Auto-generated from OpenAPI, inherit SparkModel

### unified-etl-businesscentral Package

- **Gold Transforms** (`transforms/gold.py`): BC-specific dimensional modeling
- Account hierarchy, dimension bridges, item attributes

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

## Running the Pipelines

### ConnectWise Pipeline (API-based)

```python
from unified_etl_core.main import run_etl_pipeline

# Run ConnectWise ETL using core framework
run_etl_pipeline(
    sources=["connectwise"],
    layers=["bronze", "silver", "gold"],
    lakehouse_root="/lakehouse/default/Tables/"
)
```

### Business Central Pipeline (BC2ADLS-based)

```python
from unified_etl_businesscentral import run_bc_pipeline

# Run Business Central ETL with dedicated pipeline
results = run_bc_pipeline(
    lakehouse_root="/lakehouse/default/Tables/",
    entities=["Customer", "Item", "GLEntry"],  # Optional
    layers=["bronze", "silver", "gold"]       # Optional
)
```

**Note**: BC and ConnectWise now use separate pipelines optimized for their data patterns. See [docs/separated-pipelines.md](docs/separated-pipelines.md) for details.

## Package Structure Links

- **[packages/unified-etl-core/](packages/unified-etl-core/CLAUDE.md)**: Generic ETL patterns
- **[packages/unified-etl-connectwise/](packages/unified-etl-connectwise/CLAUDE.md)**: ConnectWise adapter
- **[packages/unified-etl-businesscentral/](packages/unified-etl-businesscentral/CLAUDE.md)**: Business Central adapter