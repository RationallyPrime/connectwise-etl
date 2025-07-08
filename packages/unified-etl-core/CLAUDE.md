# CLAUDE.md - unified-etl-core

This file provides guidance to Claude Code when working with the unified-etl-core package.

## Package Overview

The unified-etl-core package is the foundation framework providing generic patterns for the Unified ETL system. It implements the medallion architecture (Bronze→Silver→Gold) with fail-fast philosophy and proper separation of concerns.

## Core Principles

1. **Fail-Fast Philosophy**: ALL function parameters required, no defaults
   ```python
   # WRONG: def create_fact(df, config=None)
   # RIGHT: def create_fact(df: DataFrame, config: dict[str, Any])
   ```

2. **No Optional Behaviors**: Explicit ValueError on missing parameters
3. **CamelCase Preservation**: Configured to maintain source system naming conventions
4. **Performance at Scale**: No collect() or row-by-row processing after Bronze layer

## Medallion Architecture Implementation

### Silver Layer (`silver.py`)

**Purpose**: Transform and standardize data structure using Spark (no row-by-row processing)

- **SKIP validation**: Data already validated in Bronze layer (line 415)
- **Apply SparkDantic schema**: Models MUST inherit from SparkModel
- **Flatten nested columns**: CamelCase preserved as `parentFieldChildField`
- **Convert data types**: Comprehensive TYPE_MAPPING with 16+ type conversions
- **SCD Type 1/2**: Slowly Changing Dimension with REQUIRED business keys

**Key Implementation Details**:
- Assumes Fabric global spark: `sys.modules["__main__"].spark`
- No optional entity_config fields - all required
- Naming conflict resolution with suffix counters
- JSON columns converted to strings for nested data

### Gold Layer Components

#### Facts (`facts.py`)
- Generic fact table creator with ALL parameters required
- No defaults allowed - explicit configuration only
- Entity type tagging for multi-entity facts

#### Dimensions (`dimensions.py`)
- Generic dimension generator from enum columns
- Tuple configuration: `(source_table, column_name, dimension_name)`
- Auto-generates surrogate keys and descriptions

#### Gold Utils (`gold.py`)
- Surrogate key generation using window-based dense_rank
- ETL metadata tracking (`_etl_gold_processed_at`, `_etl_batch_id`)
- Custom exceptions: FactTableError, SurrogateKeyError
- Inline implementations to avoid circular imports

#### Date Utils (`date_utils.py`)
- Date dimension generation
- Date spine creation for fact tables
- Comprehensive date attributes (fiscal periods, holidays, etc.)

## Error Handling (`utils/`)

The package includes comprehensive error handling utilities that are currently **NOT BEING USED PROPERLY** throughout the codebase:

### Components

- **base.py**: Base error classes and enums
  - `ErrorLevel`: DEBUG, INFO, WARNING, ERROR, CRITICAL
  - `ErrorCode`: ETL-specific error codes organized by layer:
    - 1xxx: Configuration & Validation
    - 2xxx: Source System Errors
    - 3xxx: Bronze Layer
    - 4xxx: Silver Layer
    - 5xxx: Gold Layer
    - 6xxx: Infrastructure
  - `ApplicationError`: Base exception with structured details
  - Various `ErrorDetails` models for context-specific information

- **exceptions.py**: Three main exception classes
  - `ETLConfigError`: Configuration and setup errors (1xxx codes)
  - `ETLProcessingError`: Runtime processing errors (2xxx-5xxx codes)
  - `ETLInfrastructureError`: Platform/infrastructure errors (6xxx codes)

- **decorators.py**: Error handling decorator
  - `@with_etl_error_handling()`: Structured logging & optional re-raising
  - Handles both sync and async functions
  - Logs ApplicationError with full details

### Proper Usage Pattern

```python
from unified_etl_core.utils.decorators import with_etl_error_handling
from unified_etl_core.utils.exceptions import ETLConfigError, ETLProcessingError
from unified_etl_core.utils.base import ErrorCode

@with_etl_error_handling(operation="silver_transform")
def transform_to_silver(df: DataFrame, config: dict) -> DataFrame:
    # Configuration errors
    if not config.get("business_keys"):
        raise ETLConfigError(
            "Missing business_keys in silver config",
            code=ErrorCode.CONFIG_MISSING,
            details={"config_keys": list(config.keys())}
        )
    
    # Processing errors
    try:
        return df.withColumn("new_col", F.col("old_col").cast("int"))
    except Exception as e:
        raise ETLProcessingError(
            f"Type conversion failed: {str(e)}",
            code=ErrorCode.SILVER_TYPE_CONVERSION,
            details={"column": "old_col", "target_type": "int", "error": str(e)}
        )
```

### Current Issues

1. Most functions use generic `ValueError` instead of specific error types
2. No use of the `@with_etl_error_handling` decorator
3. Missing structured error details for debugging
4. Inconsistent error handling across packages

## Model Generation (`generators/`)

Framework for generating Pydantic models from various sources:
- OpenAPI schema support
- CDM (Common Data Model) manifest support
- Auto-detection of input format
- CamelCase preservation in generated models

## Key Patterns

### Model Naming Convention
```python
models = {
    "timeentry": models.TimeEntry,    # NOT "time_entry"
    "expenseentry": models.ExpenseEntry  # NOT "expense_entry"
}
```

### Required Parameters Pattern
All functions follow fail-fast with required parameters:
```python
def create_generic_fact_table(
    df: DataFrame,
    fact_name: str,
    business_keys: List[str],
    dimensions: List[str],
    measures: List[str],
    date_column: str,
    spark: SparkSession,
    batch_id: str,
    add_entity_type: bool = False,
    entity_type_value: Optional[str] = None
) -> DataFrame:
    if add_entity_type and not entity_type_value:
        raise ValueError("entity_type_value required when add_entity_type=True")
```

## Performance Considerations

- Bronze: Row-by-row Pydantic validation (small API datasets)
- Silver: Distributed Spark operations only
- Gold: Window functions for surrogate keys, no collect()

## Testing Patterns

- Unit tests for each component
- Integration tests for layer transitions
- Mock Spark sessions for local testing
- Validation of fail-fast behavior

https://github.com/Bertverbeek4PS/bc2adls/tree/main/businessCentral/app/src