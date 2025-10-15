# ETL Core Framework - Implementation Summary

## What We Built

A protocol-based ETL framework with clean dependency inversion following hexagonal architecture principles. The framework is split into domain protocols (interfaces), infrastructure (utilities), and will support plugin-based integrations.

## Directory Structure

```
src/etl_core/
├── __init__.py                          # Main package exports
├── config.py                             # RuntimeContext and LakehouseConfig (Pydantic v2)
│
├── domain/
│   └── protocols/                        # Pure protocols (no implementation)
│       ├── __init__.py
│       ├── types.py                      # Type aliases and result types
│       ├── client.py                     # DataFetcherProtocol, DataValidatorProtocol
│       ├── registry.py                   # ModelRegistryProtocol
│       ├── processor.py                  # Bronze/Silver/Gold processor protocols
│       └── plugin.py                     # IntegrationPluginProtocol, Processors container
│
├── infrastructure/                       # Concrete implementations (to be added)
│   ├── validators/                       # Generic DataValidator implementation
│   ├── processors/                       # Generic processor implementations
│   └── orchestrator.py                   # Pipeline orchestrator
│
└── utils/
    ├── errors/                           # Error handling (from found-family)
    │   ├── __init__.py
    │   ├── base.py                       # ApplicationError, ErrorCode, ErrorLevel
    │   └── types.py                      # Domain-specific errors
    │
    ├── logging/                          # Logging system (from found-family)
    │   ├── __init__.py
    │   ├── context.py                    # ContextVar-based logging
    │   └── setup.py                      # Logfire + Structlog setup
    │
    └── incremental.py                    # IncrementalHandler (to be updated)
```

## Key Design Decisions

### 1. TYPE_CHECKING for Spark Imports

Spark types are imported under `TYPE_CHECKING` to allow type checking without runtime Spark dependency:

```python
if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any
```

### 2. Pydantic v2 with ConfigDict

All config models use Pydantic v2:

```python
class RuntimeContext(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    spark: SparkSession
    # ...
```

### 3. Split Fetcher and Validator

Data extraction is split into two concerns:

- **DataFetcherProtocol**: Integration-specific (pagination, auth, API calls)
- **DataValidatorProtocol**: Generic (Pydantic validation, DataFrame creation)

This allows integrations to focus on fetching while reusing validation logic.

### 4. Structured Result Types

All processors return structured dataclasses instead of loose dictionaries:

```python
@dataclass(frozen=True)
class BronzeResult:
    entities_processed: dict[str, int]
    tables_written: list[str]
    validation_errors: int
    duration_seconds: float
    batch_id: str
    warnings: list[str] = field(default_factory=list)
```

### 5. Named Processor Container

Processors are returned in a named dataclass instead of tuple:

```python
@dataclass(frozen=True)
class Processors:
    bronze: BronzeProcessorProtocol
    silver: SilverProcessorProtocol
    gold: GoldProcessorProtocol
```

### 6. Registry Exposes Operational Metadata

Registry provides merge keys and timestamp columns to avoid hard-coding conventions:

```python
class ModelRegistryProtocol(Protocol):
    def get_merge_keys(self, entity_name: str) -> list[str]: ...
    def get_timestamp_column(self, entity_name: str, layer: TLayer) -> str: ...
```

### 7. Flexible Table Naming

LakehouseConfig supports both Unity Catalog (3-part) and legacy (2-part) naming:

```python
# Unity Catalog
catalog="main" → "main.bronze.agreement"

# Legacy
catalog=None → "bronze.agreement"
```

### 8. Error Handling from Found-Family

Ported sophisticated error handling with:
- **Error code ranges**: 1xxx (config), 2xxx (API), 3xxx (bronze), 4xxx (silver), 5xxx (gold), 6xxx (infra)
- **Structured error details**: Pydantic models for type-safe error context
- **Flexible initialization**: Accepts both ErrorDetails objects or plain dicts

### 9. Context-Aware Logging

ContextVar-based logging for request/batch scoping:

```python
update_log_context("batch_id", "20250315_120000")
update_log_context("entity", "agreement")

info("Processing records")  # Automatically includes batch_id and entity
```

### 10. Logfire + Structlog Integration

Production-ready logging stack:
- Logfire for observability
- Structlog for structured logging
- Automatic callsite information
- ISO timestamps
- Colored console output for development

## What's Still Needed

### 1. IncrementalHandler Improvements

Update with review suggestions:
- Use DataFrame API instead of SQL string interpolation
- Safe identifier quoting
- Return `MergeResult` with insert/update/delete counts
- Support for Delta Lake metrics

### 2. Generic DataValidator Implementation

Create concrete implementation in `infrastructure/validators/`:

```python
class PydanticDataValidator:
    def validate_and_create_dataframe(...) -> ValidationResult:
        # Validate records using Pydantic
        # Collect validation errors
        # Create DataFrame with SparkDantic schema
        # Return ValidationResult with counts
```

### 3. Generic Processor Implementations

Optional generic processors in `infrastructure/processors/`:
- `GenericBronzeProcessor` - for API-based integrations
- `GenericSilverProcessor` - for standard transformations
- YAML-driven gold processor

### 4. Orchestrator

Simple orchestrator to show how pieces wire together:

```python
def run_pipeline(plugin: IntegrationPluginProtocol, context: RuntimeContext):
    registry = plugin.initialize_registry()
    fetcher = plugin.initialize_fetcher()
    validator = core_validator  # Provided by core
    incremental = IncrementalHandler(context.spark)

    processors = plugin.initialize_processors(fetcher, validator, registry, incremental)

    bronze_result = processors.bronze.process(context)
    silver_result = processors.silver.process(context)
    gold_result = processors.gold.process(context)

    return bronze_result, silver_result, gold_result
```

### 5. Import Linter Configuration

Add to pyproject.toml:

```toml
[tool.importlinter]
[[tool.importlinter.contracts]]
name = "Domain layer independence"
type = "forbidden"
source_modules = ["etl_core.domain"]
forbidden_modules = ["etl_core.infrastructure", "pyspark", "requests"]
```

## Error Code Taxonomy

```
1xxx - Configuration & Validation
├── 1001 - CONFIG_MISSING
├── 1002 - CONFIG_INVALID
├── 1003 - VALIDATION_FAILED
└── 1004 - SCHEMA_MISMATCH

2xxx - API & Source Systems
├── 2001 - API_AUTH_FAILED
├── 2002 - API_RATE_LIMITED
├── 2003 - API_RESPONSE_INVALID
├── 2004 - API_FIELD_MISSING
└── 2005 - FETCH_ERROR

3xxx - Bronze Layer
├── 3001 - BRONZE_EXTRACT_FAILED
├── 3002 - BRONZE_VALIDATION_FAILED
└── 3003 - BRONZE_WRITE_FAILED

4xxx - Silver Layer
├── 4001 - SILVER_TRANSFORM_FAILED
├── 4002 - SILVER_TYPE_CONVERSION
├── 4003 - SILVER_FLATTEN_FAILED
├── 4004 - SILVER_SCD_FAILED
└── 4005 - MERGE_ERROR

5xxx - Gold Layer
├── 5001 - GOLD_DIMENSION_FAILED
├── 5002 - GOLD_FACT_FAILED
├── 5003 - GOLD_SURROGATE_KEY
├── 5004 - GOLD_AGGREGATION
└── 5005 - PROCESSING_ERROR

6xxx - Infrastructure
├── 6001 - SPARK_SESSION_FAILED
├── 6002 - STORAGE_ACCESS_FAILED
└── 6003 - MEMORY_EXCEEDED
```

## Usage Example

```python
from etl_core import (
    RuntimeContext,
    LakehouseConfig,
    IntegrationPluginProtocol,
)
from etl_core.utils.logging import setup_logging, update_log_context

# Setup logging
setup_logging(log_level="INFO")

# Create runtime context
lakehouse = LakehouseConfig(catalog="main", bronze_schema="bronze")
context = RuntimeContext(
    spark=spark,
    lakehouse=lakehouse,
    mode="incremental",
    extra_args={"lookback_days": 7},
)

# Add logging context
update_log_context("integration", "connectwise")
update_log_context("batch_id", context.batch_id)

# Load plugin (to be implemented)
plugin = load_plugin("connectwise")

# Run pipeline (orchestrator to be implemented)
results = run_pipeline(plugin, context)
```

## Next Steps

1. ✅ Domain protocols defined
2. ✅ Error handling ported from found-family
3. ✅ Logging system ported from found-family
4. ⏳ Update IncrementalHandler with review improvements
5. ⏳ Implement generic DataValidator
6. ⏳ Implement simple orchestrator
7. ⏳ Create example integration plugin (ConnectWise)
8. ⏳ Add import linter rules
9. ⏳ Write tests for protocols and utilities

## Key Advantages

1. **Clean separation**: Domain protocols have zero infrastructure dependencies
2. **Testable**: Mock any protocol for unit testing
3. **Flexible**: Integrations can use generic implementations or write custom ones
4. **Production-ready observability**: Logfire + Structlog with error taxonomy
5. **Type-safe**: Full type hints with Pydantic v2
6. **Context-aware**: Logging context automatically propagates through pipeline
7. **Each integration isolated**: Own lakehouse, no name collisions
