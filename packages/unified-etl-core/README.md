# Unified ETL Core

The foundational ETL framework implementing a medallion architecture (Bronze → Silver → Gold) with generic, reusable components that work across different data sources.

## Overview

`unified-etl-core` provides the base ETL functionality for the unified data platform, focusing on:

- **Generic patterns** that work with any data source
- **Configuration-driven processing** to avoid entity-specific code  
- **SparkDantic integration** for automatic Spark schema generation
- **Medallion architecture** with clear separation of concerns

## Installation

```bash
pip install unified-etl-core

# With Azure dependencies (Fabric, Delta Lake, Key Vault)
pip install "unified-etl-core[azure]"
```

## Core Principles

1. **Generic where possible, specialized where necessary** - Core utilities work for any data source
2. **Fail-fast approach** - No optional behaviors, required parameters, explicit errors
3. **Configuration-driven** - Avoid hardcoding, use configs for flexibility
4. **Proper layering** - Clear separation between Bronze/Silver/Gold responsibilities
5. **SparkDantic required** - All models must inherit from `sparkdantic.SparkModel`

## Architecture

### Medallion Layers

```
Bronze Layer          Silver Layer              Gold Layer
┌─────────────┐      ┌──────────────┐       ┌─────────────┐
│ Raw Valid   │      │ Transformed  │       │ Business    │
│ API Data    │  →   │ Standardized │   →   │ Intelligence│
│ + Pydantic  │      │ + Spark SQL  │       │ + Dims/Facts│
└─────────────┘      └──────────────┘       └─────────────┘
```

- **Bronze**: Validated raw data with Pydantic models
- **Silver**: Schema transformations, type conversions, flattening
- **Gold**: Dimensional modeling, fact tables, business metrics

## Key Components

### 1. Dimensional Modeling (`dimensions.py`)

Create dimension tables from enum-like columns:

```python
from unified_etl_core.dimensions import create_dimension_from_column

# Create a status dimension
status_dim = create_dimension_from_column(
    spark=spark,
    source_table="silver.time_entries", 
    column_name="status",
    dimension_name="dim_status",
    include_counts=True
)

# Batch create multiple dimensions
from unified_etl_core.dimensions import create_all_dimensions

dimensions = create_all_dimensions(
    spark=spark,
    dimension_configs=[
        {"source_table": "silver.invoices", "column": "type"},
        {"source_table": "silver.agreements", "column": "billingCycle"}
    ]
)
```

### 2. Date/Time Utilities (`date_utils.py`)

Generate comprehensive date dimensions:

```python
from unified_etl_core.date_utils import generate_date_dimension

# Create date dimension with fiscal year support
date_dim = generate_date_dimension(
    spark=spark,
    start_date="2020-01-01",
    end_date="2030-12-31", 
    fiscal_year_start_month=7  # July fiscal year
)

# Create monthly period spine
from unified_etl_core.date_utils import create_date_spine

monthly_spine = create_date_spine(
    spark=spark,
    start_date="2023-01-01",
    frequency="month"
)
```

### 3. Fact Table Creation (`facts.py`)

Generic fact table creator with configuration-driven approach:

```python
from unified_etl_core.facts import create_generic_fact_table

fact_config = {
    "name": "fact_time_entry",
    "grain": ["time_entry_id", "date"],
    "dimensions": {
        "date_key": "add_date_key(dateEntered)",
        "agreement_key": "coalesce(agreement.id, -1)",
        "member_key": "coalesce(member.id, -1)"
    },
    "measures": {
        "hours_actual": "actualHours",
        "hours_billed": "coalesce(invoiceHours, 0.0)",
        "revenue": "hourlyRate * actualHours"
    },
    "attributes": {
        "is_billable": "billableOption == 'Billable'",
        "work_type": "workType.name"
    }
}

fact_df = create_generic_fact_table(
    spark=spark,
    source_df=time_entries_df,
    config=fact_config
)
```

### 4. Silver Layer Processing (`silver.py`)

Apply transformations with Pydantic validation:

```python
from unified_etl_core.silver import apply_silver_transformations

silver_config = {
    "entity_name": "time_entry",
    "pydantic_model": TimeEntryModel,
    "column_mappings": {
        "company.id": "company_id",
        "company.name": "company_name"
    },
    "type_conversions": {
        "dateEntered": "timestamp",
        "actualHours": "decimal(10,2)"
    }
}

silver_df = apply_silver_transformations(
    spark=spark,
    bronze_df=bronze_df,
    config=silver_config
)
```

### 5. Model Generation Framework

Generate Pydantic models from various schema formats:

```bash
# From OpenAPI specification
python -m unified_etl_core.generators generate \
    PSA_OpenAPI_schema.json \
    models.py \
    --format openapi

# Auto-detect format
python -m unified_etl_core.generators generate schema.json models.py

# With custom config
python -m unified_etl_core.generators generate \
    schema.json \
    models.py \
    --config generation.toml
```

## Integration with Source Packages

The core framework is extended by source-specific packages:

```python
# Automatic integration detection
from unified_etl_core.integrations import list_available_integrations

available = list_available_integrations()
# Returns: ["connectwise", "businesscentral", "jira"]

# Run complete pipeline
from unified_etl_core.main import run_etl_pipeline

run_etl_pipeline(
    sources=["connectwise", "businesscentral"],
    layers=["bronze", "silver", "gold"],
    lakehouse_root="/lakehouse/default/Tables/"
)
```

## Pipeline Orchestration

The framework provides flexible orchestration:

```python
# Process specific integration
from unified_etl_core.main import process_integration

process_integration(
    integration_name="connectwise",
    entities=["agreement", "time_entry"],
    layers=["silver", "gold"],
    lakehouse_root="/lakehouse/default/Tables/"
)

# Custom table naming
table_map = {
    "time_entry": "time_entries",
    "agreement": "service_agreements"
}

run_etl_pipeline(
    sources=["connectwise"],
    table_name_map=table_map
)
```

## Configuration

### Generation Config (configs/generation.toml)

```toml
[datamodel-codegen]
input-file-type = "openapi"
output-file-type = "pydantic_v2"
base-class = "sparkdantic.SparkModel"
snake-case-field = false  # Preserve camelCase
field-constraints = true

[validation]
strict-nullable = true
use-standard-collections = true
```

### Environment Variables

For Microsoft Fabric deployment:

```bash
# Fabric workspace settings
FABRIC_WORKSPACE_ID=xxx
FABRIC_LAKEHOUSE_ID=xxx

# Azure authentication (when using [azure] extras)
AZURE_TENANT_ID=xxx
AZURE_CLIENT_ID=xxx
AZURE_CLIENT_SECRET=xxx
```

## Testing

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=unified_etl_core

# Type checking
pyright

# Linting
ruff check .
```

## Performance Considerations

1. **Bronze Layer**: Row-by-row Pydantic validation is acceptable here since API data is paginated and small
2. **Silver Layer**: Use distributed Spark operations, avoid `collect()` or row-by-row processing
3. **Gold Layer**: Leverage Spark SQL for aggregations and joins
4. **Partitioning**: Use date-based partitioning for time-series data

## Error Handling

The framework follows a fail-fast approach:

```python
# All parameters are required
create_generic_fact_table(
    spark=spark,          # Required
    source_df=df,         # Required
    config=config         # Required
)  # No defaults, explicit is better

# Validation errors are explicit
try:
    silver_df = apply_silver_transformations(spark, bronze_df, config)
except ValidationError as e:
    logger.error(f"Validation failed: {e}")
    # Error includes record details and validation issues
```

## Extending the Framework

Create a new integration package:

1. Create package structure:
```
unified-etl-mysource/
├── pyproject.toml
└── src/
    └── unified_etl_mysource/
        ├── __init__.py
        ├── client.py      # API client/extractor
        ├── models.py      # Pydantic models
        └── transforms.py  # Optional special transforms
```

2. Implement required exports in `__init__.py`:
```python
from .client import MySourceClient
from .models import models as model_mapping

# Required: extractor instance
extractor = MySourceClient()

# Required: model mapping
models = model_mapping

# Optional: specialized transforms
from .transforms import special_transform

__all__ = ["extractor", "models", "special_transform"]
```

3. Register in pyproject.toml:
```toml
[project.entry-points."unified_etl.integrations"]
mysource = "unified_etl_mysource"
```

## License

MIT License - see LICENSE file for details.