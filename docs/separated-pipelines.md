# Separated ETL Pipelines

## Overview

The Unified ETL Framework now implements **separate pipelines** for ConnectWise and Business Central, respecting their fundamentally different data ingestion patterns:

- **ConnectWise**: API-based extraction → JSON flattening → Transformations
- **Business Central**: Pre-exported data (BC2ADLS) → Column operations → Business logic

## Why Separate?

### ConnectWise Pattern
```
API → Extract → Validate → Flatten JSON → Transform → Facts/Dims
```

### Business Central Pattern  
```
BC2ADLS Export → Read Tables → Validate → Column Ops → Facts/Dims
```

Forcing BC through the ConnectWise-centric pipeline caused:
- Unnecessary JSON flattening for already-flat BC tables
- Complex EntityConfig translations 
- Core framework pollution with BC-specific branches
- Impedance mismatch between API and file-based sources

## Implementation

### ConnectWise Pipeline

Uses the core framework's generic pipeline:

```python
from unified_etl_core.main import run_etl_pipeline

# Run ConnectWise ETL
run_etl_pipeline(
    sources=["connectwise"],
    layers=["bronze", "silver", "gold"],
    lakehouse_root="/lakehouse/default/Tables/"
)
```

### Business Central Pipeline

Uses BC-specific pipeline respecting its patterns:

```python
from unified_etl_businesscentral import run_bc_pipeline

# Run Business Central ETL
results = run_bc_pipeline(
    lakehouse_root="/lakehouse/default/Tables/",
    entities=["Customer", "Item", "GLEntry"],  # Optional - defaults to all
    layers=["bronze", "silver", "gold"]       # Optional - defaults to all
)
```

## Key Differences

### Bronze Layer

**ConnectWise**: 
- Pulls data via REST API
- Paginates through results
- Validates with Pydantic during extraction

**Business Central**:
- Reads pre-exported BC2ADLS tables
- Validates existing data with Pydantic
- Handles company-specific table naming (e.g., `customer18`)

### Silver Layer

**ConnectWise**:
- Flattens nested JSON structures
- Preserves camelCase field names
- Handles complex type conversions

**Business Central**:
- Simple column operations (drop, rename)
- Calculated fields (e.g., `ExtendedAmount`)
- Company-partitioned surrogate keys

### Gold Layer

**ConnectWise**:
- Time entry facts with agreement hierarchy
- Invoice line item reconciliation
- Icelandic business rules (Tímapottur, etc.)

**Business Central**:
- Dimension bridges (DimensionSetEntry/Value)
- Company-aware joins
- Account hierarchy building

## Benefits

1. **Cleaner Architecture**: Each pipeline optimized for its source
2. **Maintainability**: BC changes don't affect ConnectWise and vice versa
3. **Performance**: No unnecessary transformations
4. **Extensibility**: Easy to add new sources with different patterns

## Migration Guide

If you were using the unified pipeline for BC:

**Before**:
```python
run_etl_pipeline(
    sources=["businesscentral"],
    # ...
)
```

**After**:
```python
from unified_etl_businesscentral import run_bc_pipeline

run_bc_pipeline(
    lakehouse_root="/lakehouse/default/Tables/"
)
```

The BC pipeline handles all the same functionality but with BC-appropriate patterns.