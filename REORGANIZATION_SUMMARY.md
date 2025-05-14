# ConnectWise PSA to Microsoft Fabric ETL Reorganization

This document summarizes the reorganization of the ConnectWise PSA to Microsoft Fabric ETL pipeline to simplify the code base, eliminate redundancy, and improve maintainability.

## Directory Structure Changes

```
fabric_api/
├── core/                      # Core infrastructure utilities
│   ├── api_utils.py           # API field generation and condition building
│   ├── config.py              # Centralized configuration
│   ├── log_utils.py           # Enhanced logging utilities
│   ├── path_utils.py          # Path handling for Fabric
│   ├── spark_utils.py         # Spark session & table utilities
│   └── utils.py               # General utility functions
│
├── transform/                 # Transformation utilities
│   ├── flatten.py             # Struct flattening utilities
│   └── struct_utils.py        # Additional struct handling functions
│
├── extract/                   # Data extraction
│   ├── generic.py             # Generic extractor for any entity
│   └── [entity_modules]
│
├── storage/                   # Delta table operations
│   └── delta.py               # Unified Delta writer
│
├── connectwise_models/        # Pydantic models
│
├── client.py                  # ConnectWise API client
└── pipeline_new.py            # Streamlined pipeline orchestration
```

## Key Improvements

### 1. Consolidated Core Utilities

- **path_utils.py**: Simplified path handling for Fabric
- **spark_utils.py**: Centralized Spark session management and table utilities
- **config.py**: Unified configuration for all entities
- **api_utils.py**: Moved API utilities to core
- **log_utils.py**: Enhanced structured logging
- **utils.py**: General utilities for working with data

### 2. Generic Entity Extractor

- Created a single, reusable entity extractor in `extract/generic.py`
- Dynamically imports the correct model class for any entity
- Handles field selection and API conditions consistently
- Supports validation as part of extraction

### 3. Unified Delta Handling

- Simplified Delta table operations in `storage/delta.py`
- Consistent interface for all entity types
- Handles DataFrame preparation, table registration, and metadata

### 4. Struct Flattening

- Added robust flattening utilities for nested structures
- Supports recursive flattening of nested structs
- Handles arrays of structs via JSON conversion
- Provides verification of flattening success

### 5. Streamlined Pipeline

- Created a new simplified pipeline in `pipeline_new.py`
- Clear, consistent interface for all ETL operations
- Supports incremental and full loads
- Handles validation and error tracking

## Removed Redundancy

The following redundant implementations were consolidated:

1. **Path Handling**: 
   - Consolidated `ensure_fabric_path()`, `build_abfss_path()`, and `build_delta_path()`
   - Single, simplified `get_table_path()` function

2. **Delta Writing**:
   - Consolidated `write_to_delta()` from bronze_loader.py and delta_utils.py
   - Eliminated duplicate error handling and table registration logic

3. **Spark Session Management**:
   - Consolidated into core/spark_utils.py
   - Simple `get_spark_session()` function to get the active session

4. **Entity Configuration**:
   - Centralized in core/config.py
   - Consistent structure for all entities

## Benefits

1. **Simplified API**: Clear, intuitive interfaces for all operations
2. **Reduced Code Size**: Eliminated redundant implementations
3. **Improved Maintainability**: Logical organization with clear responsibilities
4. **Better Error Handling**: Consistent error handling and validation
5. **Enhanced Metadata**: Standardized metadata for tracking and lineage
6. **Optimized for Fabric**: Designed specifically for Microsoft Fabric environment

## Migration Path

To migrate to the new structure:

1. Update import statements to use the new module locations
2. Replace direct calls to old functions with their new counterparts
3. Use the new pipeline_new.py instead of the older orchestration approaches