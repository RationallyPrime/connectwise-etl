# Comprehensive Refactoring Plan for fabric_api

This document outlines a structured approach to refactor the `fabric_api` codebase, focusing on removing redundancies, eliminating non-Fabric environment code, and optimizing for Microsoft Fabric.

## Context

The ConnectWise PSA to Microsoft Fabric Integration project extracts company data from ConnectWise PSA API and loads it directly into Microsoft Fabric OneLake. The current implementation includes redundancies, overengineering, and code that makes unnecessary accommodations for non-Fabric environments.

In Fabric, authentication is handled via environment variables:
```python
# Set credentials
os.environ["CW_AUTH_USERNAME"] = "username" 
os.environ["CW_AUTH_PASSWORD"] = "password"
os.environ["CW_CLIENTID"] = "client-id"
```

## Phase 1: Eliminate Redundancies and Non-Fabric Code

### Task 1: Consolidate Transform Modules
1. Merge `transform/flatten.py` and `transform/struct_utils.py` into a single module
2. Keep only one implementation of `flatten_dataframe()` 
3. Create a unified API for all DataFrame structure transformations

### Task 2: Remove Non-Fabric Environment Accommodations
1. Delete `_pull_from_key_vault()` function from `client.py`
2. Strip out all Azure Key Vault integration code
3. Remove environment variable fallbacks
4. Simplify authentication to only use the environment variable approach

### Task 3: Consolidate Path Handling
1. Move all path handling to a single location in `path_utils.py`
2. Remove redundant implementations in `delta.py`
3. Standardize path generation for all entity types

## Phase 2: Leverage Fabric-Native Capabilities

### Task 1: Simplify Spark Session Management
1. Replace `get_spark_session()` with direct SparkSession.getActiveSession() calls
2. Remove unnecessary dependency injection of Spark sessions

### Task 2: Optimize Delta Table Operations
1. Refactor `write_to_delta()` to use simpler Fabric-specific patterns
2. Remove custom table registration logic that duplicates Fabric functionality
3. Simplify error handling for Delta operations

### Task 3: Streamline DataFrame Creation
1. Replace complex `dataframe_from_models()` with simpler Spark DataFrame creation
2. Use SparkModel interfaces consistently for schema conversion
3. Remove redundant validation steps

## Phase 3: Architectural Improvements

### Task 1: Simplify Extract Pipeline
1. Create a unified extractor configuration system
2. Refactor entity-specific extractors to use the generic approach
3. Simplify validation logic to happen in one place only

### Task 2: Improve Schema and Model Handling
1. Optimize the field selection utility for better performance
2. Add caching for commonly used field sets
3. Create entity-specific field sets for optimal API queries

### Task 3: Create Simpler API Client
1. Refactor the ConnectWiseClient class to remove conditional pathways
2. Simplify authentication to assume Fabric environment
3. Replace custom pagination with more efficient implementation

## Phase 4: Final Optimizations

### Task 1: Simplify Configuration
1. Centralize all configuration in a single module
2. Remove environment-dependent configuration options
3. Create Fabric-optimized defaults

### Task 2: Improve Error Handling
1. Standardize error handling approach
2. Use Fabric's native logging capabilities
3. Simplify error structure for better diagnosis

### Task 3: Performance Enhancements
1. Apply Fabric-specific optimizations for Delta operations
2. Optimize dataframe transformations for Fabric's Spark implementation
3. Reduce unnecessary data conversions and transformations

## Key Issues Addressed

### Redundant Code
- Multiple implementations of the same functionality across different modules
- Duplicate DataFrame transformation logic
- Overlapping path handling and table management

### Non-Fabric Environment Accommodations
- Azure Key Vault integration that will never be used in Fabric
- Complex environment variable checks and fallbacks
- Multiple authentication pathways when only one is needed

### Reinvention of Spark Methods
- Custom implementations duplicating PySpark functionality
- Complex DataFrame creation that could use native methods
- Custom Delta table operations where simpler alternatives exist

### Overengineered Components
- Excessive abstraction layers in extraction pipeline
- Overly complex validation structures
- Multiple flattening implementations with overlapping functionality

### Inefficient API Interactions
- Suboptimal field selection creating unnecessarily large API requests
- Limited caching of common field patterns
- Redundant field recalculation for each API call

## Benefits

This refactoring plan will result in:

1. **Simplified Codebase**: Reduced complexity with fewer redundant implementations
2. **Better Fabric Integration**: Code optimized specifically for Microsoft Fabric's environment
3. **Improved Maintainability**: Clearer architecture with well-defined responsibilities
4. **Enhanced Performance**: Optimized data handling and reduced overhead
5. **Focused Functionality**: Code that does exactly what's needed without accommodating hypothetical scenarios