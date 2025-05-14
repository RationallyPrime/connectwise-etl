# ConnectWise to Microsoft Fabric Integration - Improvement Tickets

## Core Infrastructure

### CWFAB-1: Consolidate OneLake Path Handling Logic
**Priority**: High  
**Complexity**: Medium

**Description**:  
Multiple functions handle OneLake path construction (`ensure_fabric_path()`, `build_abfss_path()`, etc.) with inconsistent implementations, leading to errors and path format inconsistencies.

**Acceptance Criteria**:
- Create a unified `OneLakePath` class in a new `fabric_utils.py` module
- Implement methods for path conversion, validation, and normalization
- Remove duplicate path handling logic across modules
- Add proper error handling for invalid paths
- Support ABFS URL format required by Fabric

**Implementation Details**:
```python
class OneLakePath:
    @staticmethod
    def from_relative(path: str) -> str:
        """Convert /lakehouse/... path to proper ABFS URL"""
        
    @staticmethod 
    def to_table_name(path: str) -> str:
        """Extract database.table_name from path"""
        
    @staticmethod
    def validate(path: str) -> bool:
        """Verify path is a valid OneLake location"""
```

**Unit Test Considerations**:
- Test path conversion with various input formats: relative paths, absolute paths, existing ABFS URLs
- Test with invalid inputs (None, empty string, malformed paths)
- Test with environment variables both set and unset for storage account info
- Test extraction of database and table names from different path formats
- Mock environment variables for consistent test execution

---

### CWFAB-2: Create Unified Delta I/O Module
**Priority**: High  
**Complexity**: High  
**Dependencies**: CWFAB-1

**Description**:  
Multiple implementations of Delta writing logic exist across `bronze_loader.py`, `delta_utils.py`, and `onelake_utils.py`, with inconsistent error handling and configuration.

**Acceptance Criteria**:
- Create a new `delta_io.py` module with a unified Delta table API
- Implement standardized write options optimized for Fabric
- Add consistent caching, partitioning, and schema evolution strategies
- Add proper error recovery from common Fabric issues
- Remove redundant implementations from existing modules

**Implementation Details**:
```python
def write_delta_table(
    df: DataFrame,
    table_path: str,
    mode: str = "append",
    partition_cols: Optional[List[str]] = None,
    options: Optional[Dict[str, str]] = None,
    register_table: bool = True
) -> Tuple[str, int]:
    """Unified function for writing to Delta with Fabric-optimized defaults"""
```

**Unit Test Considerations**:
- Mock SparkSession and DataFrame objects to test function calls without actual Delta writes
- Test function behavior with empty DataFrames
- Test different write modes (append, overwrite, etc.)
- Test error handling with simulated exceptions
- Test partition column handling
- Test schema evolution scenarios
- Implement integration tests that write small test DataFrames to actual Delta tables

---

### CWFAB-3: Standardize ETL Orchestration
**Priority**: High  
**Complexity**: High  
**Dependencies**: CWFAB-2

**Description**:  
Duplicate orchestration logic exists across `orchestration.py`, `pipeline.py`, and `bronze_loader.py`, creating confusion about the proper ETL methodology.

**Acceptance Criteria**:
- Choose one primary orchestration approach for all ETL operations
- Deprecate and refactor redundant orchestration code
- Implement a configuration-driven approach for entity definitions
- Update documentation to clarify the proper usage pattern
- Ensure consistent error handling and logging

**Implementation Details**:
- Create a declarative entity configuration structure
- Support both full and incremental ETL patterns
- Allow for entity-specific customization while maintaining consistent processing

**Unit Test Considerations**:
- Mock all dependent components (client, extraction functions, transformation functions)
- Test full ETL workflow with simple mock data
- Test incremental ETL scenarios with date-based filtering
- Test error propagation from extraction/transformation layers
- Test configuration validation
- Test entity-specific customization logic
- Use dependency injection for testability of orchestration components

---

## Performance Optimizations

### CWFAB-4: Optimize Spark Configuration for Fabric
**Priority**: Medium  
**Complexity**: Low

**Description**:  
Current Spark session configuration isn't fully optimized for Microsoft Fabric, leading to suboptimal performance.

**Acceptance Criteria**:
- Create a standardized Spark session factory optimized for Fabric
- Configure adaptive query execution and other performance optimizations
- Set appropriate Delta options for best performance
- Apply consistent memory management settings
- Document the configuration choices

**Implementation Details**:
```python
def create_fabric_spark_session(app_name: str = "ConnectWise-ETL") -> SparkSession:
    """Create a SparkSession optimized for Microsoft Fabric"""
    return (SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .getOrCreate())
```

**Unit Test Considerations**:
- Mock SparkSession builder to verify configuration options
- Test custom configuration options are correctly applied
- Test with different app names and environment variables
- Test default options versus custom options
- Verify Fabric-specific configurations are always applied

---

### CWFAB-5: Implement Parallel API Extraction
**Priority**: Medium  
**Complexity**: Medium  
**Dependencies**: CWFAB-3

**Description**:  
Current API extraction is performed sequentially, causing unnecessarily long processing times.

**Acceptance Criteria**:
- Enhance `ConnectWiseClient` to support parallel request execution
- Implement smart rate limiting to avoid API throttling
- Add configurable concurrency settings
- Ensure proper error handling and retry logic
- Measure and document performance improvements

**Implementation Details**:
- Leverage the existing `fetch_parallel()` function in `_common.py`
- Implement a connection pool for better HTTP connection reuse
- Use ThreadPoolExecutor with appropriate pool size for API parallelism

**Unit Test Considerations**:
- Mock HTTP responses to test parallel request handling
- Test rate limiting behavior with simulated fast requests
- Test error handling with simulated connection failures
- Test retry logic with temporary failures
- Test with different concurrency settings
- Test aggregation of results from parallel requests
- Use `requests-mock` to simulate API responses without real network calls

---

### CWFAB-6: Streamline Data Conversion Pipeline
**Priority**: High  
**Complexity**: High  
**Dependencies**: CWFAB-2

**Description**:  
The current ETL pipeline converts data through multiple formats (JSON → Pydantic → pandas → Spark → Delta), creating unnecessary overhead.

**Acceptance Criteria**:
- Reduce conversion steps to minimize memory usage and improve performance
- Create a direct path from API responses to Spark DataFrames where possible
- Move validation to Spark level where appropriate
- Implement schema inference optimizations
- Benchmark and document performance improvements

**Implementation Details**:
- Use Spark's schema inference for the initial DataFrame creation
- Perform validation and transformation using DataFrame operations
- Implement batch processing for large datasets

**Unit Test Considerations**:
- Test conversion from API response formats directly to DataFrames
- Test schema inference with different data structures
- Test validation at the DataFrame level
- Benchmark performance with different sized datasets
- Test with complex nested data structures
- Test with edge cases (empty datasets, datasets with nulls or missing fields)
- Create test fixtures with sample API responses for consistent testing

---

## Code Consolidation

### CWFAB-7: Create Generic Entity Extraction Utilities
**Priority**: Medium  
**Complexity**: Medium  
**Dependencies**: CWFAB-5

**Description**:  
Extraction logic is repeated across entity-specific modules with minimal differences.

**Acceptance Criteria**:
- Create a generic extraction utility that works across entity types
- Refactor entity-specific modules to use the generic utility
- Maintain entity-specific customizations where necessary
- Ensure consistent error handling and logging
- Reduce overall code redundancy

**Implementation Details**:
```python
def extract_entity(
    client: ConnectWiseClient,
    entity_config: EntityConfig,
    conditions: Optional[str] = None,
    page_size: int = 100,
    max_pages: Optional[int] = None
) -> List[Dict[str, Any]]:
    """Generic extraction function for any ConnectWise entity"""
```

**Unit Test Considerations**:
- Test with mock client and different entity configurations
- Test with various filtering conditions
- Test pagination logic with simulated multi-page responses
- Test with different page sizes and max page settings
- Test error handling for API failures
- Test entity-specific customizations
- Create a test entity configuration to verify generic logic works correctly

---

### CWFAB-8: Standardize Error Handling and Logging
**Priority**: Medium  
**Complexity**: Medium

**Description**:  
Current error handling and logging approaches are inconsistent across modules.

**Acceptance Criteria**:
- Implement a standardized error handling strategy
- Create a centralized logging configuration
- Add structured logging for better analysis
- Implement appropriate error recovery strategies for different error types
- Ensure errors are properly propagated or handled at appropriate levels

**Implementation Details**:
- Create an `ETLException` hierarchy for different error types
- Implement context managers for common operations
- Add structured logging with consistent metadata

**Unit Test Considerations**:
- Test exception hierarchy for different error types
- Test logging output format and content
- Test context managers with both success and failure scenarios
- Test error propagation through multiple function calls
- Test structured logging metadata consistency
- Mock logging handlers to capture and verify log messages
- Test different log levels and configurations

---

### CWFAB-9: Refactor Validation Logic
**Priority**: Medium  
**Complexity**: High  
**Dependencies**: CWFAB-6

**Description**:  
Current validation approach uses record-by-record Pydantic validation, which is inefficient at scale.

**Acceptance Criteria**:
- Implement a two-phase validation approach (schema + business rules)
- Use Spark's built-in schema validation for basic type checking
- Leverage DataFrame operations for business rule validation
- Track validation errors in a structured format
- Improve validation performance for large datasets

**Implementation Details**:
- Use Spark schemas for basic type validation
- Implement DataFrame-level validation functions for business rules
- Create a validation result structure that captures errors with context

**Unit Test Considerations**:
- Test schema validation with valid and invalid data structures
- Test business rule validation with compliant and non-compliant data
- Test validation error collection and reporting
- Test validation performance with large datasets
- Test complex validation rules that span multiple columns
- Test different validation strategies (fail-fast vs. collect-all-errors)
- Create test fixtures with known validation issues to verify detection

---

## OneLake Optimizations

### CWFAB-10: Implement Proper OneLake Partitioning Strategy
**Priority**: Medium  
**Complexity**: Medium  
**Dependencies**: CWFAB-2

**Description**:  
Current code disables partitioning in some areas to "avoid schema issues," missing opportunities for query optimization.

**Acceptance Criteria**:
- Design an appropriate partitioning strategy for each entity type
- Implement partition pruning optimizations
- Ensure partition columns are properly handled during schema evolution
- Test and validate query performance improvements
- Document the partitioning approach

**Implementation Details**:
- Implement date-based partitioning for time-series data
- Use entity-based partitioning for reference data
- Ensure proper handling of partition columns during writes

**Unit Test Considerations**:
- Test partition column selection logic for different entity types
- Test partition column handling during writes
- Test schema evolution with partitioned tables
- Test query performance with and without partitioning
- Test partition pruning with different query patterns
- Mock Delta write operations to verify partition settings
- Test edge cases like empty partition values or null partition columns

---

### CWFAB-11: Enhance OneLake Catalog Integration
**Priority**: Medium  
**Complexity**: Medium  
**Dependencies**: CWFAB-2, CWFAB-3

**Description**:  
Table registration with the Fabric Lakehouse catalog is inconsistent.

**Acceptance Criteria**:
- Ensure all tables are properly registered in the catalog
- Add consistent table descriptions and metadata
- Implement proper handling of schema evolution
- Support both database.table_name and path-based access
- Document the catalog integration approach

**Implementation Details**:
```python
def register_table(
    spark: SparkSession,
    table_path: str,
    table_name: str,
    description: str = "",
    replace_existing: bool = False
) -> None:
    """Register a Delta table in the Fabric catalog"""
```

**Unit Test Considerations**:
- Test table registration with different path formats
- Test table description and metadata handling
- Test behavior when tables already exist (with replace_existing=True/False)
- Test error handling for registration failures
- Test schema evolution scenarios
- Mock SQL execution to verify correct DDL statements
- Test integration with actual catalog in a test environment

---

### CWFAB-12: Clean Up Deprecated and Redundant Code
**Priority**: Low  
**Complexity**: Medium  
**Dependencies**: CWFAB-3, CWFAB-7

**Description**:  
Codebase contains deprecated, redundant, or unused functions that should be removed.

**Acceptance Criteria**:
- Identify and remove unused functions and modules
- Deprecate redundant code paths with warnings
- Update documentation to reflect changes
- Ensure backward compatibility where necessary
- Reduce overall code complexity

**Implementation Details**:
- Use static analysis tools to identify unused code
- Add deprecation warnings for functions that will be removed
- Document migration paths for deprecated functionality

**Unit Test Considerations**:
- Create tests for any functionality being preserved
- Ensure existing tests pass after code removal
- Test deprecation warnings are triggered appropriately
- Test backward compatibility layers
- Create integration tests that verify the system still works end-to-end
- Measure code coverage before and after to ensure no regression
- Create tests for migration paths from old to new functionality

---

### CWFAB-13: Implement Comprehensive Testing Framework
**Priority**: High  
**Complexity**: High

**Description**:  
The codebase lacks a systematic testing approach, making it difficult to ensure reliability as changes are made.

**Acceptance Criteria**:
- Implement a comprehensive testing framework for all modules
- Create unit tests for core functionality
- Implement integration tests for end-to-end flows
- Add mock objects for external dependencies
- Implement test fixtures for common test scenarios
- Add CI/CD pipeline integration for automated testing
- Achieve minimum code coverage targets (e.g., 80%)

**Implementation Details**:
- Use pytest as the primary testing framework
- Create mock objects for SparkSession, ConnectWiseClient, etc.
- Implement test fixtures for different entity types
- Add parameterized tests for configuration variations
- Create a test database for integration testing

**Unit Test Considerations**:
- Create a test configuration module
- Implement test fixtures for common objects (SparkSession, client, etc.)
- Add parameterized tests for different scenarios
- Implement test helpers for common assertions
- Create test utilities for generating test data
- Develop CI/CD pipeline integration
- Implement test coverage reporting

---

### CWFAB-14: Implement Robust Schema Evolution Handling
**Priority**: High  
**Complexity**: High  
**Dependencies**: CWFAB-2, CWFAB-9

**Description**:  
Current approach to schema evolution is inconsistent, leading to potential data loss or pipeline failures when API schemas change.

**Acceptance Criteria**:
- Implement consistent approach to schema evolution
- Create schema versioning mechanism
- Add detection of schema changes in API responses
- Implement safe transformation of data between schema versions
- Ensure backward compatibility with existing Delta tables
- Document schema evolution strategy

**Implementation Details**:
- Create schema version registry
- Implement schema difference detection
- Add safe transformation functions for evolving schemas
- Configure Delta for proper schema merging

**Unit Test Considerations**:
- Test schema evolution with added columns
- Test schema evolution with removed columns
- Test schema evolution with data type changes
- Test schema versioning mechanism
- Test backward compatibility with historical schemas
- Create test fixtures with different schema versions
- Test detection of incompatible schema changes
- Test transformation functions between schema versions
