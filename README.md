# PSA ETL Pipeline - Full Medallion Architecture

A comprehensive data pipeline for extracting ConnectWise PSA data into Microsoft Fabric with bronze, silver, and gold layers.

## Architecture Overview

### Bronze Layer (Raw Data Landing)
- Direct extraction from ConnectWise API
- No transformations or validations
- Complete raw data preservation
- Partitioned by extraction date

### Silver Layer (Cleansed & Conformed)
- Flattened nested structures
- Column pruning to remove unnecessary fields
- Field renaming for consistency
- Data validation and type conformance

### Gold Layer (Business Logic & BI)
- Invoice header/line separation
- Agreement hierarchy resolution
- Time entry and expense connections
- Business rule applications (e.g., Tímapottur filtering)
- BI-ready dimensional models

## Usage

```python
from fabric_api.pipeline import run_full_pipeline

# Run complete medallion pipeline
results = run_full_pipeline(
    entity_names=["Agreement", "TimeEntry", "PostedInvoice"],
    bronze_path="/lakehouse/default/Tables/bronze",
    silver_path="/lakehouse/default/Tables/silver", 
    gold_path="/lakehouse/default/Tables/gold",
    process_gold=True
)

# Run daily incremental load
from fabric_api.pipeline import run_daily_pipeline

daily_results = run_daily_pipeline(
    days_back=1,
    process_gold=True
)
```

## Modernization Phases

### Phase 1: Remove Legacy Code and Fallbacks ✅

**Goal:** Clean out any outdated or redundant code paths so the pipeline has no unnecessary complexity from earlier implementations. This includes eliminating old compatibility layers, defensive hacks, and dead code introduced in previous iterations (e.g. by Claude) that are no longer needed.

* **Retire Legacy Pydantic Models:** ✅ Removed all legacy `Manage*` Pydantic models and associated validation logic from `models.py`. The pipeline now exclusively uses the new schema-derived models from `connectwise_models`.

* **Remove "Safe Validate" Wrappers and Excess Error Handling:** ✅ Eliminated the custom validation helper `safe_validate` and similar defensive patterns. Validation is now centralized in a single workflow, with standardized error handling and logging.

* **Drop Dynamic Schema Fallbacks:** ✅ Removed code that attempted to dynamically adjust or fall back when schema mismatches occurred, such as `_flatten_nested_structures` in the bronze loader. The pipeline now has a single, clear path for data loading without branching for special cases.

* **Clean Up Unused or Duplicative Utilities:** ✅ Deleted helper functions and modules that were supporting the old pipeline structure, including legacy extractors and transformers. All remaining code directly supports the new, intended flow.

## Phase 2: Schema-Driven Model Generation and Field Selection ✅

**Goal:** Shift to a fully schema-defined process. Automatically generate Pydantic models from the official ConnectWise API schema, and use those models to drive what data we request and process. This eliminates manual model coding and hard-coded field lists, making the pipeline easier to maintain and adapt to API changes.

* **Generate Pydantic Models from OpenAPI 3 Schema:** ✅ Created new script `generate_models_from_openapi.py` that leverages ConnectWise's OpenAPI specifications to generate Pydantic models inheriting from `sparkdantic.SparkModel`. All models now have Spark-compatible methods like `model_spark_schema()`.

* **Avoid Manual Edits to Models:** ✅ Established workflow treats the OpenAPI spec as the single input for models. When ConnectWise schema changes, we update the spec and re-run the generator without manual tweaking.

* **Filter Out Unneeded Schema Elements:** ✅ Configured model generation to exclude fields we don't need, such as the `_info` field with metadata links. The resulting models contain only the business fields and IDs relevant for analytics.

* **Schema-Driven API Field Selection:** ✅ Updated `get_fields_for_api_call()` in `api_utils.py` to dynamically determine which fields to request from the ConnectWise API by introspecting the Pydantic models. Field lists are now derived from the schema instead of being hardcoded.

* **Apply Field Selection in Extractors:** ✅ Refactored all extraction modules in `fabric_api/extract/` to use schema-driven field selection. Each extractor now dynamically builds API requests based on the corresponding Pydantic model.

## Phase 3: Streamlined Validation and Loading ✅

**Goal:** Refactor the ETL process into a clear, linear flow: **extract raw data → validate against Pydantic schema → convert to Spark DataFrame → write to Delta**. All validation and transformation logic will be driven by the schema, with no ad-hoc conversions.

* **Simplify Data Extraction Functions:** ✅ Each extractor function now solely pulls data from the API and returns it without validation or transformations. The output is a clean list of dictionaries from the API.

* **Centralize Pydantic Validation:** ✅ Implemented `validate_batch()` in `extract/_common.py` that handles validation uniformly across all entity types. It processes raw JSON records and produces validated Pydantic models or error records for failures.

* **Direct Schema-Based DataFrame Conversion:** ✅ Implemented direct conversion from validated Pydantic objects to Spark DataFrames using the schema provided by the models. Removed all fallback mechanisms and schema inference.

* **Consistent Delta Lake Writing:** ✅ Standardized Delta Lake writing with a uniform approach for all entities. Partitioning is applied based on configuration in `ENTITY_CONFIG`, with no entity-specific special cases.

* **Logging and Error Tracking:** ✅ Added comprehensive logging through `log_utils.py` that tracks validation statistics, records errors without excessive verbosity, and provides clear summaries of issues encountered.

## Phase 4: Fabric-Native Lakehouse Integration ✅

**Goal:** Align the output of the ETL with Microsoft Fabric's Lakehouse conventions, ensuring that data lands in Delta tables that are immediately usable in Fabric (e.g. for Power BI Direct Lake). This phase involves configuring paths and table names, and removing any extraneous steps so that the pipeline output is the single source of truth in OneLake.

* **Direct Write to OneLake Tables:** ✅ Implemented `ensure_fabric_path()` function in `bronze_loader.py` that automatically formats paths for Fabric OneLake. This allows writing directly to the Lakehouse's `Tables` directory with proper ABFSS URL construction (e.g., `abfss://lakehouse@account.dfs.fabric.microsoft.com/path`). When running in a non-Fabric environment, paths are still properly handled as local filesystem paths. This eliminates any need for intermediate storage or post-processing steps.

* **Consistent Naming and Partitioning:** ✅ Standardized table naming with the `cw_` prefix in `ENTITY_CONFIG` and applied consistent partitioning strategies for each entity type (time-related entities partitioned by date, agreements by type, etc.). All table definitions are now centralized in a single configuration dictionary, ensuring uniform handling across entities. 

* **Fabric Compatibility:** ✅ Added Fabric-optimized Delta write options including `mergeSchema`, `delta.autoOptimize.optimizeWrite`, and `delta.autoOptimize.autoCompact`. Updated path handling to use `os.path.join()` for consistent cross-platform compatibility. Leveraged Fabric's table registration system to automatically create catalog tables for each entity via `register_table_metadata()` function.

* **Enhanced Metadata:** ✅ Added function `add_fabric_metadata()` to enrich DataFrames with standardized tracking columns like `etl_entity_name`, `etl_entity_type`, `etl_timestamp`, and `etl_version`. This enables better data lineage tracking and filtering in Fabric.

* **Validation Error Tracking:** ✅ Updated `write_validation_errors()` function to write validation errors to a dedicated `cw_validation_errors` Delta table in the Lakehouse, partitioned by entity for efficient querying. Error records include the full context (entity type, field, error message) to assist with troubleshooting.

* **Documentation and Configuration:** ✅ Updated example notebook usage with comprehensive patterns for incremental loading with date filtering, configuring Fabric storage paths, and querying the resulting tables with SQL. Added documentation comments throughout the code to explain Fabric-specific optimizations.

## Phase 5: Testing and Documentation ✅

**Goal:** Create a comprehensive test suite and documentation to ensure the system is reliable and maintainable.

* **Updated Test Suite:** ✅ Created both real API tests and mock tests for the ConnectWiseClient and extraction modules. Mock tests can run without API credentials, allowing for CI/CD integration and local development testing. The test suite now covers:
  * API parameter handling (fields, conditions, child_conditions, order_by)
  * Model validation against API responses
  * Error handling and reporting

* **Documented Test Strategy:** ✅ Added detailed `tests/README.md` explaining how to run tests, how to create new tests, and the organization of test files. The documentation includes examples for creating mocks and fixtures.

* **Environment Configuration:** ✅ Created `.env.example` to document required environment variables for testing and running the pipeline. The example file provides clear instructions for setting up credentials.

* **Code Documentation:** ✅ Added comprehensive docstrings to all modules and functions, with clear explanations of parameters, return values, and usage examples.

* **End-to-End Testing:** ✅ Implemented tests that validate the entire ETL process from extraction to validation to DataFrame creation, ensuring that the pipeline works correctly from end to end.

## Phase 6: Implementation Strategy and Future Plans

**Goal:** Finalize the modernization and outline future enhancements.

* **Incremental Implementation Completed:** We've implemented all phases sequentially, isolating issues and ensuring each component works as expected. The process started with cleaning up legacy code, then adding schema-driven model generation, streamlining validation and loading, and finally optimizing for Fabric.

* **Clean Architecture:** The final code structure is clean and coherent, with clear separation of concerns:
  * `client.py`: Handles API communication
  * `connectwise_models/`: Contains generated Pydantic models
  * `extract/`: Manages data extraction from the API
  * `storage/`: Handles Delta table operations
  * `core/`: Contains shared utilities and configuration

* **Future Enhancements:** With the pipeline now in a clean state, future enhancements can be built on this solid foundation:
  * **Incremental Loading:** Easily implemented using the existing conditions parameter
  * **Silver Layer Processing:** Clean and transform the raw data for analytics
  * **Enhanced Error Handling:** Add retry mechanisms and better reporting
  * **Data Quality Monitoring:** Track validation statistics and data patterns over time
  * **Performance Optimization:** Further tune Spark parameters for large datasets

* **Maintenance Plan:** To keep the pipeline running smoothly:
  * Regularly update OpenAPI schema and regenerate models
  * Run tests before deployments
  * Monitor validation errors to catch API changes
  * Keep dependencies updated for security and performance

## Invoice Processing Business Logic

Based on the Wise AL code analysis, here's how invoices are structured and connect to other entities:

### Invoice Header/Line Structure

1. **Invoice Header** (Main invoice record):
   - Identified by `billingLogId` (primary key) and `invoiceNumber`
   - Contains aggregated amounts, customer info, project, VAT
   - Has a `type` that can be either "Standard" or "Agreement"
   - Has an `agreementNumber` that links to the main agreement

2. **Invoice Lines** (Detail records):
   - Linked to header by `invoiceNumber`
   - Each line has a `lineNo` starting at 10000, incrementing by 1
   - Contains quantity, amount, description, dates
   - Connects to one of:
     - Time Entry (for Standard invoices)
     - Product (for Agreement invoices)
     - Expense Entry (associated with header, not lines)

### Entity Relationships

1. **Time Entry Connections** (Standard Invoices):
   - Invoice Line has `timeEntryId` field
   - Time Entry provides: employee, workRole, workType, hourlyRate, agreementHours
   - Time Entry has its own `agreementId` and `ticketId`
   - Agreement hierarchy: Time Entry → Agreement → Parent Agreement

2. **Product Connections** (Agreement Invoices):
   - Invoice Line has `productId` and `itemIdentifier`
   - Products have their own `agreementId`
   - Special handling for discount calculation based on item identifier
   - Item identifier "SALE0000" has special no-discount logic

3. **Expense Entry Connections**:
   - Linked to Invoice Header (not lines) via `invoiceId`
   - Has its own line numbering system
   - Contains: type, quantity, amount, work date, employee
   - Has its own `agreementId` with hierarchy support

### Agreement Hierarchy Logic

1. **Agreement Number Resolution**:
   - First check the entity's direct agreement
   - If empty and has parent agreement, check parent
   - Custom field array[0].value contains the actual agreement number
   - Header agreement number derived from its lines/expenses

2. **Agreement Type Filtering**:
   - "Tímapottur" agreement type is excluded from time entry processing
   - Agreement type affects invoice processing behavior

3. **Parent-Child Relationships**:
   - Entities have both `agreementId` and `parentAgreementId`
   - If agreement number missing, system checks parent agreement
   - Hierarchical lookup pattern: Entity → Agreement → Parent Agreement

### Data Flow Pattern

1. **Invoice Import Process**:
   ```
   Get Invoices → Process Header → Process Details → Process Lines
                                                  ↓
                                    Time Entries OR Products
                                                  ↓
                                            Agreements
   ```

2. **Standard Invoice Lines**:
   ```
   Invoice Line → Time Entry → Agreement → Parent Agreement (if needed)
   ```

3. **Agreement Invoice Lines**:
   ```
   Invoice Line → Product → Agreement → Parent Agreement (if needed)
                         → Product Discount (conditional)
   ```

### Field Mapping Patterns

1. **Nested Object Access**:
   - Uses dot notation: `timeEntry.id`, `agreement.type`, `member.identifier`
   - Helper function `GetObjectFromParentObject` for safe navigation

2. **Custom Field Handling**:
   - Agreement numbers stored in `customFields[0].value`
   - Product discounts in custom fields array

3. **Type Conversions**:
   - Invoice type: Text → Enum conversion
   - VAT: Decimal to percentage (multiply by 100)
   - Date fields: DateTime handling

### Implementation Considerations

1. **Bronze-to-Silver Transformations**:
   - Split invoice headers and lines into separate tables
   - Flatten nested agreement/time entry/expense relationships
   - Resolve agreement hierarchies during processing
   - Apply business logic filters (e.g., exclude "Tímapottur" agreements)

2. **Key Join Fields**:
   - Invoice headers ↔ lines: `invoiceNumber`
   - Invoice lines → time entries: `timeEntryId`
   - Invoice lines → products: `productId`
   - Header → expenses: `invoiceId`
   - All entities → agreements: `agreementId`

3. **Special Business Rules**:
   - Line numbering starts at 10000
   - Agreement numbers in custom fields
   - Parent agreement fallback logic
   - Discount calculations for products
   - VAT percentage conversion

4. **Error Handling Considerations**:
   - Missing agreement IDs (0 = not set)
   - Empty custom field arrays
   - Nested object access failures
   - Type conversion errors