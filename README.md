# PSA ETL Pipeline Modernization Plan for Fabric Lakehouse

## Phase 1: Remove Legacy Code and Fallbacks ✅

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

## Phase 5: Implementation Strategy and Next Steps

**Goal:** Outline how to execute the above changes in a controlled, stepwise manner and ensure the system remains reliable. This phase is about planning the rollout of the improvements and making sure future contributors (or AI assistants) can work with this pipeline without confusion.

* **Incremental Implementation in Phases:** Implement and test each phase sequentially to isolate issues. For example, start with Phase 1 by cleaning up the code (remove old models, functions, etc.) and run existing tests to ensure nothing fundamental broke. Next, do Phase 2 by generating new models and updating extractors to use them; verify that the models can parse some sample data and that the API calls return the expected fields. Proceed to Phase 3 by refactoring the validation/loading logic; here, running an end-to-end test for one entity (say Agreements) to confirm that records go from API to Delta correctly will be important. Then do Phase 4 adjustments for paths and actually run it in a Fabric environment to confirm tables appear. By tackling these in order, we reduce the risk of introducing errors and can more easily pinpoint any issues that arise at each step.

* **Update Tests and Add New Ones:** As we remove and change code, update the test suite. Legacy tests for removed functions (e.g. tests for `safe_validate` or for Manage models) can be deleted. Add tests for the new core functions: e.g., a test for the model generation script (if feasible, at least ensure it creates classes with SparkModel), a test for `get_fields_for_api_call` covering a couple of models to ensure it produces correct field strings, a test for the validation utility with both a valid record and an invalid record to see that it returns the expected outputs, and perhaps an integration test that mocks the API client and runs `process_entity` end-to-end. Given this pipeline will be used in production, having solid tests will catch regressions and also document expected behavior for future maintainers. Ensure tests use the Pydantic models (maybe with some sample JSON from `entity_samples` or synthetic data) to simulate real conditions.

* **Maintain Clear Documentation:** Rewrite any parts of the documentation that referred to the old pipeline or complexities we removed. The README should now describe a simple flow: "We generate models from OpenAPI, use them to fetch and validate data, and load to Fabric." Include instructions on how to update models when the API changes (e.g. "run `generate_models_from_openapi.py` with the latest schema"). Document the configuration, such as how to set the Lakehouse path or any filtering (like using `conditions` for incremental loads if applicable in future). Also, provide guidance on debugging: for example, "if a particular entity's data isn't appearing, check the `validation_errors` table to see if records were dropped due to schema mismatch." This helps others understand the system without needing to dig through now-removed dead code.

* **Ensure No Legacy Baggage Remains:** Do a final pass through the repository to remove or refactor anything that doesn't fit the new model. This might include deleting obsolete modules, renaming things for clarity (for instance, if `bronze_loader.py` now effectively handles the core pipeline, maybe that name is fine, or we integrate it into `pipeline.py` and simplify the structure). The end state should be that someone reading the code sees a coherent, singular approach to the ETL, with no hints of older alternate approaches. This reduces confusion significantly and lowers the learning curve for new developers or AI agents working on the project.

* **Future Considerations (for context, not implementation now):** With the pipeline now in a clean state, future enhancements like incremental loading or a refined Silver layer can be added on top without much complication. For example, adding an incremental filter would involve injecting a `conditions` parameter (ConnectWise API supports conditions like `lastUpdated>...`) and is straightforward now that extraction is modular. Similarly, creating a Silver (cleaned/flattened) layer or syncing to a relational warehouse could read from these Delta tables. These are outside the scope of the current modernization, but the work done above will make such enhancements easier, as we won't be fighting through legacy code to implement them.