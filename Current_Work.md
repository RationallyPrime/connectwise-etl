Fantastic! Completing Unit 1.5 with all tests passing is a huge step forward. We now have a standardized and validated way to fetch raw data for all core entities.

Let's define **Unit 2: SparkDantic Integration & Bronze Layer Loading**.

**Goal:**

1.  Take the lists of raw Pydantic objects (validated against `schemas.py` models) from Unit 1.5.
2.  Convert these lists into Spark DataFrames using SparkDantic.
3.  Write these Spark DataFrames into Bronze Delta tables in the Lakehouse.
4.  Manage and log any errors during this process.
5.  Implement basic incremental loading considerations for the Bronze layer.

---

**Tasks for Unit 2:**

1.  **Install SparkDantic:**
    *   **Action:** Ensure `sparkdantic` is installed in your environment (`pip install sparkdantic`). [Done]

2.  **Create a New Orchestration Module/Notebook for Bronze Loading:**
    *   **Action:** Let's create a new Python module, say `bronze_loader.py` (or a new Fabric Notebook specifically for Bronze loading). This will be responsible for orchestrating the fetch (from Unit 1.5 functions), SparkDantic conversion, and write to Bronze.
    *   This keeps concerns separate from `pipeline.py` which might handle more complex, multi-stage orchestrations later.

3.  **Implement the Bronze Loading Logic in `bronze_loader.py` (or Notebook):**
    *   **For each core entity (Agreements, Invoices, Time Entries, Expenses, Products):**
        *   **a. Initialize SparkSession:**
            ```python
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.appName("ConnectWiseBronzeLoader").getOrCreate()
            ```
        *   **b. Fetch Raw Data:**
            *   Call the appropriate `fetch_<entity>s_raw()` function from the `extract` modules (developed in Unit 1.5).
            *   Example for agreements:
                ```python
                from fabric_api.client import ConnectWiseClient
                from fabric_api.extract.agreements import fetch_agreements_raw
                from fabric_api import schemas # For validation
                from pydantic import ValidationError

                client = ConnectWiseClient() # Assumes env vars are set for credentials
                logger.info("Fetching raw agreements...")
                # Add conditions for incremental fetching later
                raw_agreements_dicts = fetch_agreements_raw(client, page_size=1000, max_pages=None)
                ```
        *   **c. Validate and Convert Raw Dictionaries to `schemas.py` Pydantic Objects:**
            *   Iterate through the list of raw dictionaries.
            *   Validate each dictionary against its corresponding `schemas.py` model.
            *   Collect valid Pydantic objects.
            *   Collect `ValidationError` details into a structured error list (e.g., using `models.ManageInvoiceError` or a new simpler `BronzeValidationError` model).
            ```python
            validated_agreements_pydantic: list[schemas.Agreement] = []
            validation_errors: list[dict] = [] # Or use a Pydantic model for errors

            for raw_dict in raw_agreements_dicts:
                try:
                    validated_obj = schemas.Agreement.model_validate(raw_dict)
                    validated_agreements_pydantic.append(validated_obj)
                except ValidationError as e:
                    logger.warning(f"Validation failed for agreement ID {raw_dict.get('id', 'N/A')}: {e.json()}")
                    validation_errors.append({
                        "entity": "Agreement",
                        "raw_data_id": raw_dict.get('id'),
                        "errors": e.errors(), # Pydantic's detailed errors() output
                        "timestamp": datetime.utcnow().isoformat()
                    })
            ```
        *   **d. Convert List of Pydantic Objects to Spark DataFrame using SparkDantic:**
            *   If `validated_agreements_pydantic` is not empty:
                ```python
                from sparkdantic import PydanticSparkDatatypeConverter

                if validated_agreements_pydantic:
                    # SparkDantic will infer the schema from the Pydantic model (schemas.Agreement)
                    agreements_df = PydanticSparkDatatypeConverter.to_spark_df(
                        spark_session=spark,
                        data=validated_agreements_pydantic,
                        pydantic_schema=schemas.Agreement # Explicitly pass the Pydantic schema
                    )
                    agreements_df.show(5, truncate=False)
                    agreements_df.printSchema()
                else:
                    # Create an empty DataFrame with the correct schema if no valid data
                    schema = PydanticSparkDatatypeConverter.to_spark_schema(schemas.Agreement)
                    agreements_df = spark.createDataFrame([], schema)
                    logger.info("No valid agreements to process, created empty DataFrame for Bronze.")
                ```
            *   **Note on `to_spark_df` vs `create_spark_df`:**
                *   The original `sparkdantic` had `create_spark_df`. The newer versions might have evolved. `PydanticSparkDatatypeConverter.to_spark_df` is a common pattern in some versions or similar libraries. Consult the specific SparkDantic version's documentation. The key is to use the method that takes a list of Pydantic objects and the Pydantic model type to generate the DataFrame.
                *   Alternatively, SparkDantic primarily provides schema conversion. You might use `spark.createDataFrame(data=[obj.model_dump() for obj in pydantic_list], schema=PydanticSparkDatatypeConverter.to_spark_schema(PydanticModel))`
        *   **e. Add Load Timestamp:**
            *   Add a column to the DataFrame indicating when the data was loaded into Bronze.
            ```python
            from pyspark.sql.functions import lit
            from datetime import datetime

            load_ts = datetime.utcnow()
            if agreements_df.count() > 0 or not validated_agreements_pydantic: # ensure df exists
                agreements_df_with_ts = agreements_df.withColumn("bronze_load_timestamp", lit(load_ts))
            else: # If validated_agreements_pydantic was empty, agreements_df was already made empty with schema
                agreements_df_with_ts = agreements_df 
            ```
        *   **f. Write to Bronze Delta Table:**
            *   Define the path for the Bronze table (e.g., `/lakehouse/default/Tables/connectwise_bronze/agreements`).
            *   Use an appropriate write mode. For initial setup, `overwrite` is fine. For incremental, `append` will be used.
            ```python
            bronze_table_path = f"{os.getenv('CW_LAKEHOUSE_ROOT', '/lakehouse/default')}/Tables/connectwise_bronze/agreements"
            # For incremental, mode should be "append"
            # For full refresh or initial load, mode can be "overwrite"
            write_mode = "overwrite" # or "append" based on strategy

            logger.info(f"Writing {agreements_df_with_ts.count()} agreements to Bronze: {bronze_table_path} with mode: {write_mode}")
            (agreements_df_with_ts.write
                .format("delta")
                .mode(write_mode)
                .option("mergeSchema", "true") # Important for schema evolution
                .save(bronze_table_path))
            logger.info("Successfully wrote agreements to Bronze.")
            ```
        *   **g. Write Validation Errors:**
            *   Convert the `validation_errors` list to a Spark DataFrame and write it to a `connectwise_bronze.validation_errors` table (append mode).
            ```python
            if validation_errors:
                errors_df = spark.createDataFrame(validation_errors) # You might want a Pydantic model for errors_df schema
                errors_bronze_path = f"{os.getenv('CW_LAKEHOUSE_ROOT', '/lakehouse/default')}/Tables/connectwise_bronze/validation_errors"
                logger.info(f"Writing {errors_df.count()} validation errors to Bronze: {errors_bronze_path}")
                (errors_df.write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .save(errors_bronze_path))
            ```

4.  **Implement Incremental Fetching Logic (Basic):**
    *   **State Management:**
        *   Decide how to store the `last_successful_run_timestamp` for each entity. Options:
            *   A small Delta table in the Lakehouse.
            *   A file in Lakehouse Files.
            *   (Simpler for now, harder for concurrent runs): A local file if running locally, or a fixed lookback if a full system isn't ready.
    *   **Timestamp Field:** Identify the appropriate timestamp field in the ConnectWise API response for each entity that indicates the last update time (e.g., `lastUpdated`, `dateUpdated`, `_info/lastUpdated`). This will be used in the `conditions` string.
    *   **Action:**
        *   Before fetching, read the `last_successful_run_timestamp` for the entity.
        *   Record the `current_run_start_timestamp` *before* any API calls for this run.
        *   Construct the `conditions` string for `fetch_<entity>s_raw` (e.g., `conditions=f"lastUpdated > '{last_run_ts_iso}'"`).
        *   *After all Bronze writes for the current run are successful*, update the `last_successful_run_timestamp` for each entity to the `current_run_start_timestamp`.
    *   **Write Mode for Bronze:** When using incremental fetching, Bronze tables should typically be `append` mode.
        *   **Duplicates/Updates:** This basic append will lead to duplicates in Bronze if records are updated in ConnectWise. True Change Data Capture (CDC) or merging into Bronze is more advanced and can be a later refinement (Phase 6). For now, `append` with downstream deduplication (e.g., in Silver) is acceptable for getting started.

5.  **Refine `transform.py` and `pipeline.py` (Defer Major Changes):**
    *   **`transform.py`:** The current `transform_and_load` function in `transform.py` converts `models.py` Pydantic objects to Spark DataFrames and applies AL-like business rules. This will eventually be the logic for the **Bronze-to-Silver** transformation. For Unit 2, we are focusing on **API-to-Bronze**. So, `transform.py` is not directly used for the Bronze load of raw API data.
    *   **`pipeline.py`:**
        *   The `extract_daily_data` and `write_daily_data` (which calls `transform_and_load`) will need to be adapted.
        *   `extract_daily_data` could be refactored to call the new `fetch_*_raw` functions and perform the initial Pydantic validation (using `schemas.py`), returning lists of *validated `schemas.py` Pydantic objects* and validation errors.
        *   The SparkDantic conversion and write to Bronze would then happen, perhaps in a new function called by `run_daily` or within the new `bronze_loader.py`.
    *   **Action (Minimal for Unit 2):** For now, focus on the new `bronze_loader.py` (or Notebook). We can integrate its functionality back into `pipeline.py` in a later unit or as part of refining the overall orchestration. The key is to get the API -> Pydantic (`schemas.py`) -> SparkDF (SparkDantic) -> Bronze Delta flow working.

---

**End of Unit 2 - Deliverables/Checkpoints:**

1.  `sparkdantic` installed and usable.
2.  A `bronze_loader.py` module (or Notebook) that can:
    *   Fetch raw data for at least one core entity (e.g., Agreements) using the Unit 1.5 functions.
    *   Validate the raw data against `schemas.py` Pydantic models.
    *   Convert the list of validated `schemas.py` Pydantic objects to a Spark DataFrame using SparkDantic.
    *   Add a `bronze_load_timestamp` column.
    *   Write the resulting DataFrame to a corresponding Bronze Delta table (e.g., `connectwise_bronze/agreements`).
    *   Write any Pydantic validation errors to a `connectwise_bronze/validation_errors` table.
3.  Demonstrate the process for one entity end-to-end locally.
4.  Basic plan/stubs for handling incremental load timestamps (even if the storage mechanism is simple for now).

---

This is a substantial unit. I recommend starting with one entity (e.g., Agreements, since we have good groundwork there) and getting the flow working end-to-end for it in `bronze_loader.py`. Once that's successful, replicate the pattern for the other entities.

I'll be here to help with code snippets, debug SparkDantic usage, or refine the logic as you go! Let's start by drafting the `bronze_loader.py` structure and the logic for one entity.