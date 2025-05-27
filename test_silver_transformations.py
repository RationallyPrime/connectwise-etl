# Test Silver Transformations for ConnectWise Data
# This can be run in a Fabric notebook after Bronze extraction

import yaml
from pyspark.sql.functions import col, current_timestamp
from unified_etl_core.silver.flatten import flatten_nested_columns


def load_silver_config():
    """Load the silver configuration from YAML file."""
    # In production, this would be loaded from the lakehouse
    with open("/lakehouse/default/Files/connectwise_silver_config.yaml") as f:
        return yaml.safe_load(f)


def apply_silver_transformations(spark, entity_name, bronze_df, config):
    """
    Apply Silver layer transformations to a Bronze DataFrame.

    Args:
        spark: SparkSession
        entity_name: Name of the entity (e.g., "Agreement")
        bronze_df: Bronze layer DataFrame
        config: Silver configuration dictionary

    Returns:
        Transformed Silver DataFrame
    """
    entity_config = config["entities"].get(entity_name, {})

    print(f"\nApplying Silver transformations for {entity_name}")
    print(f"Original columns: {len(bronze_df.columns)}")
    print(f"Original row count: {bronze_df.count()}")

    # Step 1: Flatten nested structures
    print("\n1. Flattening nested columns...")
    flattened_df = flatten_nested_columns(bronze_df, max_depth=2)
    print(f"   Columns after flattening: {len(flattened_df.columns)}")

    # Step 2: Apply column mappings (standardization)
    print("\n2. Standardizing column names...")
    column_mappings = entity_config.get("column_mappings", {})
    standardized_df = flattened_df

    for old_name, new_name in column_mappings.items():
        if old_name in standardized_df.columns:
            standardized_df = standardized_df.withColumnRenamed(old_name, new_name)
            print(f"   Renamed: {old_name} -> {new_name}")

    # Step 3: Apply data type conversions
    print("\n3. Converting data types...")
    column_types = entity_config.get("column_types", {})
    typed_df = standardized_df

    for col_name, data_type in column_types.items():
        if col_name in typed_df.columns:
            if data_type == "timestamp":
                typed_df = typed_df.withColumn(col_name, col(col_name).cast("timestamp"))
            elif data_type == "date":
                typed_df = typed_df.withColumn(col_name, col(col_name).cast("date"))
            elif data_type == "integer":
                typed_df = typed_df.withColumn(col_name, col(col_name).cast("int"))
            elif data_type == "decimal":
                typed_df = typed_df.withColumn(col_name, col(col_name).cast("decimal(18,2)"))
            elif data_type == "boolean":
                typed_df = typed_df.withColumn(col_name, col(col_name).cast("boolean"))
            print(f"   Cast {col_name} to {data_type}")

    # Step 4: Add audit columns
    print("\n4. Adding audit columns...")
    silver_df = typed_df.withColumn("SilverCreatedAt", current_timestamp()).withColumn(
        "SilverModifiedAt", current_timestamp()
    )

    print(f"\nFinal Silver columns: {len(silver_df.columns)}")
    print(f"Final row count: {silver_df.count()}")

    return silver_df


# Example usage in notebook:
def process_entity_to_silver(spark, entity_name, extraction_results, config):
    """
    Process a single entity from Bronze to Silver.

    Args:
        spark: SparkSession
        entity_name: Entity name (e.g., "Agreement")
        extraction_results: Results from Bronze extraction
        config: Silver configuration
    """
    if entity_name not in extraction_results:
        print(f"No extraction results for {entity_name}")
        return None

    result = extraction_results[entity_name]
    if not result.get("success"):
        print(f"Bronze extraction failed for {entity_name}")
        return None

    bronze_df = result["df"]

    # Apply Silver transformations
    silver_df = apply_silver_transformations(spark, entity_name, bronze_df, config)

    # Write to Silver table
    silver_table_name = config["entities"][entity_name].get(
        "silver_table", f"silver_{entity_name.lower()}"
    )
    silver_path = f"Tables/{silver_table_name}"

    print(f"\nWriting to Silver table: {silver_path}")
    silver_df.write.mode("overwrite").option("mergeSchema", "true").format("delta").save(
        silver_path
    )

    return silver_df


# Batch process all entities
def process_all_to_silver(spark, extraction_results):
    """Process all extracted entities to Silver layer."""
    # Load configuration
    config = load_silver_config()

    silver_results = {}
    for entity_name in extraction_results.keys():
        print(f"\n{'=' * 60}")
        print(f"Processing {entity_name} to Silver")
        print(f"{'=' * 60}")

        try:
            silver_df = process_entity_to_silver(spark, entity_name, extraction_results, config)
            silver_results[entity_name] = {
                "success": True,
                "df": silver_df,
                "count": silver_df.count() if silver_df else 0,
            }
        except Exception as e:
            print(f"❌ Failed to process {entity_name}: {e!s}")
            silver_results[entity_name] = {"success": False, "error": str(e)}

    # Summary report
    print(f"\n{'=' * 60}")
    print("Silver Layer Processing Summary")
    print(f"{'=' * 60}")
    for entity, result in silver_results.items():
        if result["success"]:
            print(f"✅ {entity}: {result['count']} records")
        else:
            print(f"❌ {entity}: {result['error']}")

    return silver_results


# Example notebook usage:
# silver_results = process_all_to_silver(spark, extraction_results)
