from pyspark.sql import DataFrame, SparkSession

from unified_etl.utils import logging
from unified_etl.utils.exceptions import AnalysisException


def write_with_schema_conflict_handling(
    df: DataFrame,
    table_path: str,
    write_mode: str = "overwrite",
    partition_by: list[str] | None = None,
    post_write_sql: str | None = None,
    spark: SparkSession | None = None,
):
    """
    Write a DataFrame to a Delta table with advanced schema conflict handling.

    This function handles various Delta Lake schema evolution issues, including:
    - Failed to merge incompatible schemas
    - Partition column type conflicts
    - Column type mismatches

    Args:
        df: DataFrame to write
        table_path: Path to Delta table
        write_mode: Write mode (append, overwrite, etc.)
        partition_by: List of columns to partition by
        post_write_sql: Optional SQL to execute after write
        spark: Optional SparkSession (will use active session if None)

    Raises:
        Exception: If write fails after conflict resolution attempts
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise ValueError("No active Spark session found")

    # Check if table exists using Spark Catalog API
    table_exists = False
    try:
        # Use spark.catalog.tableExists for better reliability
        table_exists = spark.catalog.tableExists(table_path)
        if table_exists:
            logging.info(f"Table {table_path} exists.")
        else:
            logging.info(f"Table {table_path} does not exist, will create.")
    except Exception as e:
        logging.warning(f"Error checking table existence for {table_path}: {e!s}")
        # Assume doesn't exist if check fails unexpectedly

    # Set write options
    write_options = {"mergeSchema": "true"}
    if table_exists and write_mode == "overwrite":
        write_options["overwriteSchema"] = "true"
        logging.info(f"Setting overwriteSchema=true for overwrite on {table_path}")

    # Ensure partition columns are escaped correctly if they contain special chars
    escaped_partition_columns = []
    if partition_by:
        escaped_partition_columns = [
            f"`{col.strip('`')}`"
            if "$" in col or "." in col or "-" in col or any(c in col for c in "[]().,;:")
            else col
            for col in partition_by
        ]

    try:
        writer = df.write.format("delta").options(**write_options).mode(write_mode)

        if escaped_partition_columns:
            writer = writer.partitionBy(*escaped_partition_columns)

        # Use saveAsTable for better catalog integration in Fabric
        writer.saveAsTable(table_path)

        logging.info(f"Successfully wrote to {table_path} with mode {write_mode}")

        if post_write_sql:
            try:
                spark.sql(post_write_sql)
                logging.info(f"Executed post-write SQL: {post_write_sql}")
            except Exception as sql_e:
                logging.error(f"Failed to execute post-write SQL on {table_path}: {sql_e!s}")
                # Decide if this should be a critical failure

    except Exception as e:
        error_msg = str(e)
        logging.error(f"Initial write attempt failed for {table_path}: {error_msg}")

        # --- Conflict Resolution ---
        needs_recreate = False
        # Check for common conflict indicators
        if (
            "DELTA_FAILED_TO_MERGE_FIELDS" in error_msg
            or "Partition columns do not match" in error_msg
            or "Schema mismatch detected" in error_msg
            or "_LEGACY_ERROR_TEMP_DELTA_0007" in error_msg
            or "HTTP 400 Bad Request" in error_msg
        ):  # Handle the HTTP error here too
            logging.warning(f"Detected schema/partition/connection conflict for {table_path}.")
            needs_recreate = True

        # Don't try to recreate if it's an append operation failing - schema must match exactly
        if write_mode == "append" and needs_recreate:
            logging.error(
                f"Schema conflict detected during APPEND to {table_path}. Incoming schema must match target. Failing write.",
                incoming_schema=df.schema.simpleString(),
            )
            # Optionally describe target schema
            try:
                target_schema = spark.table(table_path).schema.simpleString()
                logging.error(f"Target table schema: {target_schema}")
            except AnalysisException:
                logging.error("Could not retrieve target table schema.")
            raise Exception(
                f"Schema mismatch during append to {table_path}. Fix source DataFrame schema."
            ) from e

        if needs_recreate and write_mode == "overwrite":
            logging.warning(f"Attempting DROP and recreate for {table_path} due to conflict.")
            try:
                # Use Fabric-compatible DROP syntax (no CASCADE needed usually)
                # Escape the table path for DROP command
                escaped_drop_path = ".".join([f"`{part}`" for part in table_path.split(".")])
                spark.sql(f"DROP TABLE IF EXISTS {escaped_drop_path}")
                logging.info(f"Dropped table {table_path}, attempting rewrite...")

                # Retry write with overwriteSchema explicitly true
                writer = (
                    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
                )
                if escaped_partition_columns:
                    writer = writer.partitionBy(*escaped_partition_columns)
                writer.saveAsTable(table_path)  # Use saveAsTable again

                logging.info(f"Successfully recreated and wrote to {table_path} after conflict.")
                # Execute post-write SQL if provided (needed after recreate too)
                if post_write_sql:
                    try:
                        spark.sql(post_write_sql)
                        logging.info(f"Executed post-write SQL after recreate: {post_write_sql}")
                    except Exception as sql_e:
                        logging.error(
                            f"Failed to execute post-write SQL on recreated {table_path}: {sql_e!s}"
                        )

                return  # Success after recreate

            except Exception as recreate_e:
                logging.error(
                    f"Failed to recreate/rewrite table {table_path} after conflict: {recreate_e!s}"
                )
                # Raise a more specific error after failing the recreate attempt
                raise Exception(
                    f"Failed to write to {table_path} even after attempting recreate: {recreate_e}"
                ) from recreate_e

        # If conflict resolution didn't apply or failed, raise the original error
        raise Exception(f"Failed to write to {table_path}. Initial error: {error_msg}") from e
