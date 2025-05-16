# bc_etl/gold/hierarchy.py


import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

from bc_etl.utils import logging
from bc_etl.utils.exceptions import HierarchyBuildError


def build_account_hierarchy(
    df: DataFrame,
    indentation_col: str | None = None,
    no_col: str = "No",
    surrogate_key_col: str | None = None,
) -> DataFrame:
    """
    Build a parent-child hierarchy for accounts based on indentation level using Spark window functions.

    Args:
        df: DataFrame containing account data
        indentation_col: Column containing indentation level (auto-detected if None)
        no_col: Column containing account number
        surrogate_key_col: Column containing surrogate key (auto-detected if None)

    Returns:
        DataFrame with parent-child relationships (ParentKey, IsParent, HierarchyLevel, NodePath)
    """
    try:
        with logging.span("build_account_hierarchy"):
            # Auto-detect columns if not provided
            if surrogate_key_col is None:
                # Find surrogate key column (ends with 'Key')
                key_cols = [col for col in df.columns if col.endswith("Key")]
                if not key_cols:
                    raise ValueError("No surrogate key column found (ending with 'Key')")
                surrogate_key_col = key_cols[0]
                logging.info(f"Auto-detected surrogate key column: {surrogate_key_col}")

            if indentation_col is None:
                # Find indentation column (contains 'indent' or 'Indent')
                indent_cols = [col for col in df.columns if "indent" in col.lower()]
                if not indent_cols:
                    raise ValueError("No indentation column found")
                indentation_col = indent_cols[0]
                logging.info(f"Auto-detected indentation column: {indentation_col}")

            # Define company column
            company_col = "$Company"

            # Verify the no_col exists in the DataFrame
            if no_col not in df.columns:
                # Try to find a suitable account number column
                potential_no_cols = [
                    col
                    for col in df.columns
                    if col.lower() in ["no", "no2", "accountno", "account_no"]
                ]
                if potential_no_cols:
                    no_col = potential_no_cols[0]
                    logging.info(f"Using {no_col} as the account number column")
                else:
                    raise ValueError(f"Account number column '{no_col}' not found in DataFrame")

            # Ensure indentation is numeric
            df_processed = df.withColumn(
                "indentation_numeric",
                F.when(
                    F.col(indentation_col).cast("int").isNotNull(),
                    F.col(indentation_col).cast("int"),
                ).otherwise(F.lit(0)),
            )

            # Create a window partitioned by company and ordered by account number
            window_order = Window.partitionBy(company_col).orderBy(no_col)

            # Add row numbers for reference
            df_with_row_num = df_processed.withColumn("row_num", F.row_number().over(window_order))

            # Create a window partitioned by company and ordered by row_num
            # This is crucial for finding parents in the correct order
            Window.partitionBy(company_col).orderBy("row_num")

            # Create a window for looking backwards within the same company
            window_unbounded = (
                Window.partitionBy(company_col)
                .orderBy("row_num")
                .rowsBetween(Window.unboundedPreceding, Window.currentRow)
            )

            # Find potential parent rows by creating an array of all previous rows' data
            df_with_prev = df_with_row_num.withColumn(
                "prev_rows",
                F.collect_list(F.struct("row_num", "indentation_numeric", surrogate_key_col)).over(
                    window_unbounded
                ),
            )

            # Define a UDF to find the parent key based on indentation
            @F.udf(returnType=LongType())
            def find_parent_key(current_indent, prev_rows):
                if current_indent <= 0 or not prev_rows:
                    return None

                # Look for the closest previous row with indentation level one less
                target_indent = current_indent - 1
                for i in range(len(prev_rows) - 2, -1, -1):  # Skip the current row
                    row = prev_rows[i]
                    if row.indentation_numeric == target_indent:
                        return row[2]  # Return the surrogate key
                return None

            # Apply the UDF to find parent keys
            df_with_parent = df_with_prev.withColumn(
                "ParentKey", find_parent_key(F.col("indentation_numeric"), F.col("prev_rows"))
            )

            # Create a view to identify parent accounts
            df_with_parent.createOrReplaceTempView("account_hierarchy")

            # Find all keys that are parents (appear as ParentKey for any row)
            parent_keys_df = df_with_parent.sparkSession.sql("""
                SELECT DISTINCT ParentKey
                FROM account_hierarchy
                WHERE ParentKey IS NOT NULL
            """)

            # Join back to identify which accounts are parents
            df_with_is_parent = df_with_parent.join(
                parent_keys_df.withColumnRenamed("ParentKey", "is_parent_key"),
                df_with_parent[surrogate_key_col] == F.col("is_parent_key"),
                "left",
            )

            # Mark accounts as parents if they appear in the parent keys list
            df_with_is_parent = df_with_is_parent.withColumn(
                "IsParent",
                F.when(F.col("is_parent_key").isNotNull(), F.lit(True)).otherwise(F.lit(False)),
            )

            # Add hierarchy level from indentation
            df_with_hierarchy = df_with_is_parent.withColumn(
                "HierarchyLevel", F.col("indentation_numeric")
            )

            # Add node path for visualization
            df_final = df_with_hierarchy.withColumn(
                "NodePath",
                F.when(F.col("IsParent"), F.concat(F.col(no_col), F.lit("→*"))).otherwise(
                    F.col(no_col)
                ),
            )

            # Clean up temporary columns
            result_df = df_final.drop(
                "row_num", "prev_rows", "is_parent_key", "indentation_numeric"
            )

            # Log hierarchy statistics
            parent_count = result_df.filter(F.col("IsParent")).count()
            child_count = result_df.filter(F.col("ParentKey").isNotNull()).count()
            total_count = result_df.count()

            logging.info(
                f"Account hierarchy built successfully: {parent_count} parents, "
                f"{child_count} children, {total_count} total accounts"
            )

            return result_df

    except Exception as e:
        error_msg = f"Failed to build account hierarchy: {str(e)}"
        logging.error(error_msg)
        raise HierarchyBuildError(error_msg) from e
