"""
Business Central fact table transformation functions.

Following CLAUDE.md principles:
- ALL parameters required (fail-fast)
- Business logic in integration packages
- Use generic patterns from unified-etl-core
"""

import logging

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from unified_etl_core.utils.base import ErrorCode
from unified_etl_core.utils.decorators import with_etl_error_handling
from unified_etl_core.utils.exceptions import ETLConfigError, ETLProcessingError

from ..config import BC_FACT_CONFIGS
from .gold import join_bc_dimension


@with_etl_error_handling(operation="create_purchase_fact")
def create_purchase_fact(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    batch_id: str,
) -> DataFrame:
    """
    Create Business Central purchase fact table.

    Combines PurchInvLine and PurchCrMemoLine with proper dimension joins.

    Args:
        spark: REQUIRED SparkSession
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer
        batch_id: REQUIRED batch identifier
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not silver_path:
        raise ETLConfigError("silver_path is required", code=ErrorCode.CONFIG_MISSING)
    if not gold_path:
        raise ETLConfigError("gold_path is required", code=ErrorCode.CONFIG_MISSING)
    if not batch_id:
        raise ETLConfigError("batch_id is required", code=ErrorCode.CONFIG_MISSING)

    # Create purchase fact
    logging.info("Creating Business Central purchase fact table")

    # Get fact configuration
    fact_config = BC_FACT_CONFIGS.get("fact_Purchase")
    if not fact_config:
        raise ETLConfigError("fact_Purchase configuration not found", code=ErrorCode.CONFIG_MISSING)

    # Load source entities
    source_dfs = []
    for entity in fact_config["source_entities"]:
                try:
                    df = spark.table(f"{silver_path}.{entity}")
                    # Add entity type for tracking
                    df = df.withColumn("_entity_type", F.lit(entity))
                    source_dfs.append(df)
                    logging.info(f"Loaded {entity}: {df.count()} rows")
                except Exception as e:
                    raise ETLProcessingError(
                        f"Failed to load entity {entity}",
                        code=ErrorCode.DATA_ACCESS_ERROR,
                        details={"entity": entity, "silver_path": silver_path}
                    ) from e

    # Union all purchase line sources
    if not source_dfs:
                raise ETLProcessingError(
                    "No source data found for purchase fact",
                    code=ErrorCode.DATA_NOT_FOUND,
                    details={"fact_type": "purchase"}
                )

    union_df = source_dfs[0]
    for df in source_dfs[1:]:
        union_df = union_df.unionByName(df, allowMissingColumns=True)

    logging.info(f"Combined purchase lines: {union_df.count()} rows")

    # Add calculated columns
    for col_name, expr in fact_config["calculated_columns"].items():
        union_df = union_df.withColumn(col_name, F.expr(expr))

    # Join dimensions using BC-specific logic
    result_df = union_df

    # Join Item dimension - fix the mapping
    if "Item" in fact_config["dimensions"]:
                dim_config = fact_config["dimensions"]["Item"]
                # The fact table has "No" column, dimension has "No" column
                result_df = join_bc_dimension(
                    spark=spark,
                    fact_df=result_df,
                    dimension_name="Item",
                    fact_join_keys=dim_config["join_keys"],  # {"No": "No"}
                    gold_path=gold_path,
                    dimension_config={
                        "business_keys": ["No", "$Company"],
                        "gold_name": "dim_Item"
                    }
                )

    # Join Vendor dimension - fix the mapping
    if "Vendor" in fact_config["dimensions"]:
                dim_config = fact_config["dimensions"]["Vendor"]
                # The fact table has "BuyfromVendorNo" column, dimension has "No" column
                result_df = join_bc_dimension(
                    spark=spark,
                    fact_df=result_df,
                    dimension_name="Vendor",
                    fact_join_keys=dim_config["join_keys"],  # {"BuyfromVendorNo": "No"}
                    gold_path=gold_path,
                    dimension_config={
                        "business_keys": ["No", "$Company"],
                        "gold_name": "dim_Vendor"
                    }
                )

    # Join Date dimension
    if "Date" in fact_config["dimensions"]:
                dim_config = fact_config["dimensions"]["Date"]
                result_df = join_bc_dimension(
                    spark=spark,
                    fact_df=result_df,
                    dimension_name="Date",
                    fact_join_keys=dim_config["join_keys"],
                    gold_path=gold_path,
                    dimension_config={
                        "business_keys": ["DateKey"],
                        "gold_name": "dim_Date"
                    }
                )

    # Join DimensionBridge if available
    if "DimensionBridge" in fact_config["dimensions"]:
        try:
            dim_config = fact_config["dimensions"]["DimensionBridge"]
            result_df = join_bc_dimension(
                spark=spark,
                fact_df=result_df,
                dimension_name="DimensionBridge",
                fact_join_keys=dim_config["join_keys"],
                gold_path=gold_path,
                dimension_config={
                    "business_keys": ["DimensionSetID", "$Company"],
                    "gold_name": "dim_DimensionBridge"
                }
            )
        except Exception as e:
            logging.warning(f"DimensionBridge join failed (may not exist yet): {e}")

    # Add ETL metadata
    result_df = result_df.withColumn("_etl_batch_id", F.lit(batch_id))
    result_df = result_df.withColumn("_etl_processed_at", F.current_timestamp())

    logging.info(f"Purchase fact created: {result_df.count()} rows")
    return result_df


@with_etl_error_handling(operation="create_agreement_fact")
def create_agreement_fact(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    batch_id: str,
) -> DataFrame:
    """
    Create Business Central agreement fact table.

    Joins AMSAgreementHeader with AMSAgreementLine.

    Args:
        spark: REQUIRED SparkSession
        silver_path: REQUIRED path to silver layer
        gold_path: REQUIRED path to gold layer
        batch_id: REQUIRED batch identifier
    """
    if not spark:
        raise ETLConfigError("SparkSession is required", code=ErrorCode.CONFIG_MISSING)
    if not silver_path:
        raise ETLConfigError("silver_path is required", code=ErrorCode.CONFIG_MISSING)
    if not gold_path:
        raise ETLConfigError("gold_path is required", code=ErrorCode.CONFIG_MISSING)
    if not batch_id:
        raise ETLConfigError("batch_id is required", code=ErrorCode.CONFIG_MISSING)

    # Create agreement fact
    logging.info("Creating Business Central agreement fact table")

    # Get fact configuration
    fact_config = BC_FACT_CONFIGS.get("fact_Agreement")
    if not fact_config:
        raise ETLConfigError("fact_Agreement configuration not found", code=ErrorCode.CONFIG_MISSING)

    # Load header and line entities
    try:
        header_df = spark.table(f"{silver_path}.{fact_config['header_entity']}")
        line_df = spark.table(f"{silver_path}.{fact_config['source_entities'][0]}")
        logging.info(f"Loaded headers: {header_df.count()}, lines: {line_df.count()}")
    except Exception as e:
        raise ETLProcessingError(
            "Failed to load agreement entities",
            code=ErrorCode.DATA_ACCESS_ERROR,
            details={
                "header_entity": fact_config.get('header_entity'),
                "line_entity": fact_config.get('source_entities', [None])[0],
                "silver_path": silver_path
            }
        ) from e

    # Join header to lines with proper mapping
    join_config = fact_config["join_config"]["header_to_line"]
    join_conditions = []

    # Map Header.No to Line.DocumentNo
    for header_col, line_col in join_config.items():
        if header_col in header_df.columns and line_col in line_df.columns:
            join_conditions.append(header_df[header_col] == line_df[line_col])
        else:
            raise ETLConfigError(
                f"Join columns not found: {header_col} in header or {line_col} in line",
                code=ErrorCode.COLUMN_NOT_FOUND,
                details={"header_col": header_col, "line_col": line_col}
            )

    # Add $Company join if both have it
    if "$Company" in header_df.columns and "$Company" in line_df.columns:
        join_conditions.append(header_df["$Company"] == line_df["$Company"])

    # Perform the join
    if not join_conditions:
        raise ETLConfigError(
            "No valid join conditions for agreement header to line",
            code=ErrorCode.CONFIG_INVALID,
            details={"join_config": join_config}
        )

    join_condition = join_conditions[0]
    for condition in join_conditions[1:]:
        join_condition = join_condition & condition

    result_df = line_df.join(header_df, join_condition, "inner")
    logging.info(f"Joined agreement data: {result_df.count()} rows")

    # Add calculated columns
    for col_name, expr in fact_config["calculated_columns"].items():
        result_df = result_df.withColumn(col_name, F.expr(expr))

    # Join dimensions
    for dim_name, dim_config in fact_config["dimensions"].items():
        if dim_name == "Customer":
                    result_df = join_bc_dimension(
                        spark=spark,
                        fact_df=result_df,
                        dimension_name="Customer",
                        fact_join_keys=dim_config["join_keys"],
                        gold_path=gold_path,
                        dimension_config={
                            "business_keys": ["No", "$Company"],
                            "gold_name": "dim_Customer"
                        }
                    )
        elif dim_name == "Date":
                    result_df = join_bc_dimension(
                        spark=spark,
                        fact_df=result_df,
                        dimension_name="Date",
                        fact_join_keys=dim_config["join_keys"],
                        gold_path=gold_path,
                        dimension_config={
                            "business_keys": ["DateKey"],
                            "gold_name": "dim_Date"
                        }
                    )

    # Add ETL metadata
    result_df = result_df.withColumn("_etl_batch_id", F.lit(batch_id))
    result_df = result_df.withColumn("_etl_processed_at", F.current_timestamp())

    logging.info(f"Agreement fact created: {result_df.count()} rows")
    return result_df
