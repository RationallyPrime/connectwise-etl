# bc_etl/silver/dimension.py

import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession

from bc_etl.utils import logging
from bc_etl.utils.config_loader import get_table_config
from bc_etl.utils.exceptions import DimensionResolutionError


def detect_global_dimension_columns(df: DataFrame) -> tuple[str | None, str | None]:
    """
    Detect global dimension columns in a DataFrame.

    Args:
        df: DataFrame to analyze

    Returns:
        Tuple of (GlobalDimension1Code column, GlobalDimension2Code column)
    """
    global_dim1_col = next((col for col in df.columns if "GlobalDimension1Code" in col), None)
    global_dim2_col = next((col for col in df.columns if "GlobalDimension2Code" in col), None)

    logging.debug(
        "Detected global dimension columns",
        global_dim1=global_dim1_col,
        global_dim2=global_dim2_col,
    )

    return global_dim1_col, global_dim2_col


def resolve_dimensions(
    df: DataFrame,
    spark_session: SparkSession,
    dimension_codes: dict[str, str] = {},  # noqa: B006
    silver_path: str = "silver",
) -> DataFrame:
    """
    Resolve dimensions for transactions with global dimension codes.

    Args:
        df: DataFrame with GlobalDimension1Code and GlobalDimension2Code
        spark_session: Active SparkSession
        dimension_codes: Optional dict mapping dimension types to dimension codes. If None, will use config.
        silver_path: Path to silver layer

    Returns:
        DataFrame with resolved dimension values and flags

    Raises:
        DimensionResolutionError: If dimension resolution fails
    """
    try:
        with logging.span("resolve_dimensions"):
            # Get global dimension columns
            global_dim1_col, global_dim2_col = detect_global_dimension_columns(df)

            if not (global_dim1_col or global_dim2_col):
                logging.info("No global dimension columns found, skipping dimension resolution")
                return df

            # If no dimension codes provided, try to get from config
            if not dimension_codes:
                # Try to get dimension codes from DimensionSetup config if it exists
                dim_setup_config = get_table_config("DimensionSetup")
                if dim_setup_config and "dimension_mapping" in dim_setup_config:
                    dimension_codes = dim_setup_config["dimension_mapping"]
                    logging.info("Using dimension codes from configuration", codes=dimension_codes)
                else:
                    logging.warning("No dimension codes provided and no configuration found")
                    dimension_codes = {}

            result_df = df

            # Process TEAM dimension (usually GlobalDimension1)
            if global_dim1_col and "TEAM" in dimension_codes:
                dim_values = spark_session.table(f"{silver_path}.DimensionValue")

                team_code = dimension_codes["TEAM"]
                logging.debug(
                    "Resolving TEAM dimension", dim_code=team_code, source_column=global_dim1_col
                )

                # Filter dimension values for the team code
                team_values = dim_values.filter(f.col("DimensionCode") == team_code)

                # Join with dimension values
                result_df = (
                    result_df.join(
                        team_values.select(
                            f.col("Code").alias("dim_team_code"),
                            f.col("Name").alias("dim_team_name"),
                            f.col("$Company").alias("dim_team_company"),
                        ),
                        (result_df[global_dim1_col] == f.col("dim_team_code"))
                        & (result_df["$Company"] == f.col("dim_team_company")),
                        "left",
                    )
                    .withColumn("TeamCode", f.col("dim_team_code"))
                    .withColumn("TeamName", f.col("dim_team_name"))
                    .drop("dim_team_code", "dim_team_name", "dim_team_company")
                )

            # Process PRODUCT dimension (usually GlobalDimension2)
            if global_dim2_col and "PRODUCT" in dimension_codes:
                dim_values = spark_session.table(f"{silver_path}.DimensionValue")

                product_code = dimension_codes["PRODUCT"]
                logging.debug(
                    "Resolving PRODUCT dimension",
                    dim_code=product_code,
                    source_column=global_dim2_col,
                )

                # Filter dimension values for the product code
                product_values = dim_values.filter(f.col("DimensionCode") == product_code)

                # Join with dimension values
                result_df = (
                    result_df.join(
                        product_values.select(
                            f.col("Code").alias("dim_product_code"),
                            f.col("Name").alias("dim_product_name"),
                            f.col("Indentation").alias("dim_product_indentation"),
                            f.col("$Company").alias("dim_product_company"),
                        ),
                        (result_df[global_dim2_col] == f.col("dim_product_code"))
                        & (result_df["$Company"] == f.col("dim_product_company")),
                        "left",
                    )
                    .withColumn("ProductCode", f.col("dim_product_code"))
                    .withColumn("ProductName", f.col("dim_product_name"))
                    .withColumn("ProductIndentation", f.col("dim_product_indentation"))
                    .drop(
                        "dim_product_code",
                        "dim_product_name",
                        "dim_product_indentation",
                        "dim_product_company",
                    )
                )

            # Add dimension flags
            if "TeamCode" in result_df.columns:
                result_df = result_df.withColumn(
                    "HasTeamDimension",
                    f.when(f.col("TeamCode").isNotNull(), f.lit(True)).otherwise(f.lit(False)),
                )

            if "ProductCode" in result_df.columns:
                result_df = result_df.withColumn(
                    "HasProductDimension",
                    f.when(f.col("ProductCode").isNotNull(), f.lit(True)).otherwise(f.lit(False)),
                )

            return result_df

    except Exception as e:
        error_msg = f"Failed to resolve dimensions: {str(e)}"
        logging.error(error_msg)
        raise DimensionResolutionError(error_msg) from e
