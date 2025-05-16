# bc_etl/gold/facts/fact_agreement.py
"""
Agreement fact table creation module.
Refactored for clarity, standard PySpark practices,
and consistent configuration loading via config_loader.
"""

from datetime import datetime

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession

from bc_etl.gold.keys import generate_surrogate_key
from bc_etl.utils import logging
from bc_etl.utils.config_utils import extract_join_keys, extract_table_config, find_business_keys
from bc_etl.utils.dataframe_utils import add_audit_columns, year_filter
from bc_etl.utils.decorators import fact_table_handler
from bc_etl.utils.exceptions import BCETLExceptionError, ConfigurationError, FactTableError
from bc_etl.utils.table_utils import read_table_from_config


@fact_table_handler(error_msg="Failed to create agreement fact")
def create_agreement_fact(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,  # Keep for consistency, though less used directly now
    min_year: int | None = None,
    incremental: bool = False,
    last_processed_time: datetime | None = None,
) -> DataFrame:
    """
    Create agreement fact table from header, line, and BOM history tables,
    using configuration loaded via config_loader for table names, join keys,
    and business keys.

    Args:
        spark: Spark session
        silver_path: Path to silver layer (database/schema name)
        gold_path: Path to gold layer (database/schema name)
        min_year: Optional minimum year to include
        incremental: Whether to perform incremental loading
        last_processed_time: Timestamp of last successful processing

    Returns:
        DataFrame containing the agreement fact table base structure.

    Raises:
        ConfigurationError: If required configuration, join keys, or business keys are missing.
        BCETLExceptionError: If processing fails for other reasons.
    """
    with logging.span("create_agreement_fact_std_config"):
        logging.info("Starting agreement fact creation using standard config loader")
        logging.info(f"Incremental: {incremental}, Last processed time: {last_processed_time}")

        # --- 1. Load Configurations using config_utils ---
        header_config = extract_table_config("AMSAgreementHeader")
        line_config = extract_table_config("AMSAgreementLine")
        bom_history_config = extract_table_config("AMSAgreementBOMHistory")

        if not header_config:
            raise ConfigurationError("Missing config: AMSAgreementHeader")
        if not line_config:
            raise ConfigurationError("Missing config: AMSAgreementLine")
        if not bom_history_config:
            raise ConfigurationError("Missing config: AMSAgreementBOMHistory")

        # --- 2. Read tables using table_utils ---
        header_df, header_table = read_table_from_config(
            spark=spark,
            table_base_name="AMSAgreementHeader",
            db_path=silver_path,
            required=True,
        )

        # Ensure header_df is not None before proceeding
        if header_df is None:
            raise FactTableError("Failed to read AMSAgreementHeader table")

        line_df, line_table = read_table_from_config(
            spark=spark,
            table_base_name="AMSAgreementLine",
            db_path=silver_path,
            required=True,
        )

        # Ensure line_df is not None before proceeding
        if line_df is None:
            raise FactTableError("Failed to read AMSAgreementLine table")

        bom_df, bom_table = read_table_from_config(
            spark=spark,
            table_base_name="AMSAgreementBOMHistory",
            db_path=silver_path,
            required=True,
        )

        # Ensure bom_df is not None before proceeding
        if bom_df is None:
            raise FactTableError("Failed to read AMSAgreementBOMHistory table")

        # --- 3. Apply year filter if specified ---
        if min_year is not None and header_df is not None:
            header_df = year_filter(
                df=header_df,
                min_year=min_year,
                date_column="StartingDate",  # Adjust based on actual date column
                year_column="StartingYear",
                add_year_column=True,
            )

        # --- 4. Determine Business Keys using config_utils ---
        header_biz_keys = find_business_keys("AMSAgreementHeader")
        line_biz_keys = find_business_keys("AMSAgreementLine")
        bom_biz_keys = find_business_keys("AMSAgreementBOMHistory")

        if not header_biz_keys:
            raise ConfigurationError("Missing business keys for AMSAgreementHeader")
        if not line_biz_keys:
            raise ConfigurationError("Missing business keys for AMSAgreementLine")

        logging.info(f"Header business keys: {header_biz_keys}")
        logging.info(f"Line business keys: {line_biz_keys}")
        logging.info(f"BOM business keys: {bom_biz_keys}")

        # --- 5. Determine Join Keys using config_utils ---
        line_to_header_keys = extract_join_keys(line_config, "AMSAgreementHeader")
        bom_to_line_keys = extract_join_keys(bom_history_config, "AMSAgreementLine")

        if not line_to_header_keys:
            raise ConfigurationError("Missing join keys from Line to Header")
        if not bom_to_line_keys:
            raise ConfigurationError("Missing join keys from BOM to Line")

        logging.info(f"Line to Header join keys: {line_to_header_keys}")
        logging.info(f"BOM to Line join keys: {bom_to_line_keys}")

        # --- 6. Join Tables ---
        # First, join header and line
        join_conditions = []
        for line_col, header_col in line_to_header_keys:
            join_conditions.append(F.col(f"line.{line_col}") == F.col(f"header.{header_col}"))

        # Add company join condition if available
        company_col = "$Company"  # Default company column name
        if company_col in header_df.columns and company_col in line_df.columns:
            join_conditions.append(F.col(f"line.{company_col}") == F.col(f"header.{company_col}"))

        if not join_conditions:
            raise ConfigurationError("Could not determine join conditions between Line and Header")

        # Join header and line
        header_line_df = line_df.alias("line").join(
            header_df.alias("header"),
            F.reduce(lambda x, y: x & y, join_conditions),
            "inner",
        )

        logging.info(f"Joined header and line tables: {header_line_df.count()} rows")

        # Next, join with BOM history if available
        final_df_joined = header_line_df
        if bom_df is not None and bom_to_line_keys:
            bom_join_conditions = []
            for bom_col, line_col in bom_to_line_keys:
                bom_join_conditions.append(F.col(f"bom.{bom_col}") == F.col(f"line.{line_col}"))

            # Add company join condition if available
            if company_col in bom_df.columns:
                bom_join_conditions.append(
                    F.col(f"bom.{company_col}") == F.col(f"line.{company_col}")
                )

            if bom_join_conditions:
                final_df_joined = header_line_df.join(
                    bom_df.alias("bom"),
                    F.reduce(lambda x, y: x & y, bom_join_conditions),
                    "left",  # Use left join to keep all header-line rows
                )
                logging.info(f"Joined with BOM history: {final_df_joined.count()} rows")
            else:
                logging.warning("No join conditions found for BOM history, skipping join")
        else:
            logging.warning("BOM history data not available, skipping join")

        # --- 7. Define Business Keys for Surrogate Key ---
        # Combine keys from config, ensuring they exist in the final joined DF
        combined_biz_keys = header_biz_keys + line_biz_keys + bom_biz_keys
        business_keys_final = [bk for bk in combined_biz_keys if bk in final_df_joined.columns]

        # Add company if needed and not already present
        # Use our unambiguous CompanyId column instead of the ambiguous $Company
        if "CompanyId" in final_df_joined.columns and "CompanyId" not in business_keys_final:
            business_keys_final.append("CompanyId")

        if not business_keys_final:
            # If still no keys, raise error or handle as needed
            raise ConfigurationError(
                "Could not determine final business keys for surrogate key generation."
            )

        # --- 8. Generate Surrogate Key ---
        try:
            # Pass the list of verified column names
            final_df = generate_surrogate_key(
                final_df_joined,
                business_keys=business_keys_final,
                key_name="AgreementKey",
                company_partition=False,  # Handled by including company_col in keys
            )
            logging.info(f"Generated surrogate keys using business keys: {business_keys_final}")
        except Exception as err:
            raise BCETLExceptionError(f"Failed to generate surrogate keys: {err}") from err

        # --- 9. Select Final Columns and Rename ---
        # Define the columns required for the agreement fact base structure.
        # This selection MUST be explicit and map to your desired output schema.
        required_cols_map = {
            "CompanyId": company_col,  # Use our unambiguous column but map to standard name
            "AgreementKey": "AgreementKey",
            header_biz_keys[0]: "AgreementNo",  # Assumes first header biz key is AgreementNo
            line_biz_keys[0]: "LineNo",  # Assumes first line biz key is LineNo
            "Amount": "LineAmount",  # Standardized name from Line config
            "Quantity": "LineQuantity",  # Standardized name from Line config
            "ItemNo": "BOMItemNo",  # Standardized name from BOM config
            # Add other columns from header, line, bom as needed, using their standardized names
        }

        select_exprs = []
        for source_col, target_alias in required_cols_map.items():
            if source_col in final_df.columns:
                select_exprs.append(F.col(source_col).alias(target_alias))
            else:
                logging.warning(
                    f"Required source column '{source_col}' not found in joined data for final selection. Skipping."
                )

        if not select_exprs:
            raise BCETLExceptionError(
                "Final column selection resulted in an empty list. Check required_cols_map and joined data."
            )

        final_df = final_df.select(*select_exprs)

        # --- 10. Add audit columns ---
        final_df = add_audit_columns(final_df, layer="gold")

        logging.info("Agreement fact base structure created successfully.")
        return final_df
