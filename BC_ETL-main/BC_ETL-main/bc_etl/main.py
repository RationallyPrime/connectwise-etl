# bc_etl/main.py
"""
Main orchestration module for BC ETL processing.
Handles bronze to silver and silver to gold transformations.
"""

import argparse
import sys
from datetime import datetime

from pyspark.sql import SparkSession

from bc_etl.gold.dimensions import generate_date_dimension
from bc_etl.pipeline import (
    bronze_to_silver_transformation,
    silver_to_gold_transformation,
    write_with_schema_conflict_handling,
)
from bc_etl.utils import logging
from bc_etl.utils.config_loader import get_all_table_configs, set_config_path
from bc_etl.utils.exceptions import BCETLExceptionError
from bc_etl.utils.naming import standardize_table_reference


def process_bc_tables(
    spark: SparkSession,
    bronze_path: str,
    silver_path: str,
    gold_path: str,
    table_types: list[str] | None = None,
    specific_tables: list[str] | None = None,
    include_gold: bool = True,
    run_params: dict | None = None,
) -> None:
    """
    Process BC tables through bronze → silver → gold layers.

    Args:
        spark: SparkSession to use
        bronze_path: Path to bronze layer (e.g., "Database.Schema")
        silver_path: Path to silver layer (e.g., "Database.Schema")
        gold_path: Path to gold layer (e.g., "Database.Schema")
        table_types: Optional list of table types to process (e.g., ['master_data'])
        specific_tables: Optional list of specific tables to process (using actual bronze names or standardized base names)
        include_gold: Whether to include gold layer transformations after silver processing.
        run_params: Optional dictionary of run parameters (e.g., {"full_refresh": True})
    """
    start_time = datetime.now()
    run_id = f"bc_etl_{start_time.strftime('%Y%m%d_%H%M%S')}"

    logging.info(
        "Starting BC ETL processing",
        run_id=run_id,
        bronze_path=bronze_path,
        silver_path=silver_path,
        gold_path=gold_path,
        include_gold=include_gold,
        specific_tables=specific_tables,
        table_types_filter=table_types,
    )

    # Track processed tables
    processed_silver_tables = 0
    failed_silver_tables = 0

    try:
        # Load all table configs once
        all_configs = get_all_table_configs()
        if not all_configs:
            logging.error("Failed to load any table configurations. Aborting.")
            return  # Cannot proceed without configs

        logging.info(f"Loaded {len(all_configs)} table configurations.")

        # --- Determine Processing Order ---
        processing_order = []
        config_table_types = set()
        for config in all_configs.values():
            if isinstance(config, dict) and "type" in config:
                config_table_types.add(config["type"])

        # Define the preferred processing order
        preferred_order = [
            "dimension_framework",
            "master_data",
            "ledger_entries",
            "transaction_headers",
            "transaction_lines",
            "setup_config",  # Add any other known types
            "system_internal",
            "budget_data",
            "translation",
        ]

        for type_in_order in preferred_order:
            if type_in_order in config_table_types:
                processing_order.append(type_in_order)
                config_table_types.remove(type_in_order)
        processing_order.extend(list(config_table_types))  # Add any remaining types
        logging.info(f"Determined processing order: {processing_order}")

        # --- List Actual Bronze Tables ---
        try:
            bronze_tables_df = spark.sql(f"SHOW TABLES IN {bronze_path}")
            actual_bronze_tables_in_path = [row.tableName for row in bronze_tables_df.collect()]
            logging.info(
                f"Found {len(actual_bronze_tables_in_path)} actual tables in {bronze_path}"
            )
        except Exception as e:
            logging.error(
                f"CRITICAL: Failed to list tables in Bronze path {bronze_path}: {e}. Aborting."
            )
            raise BCETLExceptionError(f"Cannot list Bronze tables at {bronze_path}") from e

        # --- Process Tables by Type ---
        for table_type in processing_order:
            # Apply type filter if provided
            if table_types and table_type not in table_types:
                logging.debug(f"Skipping type '{table_type}' due to table_types filter.")
                continue

            logging.info(f"--- Processing Type: {table_type} ---")

            # Filter actual tables found in Bronze based on config type and specific_tables list
            tables_to_process_for_type = []
            for actual_table_name in actual_bronze_tables_in_path:
                # Standardize the actual table name found in Bronze
                standardized_base_name = standardize_table_reference(
                    actual_table_name, include_suffix=False
                )
                # Look up config using the standardized name
                table_config = all_configs.get(standardized_base_name)

                # Check 1: Does a config exist for this standardized name?
                # Check 2: Does the config type match the current loop's type?
                if table_config and table_config.get("type") == table_type:
                    # Check 3: Apply specific_tables filter (if provided)
                    should_process = True
                    if specific_tables:  # noqa: SIM102
                        # Allow match on either the actual name or the standardized name
                        if (
                            actual_table_name not in specific_tables
                            and standardized_base_name not in specific_tables
                        ):
                            should_process = False
                            logging.debug(
                                f"Skipping '{actual_table_name}' (standardized: '{standardized_base_name}') due to specific_tables filter."
                            )

                    if should_process:
                        tables_to_process_for_type.append(
                            actual_table_name
                        )  # Add the *actual* name found in Bronze

            logging.info(
                f"Found {len(tables_to_process_for_type)} '{table_type}' tables matching criteria: {tables_to_process_for_type}"
            )

            # Process the correctly identified tables for this type
            for actual_table_name_to_process in tables_to_process_for_type:
                try:
                    logging.info(
                        f"Processing Bronze->Silver for table: {actual_table_name_to_process}"
                    )

                    # Process bronze to silver, passing the *actual* table name
                    success, _ = bronze_to_silver_transformation(
                        spark=spark,
                        bronze_path=bronze_path,  # Pass the FULL bronze_path
                        silver_db=silver_path,  # Silver path still needs DB part for target table name construction
                        table_name=actual_table_name_to_process,  # Pass the actual name
                        is_full_refresh=(run_params or {}).get("full_refresh", False),
                    )
                    if success:
                        processed_silver_tables += 1
                    else:
                        failed_silver_tables += (
                            1  # Increment failure count if transform returns False
                        )

                except Exception as e:
                    failed_silver_tables += 1
                    logging.error(
                        f"Failed during Bronze->Silver for table {actual_table_name_to_process}",
                        table_type=table_type,
                        error=str(e),
                        exc_info=True,  # Include traceback in log
                    )
                    # Continue with the next table even if one fails

        # --- Process Gold Layer (if requested) ---
        if include_gold:
            logging.info("--- Starting Gold Layer Processing ---")
            # The process_gold_layer function will handle its own config loading and processing logic
            # It needs to be aware of which Silver tables were successfully created/updated if it needs dependencies.
            # However, the current implementation seems to process based on config entries having a 'gold_target'.
            global_config = all_configs.get("global_settings", {})
            process_gold_layer(
                spark=spark,
                silver_path=silver_path,
                gold_path=gold_path,
                fiscal_year_start=global_config.get("fiscal_year_start", 1),
                min_year=global_config.get("min_year"),
                # Pass incremental flag based on run_params if needed by gold processing logic
                incremental=(run_params or {}).get("incremental", True),
                run_params=run_params,
                # specific_phase might be useful later but not used now based on process_gold_layer logic shown
            )
            logging.info("--- Gold Layer Processing Attempted ---")
        else:
            logging.info("Skipping Gold layer processing as per request.")

        # --- Log Completion Summary ---
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        logging.info(
            "BC ETL processing finished.",
            run_id=run_id,
            duration_seconds=duration_seconds,
            silver_tables_processed_successfully=processed_silver_tables,
            silver_tables_failed=failed_silver_tables,
            gold_included=include_gold,
        )
        if failed_silver_tables > 0:
            logging.warning(
                f"{failed_silver_tables} tables failed during Bronze->Silver processing. Check logs for details."
            )

    except Exception as e:
        # Log critical failure for the entire process
        logging.critical(
            "BC ETL processing failed with unhandled exception.",
            run_id=run_id,
            error=str(e),
            exc_info=True,
        )
        raise  # Re-raise the exception after logging


def process_gold_layer(
    spark: SparkSession,
    silver_path: str,
    gold_path: str,
    fiscal_year_start: int = 1,
    min_year: int | None = None,
    incremental: bool = False,
    specific_phase: str | None = None,
    run_params: dict | None = None,
) -> None:
    """
    Process the gold layer with proper dependencies and table order.

    Args:
        spark: SparkSession to use
        silver_path: Path to silver layer
        gold_path: Path to gold layer
        fiscal_year_start: Fiscal year starting month (1-12)
        min_year: Optional minimum year to include for fact tables
        incremental: Whether to perform incremental loading
        specific_phase: Optional phase to process ('date_dimension', 'master_dimensions',
                        'dimension_bridge', 'core_facts', 'agreement_fact')
        run_params: Optional dictionary of run parameters
    """
    run_id = f"gold_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logging.info(
        "Starting gold layer processing",
        run_id=run_id,
        incremental=incremental,
        specific_phase=specific_phase,
    )

    # Get last processing time for incremental loads
    last_processed_time = None
    if incremental:
        try:
            # Try to get the last processing time from metadata table
            metadata_df = spark.sql(
                f"SELECT MAX(processing_time) as last_run FROM {gold_path}.etl_metadata WHERE layer = 'gold'"
            )
            last_time = metadata_df.first()
            if last_time and last_time["last_run"] is not None:
                last_processed_time = last_time["last_run"]
                logging.info(f"Found last processing time: {last_processed_time}")
            else:
                logging.info("No previous processing time found, using default lookback period")
        except Exception as e:
            logging.warning(f"Could not retrieve last processing time: {str(e)}")

    try:
        # Load all table configs
        all_configs = get_all_table_configs()
        global_settings = all_configs.get("global_settings", {})

        # Get fiscal year start from config if not provided
        if "fiscal_year_start" in global_settings and fiscal_year_start == 1:
            fiscal_year_start = global_settings.get("fiscal_year_start")
            logging.info(f"Using fiscal year start from config: {fiscal_year_start}")

        # Get min year from config if not provided
        if "min_year" in global_settings and min_year is None:
            min_year = global_settings.get("min_year")
            logging.info(f"Using min year from config: {min_year}")

        # Define the gold processing phases
        phases = {
            "date_dimension": [],
            "master_dimensions": [],
            "dimension_bridge": ["DimensionValue"],
            "core_facts": [],
            "agreement_fact": ["AMSAgreementLine"],
        }

        # Populate master dimensions and core facts from config
        for table_name, config in all_configs.items():
            # Check config is a dict and has type before accessing keys
            if isinstance(config, dict) and "type" in config:
                # Check for master_data type AND gold_name key (or gold_target fallback)
                if config.get("type") == "master_data" and (
                    config.get("gold_name") or config.get("gold_target")
                ):
                    phases["master_dimensions"].append(table_name)
                # Check for relevant fact types AND gold_name key (or gold_target fallback)
                elif config.get("type") in [
                    "ledger_entries",
                    "transaction_lines",
                    "transaction_headers",
                ] and (config.get("gold_name") or config.get("gold_target")):
                    # Ensure Agreement tables are handled correctly if they are part of a combined fact
                    if "AMSAgreement" in table_name:
                        # Add only one trigger for the combined agreement fact (e.g., based on the line table)
                        if table_name == "AMSAgreementLine" and "agreement_fact" not in phases:
                            phases["agreement_fact"] = [table_name]  # Use line table as trigger
                    elif (
                        table_name not in phases["core_facts"]
                    ):  # Avoid duplicates if multiple configs point to same fact
                        phases["core_facts"].append(table_name)

        # Phase 1: Create Date Dimension
        if specific_phase is None or specific_phase == "date_dimension":
            logging.info("Processing date dimension phase")
            try:
                from datetime import date

                # Define start and end dates (adjust as needed or pull from config)
                # Example: Use min_year from config/params if available, else default
                start_year = min_year if min_year else datetime.now().year - 5
                start_dt = date(start_year, 1, 1)
                end_dt = date(datetime.now().year + 5, 12, 31)  # 5 years ahead

                logging.info(
                    f"Generating date dimension from {start_dt} to {end_dt} with fiscal start month {fiscal_year_start}"
                )

                date_df = generate_date_dimension(
                    spark=spark,
                    start_date=start_dt,
                    end_date=end_dt,
                    fiscal_year_start_month=fiscal_year_start,
                )

                # Ensure we're using the full path for the table
                # This should match how tables are referenced in the rest of the codebase
                dim_date_target = f"{gold_path}.dim_Date"  # Use the full gold_path
                logging.info(f"Writing Date Dimension to {dim_date_target}")

                # Use the robust write function
                write_with_schema_conflict_handling(
                    df=date_df,
                    table_path=dim_date_target,
                    write_mode="overwrite",  # Date dimension is always overwritten
                    spark=spark,
                )
                logging.info(
                    f"Successfully generated and wrote date dimension to {dim_date_target}"
                )

            except Exception as e:
                logging.error(
                    f"Failed to process date dimension: {str(e)}", exc_info=True
                )  # Add traceback
            # --- Keep the specific_phase check below ---
            if specific_phase == "date_dimension":
                logging.info("Completed specific phase: date_dimension")
                return

        # Process each phase
        for phase_name in ["master_dimensions", "dimension_bridge", "core_facts", "agreement_fact"]:
            if specific_phase is None or specific_phase == phase_name:
                logging.info(f"Processing {phase_name} phase")
                # Process each table in the phase
                for table_name in phases[phase_name]:
                    try:
                        # Check if the silver table exists before attempting to process it
                        silver_table_path = f"{silver_path}.{table_name}"
                        table_exists = True
                        try:
                            spark.table(silver_table_path)
                        except Exception as e:
                            if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
                                table_exists = False
                                logging.warning(
                                    f"Silver table {silver_table_path} does not exist, skipping"
                                )
                            else:
                                raise

                        if table_exists:
                            # Process the table
                            silver_to_gold_transformation(
                                spark=spark,
                                silver_db=silver_path,
                                gold_db=gold_path,
                                table_name=table_name,
                                is_full_refresh=not incremental,
                            )
                    except Exception as e:
                        logging.error(
                            f"Failed to process {table_name} in {phase_name} phase: {str(e)}"
                        )
                        # Continue with other tables in the phase
                        continue

                if specific_phase == phase_name:
                    logging.info(f"Completed specific phase: {phase_name}")
                    return

        # Phase 5: Create Reconciliation Views
        # TODO: Implement reconciliation views in a future update
        # try:
        #     create_reconciliation_views(spark=spark, gold_path=gold_path, run_id=run_id)
        # except Exception as e:
        #     logging.error(f"Failed to create reconciliation views: {str(e)}")

        # Log completion with success metrics
        gold_tables_count = len(spark.sql(f"SHOW TABLES IN {gold_path}").collect())
        logging.info(
            "Gold layer processing complete",
            run_id=run_id,
            gold_tables_created=gold_tables_count,
            dimensions_processed=len(phases["master_dimensions"])
            if specific_phase is None or specific_phase == "master_dimensions"
            else 0,
            facts_processed=len(phases["core_facts"])
            if specific_phase is None or specific_phase == "core_facts"
            else 0,
            incremental=incremental,
        )

        # Update metadata table with processing time
        if incremental:
            try:
                # Create metadata table if it doesn't exist
                spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {gold_path}.etl_metadata (
                    layer STRING,
                    run_id STRING,
                    processing_time TIMESTAMP,
                    success BOOLEAN,
                    details STRING
                )
                """)

                # Insert processing record
                spark.sql(f"""
                INSERT INTO {gold_path}.etl_metadata
                VALUES (
                    'gold',
                    '{run_id}',
                    current_timestamp(),
                    true,
                    'Incremental processing completed successfully'
                )
                """)

                logging.info("Updated metadata table with processing time")
            except Exception as e:
                logging.warning(f"Failed to update metadata table: {str(e)}")

    except Exception as e:
        error_msg = f"Gold layer processing failed: {str(e)}"
        logging.critical(error_msg, run_id=run_id)
        raise


def main() -> None:
    """
    Main entry point for BC ETL processing.
    """
    # Set up logging
    logging.configure(level="INFO", use_logfire=True)

    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(description="Business Central ETL Pipeline")
        parser.add_argument(
            "--bronze-path", type=str, default="LH.bronze", help="Path to bronze layer"
        )
        parser.add_argument(
            "--silver-path", type=str, default="LH.silver", help="Path to silver layer"
        )
        parser.add_argument("--gold-path", type=str, default="LH.gold", help="Path to gold layer")
        parser.add_argument("--table-types", type=str, nargs="+", help="Table types to process")
        parser.add_argument(
            "--specific-tables", type=str, nargs="+", help="Specific tables to process"
        )
        parser.add_argument(
            "--include-gold",
            action="store_true",
            default=True,
            help="Include gold layer transformations",
        )
        parser.add_argument(
            "--full-refresh",
            action="store_true",
            help="Perform full refresh instead of incremental",
        )
        parser.add_argument("--config-path", type=str, help="Path to custom config file")

        args = parser.parse_args()

        # Load configuration
        if args.config_path:
            # Set custom config path if provided
            set_config_path(args.config_path)
            logging.info(f"Using custom config path: {args.config_path}")

        # Create Spark session
        spark = (
            SparkSession.builder.appName("BC_ETL")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )

        # Instrument Spark job
        logging.instrument_spark_job(spark, "BC_ETL")

        # Prepare run parameters
        run_params = {
            "full_refresh": args.full_refresh,
            "incremental": not args.full_refresh,
        }

        # Process tables with both bronze-silver and silver-gold transformations
        process_bc_tables(
            spark=spark,
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
            gold_path=args.gold_path,
            table_types=args.table_types,
            specific_tables=args.specific_tables,
            include_gold=args.include_gold,
            run_params=run_params,
        )

        # Exit successfully
        sys.exit(0)

    except BCETLExceptionError as e:
        logging.critical(f"BC ETL failed with error: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logging.critical(f"Unexpected error: {str(e)}")
        sys.exit(2)


if __name__ == "__main__":
    main()
