"""ConnectWise-specific dimension configurations.

Defines which columns should become dimensions for ConnectWise data.
"""

# Configuration for ConnectWise dimensions
CONNECTWISE_DIMENSION_CONFIG = [
    # Time entry dimensions
    ("Lakehouse.silver.silver_cw_timeentry", "billableOption", "billable_status"),
    ("Lakehouse.silver.silver_cw_timeentry", "chargeToType", "charge_type"),
    ("Lakehouse.silver.silver_cw_timeentry", "status", "time_status"),
    ("Lakehouse.silver.silver_cw_timeentry", "workTypeId", "work_type"),
    ("Lakehouse.silver.silver_cw_timeentry", "workRoleId", "work_role"),
    ("Lakehouse.silver.silver_cw_timeentry", "departmentId", "department"),
    ("Lakehouse.silver.silver_cw_timeentry", "businessUnitId", "business_unit"),
    # Agreement dimensions
    ("Lakehouse.silver.silver_cw_agreement", "agreementStatus", "agreement_status"),
    ("Lakehouse.silver.silver_cw_agreement", "typeId", "agreement_type"),  # The misused billable tracker!
    ("Lakehouse.silver.silver_cw_agreement", "billingCycleId", "billing_cycle"),
    # Invoice dimensions
    ("Lakehouse.silver.silver_cw_invoice", "type", "invoice_type"),
    ("Lakehouse.silver.silver_cw_invoice", "statusId", "invoice_status"),
    ("Lakehouse.silver.silver_cw_invoice", "applyToType", "invoice_apply_type"),
    # Product dimensions
    ("Lakehouse.silver.silver_cw_productitem", "productClass", "product_class"),
    ("Lakehouse.silver.silver_cw_productitem", "billableOption", "product_billable_status"),
    # Expense dimensions
    ("Lakehouse.silver.silver_cw_expenseentry", "status", "expense_status"),
    ("Lakehouse.silver.silver_cw_expenseentry", "chargeToType", "expense_charge_type"),
    ("Lakehouse.silver.silver_cw_expenseentry", "billableOption", "expense_billable_status"),
    ("Lakehouse.silver.silver_cw_expenseentry", "classificationId", "expense_classification"),
]


def refresh_connectwise_dimensions(spark):
    """Refresh all ConnectWise dimension tables."""
    import logging
    from datetime import datetime

    from unified_etl_core.dimensions import create_all_dimensions

    logger = logging.getLogger(__name__)

    logger.info("Starting ConnectWise dimension refresh...")
    start_time = datetime.now()

    dimensions = create_all_dimensions(
        spark=spark, dimension_configs=CONNECTWISE_DIMENSION_CONFIG
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"ConnectWise dimension refresh completed in {duration:.2f} seconds")
    logger.info(f"Created {len(dimensions)} dimension tables")

    # Print summary
    for dim_name, dim_df in dimensions.items():
        count = dim_df.count()
        logger.info(f"  {dim_name}: {count} rows")

    return dimensions
