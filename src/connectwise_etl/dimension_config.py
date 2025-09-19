"""ConnectWise-specific dimension configurations.

Defines rich dimensions with business context beyond simple ID/name pairs.
Based on comprehensive analysis of ConnectWise models and business logic.
"""

# Status category mapping for business intelligence
STATUS_CATEGORIES = {
    'Open': 'Open',
    'PendingApproval': 'Pending',
    'PendingProjectApproval': 'Pending',
    'Rejected': 'Rejected',
    'ErrorsCorrected': 'Pending',
    'ApprovedByTierOne': 'Approved',
    'ApprovedByTierTwo': 'Approved',
    'ReadyToBill': 'Ready',
    'Billed': 'Billed',
    'BilledAgreement': 'Billed',
    'WrittenOff': 'Closed'
}

# Rich dimension configurations with business context
# SIMPLIFIED VERSION - Only using columns that definitely exist
CONNECTWISE_RICH_DIMENSIONS = {
    # Time Entry Status - 11-state workflow
    "TimeEntryStatus": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "status",
        "additional_columns": {
            "statusCategory": "CASE " + " ".join([
                f"WHEN status = '{status}' THEN '{category}'"
                for status, category in STATUS_CATEGORIES.items()
            ]) + " ELSE 'Unknown' END",
            "isOpen": "CASE WHEN status = 'Open' THEN true ELSE false END",
            "isPending": "CASE WHEN status IN ('PendingApproval', 'PendingProjectApproval', 'ErrorsCorrected') THEN true ELSE false END",
            "isApproved": "CASE WHEN status IN ('ApprovedByTierOne', 'ApprovedByTierTwo') THEN true ELSE false END",
            "isBilled": "CASE WHEN status IN ('Billed', 'BilledAgreement') THEN true ELSE false END",
            "isClosed": "CASE WHEN status IN ('WrittenOff', 'Billed', 'BilledAgreement') THEN true ELSE false END"
        }
    },

    # Billable Option - Universal billing status
    "BillableStatus": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "billableOption",
        "additional_columns": {
            "isBillable": "CASE WHEN billableOption = 'Billable' THEN true ELSE false END",
            "isNonBillable": "CASE WHEN billableOption = 'DoNotBill' THEN true ELSE false END",
            "isNoCharge": "CASE WHEN billableOption = 'NoCharge' THEN true ELSE false END",
            "billableCategory": "CASE WHEN billableOption = 'Billable' THEN 'Billable' WHEN billableOption IN ('DoNotBill', 'NoCharge') THEN 'NonBillable' ELSE 'Unknown' END"
        }
    },

    # Charge To Type - Where work is charged
    "ChargeType": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "chargeToType",
        "additional_columns": {
            "isCompanyWork": "CASE WHEN chargeToType = 'Company' THEN true ELSE false END",
            "isTicketWork": "CASE WHEN chargeToType IN ('ServiceTicket', 'ProjectTicket') THEN true ELSE false END",
            "isProjectWork": "CASE WHEN chargeToType IN ('ProjectTicket', 'ChargeCode', 'Activity') THEN true ELSE false END",
            "chargeCategory": "CASE WHEN chargeToType = 'Company' THEN 'Company' WHEN chargeToType IN ('ServiceTicket', 'ProjectTicket') THEN 'Ticket' ELSE 'Project' END"
        }
    },

    # Work Type - basic version without utilization flag for now
    "WorkType": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "workTypeId",
        "additional_columns": {
            # Only use basic columns that definitely exist
        }
    },

    # Work Role - basic version
    "WorkRole": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "workRoleId",
        "additional_columns": {
            # Only use basic columns that definitely exist
        }
    },

    # Agreement Status - lifecycle management
    "AgreementStatus": {
        "source_table": "Lakehouse.silver.silver_cw_agreement",
        "primary_column": "agreementStatus",
        "additional_columns": {
            "isActive": "CASE WHEN agreementStatus = 'Active' THEN true ELSE false END",
            "isCancelled": "CASE WHEN agreementStatus = 'Cancelled' THEN true ELSE false END",
            "isExpired": "CASE WHEN agreementStatus = 'Expired' THEN true ELSE false END",
            "isInactive": "CASE WHEN agreementStatus = 'Inactive' THEN true ELSE false END",
            "statusCategory": "CASE WHEN agreementStatus = 'Active' THEN 'Active' ELSE 'Inactive' END"
        }
    },

    # Agreement Type - CRITICAL for Icelandic business logic
    "AgreementType": {
        "source_table": "Lakehouse.silver.silver_cw_agreement",
        "primary_column": "typeId",
        "additional_columns": {
            "isTimapottur": "CASE WHEN typeName RLIKE 'Tímapottur\\\\s*:?' THEN true ELSE false END",
            "isService": "CASE WHEN typeName LIKE '%þjónusta%' THEN true ELSE false END",
            "isInternal": "CASE WHEN typeName LIKE '%Innri%' THEN true ELSE false END",
            "isOperations": "CASE WHEN typeName LIKE '%Rekstrar%' OR typeName LIKE '%Alrekstur%' THEN true ELSE false END",
            "isSoftware": "CASE WHEN typeName LIKE '%Hugbúnaðar%' OR typeName LIKE '%Office 365%' THEN true ELSE false END",
            "agreementCategory": """CASE
                WHEN typeName RLIKE 'Tímapottur\\\\s*:?' THEN 'Timapottur'
                WHEN typeName LIKE '%Innri%' THEN 'Internal'
                WHEN typeName LIKE '%Rekstrar%' OR typeName LIKE '%Alrekstur%' THEN 'Operations'
                WHEN typeName LIKE '%Hugbúnaðar%' OR typeName LIKE '%Office 365%' THEN 'Software'
                WHEN typeName LIKE '%þjónusta%' THEN 'Service'
                ELSE 'Other'
            END"""
        }
    },

    # Billing Cycle - basic version
    "BillingCycle": {
        "source_table": "Lakehouse.silver.silver_cw_agreement",
        "primary_column": "billingCycleId",
        "additional_columns": {}
    },

    # Invoice Type - 6 distinct types
    "InvoiceType": {
        "source_table": "Lakehouse.silver.silver_cw_invoice",
        "primary_column": "type",
        "additional_columns": {
            "isAgreement": "CASE WHEN type = 'Agreement' THEN true ELSE false END",
            "isCreditMemo": "CASE WHEN type = 'CreditMemo' THEN true ELSE false END",
            "isDownPayment": "CASE WHEN type = 'DownPayment' THEN true ELSE false END",
            "isStandard": "CASE WHEN type = 'Standard' THEN true ELSE false END",
            "invoiceCategory": "CASE WHEN type IN ('Agreement', 'Standard') THEN 'Regular' WHEN type = 'CreditMemo' THEN 'Credit' ELSE 'Special' END"
        }
    },

    # Invoice Status - basic version
    "InvoiceStatus": {
        "source_table": "Lakehouse.silver.silver_cw_invoice",
        "primary_column": "statusId",
        "additional_columns": {}
    },

    # Invoice Apply Type - what invoice applies to
    "InvoiceApplyType": {
        "source_table": "Lakehouse.silver.silver_cw_invoice",
        "primary_column": "applyToType",
        "additional_columns": {
            "isAgreementApply": "CASE WHEN applyToType = 'Agreement' THEN true ELSE false END",
            "isProjectApply": "CASE WHEN applyToType IN ('Project', 'ProjectPhase') THEN true ELSE false END",
            "isTicketApply": "CASE WHEN applyToType = 'Ticket' THEN true ELSE false END",
            "applyCategory": "CASE WHEN applyToType = 'Agreement' THEN 'Agreement' WHEN applyToType IN ('Project', 'ProjectPhase') THEN 'Project' ELSE 'Other' END"
        }
    },

    # Product Class - 5 distinct classes
    "ProductClass": {
        "source_table": "Lakehouse.silver.silver_cw_productitem",
        "primary_column": "productClass",
        "additional_columns": {
            "isInventory": "CASE WHEN productClass = 'Inventory' THEN true ELSE false END",
            "isService": "CASE WHEN productClass = 'Service' THEN true ELSE false END",
            "isBundle": "CASE WHEN productClass = 'Bundle' THEN true ELSE false END",
            "isAgreement": "CASE WHEN productClass = 'Agreement' THEN true ELSE false END",
            "productCategory": "CASE WHEN productClass IN ('Inventory', 'NonInventory') THEN 'Physical' WHEN productClass = 'Service' THEN 'Service' ELSE 'Other' END"
        }
    },

    # Expense Status - same as time entry
    "ExpenseStatus": {
        "source_table": "Lakehouse.silver.silver_cw_expenseentry",
        "primary_column": "status",
        "additional_columns": {
            "statusCategory": "CASE " + " ".join([
                f"WHEN status = '{status}' THEN '{category}'"
                for status, category in STATUS_CATEGORIES.items()
            ]) + " ELSE 'Unknown' END",
            "isOpen": "CASE WHEN status = 'Open' THEN true ELSE false END",
            "isPending": "CASE WHEN status IN ('PendingApproval', 'PendingProjectApproval', 'ErrorsCorrected') THEN true ELSE false END",
            "isApproved": "CASE WHEN status IN ('ApprovedByTierOne', 'ApprovedByTierTwo') THEN true ELSE false END",
            "isBilled": "CASE WHEN status IN ('Billed', 'BilledAgreement') THEN true ELSE false END",
            "isClosed": "CASE WHEN status IN ('WrittenOff', 'Billed', 'BilledAgreement') THEN true ELSE false END"
        }
    },

    # Basic dimensions - core columns only
    "ExpenseClassification": {
        "source_table": "Lakehouse.silver.silver_cw_expenseentry",
        "primary_column": "classificationId",
        "additional_columns": {}
    },

    "Member": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "memberId",
        "additional_columns": {}
    },

    "Company": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "companyId",
        "additional_columns": {}
    },

    "Department": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "departmentId",
        "additional_columns": {}
    },

    "BusinessUnit": {
        "source_table": "Lakehouse.silver.silver_cw_timeentry",
        "primary_column": "businessUnitId",
        "additional_columns": {}
    }
}


def refresh_connectwise_dimensions(spark):
    """Refresh all ConnectWise dimension tables with rich business context."""
    import logging
    from datetime import datetime

    from .dimensions import create_rich_dimensions

    logger = logging.getLogger(__name__)

    logger.info("Starting ConnectWise rich dimension refresh...")
    start_time = datetime.now()

    dimensions = create_rich_dimensions(
        spark=spark,
        rich_dimension_configs=CONNECTWISE_RICH_DIMENSIONS
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"ConnectWise dimension refresh completed in {duration:.2f} seconds")
    logger.info(f"Created {len(dimensions)} rich dimension tables")

    # Print summary with sample data
    for dim_name, dim_df in dimensions.items():
        count = dim_df.count()
        logger.info(f"  {dim_name}: {count} rows")

        # Show sample of the rich dimension
        logger.info(f"  Sample columns: {dim_df.columns}")

        # Show first few rows for validation
        sample_rows = dim_df.limit(3).collect()
        for row in sample_rows:
            logger.info(f"    {row.asDict()}")

    return dimensions


def refresh_connectwise_dimensions_legacy(spark):
    """Legacy dimension refresh using simple ID/name pairs."""
    import logging
    from datetime import datetime

    from .dimensions import create_batch_dimensions

    logger = logging.getLogger(__name__)

    logger.info("Starting ConnectWise legacy dimension refresh...")
    start_time = datetime.now()

    # Convert rich configs to simple configs for backward compatibility
    simple_configs = []
    for dim_name, config in CONNECTWISE_RICH_DIMENSIONS.items():
        simple_configs.append((
            config["source_table"],
            config["primary_column"],
            dim_name
        ))

    dimensions = create_batch_dimensions(
        spark=spark, dimension_configs=simple_configs
    )

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()

    logger.info(f"ConnectWise legacy dimension refresh completed in {duration:.2f} seconds")
    logger.info(f"Created {len(dimensions)} simple dimension tables")

    return dimensions
