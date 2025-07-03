"""Gold layer transformations specific to ConnectWise."""

import logging
from logging import Logger
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from unified_etl_core.date_utils import add_date_key
from unified_etl_core.gold import add_etl_metadata

from .agreement_utils import (
    calculate_effective_billing_status,
    extract_agreement_number,
    resolve_agreement_hierarchy,
)

logger: Logger = logging.getLogger(name=__name__)


# Time Entry Facts


def create_time_entry_fact(
    spark: SparkSession,
    time_entry_df: DataFrame,
    agreement_df: DataFrame | None = None,
    member_df: DataFrame | None = None,
    config: dict[str, Any] | None = None,
) -> DataFrame:
    """Create time entry fact table with ConnectWise-specific business logic.

    This handles the bulk of service delivery metrics including:
    - Billable vs non-billable work
    - Utilization calculations
    - Agreement coverage
    - Internal vs external work
    - Tímapottur identification

    Captures ALL work to avoid missing $18M+ in internal projects.

    Args:
        spark: SparkSession
        time_entry_df: Silver time entry DataFrame
        agreement_df: Optional agreement DataFrame for hierarchy resolution
        member_df: Optional member DataFrame for cost enrichment
        config: Optional configuration

    Returns:
        DataFrame with comprehensive time entry facts
    """
    # Validate required columns
    required_columns: list[str] = [
        "id",
        "chargeToType",
        "chargeToId",
        "memberId",
        "memberName",
        "workTypeId",
        "workTypeName",
        "workRoleId",
        "workRoleName",
        "timeStart",
        "actualHours",
        "billableOption",
        "agreementId",
        "invoiceId",
        "hourlyRate",
    ]

    missing_columns: list[str] = [col for col in required_columns if col not in time_entry_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns for time entry fact: {missing_columns}")

    # Start with all time entries
    fact_df: DataFrame = time_entry_df.select(
        # Keys
        F.sha2(F.col("id").cast("string"), 256).alias("TimeentrySK"),
        F.col("id").alias("timeEntryId"),
        # Foreign keys
        "memberId",
        "agreementId",
        "chargeToId",
        "chargeToType",
        "invoiceId",
        F.col("invoiceIdentifier").alias("invoiceNumber"),  # From invoice.identifier
        "ticketId",
        "projectId",
        "companyId",
        "locationId",
        "departmentId",
        "businessUnitId",
        "timeSheetId",
        # Dimensions
        "memberName",
        "workTypeId",
        "workTypeName",
        "workRoleId",
        "workRoleName",
        "billableOption",
        "status",
        "companyIdentifier",
        "companyName",
        "agreementName",
        "agreementType",
        "locationName",
        "departmentName",
        "projectName",
        "ticketSummary",
        # Ticket classification
        F.col("ticketBoard").alias("ticketBoard"),
        F.col("ticketType").alias("ticketType"),
        F.col("ticketSubType").alias("ticketSubType"),
        # Time attributes
        "timeStart",
        "timeEnd",
        "enteredBy",
        "dateEntered",
        # Metrics - Hours
        "actualHours",
        F.coalesce("hoursDeduct", F.lit(0)).alias("hoursDeduct"),
        F.coalesce("hoursBilled", F.lit(0)).alias("hoursBilled"),
        F.coalesce("invoiceHours", F.lit(0)).alias("invoiceHours"),
        F.coalesce("agreementHours", F.lit(0)).alias("agreementHours"),
        # Metrics - Rates & Costs
        "hourlyRate",
        F.coalesce("hourlyCost", F.lit(0)).alias("hourlyCost"),
        F.coalesce("overageRate", F.lit(0)).alias("overageRate"),
        F.coalesce("agreementAmount", F.lit(0)).alias("agreementAmount"),
        # Financial adjustments
        F.coalesce("agreementAdjustment", F.lit(0)).alias("agreementAdjustment"),
        F.coalesce("adjustment", F.lit(0)).alias("adjustment"),
        F.coalesce("extendedInvoiceAmount", F.lit(0)).alias("extendedInvoiceAmount"),
        # Tax/billing metadata
        F.col("taxCodeId").alias("taxCodeId"),
        F.col("taxCodeName").alias("taxCodeName"),
        F.coalesce("invoiceFlag", F.lit(False)).alias("invoiceFlag"),
        F.coalesce("invoiceReady", F.lit(0)).alias("invoiceReady"),
        # Utilization & Capacity
        F.coalesce("workTypeUtilizationFlag", F.lit(False)).alias("workTypeUtilizationFlag"),
        F.coalesce("memberDailyCapacity", F.lit(8)).alias("memberDailyCapacity"),
        # Notes
        "notes",
        "internalNotes",
    )

    # Add date key
    fact_df = add_date_key(fact_df, "timeStart", "WorkDateSK")

    # Enrich with member costs if available
    if member_df is not None:
        fact_df = fact_df.join(
            member_df.select(
                F.col("id").alias("member_id"),
                F.col("hourlyCost").alias("memberHourlyCost"),
            ),
            fact_df.memberId == F.col("member_id"),
            "left",
        ).drop("member_id")

        # Use member cost if available, otherwise use time entry cost
        fact_df = fact_df.withColumn(
            "effectiveHourlyCost", F.coalesce("memberHourlyCost", "hourlyCost", F.lit(0))
        ).drop("memberHourlyCost")
    else:
        fact_df = fact_df.withColumn("effectiveHourlyCost", F.col("hourlyCost"))

    # Calculate amounts
    fact_df = (
        fact_df.withColumn("potentialRevenue", F.col("actualHours") * F.col("hourlyRate"))
        .withColumn("actualCost", F.col("actualHours") * F.col("effectiveHourlyCost"))
        .withColumn("margin", F.col("potentialRevenue") - F.col("actualCost"))
        .withColumn(
            "marginPercentage",
            F.when(
                F.col("potentialRevenue") > 0, (F.col("margin") / F.col("potentialRevenue") * 100)
            ).otherwise(0),
        )
    )

    # If agreements provided, resolve types and add flags
    if agreement_df is not None:
        fact_df = resolve_agreement_hierarchy(fact_df, agreement_df, "agreementId", "time_entries")
        fact_df = calculate_effective_billing_status(fact_df)
        # The resolve_agreement_hierarchy function adds parentAgreementId and final_agreement_number

    # Add utilization type
    fact_df = fact_df.withColumn(
        "utilizationType",
        F.when(F.col("agreement_type_normalized") == "internal_project", "Internal")
        .when(F.col("billableOption") == "Billable", "Billable")
        .when(F.col("billableOption") == "DoNotBill", "Non-Billable")
        .when(F.col("billableOption") == "NoCharge", "No Charge")
    )

    # Add cross-product of utilizationType and status
    fact_df = fact_df.withColumn(
        "utilizationStatus",
        F.concat(F.col("utilizationType"), F.lit(" - "), F.col("status"))
    )

    # Add dimension keys for enum columns
    from unified_etl_core.dimensions import add_dimension_keys
    
    dimension_mappings = [
        # (fact_column, dim_table, dim_code_column, key_column)
        ("billableOption", "dim_billable_status", "billable_status_code", "billable_status_key"),
        ("status", "dim_time_status", "time_status_code", "time_status_key"),
        ("chargeToType", "dim_charge_type", "charge_type_code", "charge_type_key"),
        ("workTypeId", "dim_work_type", "work_type_code", "work_type_key"),
        ("workRoleId", "dim_work_role", "work_role_code", "work_role_key"),
        ("departmentId", "dim_department", "department_code", "department_key"),
        ("businessUnitId", "dim_business_unit", "business_unit_code", "business_unit_key"),
    ]
    
    fact_df = add_dimension_keys(fact_df, spark, dimension_mappings)

    # Add ETL metadata
    fact_df = add_etl_metadata(fact_df, layer="gold", source="connectwise")

    return fact_df


# Wrapper functions for framework compatibility
def create_agreement_dimensions(
    spark: SparkSession, agreement_df: DataFrame, config: dict[str, Any]
) -> dict[str, DataFrame]:
    """Wrapper for framework compatibility - creates agreement dimension."""
    dim_df = create_agreement_dimension(spark, agreement_df, config)
    return {"dim_agreement": dim_df}


def create_invoice_facts(
    spark: SparkSession,
    invoice_df: DataFrame,
    config: dict[str, Any],
    timeEntryDf: DataFrame,  # noqa: N803
    productItemDf: DataFrame,  # noqa: N803
    agreementDf: DataFrame,  # noqa: N803
) -> dict[str, DataFrame]:
    """Wrapper for framework compatibility - creates invoice line facts."""
    fact_df = create_invoice_line_fact(
        spark=spark,
        invoice_df=invoice_df,
        time_entry_df=timeEntryDf,
        product_df=productItemDf,
        agreement_df=agreementDf,
        config=config,
    )
    return {"fact_invoice_line": fact_df}


# Invoice Line Facts


def create_invoice_line_fact(
    spark: SparkSession,  # pylint: disable=unused-argument
    invoice_df: DataFrame,
    time_entry_df: DataFrame,
    product_df: DataFrame,
    agreement_df: DataFrame,
    config: dict[str, Any],  # pylint: disable=unused-argument
) -> DataFrame:
    """Create invoice line fact that includes both invoiced items AND uninvoiced billable work.

    This captures revenue from multiple sources:
    1. Actual invoice lines (from posted invoices)
    2. Uninvoiced time entries (billable but not yet invoiced)
    3. Product sales

    Critical for identifying the missing $18M in unbilled work.
    """
    logger.info("Creating comprehensive invoice line facts")

    # Start with invoices as the base
    invoice_lines = []

    # Get time-based lines if time entries provided
    if time_entry_df is not None:
        # Include time entries that are either invoiced or billable
        # This captures both posted invoices and work ready to bill
        time_enriched = time_entry_df.alias("t").filter(F.col("invoiceId").isNotNull())

        # Don't enrich time entries with agreement - we'll get it from invoice instead

        # Create time-based invoice lines
        time_lines = time_enriched.select(
            F.col("invoiceId").cast("int"),
            F.monotonically_increasing_id().alias("lineNumber"),
            F.col("id").alias("timeEntryId"),
            F.lit(None).alias("productId"),
            F.col("notes").alias("description"),
            F.col("actualHours").alias("quantity"),
            F.col("hourlyRate").alias("price"),
            F.coalesce("hourlyCost", F.lit(0)).alias("cost"),
            (F.col("actualHours") * F.col("hourlyRate")).alias("lineAmount"),
            "agreementId",
            "workTypeId",
            "workRoleId",
            F.col("memberId").alias("employeeId"),
            F.concat_ws(" - ", "workTypeName", "memberName").alias("memo"),
            F.lit("Service").alias("productClass"),
            # Include agreement type directly from time entry
            F.col("agreementType").alias("time_entry_agreement_type"),
        )

        invoice_lines.append(time_lines)
        logger.info(f"Added {time_lines.count()} time-based lines")

    # Get product-based lines if products provided
    if product_df is not None:
        # Products have invoiceId - join from products to get product lines
        product_lines = (
            product_df.alias("p")
            .filter(F.col("p.invoiceId").isNotNull())
            .select(
                F.col("p.invoiceId"),
                F.monotonically_increasing_id().alias("lineNumber"),
                F.lit(None).alias("timeEntryId"),
                F.col("p.id").alias("productId"),
                F.col("p.description"),
                F.col("p.quantity"),
                F.col("p.price"),
                F.col("p.cost"),
                (F.col("p.quantity") * F.col("p.price")).alias("lineAmount"),
                F.col("p.agreementId"),  # Product items have agreementId
                F.lit(None).alias("workTypeId"),
                F.lit(None).alias("workRoleId"),
                F.lit(None).alias("employeeId"),
                F.col("p.productClass"),
                F.col("p.description").alias("memo"),
                # Include agreement type directly from product item
                F.col("p.agreementType").alias("product_agreement_type"),
                F.lit(None).alias("time_entry_agreement_type"),
            )
        )

        invoice_lines.append(product_lines)
        logger.info(f"Added {product_lines.count()} product lines")

    # Union all line types
    if not invoice_lines:
        raise ValueError("No line data available - need at least time entries or products")

    fact_df = invoice_lines[0]
    for df in invoice_lines[1:]:
        fact_df = fact_df.unionByName(df, allowMissingColumns=True)

    # Add line metadata
    fact_df = (
        fact_df.withColumn(
            "LineType",
            F.when(F.col("productClass") == "Service", "SERVICE")
             .when(F.col("productClass").isNull() & F.col("timeEntryId").isNotNull(), "SERVICE")
             .when(F.col("productClass").isNotNull(), "PRODUCT") 
             .when(F.col("agreementId").isNotNull(), "AGREEMENT")
        )
    )

    # Calculate line cost and margin
    fact_df = (
        fact_df.withColumn("lineCost", F.col("quantity") * F.col("cost"))
        .withColumn("margin", F.col("lineAmount") - F.col("lineCost"))
        .withColumn(
            "marginPercentage",
            F.when(
                F.col("lineAmount") > 0, (F.col("margin") / F.col("lineAmount") * 100)
            ).otherwise(0),
        )
    )

    # Add invoice header info including the invoice's own agreementType
    fact_df = (
        fact_df.alias("lines")
        .join(
            invoice_df.select(
                F.col("id").alias("inv_id"),
                "date",
                "dueDate",
                F.col("statusName").alias("status"),
                F.col("companyId"),
                F.col("companyName"),
                "applyToType",
                "applyToId",
                F.col("agreementType").alias("invoice_agreement_type"),
            ).alias("inv"),
            F.col("lines.invoiceId") == F.col("inv.inv_id"),
            "left",
        )
        .drop("inv_id")
    )

    # Default applyToType to "Services" when null
    fact_df = fact_df.withColumn(
        "applyToType",
        F.coalesce(F.col("applyToType"), F.lit("Services"))
    )

    # Add surrogate keys
    fact_df = fact_df.withColumn(
        "InvoiceLineSK",
        F.sha2(
            F.concat_ws(
                "|",
                F.coalesce("invoiceId", F.lit(-1)),
                "lineNumber",
                F.coalesce("timeEntryId", F.lit(-1)),
                F.coalesce("productId", F.lit(-1)),
            ),
            256,
        ),
    ).withColumn("InvoiceSK", F.sha2(F.coalesce("invoiceId", F.lit(-1)).cast("string"), 256))

    # Add date key
    fact_df = add_date_key(fact_df, "date", "InvoiceDateSK")

    # Only use agreementType when applyToType = "Agreement"
    # Use the most granular agreement type available (line-level > invoice-level)
    fact_df = fact_df.withColumn(
        "agreement_type",
        F.coalesce(F.col("time_entry_agreement_type"), F.col("product_agreement_type")),
    ).withColumn(
        "agreement_type_final",
        F.when(
            F.col("applyToType") == "Agreement",
            # Add "Agr - " prefix to agreement types
            # Fail fast - if agreement invoice doesn't have agreement type, that's an error
            F.concat(F.lit("Agr - "), F.col("invoice_agreement_type")),
        )
        # For all other applyToTypes, leave agreement_type_final as NULL
        .otherwise(F.lit(None)),
    )

    # Add dimension keys for enum columns
    from unified_etl_core.dimensions import add_dimension_keys
    
    dimension_mappings = [
        # (fact_column, dim_table, dim_code_column, key_column)
        ("LineType", "dim_line_type", "line_type_code", "line_type_key"),
        ("productClass", "dim_product_class", "product_class_code", "product_class_key"),
        ("applyToType", "dim_invoice_apply_type", "invoice_apply_type_code", "invoice_apply_type_key"),
        ("status", "dim_invoice_status", "invoice_status_code", "invoice_status_key"),
    ]
    
    fact_df = add_dimension_keys(fact_df, spark, dimension_mappings)

    # Add ETL metadata
    fact_df = add_etl_metadata(fact_df, layer="gold", source="connectwise")

    return fact_df


# Supporting Table Functions


def filter_billable_time_entries(time_entry_df: DataFrame) -> DataFrame:
    """Filter time entries to billable only."""
    return time_entry_df.filter(
        (F.col("billableOption") == "Billable") | (F.col("invoiceId").isNotNull())
    )


def add_surrogate_keys(df: DataFrame, key_configs: dict[str, dict]) -> DataFrame:
    """Add surrogate keys based on configuration."""
    for key_name, key_config in key_configs.items():
        if key_config["type"] == "hash":
            source_cols = [
                F.coalesce(col, F.lit("")).cast("string") for col in key_config["source_columns"]
            ]
            df = df.withColumn(key_name, F.sha2(F.concat_ws("|", *source_cols), 256))
        elif key_config["type"] == "date_int":
            df = df.withColumn(
                key_name,
                F.date_format(F.col(key_config["source_columns"][0]), "yyyyMMdd").cast("int"),
            )
    return df


# Agreement Facts


def create_agreement_dimension(
    spark: SparkSession,  # pylint: disable=unused-argument
    agreement_df: DataFrame,
    config: dict[str, Any] | None = None,  # pylint: disable=unused-argument
) -> DataFrame:
    """Create agreement dimension table with comprehensive business attributes.

    This dimension provides:
    - Agreement hierarchy and relationships
    - Billing model classification (Amount/Hours/Incidents)
    - Coverage details (what services are included)
    - Customer and billing relationships
    - Lifecycle status and dates
    - Financial terms and limits
    """
    # Extract agreement number from customFields
    dim_df = extract_agreement_number(agreement_df)

    # Create comprehensive dimension with all attributes
    dim_df = dim_df.select(
        # Keys
        F.sha2(F.col("id").cast("string"), 256).alias("AgreementSK"),
        F.col("id").alias("agreementId"),
        F.col("agreementNumber"),

        # Basic attributes
        "name",
        F.col("typeId").alias("agreementTypeId"),
        F.col("typeName").alias("agreementTypeName"),
        F.col("agreementStatus").alias("status"),

        # Hierarchy
        F.col("parentAgreementId"),
        F.col("parentAgreementName"),

        # Customer relationships
        "companyId",
        "companyName",
        F.col("companyIdentifier").alias("companyNumber"),
        "contactId",
        "contactName",
        "siteId",
        "siteName",

        # Billing relationships
        "billToCompanyId",
        "billToCompanyName",
        "billToContactId",
        "billToContactName",
        "billToSiteId",
        "billToSiteName",

        # Billing model
        F.col("applicationUnits").alias("billingModel"),  # Amount/Hours/Incidents
        F.col("applicationLimit").alias("contractLimit"),
        F.col("applicationCycle").alias("billingCycle"),
        F.col("applicationUnlimitedFlag").alias("isUnlimited"),

        # Coverage flags
        F.col("coverAgreementTime").alias("coversTime"),
        F.col("coverAgreementProduct").alias("coversProducts"),
        F.col("coverAgreementExpense").alias("coversExpenses"),
        F.col("coverSalesTax").alias("coversSalesTax"),

        # Billing rules
        F.col("carryOverUnused").alias("allowsCarryOver"),
        F.col("allowOverruns").alias("allowsOverruns"),
        F.col("expireWhenZero").alias("expiresWhenZero"),
        F.col("chargeToFirm").alias("chargeToFirm"),

        # Financial terms
        F.col("billAmount").alias("recurringAmount"),
        F.lit(None).cast("double").alias("defaultHourlyRate"),  # hourlyRate not in agreement table
        F.col("compHourlyRate").alias("compensationHourlyRate"),
        F.col("compLimitAmount").alias("compensationLimit"),
        F.col("taxable").alias("isTaxable"),
        F.col("taxCodeId"),
        F.col("taxCodeName"),

        # Billing configuration
        F.col("billTime").alias("defaultTimeBilling"),  # Billable/DoNotBill/NoCharge
        F.col("billExpenses").alias("defaultExpenseBilling"),
        F.col("billProducts").alias("defaultProductBilling"),
        F.col("billOneTimeFlag").alias("isOneTimeBilling"),
        F.col("prorateFlag").alias("allowsProration"),

        # Work defaults
        F.col("workRoleId").alias("defaultWorkRoleId"),
        F.col("workRoleName").alias("defaultWorkRoleName"),
        F.col("workTypeId").alias("defaultWorkTypeId"),
        F.col("workTypeName").alias("defaultWorkTypeName"),

        # Lifecycle dates
        F.to_date("startDate").alias("startDate"),
        F.to_date("endDate").alias("endDate"),
        F.col("noEndingDateFlag").alias("isOpenEnded"),
        F.to_date("dateCancelled").alias("cancelledDate"),
        F.col("cancelledFlag").alias("isCancelled"),
        F.col("reasonCancelled").alias("cancellationReason"),

        # Derived fields
        F.when(F.col("endDate").isNull() | (F.col("endDate") > F.current_date()), True)
         .otherwise(False).alias("isActive"),
        F.months_between(F.current_date(), F.col("startDate")).alias("ageInMonths"),
        F.datediff(F.coalesce("endDate", F.current_date()), F.col("startDate")).alias("durationDays"),

        # Invoice configuration
        F.col("invoiceTemplateId"),
        F.col("invoiceTemplateName"),
        F.col("autoInvoiceFlag").alias("isAutoInvoice"),
        F.col("nextInvoiceDate"),

        # Other attributes
        F.col("customerPO").alias("customerPONumber"),
        F.col("workOrder").alias("workOrderNumber"),
        F.col("slaId").alias("slaId"),
        F.col("slaName").alias("slaName"),
        "internalNotes",
    )

    # Add agreement type normalization
    dim_df = dim_df.withColumn(
        "agreementTypeNormalized",
        F.when(F.col("agreementTypeName").rlike(r"(?i)yÞjónusta"), "billable_service")
        .when(F.col("agreementTypeName").rlike(r"(?i)Tímapottur\s*:?"), "prepaid_hours")
        .when(F.col("agreementTypeName").rlike(r"(?i)Innri verkefni"), "internal_project")
        .when(F.col("agreementTypeName").rlike(r"(?i)Rekstrarþjónusta|Alrekstur"), "operations")
        .when(F.col("agreementTypeName").rlike(r"(?i)Hugbúnaðarþjónusta|Office 365"), "software_service")
        .otherwise("other")
    )

    # Add billing behavior classification
    dim_df = dim_df.withColumn(
        "billingBehavior",
        F.when(F.col("agreementTypeNormalized").isin("billable_service", "software_service"), "billable")
        .when(F.col("agreementTypeNormalized") == "prepaid_hours", "prepaid")
        .when(F.col("agreementTypeNormalized") == "internal_project", "internal")
        .when(F.col("agreementTypeNormalized") == "operations", "operations")
        .otherwise("unknown")
    )

    # Add ETL metadata
    dim_df = add_etl_metadata(dim_df, layer="gold", source="connectwise")

    return dim_df


def create_expense_entry_fact(
    spark: SparkSession,  # pylint: disable=unused-argument
    expense_df: DataFrame,
    agreement_df: DataFrame | None = None,
    config: dict[str, Any] | None = None,  # pylint: disable=unused-argument
) -> DataFrame:
    """Create expense entry fact table.

    Similar to time entries but for expense tracking.
    """
    # Validate required columns
    required_columns = [
        "id",
        "chargeToType",
        "chargeToId",
        "memberId",
        "typeId",
        "typeName",
        "date",
        "amount",
        "agreementId",
        "invoiceId",
        "billableOption",
    ]

    missing_columns = [col for col in required_columns if col not in expense_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns for expense fact: {missing_columns}")

    # Start with base expense data
    fact_df = expense_df.select(
        # Keys
        F.sha2(F.col("id").cast("string"), 256).alias("ExpenseSK"),
        F.col("id").alias("expenseId"),
        # Foreign keys
        "memberId",
        "agreementId",
        "chargeToId",
        "chargeToType",
        "invoiceId",
        "ticketId",
        "projectId",
        # Dimensions
        F.col("typeId").alias("expenseTypeId"),
        F.col("typeName").alias("expenseTypeName"),
        "billableOption",
        # Metrics
        "amount",
        F.lit(1).alias("quantity"),  # Expenses don't have quantity, default to 1
        # Date
        "date",
        # Notes
        "notes",
    )

    # Add date key
    fact_df = add_date_key(fact_df, "date", "ExpenseDateSK")

    # Calculate totals (no cost data available for expenses)
    fact_df = (
        fact_df.withColumn("totalAmount", F.col("amount") * F.col("quantity"))
        .withColumn(
            "totalCost",
            F.lit(0),  # No cost data available
        )
        .withColumn(
            "margin",
            F.col("totalAmount"),  # All amount is margin without cost data
        )
    )

    # If agreements provided, resolve types and add flags
    if agreement_df is not None:
        fact_df = resolve_agreement_hierarchy(fact_df, agreement_df, "agreementId", "expenses")
        fact_df = calculate_effective_billing_status(fact_df)

    # Add dimension keys for enum columns
    from unified_etl_core.dimensions import add_dimension_keys
    
    dimension_mappings = [
        # (fact_column, dim_table, dim_code_column, key_column)
        ("billableOption", "dim_expense_billable_status", "expense_billable_status_code", "expense_billable_status_key"),
        ("chargeToType", "dim_expense_charge_type", "expense_charge_type_code", "expense_charge_type_key"),
        # Note: expenses don't have status column, removed from dimension mappings
        # ("classificationId", "dim_expense_classification", "expense_classification_code", "expense_classification_key"),
    ]
    
    fact_df = add_dimension_keys(fact_df, spark, dimension_mappings)

    # Add ETL metadata
    fact_df = add_etl_metadata(fact_df, layer="gold", source="connectwise")

    return fact_df
