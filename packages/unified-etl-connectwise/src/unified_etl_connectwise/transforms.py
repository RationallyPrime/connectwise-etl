"""Gold layer transformations specific to ConnectWise."""

import logging
from typing import Any, Union

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, SparkSession

from unified_etl_core.date_utils import add_date_key
from unified_etl_core.gold import add_etl_metadata

from .agreement_utils import (
    add_agreement_flags,
    calculate_effective_billing_status,
    resolve_agreement_hierarchy,
)

logger = logging.getLogger(__name__)


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
    required_columns = [
        "id", "chargeToType", "chargeToId", "memberId", "memberName",
        "workTypeId", "workTypeName", "workRoleId", "workRoleName",
        "timeStart", "actualHours", "billableOption", "agreementId",
        "invoiceId", "hourlyRate",
    ]

    missing_columns = [col for col in required_columns if col not in time_entry_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns for time entry fact: {missing_columns}")

    # Start with all time entries
    fact_df = time_entry_df.select(
        # Keys
        F.sha2(F.col("id").cast("string"), 256).alias("TimeEntrySK"),
        F.col("id").alias("timeEntryId"),

        # Foreign keys
        "memberId", "agreementId", "chargeToId", "chargeToType",
        "invoiceId", "ticketId", "projectId",
        "companyId", "locationId", "departmentId", "businessUnitId",
        "timeSheetId",

        # Dimensions
        "memberName", "workTypeId", "workTypeName", "workRoleId",
        "workRoleName", "billableOption", "status",
        "companyIdentifier", "companyName",
        "agreementName", "agreementType",
        "locationName", "departmentName",
        "projectName", "ticketSummary",

        # Time attributes
        "timeStart", "timeEnd",
        "enteredBy", "dateEntered",

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
        
        # Utilization & Capacity
        F.coalesce("workTypeUtilizationFlag", F.lit(False)).alias("workTypeUtilizationFlag"),
        F.coalesce("memberDailyCapacity", F.lit(8)).alias("memberDailyCapacity"),

        # Notes
        "notes", "internalNotes",
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
            "left"
        ).drop("member_id")

        # Use member cost if available, otherwise use time entry cost
        fact_df = fact_df.withColumn(
            "effectiveHourlyCost",
            F.coalesce("memberHourlyCost", "hourlyCost", F.lit(0))
        ).drop("memberHourlyCost")
    else:
        fact_df = fact_df.withColumn("effectiveHourlyCost", F.col("hourlyCost"))

    # Calculate amounts
    fact_df = fact_df.withColumn(
        "potentialRevenue",
        F.col("actualHours") * F.col("hourlyRate")
    ).withColumn(
        "actualCost",
        F.col("actualHours") * F.col("effectiveHourlyCost")
    ).withColumn(
        "margin",
        F.col("potentialRevenue") - F.col("actualCost")
    ).withColumn(
        "marginPercentage",
        F.when(F.col("potentialRevenue") > 0,
            (F.col("margin") / F.col("potentialRevenue") * 100)
        ).otherwise(0)
    )

    # If agreements provided, resolve types and add flags
    if agreement_df is not None:
        fact_df = resolve_agreement_hierarchy(
            fact_df, agreement_df, "agreementId", "time_entries"
        )
        fact_df = add_agreement_flags(fact_df)
        fact_df = calculate_effective_billing_status(fact_df)

    # Add utilization type
    fact_df = fact_df.withColumn(
        "utilizationType",
        F.when(F.col("isInternalWork"), "Internal")
        .when(F.col("billableOption") == "Billable", "Billable")
        .when(F.col("billableOption") == "DoNotBill", "Non-Billable")
        .when(F.col("billableOption") == "NoCharge", "No Charge")
        .otherwise("Other")
    )

    # Add ETL metadata
    fact_df = add_etl_metadata(fact_df, layer="gold", source="connectwise")

    return fact_df


# Wrapper functions for framework compatibility
def create_agreement_facts(spark: SparkSession, agreement_df: DataFrame, config: dict[str, Any]) -> dict[str, DataFrame]:
    """Wrapper for framework compatibility - creates agreement period facts."""
    fact_df = create_agreement_period_fact(spark, agreement_df, config)
    return {"fact_agreement_period": fact_df}


def create_invoice_facts(
    spark: SparkSession,
    invoice_df: DataFrame,
    config: dict[str, Any],
    timeEntryDf: DataFrame,
    productItemDf: DataFrame,
    agreementDf: DataFrame,
) -> dict[str, DataFrame]:
    """Wrapper for framework compatibility - creates invoice line facts."""
    fact_df = create_invoice_line_fact(
        spark=spark,
        invoice_df=invoice_df,
        time_entry_df=timeEntryDf,
        product_df=productItemDf,
        agreement_df=agreementDf,
        config=config
    )
    return {"fact_invoice_line": fact_df}


# Invoice Line Facts


def create_invoice_line_fact(
    spark: SparkSession,
    invoice_df: DataFrame,
    time_entry_df: DataFrame,
    product_df: DataFrame,
    agreement_df: DataFrame,
    config: dict[str, Any],
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
        # Include ALL billable time entries, not just invoiced ones
        time_enriched = time_entry_df.alias("t").filter(
            (F.col("billableOption") == "Billable") |
            (F.col("invoiceId").isNotNull())
        )
        
        # Enrich with agreement info if available
        if agreement_df is not None:
            logger.info("Resolving agreement hierarchy for invoice_lines")
            time_enriched = resolve_agreement_hierarchy(
                time_enriched, agreement_df, "agreementId", "invoice_lines"
            )
            time_enriched = add_agreement_flags(time_enriched)
        
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
            "agreementId", "workTypeId", "workRoleId",
            F.col("memberId").alias("employeeId"),
            F.concat_ws(" - ", "workTypeName", "memberName").alias("memo"),
            F.lit("Service").alias("productClass"),
        )
        
        invoice_lines.append(time_lines)
        logger.info(f"Added {time_lines.count()} time-based lines")
    
    # Get product-based lines if products provided
    if product_df is not None:
        # Products have invoiceId - join from products to get product lines
        product_lines = product_df.alias("p").filter(
            F.col("p.invoiceId").isNotNull()
        ).select(
            F.col("p.invoiceId"),
            F.monotonically_increasing_id().alias("lineNumber"),
            F.lit(None).alias("timeEntryId"),
            F.col("p.id").alias("productId"),
            F.col("p.description"),
            F.col("p.quantity"),
            F.col("p.price"),
            F.col("p.cost"),
            (F.col("p.quantity") * F.col("p.price")).alias("lineAmount"),
            F.lit(None).alias("agreementId"),
            F.lit(None).alias("workTypeId"),
            F.lit(None).alias("workRoleId"),
            F.lit(None).alias("employeeId"),
            F.col("p.productClass"),
            F.col("p.description").alias("memo"),
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
    fact_df = fact_df.withColumn(
        "isService", F.col("productClass") == "Service"
    ).withColumn(
        "isProduct", F.col("productClass") != "Service"
    ).withColumn(
        "isAgreement", F.col("agreementId").isNotNull()
    ).withColumn(
        "isBillable", F.col("lineAmount") > 0
    )
    
    # Calculate margin
    fact_df = fact_df.withColumn(
        "margin", F.col("lineAmount") - (F.col("quantity") * F.col("cost"))
    ).withColumn(
        "marginPercentage",
        F.when(F.col("lineAmount") > 0,
            ((F.col("lineAmount") - (F.col("quantity") * F.col("cost"))) / F.col("lineAmount") * 100)
        ).otherwise(0)
    )
    
    # Add invoice header info if available
    if invoice_df is not None:
        fact_df = fact_df.alias("lines").join(
            invoice_df.select(
                F.col("id").alias("inv_id"),
                "date", "dueDate", "status",
                F.col("companyId"), F.col("companyName"),
            ).alias("inv"),
            F.col("lines.invoiceId") == F.col("inv.inv_id"),
            "left"
        ).drop("inv_id")
    else:
        # For uninvoiced items, use time entry date
        fact_df = fact_df.withColumn("date", F.current_date())
        fact_df = fact_df.withColumn("status", F.lit("Unbilled"))
    
    # Add surrogate keys
    fact_df = fact_df.withColumn(
        "InvoiceLineSK", F.sha2(
            F.concat_ws("|",
                F.coalesce("invoiceId", F.lit(-1)),
                "lineNumber",
                F.coalesce("timeEntryId", F.lit(-1)),
                F.coalesce("productId", F.lit(-1))
            ), 256
        )
    ).withColumn(
        "InvoiceSK", F.sha2(F.coalesce("invoiceId", F.lit(-1)).cast("string"), 256)
    )
    
    # Add date key
    fact_df = add_date_key(fact_df, "date", "InvoiceDateSK")
    
    # Add ETL metadata
    fact_df = add_etl_metadata(fact_df, layer="gold", source="connectwise")
    
    return fact_df


# Supporting Table Functions


def filter_billable_time_entries(time_entry_df: DataFrame) -> DataFrame:
    """Filter time entries to billable only."""
    return time_entry_df.filter(
        (F.col("billableOption") == "Billable") |
        (F.col("invoiceId").isNotNull())
    )


def add_surrogate_keys(df: DataFrame, key_configs: dict[str, dict]) -> DataFrame:
    """Add surrogate keys based on configuration."""
    for key_name, config in key_configs.items():
        if config["type"] == "hash":
            source_cols = [F.coalesce(col, F.lit("")).cast("string") for col in config["source_columns"]]
            df = df.withColumn(key_name, F.sha2(F.concat_ws("|", *source_cols), 256))
        elif config["type"] == "date_int":
            df = df.withColumn(key_name, F.date_format(F.col(config["source_columns"][0]), "yyyyMMdd").cast("int"))
    return df


# Agreement Facts


def create_agreement_period_fact(
    spark: SparkSession,
    agreement_df: DataFrame,
    config: dict[str, Any],
    start_date: str = "2020-01-01",
    end_date: str = None,
) -> DataFrame:
    """Create monthly agreement period facts for MRR tracking.
    
    Generates one row per agreement per month for:
    - Active period tracking
    - MRR calculation
    - Churn analysis
    - Growth metrics
    """
    # Prepare agreements with proper date handling
    agreements_prep = agreement_df.select(
        "id", "name", "agreementStatus", "typeId", "typeName",
        "companyId", "companyName", "contactId", "billingCycleId",
        "billAmount", "applicationUnits",
        F.to_date(F.coalesce("startDate", F.current_date())).alias("effective_start"),
        F.coalesce("endDate", F.to_date(F.lit("2099-12-31"))).alias("effective_end"),
        "cancelledFlag", "customFields",
    )

    # Create date spine
    from unified_etl_core.date_utils import create_date_spine
    
    if end_date is None:
        end_date = F.current_date()
    
    date_spine = create_date_spine(spark, start_date, end_date, "M")
    
    # Cross join agreements with date spine
    period_facts = agreements_prep.crossJoin(date_spine).filter(
        (F.col("period_start") >= F.col("effective_start")) &
        (F.col("period_start") <= F.col("effective_end"))
    )
    
    # Add period metrics
    period_facts = period_facts.withColumn(
        "is_active_period",
        (F.col("agreementStatus") == "Active") &
        (F.col("cancelledFlag") == F.lit(False))
    ).withColumn(
        "is_new_agreement",
        F.date_trunc("month", F.col("effective_start")) == F.col("period_start")
    ).withColumn(
        "is_churned_agreement",
        (F.date_trunc("month", F.col("effective_end")) == F.col("period_start")) &
        (F.col("cancelledFlag") == F.lit(True))
    ).withColumn(
        "monthly_revenue",
        F.when(F.col("applicationUnits") == "Amount", F.col("billAmount")).otherwise(0)
    ).withColumn(
        "months_since_start",
        F.months_between(F.col("period_start"), F.col("effective_start"))
    )
    
    # Add date dimensions
    period_facts = period_facts.withColumn("year", F.year("period_start"))
    period_facts = period_facts.withColumn("month", F.month("period_start"))
    period_facts = period_facts.withColumn("quarter", F.quarter("period_start"))
    
    # Add surrogate keys
    period_facts = add_surrogate_keys(period_facts, {
        "AgreementPeriodSK": {"type": "hash", "source_columns": ["id", "period_start"]},
        "AgreementSK": {"type": "hash", "source_columns": ["id"]},
        "PeriodDateSK": {"type": "date_int", "source_columns": ["period_start"]},
    })

    return period_facts


def create_expense_entry_fact(
    spark: SparkSession,
    expense_df: DataFrame,
    agreement_df: DataFrame | None = None,
    config: dict[str, Any] | None = None,
) -> DataFrame:
    """Create expense entry fact table.
    
    Similar to time entries but for expense tracking.
    """
    # Validate required columns
    required_columns = [
        "id", "chargeToType", "chargeToId", "memberId",
        "typeId", "typeName", "date", "amount",
        "agreementId", "invoiceId", "billableOption",
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
        "memberId", "agreementId", "chargeToId", "chargeToType",
        "invoiceId", "ticketId", "projectId",

        # Dimensions
        F.col("typeId").alias("expenseTypeId"), 
        F.col("typeName").alias("expenseTypeName"), 
        "billableOption",

        # Metrics
        "amount", "quantity",
        F.coalesce("cost", F.lit(0)).alias("cost"),

        # Date
        "date",

        # Notes
        "notes", "description",
    )

    # Add date key
    fact_df = add_date_key(fact_df, "date", "ExpenseDateSK")

    # Calculate totals
    fact_df = fact_df.withColumn(
        "totalAmount", F.col("amount") * F.coalesce("quantity", F.lit(1))
    ).withColumn(
        "totalCost", F.col("cost") * F.coalesce("quantity", F.lit(1))
    ).withColumn(
        "margin", F.col("totalAmount") - F.col("totalCost")
    )

    # If agreements provided, resolve types and add flags
    if agreement_df is not None:
        fact_df = resolve_agreement_hierarchy(
            fact_df, agreement_df, "agreementId", "expenses"
        )
        fact_df = add_agreement_flags(fact_df)
        fact_df = calculate_effective_billing_status(fact_df)

    # Add ETL metadata
    fact_df = add_etl_metadata(fact_df, layer="gold", source="connectwise")

    return fact_df