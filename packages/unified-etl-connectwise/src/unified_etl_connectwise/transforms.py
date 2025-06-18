"""ConnectWise-specific business transformations.

Leverages core utilities and agreement logic to create fact tables.
Following CLAUDE.md principles: Fail fast, no silent masking.
"""

import logging
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from unified_etl_core.date_utils import add_date_key, create_date_spine
from .agreement_utils import (
    add_agreement_flags,
    add_surrogate_keys,
    calculate_effective_billing_status,
    filter_billable_time_entries,
    resolve_agreement_hierarchy,
)

logger = logging.getLogger(__name__)


def create_time_entry_fact(
    spark: SparkSession,
    time_entry_df: DataFrame,
    agreement_df: DataFrame | None = None,
    member_df: DataFrame | None = None,
    config: dict[str, Any] | None = None,
) -> DataFrame:
    """Create comprehensive time entry fact table capturing ALL work.
    
    This creates one row per time entry, enabling:
    - Complete cost tracking (including internal work)
    - Billable vs non-billable analysis
    - Tímapottur consumption tracking
    - Real-time profitability before invoicing
    - Employee utilization metrics
    
    Args:
        spark: SparkSession
        time_entry_df: All time entries from silver layer
        agreement_df: Optional agreement data for type resolution
        member_df: Optional member data for cost rates
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
        
        # Dimensions
        "memberName", "workTypeId", "workTypeName", "workRoleId",
        "workRoleName", "billableOption", "status",
        
        # Time attributes
        "timeStart", "timeEnd",
        
        # Metrics
        "actualHours", "hourlyRate",
        F.coalesce("hourlyCost", F.lit(0)).alias("hourlyCost"),
        
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
        .when(F.col("isTimapottur"), "Prepaid")
        .otherwise("Non-Billable")
    )
    
    # Add aging calculations
    fact_df = fact_df.withColumn(
        "daysSinceWork",
        F.datediff(F.current_date(), F.col("timeStart"))
    ).withColumn(
        "daysUninvoiced",
        F.when(
            (F.col("effectiveBillingStatus") == "Billable") & 
            (F.col("invoiceId").isNull()),
            F.col("daysSinceWork")
        ).otherwise(None)
    )
    
    return fact_df


def create_invoice_line_fact(
    spark: SparkSession,
    invoice_df: DataFrame,
    time_entry_df: DataFrame | None = None,
    product_df: DataFrame | None = None,
    agreement_df: DataFrame | None = None,
) -> DataFrame:
    """Create invoice line-item fact table.
    
    Creates invoice lines from billable time entries and products,
    excluding Tímapottur and internal work per Business Central logic.
    """
    lines = []
    
    # Process time entries if available
    if time_entry_df is not None:
        # Enrich time entries with agreement info
        if agreement_df is not None:
            time_enriched = resolve_agreement_hierarchy(
                time_entry_df, agreement_df, "agreementId", "time_entries"
            )
            time_enriched = add_agreement_flags(time_enriched)
            # Filter out Tímapottur and internal work
            time_enriched = filter_billable_time_entries(time_enriched)
        else:
            # Without agreement info, only include invoiced entries
            time_enriched = time_entry_df.filter(F.col("invoiceId").isNotNull())
        
        # Create lines from time entries
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
        lines.append(time_lines)
    
    # Process products if available
    if product_df is not None:
        product_lines = product_df.filter(F.col("invoiceId").isNotNull()).select(
            F.col("invoiceId").cast("int"),
            F.monotonically_increasing_id().alias("lineNumber"),
            F.lit(None).alias("timeEntryId"),
            F.col("id").alias("productId"),
            "description", "quantity", "price", "cost",
            (F.col("quantity") * F.col("price")).alias("lineAmount"),
            "agreementId",
            F.lit(None).alias("workTypeId"),
            F.lit(None).alias("workRoleId"),
            F.lit(None).alias("employeeId"),
            F.col("description").alias("memo"),
            "productClass",
        )
        lines.append(product_lines)
    
    if not lines:
        raise ValueError("No time entries or products provided for invoice lines")
    
    # Union all lines
    invoice_lines = lines[0]
    for line_df in lines[1:]:
        invoice_lines = invoice_lines.unionByName(line_df, allowMissingColumns=True)
    
    # Join with invoice headers
    line_facts = invoice_lines.join(
        invoice_df.select(
            F.col("id").alias("inv_id"),
            "invoiceNumber", "statusName", "type", "date", "dueDate",
            "companyId", "companyName", "billToCompanyId",
        ),
        invoice_lines.invoiceId == F.col("inv_id"),
        "left"
    ).drop("inv_id")
    
    # Add surrogate keys
    line_facts = add_surrogate_keys(line_facts, {
        "InvoiceLineSK": {"type": "hash", "source_columns": ["invoiceId", "lineNumber"]},
        "InvoiceSK": {"type": "hash", "source_columns": ["invoiceId"]},
    })
    
    # Add date keys
    line_facts = add_date_key(line_facts, "date", "InvoiceDateSK")
    line_facts = add_date_key(line_facts, "dueDate", "DueDateSK")
    
    # Calculate line-level metrics
    line_facts = line_facts.withColumn(
        "lineCost", F.col("quantity") * F.col("cost")
    ).withColumn(
        "lineMargin", F.col("lineAmount") - F.col("lineCost")
    ).withColumn(
        "marginPercentage",
        F.when(F.col("lineAmount") > 0,
            (F.col("lineMargin") / F.col("lineAmount") * 100)
        ).otherwise(0)
    )
    
    return line_facts


def create_agreement_period_fact(
    spark: SparkSession,
    agreement_df: DataFrame,
    config: dict[str, Any],
) -> DataFrame:
    """Create agreement period fact with monthly grain for MRR tracking."""
    # Extract configuration
    start_date = config.get("start_date", "2020-01-01")
    frequency = config.get("frequency", "month")
    
    # Generate date spine
    date_spine = create_date_spine(spark, start_date, frequency=frequency)
    
    # Prepare agreements
    agreements_prep = agreement_df.select(
        "id", "name", "agreementStatus", "typeId", "typeName",
        "companyId", "companyName", "contactId", "billingCycleId",
        "billAmount", "applicationUnits",
        F.to_date(F.coalesce("startDate", F.current_date())).alias("effective_start"),
        F.coalesce("endDate", F.to_date(F.lit("2099-12-31"))).alias("effective_end"),
        "cancelledFlag", "customFields",
    )
    
    # Cross join and filter to active periods
    period_facts = date_spine.crossJoin(agreements_prep).filter(
        (F.col("period_start") >= F.col("effective_start")) &
        (F.col("period_start") <= F.col("effective_end"))
    )
    
    # Add period metrics
    period_facts = period_facts.withColumn(
        "is_active_period",
        (F.col("agreementStatus") == "Active") & (~F.col("cancelledFlag"))
    ).withColumn(
        "is_new_agreement",
        F.date_format("effective_start", "yyyy-MM") == F.date_format("period_start", "yyyy-MM")
    ).withColumn(
        "is_churned_agreement",
        (F.date_format("effective_end", "yyyy-MM") == F.date_format("period_start", "yyyy-MM")) &
        (F.col("effective_end") < F.lit("2099-01-01"))
    ).withColumn(
        "monthly_revenue",
        F.when(F.col("is_active_period"), F.col("billAmount")).otherwise(0)
    )
    
    # Add running calculations
    window_spec = Window.partitionBy("id").orderBy("period_start")
    
    period_facts = period_facts.withColumn(
        "months_since_start",
        F.months_between("period_start", "effective_start")
    ).withColumn(
        "revenue_change",
        F.col("monthly_revenue") - F.lag("monthly_revenue", 1).over(window_spec)
    ).withColumn(
        "cumulative_revenue",
        F.sum("monthly_revenue").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
    
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
        "expenseTypeId", "expenseTypeName", "date", "amount",
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
        "expenseTypeId", "expenseTypeName", "billableOption",
        
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
    
    return fact_df