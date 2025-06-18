"""ConnectWise business transformations - sophisticated logic for ConnectWise PSA data.

Implements proper fact table patterns with multiple grains:
1. Agreement facts: Period and lifetime summary grains
2. Invoice facts: Line (from time/products), header, and period grains
3. Generic transformations: Reusable patterns

Following CLAUDE.md principles: Fail fast, no silent masking.
"""

import logging
from datetime import datetime
from typing import Any

import pyspark.sql.functions as F  # noqa: N812
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def generate_date_dimension(
    spark: SparkSession,
    start_date: str,
    end_date: str | None = None,
    fiscal_year_start_month: int = 1,
) -> DataFrame:
    """Generate a comprehensive date dimension with calendar and fiscal hierarchies.

    Args:
        spark: Spark session
        start_date: Start date for the dimension (YYYY-MM-DD format)
        end_date: End date for the dimension (defaults to current date)
        fiscal_year_start_month: Month when fiscal year starts (1=Jan, 2=Feb, etc.)

    Returns:
        DataFrame with date dimension
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Validate input parameters
    if fiscal_year_start_month < 1 or fiscal_year_start_month > 12:
        raise ValueError("fiscal_year_start_month must be between 1 and 12")

    # Generate date range using Spark SQL
    date_df = spark.sql(f"""
        SELECT EXPLODE(SEQUENCE(
            TO_DATE('{start_date}'),
            TO_DATE('{end_date}'),
            INTERVAL 1 DAY
        )) as Date
    """)

    # Add DateKey (yyyyMMdd format) for joins
    date_df = date_df.withColumn("DateKey", F.date_format("Date", "yyyyMMdd").cast("int"))

    # Standard calendar hierarchy
    date_df = date_df.withColumn("CalendarYear", F.year("Date"))
    date_df = date_df.withColumn("CalendarQuarterNo", F.quarter("Date"))
    date_df = date_df.withColumn(
        "CalendarQuarter", F.concat(F.lit("Q"), F.col("CalendarQuarterNo"))
    )
    date_df = date_df.withColumn("CalendarMonthNo", F.month("Date"))
    date_df = date_df.withColumn("CalendarMonth", F.date_format("Date", "MMMM"))
    date_df = date_df.withColumn("CalendarDay", F.dayofmonth("Date"))

    # Week hierarchy
    date_df = date_df.withColumn("WeekNumber", F.weekofyear("Date"))
    date_df = date_df.withColumn("WeekDay", F.date_format("Date", "EEEE"))
    date_df = date_df.withColumn("WeekDayNo", F.dayofweek("Date"))

    # YearMonth for sorting and filtering
    date_df = date_df.withColumn("YearMonth", F.date_format("Date", "yyyy-MM"))
    date_df = date_df.withColumn(
        "YearMonthNo", F.expr("CalendarYear * 100 + CalendarMonthNo")
    )

    # Add fiscal year calculations based on fiscal_year_start_month
    date_df = date_df.withColumn(
        "FiscalYear",
        F.when(F.month("Date") >= fiscal_year_start_month, F.year("Date") + 1).otherwise(
            F.year("Date")
        ),
    )

    # Calculate fiscal quarter (1-4) based on fiscal year start
    date_df = date_df.withColumn(
        "FiscalQuarterNo",
        F.expr(
            f"mod(floor((month(Date) - {fiscal_year_start_month} + 12) % 12 / 3) + 1, 4) + 1"
        ),
    )

    # Create FiscalQuarter as a formatted string (e.g., "FQ1")
    date_df = date_df.withColumn(
        "FiscalQuarter", F.concat(F.lit("FQ"), F.col("FiscalQuarterNo"))
    )

    # Calculate fiscal month (1-12) based on fiscal year start
    date_df = date_df.withColumn(
        "FiscalMonthNo",
        F.expr(f"mod((month(Date) - {fiscal_year_start_month} + 12), 12) + 1"),
    )

    # Add IsStartOfMonth, IsEndOfMonth, etc. indicators
    date_df = date_df.withColumn("IsStartOfMonth", F.dayofmonth("Date") == 1)
    date_df = date_df.withColumn(
        "IsEndOfMonth", F.dayofmonth("Date") == F.dayofmonth(F.last_day("Date"))
    )
    date_df = date_df.withColumn(
        "IsWeekend", (F.dayofweek("Date") == 1) | (F.dayofweek("Date") == 7)
    )

    # Add previous/next date keys for easy navigation
    date_df = date_df.withColumn(
        "PreviousDateKey", F.date_format(F.date_sub("Date", 1), "yyyyMMdd").cast("int")
    )
    date_df = date_df.withColumn(
        "NextDateKey", F.date_format(F.date_add("Date", 1), "yyyyMMdd").cast("int")
    )

    logger.info(f"Date dimension generated: {date_df.count()} records")
    return date_df


def create_date_spine(
    spark: SparkSession, start_date: str, end_date: str | None = None, frequency: str = "month"
) -> DataFrame:
    """Create a date spine for period-based facts.

    Args:
        spark: SparkSession
        start_date: Start date for spine generation
        end_date: End date (defaults to current date)
        frequency: 'month' or 'day'

    Returns:
        DataFrame with period columns (all DATE type)
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if frequency == "month":
        return spark.sql(f"""
            SELECT
                TO_DATE(DATE_FORMAT(date_col, 'yyyy-MM-01')) as period_start,
                LAST_DAY(date_col) as period_end,
                YEAR(date_col) as year,
                MONTH(date_col) as month,
                QUARTER(date_col) as quarter
            FROM (
                SELECT EXPLODE(SEQUENCE(
                    TO_DATE('{start_date}'),
                    TO_DATE('{end_date}'),
                    INTERVAL 1 MONTH
                )) as date_col
            )
        """).distinct()
    else:  # daily
        return spark.sql(f"""
            SELECT
                date_col as period_start,
                date_col as period_end,
                YEAR(date_col) as year,
                MONTH(date_col) as month,
                QUARTER(date_col) as quarter
            FROM (
                SELECT EXPLODE(SEQUENCE(
                    TO_DATE('{start_date}'),
                    TO_DATE('{end_date}'),
                    INTERVAL 1 DAY
                )) as date_col
            )
        """)


def add_surrogate_keys(df: DataFrame, key_config: dict[str, dict]) -> DataFrame:
    """Add surrogate keys based on configuration.

    Args:
        df: Source DataFrame
        key_config: Dictionary mapping key names to generation config

    Returns:
        DataFrame with surrogate keys added
    """
    result_df = df

    for key_name, config in key_config.items():
        key_type = config.get("type", "hash")
        source_columns = config["source_columns"]

        if key_type == "hash":
            # Create hash-based surrogate key
            result_df = result_df.withColumn(
                key_name, F.sha2(F.concat_ws("_", *[F.col(c) for c in source_columns]), 256)
            )
        elif key_type == "date_int":
            # Create integer date key (YYYYMMDD)
            result_df = result_df.withColumn(
                key_name, F.date_format(F.col(source_columns[0]), "yyyyMMdd").cast("int")
            )

    return result_df


# AGREEMENT TRANSFORMATIONS


def create_agreement_period_fact(
    spark: SparkSession, agreement_df: DataFrame, config: dict[str, Any]
) -> DataFrame:
    """Create agreement period fact table with monthly grain.

    This creates one row per agreement per month, allowing for:
    - Monthly Recurring Revenue (MRR) tracking
    - Agreement lifecycle analysis
    - Churn and retention metrics

    Args:
        spark: SparkSession
        agreement_df: Silver layer agreement data
        config: Transformation configuration

    Returns:
        DataFrame with monthly agreement facts
    """
    # Validate required columns exist - fail fast
    required_columns = [
        "id",
        "name",
        "agreementStatus",
        "typeId",
        "typeName",
        "companyId",
        "contactId",
        "billingCycleId",
        "billAmount",
        "applicationUnits",
        "startDate",
        "endDate",
        "cancelledFlag",
        "customFields",
    ]

    missing_columns = [col for col in required_columns if col not in agreement_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns for agreement period fact: {missing_columns}")

    start_date = config.get("date_spine", {}).get("start", "2020-01-01")
    frequency = config.get("date_spine", {}).get("frequency", "month")

    # Generate date spine
    date_spine = create_date_spine(spark, start_date, frequency=frequency)

    # Prepare agreements with effective dates - use actual column names from silver
    agreements_prep = agreement_df.select(
        "id",
        "name",
        "agreementStatus",
        "typeId",
        "typeName",
        "companyId",
        "contactId",
        "billingCycleId",
        "billAmount",
        "applicationUnits",
        F.to_date(F.coalesce("startDate", F.current_date())).alias("effective_start"),
        F.coalesce("endDate", F.to_date(F.lit("2099-12-31"))).alias("effective_end"),
        "cancelledFlag",
        "customFields",
    )

    # Cross join with date spine and filter to active periods
    period_facts = date_spine.crossJoin(agreements_prep).filter(
        (F.col("period_start") >= F.col("effective_start"))
        & (F.col("period_start") <= F.col("effective_end"))
    )

    # Calculate period-specific metrics
    metrics = config.get("metrics", [])

    if "is_active_period" in metrics:
        period_facts = period_facts.withColumn(
            "is_active_period",
            F.when(
                (F.col("agreementStatus") == "Active") & (~F.col("cancelledFlag")), True
            ).otherwise(False),
        )

    if "is_new_agreement" in metrics:
        period_facts = period_facts.withColumn(
            "is_new_agreement",
            F.when(
                F.date_format("effective_start", "yyyy-MM")
                == F.date_format("period_start", "yyyy-MM"),
                True,
            ).otherwise(False),
        )

    if "is_churned_agreement" in metrics:
        period_facts = period_facts.withColumn(
            "is_churned_agreement",
            F.when(
                (
                    F.date_format("effective_end", "yyyy-MM")
                    == F.date_format("period_start", "yyyy-MM")
                )
                & (F.col("effective_end") < F.lit("2099-01-01")),
                True,
            ).otherwise(False),
        )

    if "monthly_revenue" in metrics:
        period_facts = period_facts.withColumn(
            "monthly_revenue", F.when(F.col("is_active_period"), F.col("billAmount")).otherwise(0)
        )

    if "prorated_revenue" in metrics:
        period_facts = period_facts.withColumn(
            "days_in_period",
            F.datediff(
                F.least("period_end", "effective_end"),
                F.greatest("period_start", "effective_start"),
            )
            + 1,
        ).withColumn(
            "prorated_revenue",
            F.col("monthly_revenue")
            * F.col("days_in_period")
            / F.dayofmonth(F.last_day("period_start")),
        )

    # Add running totals and period-over-period calculations
    if any(m in metrics for m in ["months_since_start", "revenue_change", "cumulative_revenue"]):
        window_spec = Window.partitionBy("id").orderBy("period_start")

        if "months_since_start" in metrics:
            period_facts = period_facts.withColumn(
                "months_since_start", F.months_between("period_start", "effective_start")
            )

        if "revenue_change" in metrics:
            period_facts = period_facts.withColumn(
                "revenue_change",
                F.col("monthly_revenue") - F.lag("monthly_revenue", 1).over(window_spec),
            )

        if "cumulative_revenue" in metrics:
            period_facts = period_facts.withColumn(
                "cumulative_revenue",
                F.sum("monthly_revenue").over(
                    window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
            )

    # Add surrogate keys
    key_config = config.get("keys", [])
    for key in key_config:
        period_facts = add_surrogate_keys(period_facts, {key["name"]: key})

    return period_facts


def create_agreement_summary_fact(
    spark: SparkSession,
    agreement_df: DataFrame,
    config: dict[str, Any],
    period_fact_df: DataFrame | None = None,
) -> DataFrame:
    """Create agreement lifetime summary facts.

    This creates one row per agreement with lifetime metrics:
    - Total revenue, Average monthly revenue
    - Lifetime duration, Churn indicators

    Args:
        spark: SparkSession
        agreement_df: Silver layer agreement data
        config: Transformation configuration
        period_fact_df: Optional period facts for enhanced metrics

    Returns:
        DataFrame with agreement lifetime summaries
    """
    # Validate required columns
    required_columns = [
        "id",
        "name",
        "agreementStatus",
        "typeId",
        "typeName",
        "companyId",
        "companyName",
        "contactId",
        "billAmount",
        "startDate",
        "endDate",
        "cancelledFlag",
        "customFields",
    ]

    missing_columns = [col for col in required_columns if col not in agreement_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns for agreement summary fact: {missing_columns}")

    # Start with base agreement data - use actual column names from silver
    summary_df = agreement_df.select(
        "id",
        "name",
        "agreementStatus",
        "typeId",
        "typeName",
        "companyId",
        "companyName",
        "contactId",
        "billAmount",
        F.coalesce("startDate", F.current_date()).alias("start_date"),
        "endDate",
        "cancelledFlag",
        "customFields",
    )

    # Calculate metrics based on configuration
    metrics = config.get("metrics", [])

    if "lifetime_days" in metrics:
        summary_df = summary_df.withColumn(
            "lifetime_days",
            F.when(F.col("endDate").isNotNull(), F.datediff("endDate", "start_date")).otherwise(
                F.datediff(F.current_date(), "start_date")
            ),
        )

    if "lifetime_months" in metrics:
        summary_df = summary_df.withColumn(
            "lifetime_months", F.round(F.col("lifetime_days") / 30.44, 1)
        )

    if "estimated_lifetime_value" in metrics:
        summary_df = summary_df.withColumn(
            "estimated_lifetime_value", F.col("billAmount") * F.col("lifetime_months")
        )

    # If period facts are available, aggregate them for actual metrics
    if period_fact_df is not None and any(
        m in metrics
        for m in ["actual_total_revenue", "actual_avg_monthly_revenue", "active_periods"]
    ):
        period_agg = period_fact_df.groupBy("id").agg(
            F.sum("monthly_revenue").alias("actual_total_revenue"),
            F.avg("monthly_revenue").alias("actual_avg_monthly_revenue"),
            F.count("*").alias("active_periods"),
            F.sum(F.col("is_new_agreement").cast("int")).alias("new_periods"),
            F.max("cumulative_revenue").alias("cumulative_lifetime_revenue"),
        )

        # Join with summary
        summary_df = summary_df.join(period_agg, on="id", how="left")

    # Add surrogate keys
    key_config = config.get("keys", [])
    for key in key_config:
        summary_df = add_surrogate_keys(summary_df, {key["name"]: key})

    return summary_df


# INVOICE TRANSFORMATIONS


def create_invoice_line_fact(
    spark: SparkSession,
    invoice_df: DataFrame,
    invoice_line_df: DataFrame,
    agreement_df: DataFrame | None = None,
) -> DataFrame:
    """Create invoice line-item fact table (most detailed grain).

    One row per invoice line item, enabling:
    - Product/service revenue analysis
    - Margin calculations
    - Resource utilization tracking

    Args:
        spark: SparkSession
        invoice_df: Invoice header data
        invoice_line_df: Invoice line item data (created from time entries/products)
        agreement_df: Optional agreement data for hierarchy resolution

    Returns:
        DataFrame with line-item level facts
    """
    # Validate invoice columns
    invoice_required = [
        "id",
        "invoiceNumber",
        "type",
        "statusName",
        "date",
        "dueDate",
        "companyId",
        "billToCompanyId",
        "agreementId",
    ]
    missing = [col for col in invoice_required if col not in invoice_df.columns]
    if missing:
        raise ValueError(f"Missing required invoice columns: {missing}")

    # Join invoice headers with lines
    line_facts = invoice_line_df.alias("lines").join(
        invoice_df.alias("inv"), F.col("lines.invoiceId") == F.col("inv.id"), "left"
    )

    # Select and transform fields
    line_facts = line_facts.select(
        # Keys
        F.sha2(F.concat_ws("_", "lines.lineNumber", "lines.invoiceId"), 256).alias("InvoiceLineSK"),
        F.sha2(F.col("lines.invoiceId").cast("string"), 256).alias("InvoiceSK"),
        F.coalesce("lines.agreementId", "inv.agreementId").alias("agreementId"),
        F.sha2(F.coalesce("lines.agreementId", "inv.agreementId").cast("string"), 256).alias(
            "AgreementSK"
        ),
        # Line details
        "lines.invoiceId",
        "lines.lineNumber",
        "lines.description",
        "lines.quantity",
        "lines.price",
        F.coalesce("lines.cost", F.lit(0)).alias("cost"),
        "lines.productClass",
        "lines.timeEntryId",
        "lines.productId",
        "lines.itemIdentifier",
        # Calculations
        "lines.lineAmount",
        (F.col("lines.quantity") * F.coalesce("lines.cost", F.lit(0))).alias("lineCost"),
        (
            F.col("lines.lineAmount")
            - (F.col("lines.quantity") * F.coalesce("lines.cost", F.lit(0)))
        ).alias("lineMargin"),
        # Margin percentage
        F.when(
            F.col("lines.lineAmount") > 0,
            (
                (
                    F.col("lines.lineAmount")
                    - (F.col("lines.quantity") * F.coalesce("lines.cost", F.lit(0)))
                )
                / F.col("lines.lineAmount")
                * 100
            ),
        )
        .otherwise(0)
        .alias("marginPercentage"),
        # Invoice header info
        "inv.invoiceNumber",
        "inv.statusName",
        "inv.type",
        "inv.date",
        "inv.dueDate",
        "inv.companyId",
        "inv.billToCompanyId",
        "inv.shipToCompanyId",
        # Dates for joining
        F.date_format("inv.date", "yyyyMMdd").cast("int").alias("InvoiceDateSK"),
        F.date_format("inv.dueDate", "yyyyMMdd").cast("int").alias("DueDateSK"),
        # Type flags
        F.when(F.col("lines.productClass") == "Service", True).otherwise(False).alias("isService"),
        F.when(F.col("lines.productClass") == "Product", True).otherwise(False).alias("isProduct"),
        F.when(F.col("lines.productClass") == "Agreement", True)
        .otherwise(False)
        .alias("isAgreement"),
        # Agreement info if available  
        "lines.agreement_type",
        "lines.final_agreement_number",
    )

    return line_facts


def create_invoice_header_fact(
    spark: SparkSession,
    invoice_df: DataFrame,
    line_facts_df: DataFrame | None = None,
) -> DataFrame:
    """Create invoice header fact table (document grain).

    One row per invoice, with aggregated line information.

    Args:
        spark: SparkSession
        invoice_df: Invoice header data
        line_facts_df: Optional pre-calculated line facts

    Returns:
        DataFrame with invoice-level facts
    """
    # Validate required columns
    required_columns = [
        "id",
        "invoiceNumber",
        "statusName",
        "type",
        "date",
        "dueDate",
        "total",
        "subtotal",
        "salesTax",
        "agreementId",
        "companyId",
        "billToCompanyId",
    ]
    missing = [col for col in required_columns if col not in invoice_df.columns]
    if missing:
        raise ValueError(f"Missing required invoice columns for header fact: {missing}")

    header_facts = invoice_df.select(
        # Keys
        F.sha2(F.col("id").cast("string"), 256).alias("InvoiceSK"),
        F.sha2(F.coalesce("agreementId", F.lit(-1)).cast("string"), 256).alias("AgreementSK"),
        # Invoice details
        "id",
        "invoiceNumber",
        "statusName",
        "type",
        "date",
        "dueDate",
        "total",
        "subtotal",
        "salesTax",
        "agreementId",
        "companyId",
        "companyName",
        "billToCompanyId",
        "billToCompanyName",
        # Date calculations
        F.datediff("dueDate", "date").alias("paymentTermsDays"),
        F.when((F.col("dueDate") < F.current_date()) & (F.col("statusName") != "Paid"), True)
        .otherwise(False)
        .alias("isOverdue"),
        # Date keys
        F.date_format("date", "yyyyMMdd").cast("int").alias("InvoiceDateSK"),
        F.date_format("dueDate", "yyyyMMdd").cast("int").alias("DueDateSK"),
    )

    # If line facts are available, aggregate them
    if line_facts_df is not None:
        line_agg = line_facts_df.groupBy("InvoiceSK").agg(
            F.count("*").alias("lineItemCount"),
            F.sum("lineAmount").alias("calculatedTotal"),
            F.sum("lineCost").alias("totalCost"),
            F.sum("lineMargin").alias("totalMargin"),
            F.avg("marginPercentage").alias("avgMarginPercentage"),
            F.sum(F.when(F.col("isService"), 1).otherwise(0)).alias("serviceLineCount"),
            F.sum(F.when(F.col("isProduct"), 1).otherwise(0)).alias("productLineCount"),
        )

        header_facts = header_facts.join(line_agg, on="InvoiceSK", how="left")

    return header_facts


def create_invoice_period_fact(
    spark: SparkSession, invoice_df: DataFrame, config: dict[str, Any]
) -> DataFrame:
    """Create invoice period fact table (temporal grain for revenue recognition).

    This spreads invoice revenue across accounting periods based on
    service dates or recognition rules.

    Args:
        spark: SparkSession
        invoice_df: Invoice data with line items
        config: Configuration with period settings

    Returns:
        DataFrame with period-level revenue facts
    """
    # Validate required columns
    required_columns = [
        "id",
        "invoiceNumber",
        "date",
        "total",
        "agreementId",
        "companyId",
        "statusName",
        "type",
    ]
    missing = [col for col in required_columns if col not in invoice_df.columns]
    if missing:
        raise ValueError(f"Missing required invoice columns for period fact: {missing}")

    period_type = config.get("period_type", "month")

    # For invoices with service periods, we need to spread revenue
    # For now, let's assign all revenue to invoice date period
    # In production, this would use service dates from time entries

    period_facts = invoice_df.select(
        "id",
        "invoiceNumber",
        "date",
        "total",
        "agreementId",
        "companyId",
        "statusName",
        "type",
    )

    if period_type == "month":
        # Assign to month
        period_facts = (
            period_facts.withColumn("period_start", F.date_format("date", "yyyy-MM-01"))
            .withColumn("period_end", F.last_day("date"))
            .withColumn("year", F.year("date"))
            .withColumn("month", F.month("date"))
            .withColumn("quarter", F.quarter("date"))
        )
    else:
        # Daily grain
        period_facts = period_facts.withColumn("period_start", F.col("date")).withColumn(
            "period_end", F.col("date")
        )

    # Add period-specific calculations
    period_facts = (
        period_facts.withColumn(
            "InvoicePeriodSK", F.sha2(F.concat_ws("_", "id", "period_start"), 256)
        )
        .withColumn("InvoiceSK", F.sha2(F.col("id").cast("string"), 256))
        .withColumn("PeriodDateSK", F.date_format("period_start", "yyyyMMdd").cast("int"))
        .withColumn(
            "recognizedRevenue",
            # In a real implementation, this would be prorated based on service dates
            F.when(F.col("statusName") == "Paid", F.col("total")).otherwise(0),
        )
        .withColumn("billedRevenue", F.col("total"))
    )

    # Add running totals by company
    window_spec = Window.partitionBy("companyId").orderBy("period_start")

    period_facts = period_facts.withColumn(
        "cumulativeRevenue",
        F.sum("billedRevenue").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        ),
    )

    return period_facts


# MAIN FACTORY FUNCTIONS


def create_agreement_facts(
    spark: SparkSession, agreement_df: DataFrame, config: dict[str, Any]
) -> dict[str, DataFrame]:
    """Create all agreement fact tables based on configuration.

    Args:
        spark: SparkSession
        agreement_df: Silver layer agreement data
        config: Transformation configuration

    Returns:
        Dictionary of fact DataFrames by name
    """
    facts = {}
    gold_transforms = config.get("gold_transforms", {})

    # Create period facts if configured
    if "fact_agreement_period" in gold_transforms:
        period_config = gold_transforms["fact_agreement_period"]
        facts["fact_agreement_period"] = create_agreement_period_fact(
            spark, agreement_df, period_config
        )

    # Create summary facts if configured
    if "fact_agreement_summary" in gold_transforms:
        summary_config = gold_transforms["fact_agreement_summary"]
        period_df = facts.get("fact_agreement_period")
        facts["fact_agreement_summary"] = create_agreement_summary_fact(
            spark, agreement_df, summary_config, period_df
        )

    return facts


def extract_agreement_number(df: DataFrame, custom_fields_col: str = "customFields") -> DataFrame:
    """Extract agreement number from customFields JSON string."""
    return df.withColumn(
        "agreementNumber",
        F.when(
            F.col(custom_fields_col).isNotNull(),
            F.get_json_object(F.get_json_object(F.col(custom_fields_col), "$[0]"), "$.value"),
        ).otherwise(None),
    )


def resolve_agreement_hierarchy(
    df: DataFrame,
    agreements_df: DataFrame,
    entity_agreement_col: str = "agreementId",
    entity_type: str = "entity",
) -> DataFrame:
    """Resolve agreement hierarchy with parent fallback logic."""
    logger.info(f"Resolving agreement hierarchy for {entity_type}")

    # Get agreement numbers from agreements table
    agreements_with_numbers = extract_agreement_number(agreements_df)

    # Join entity with agreements
    df_with_agreement = df.join(
        agreements_with_numbers.select(
            F.col("id").alias("agr_id"),
            F.col("agreementNumber").alias("direct_agreement_number"),
            F.col("parentAgreementId"),
            F.col("typeName").alias("agreement_type"),
        ),
        df[entity_agreement_col] == F.col("agr_id"),
        "left",
    )

    # If no direct agreement number, check parent
    df_with_parent = df_with_agreement.join(
        agreements_with_numbers.select(
            F.col("id").alias("parent_id"),
            F.col("agreementNumber").alias("parent_agreement_number"),
        ),
        F.col("parentAgreementId") == F.col("parent_id"),
        "left",
    )

    # Final agreement number with fallback logic
    result = df_with_parent.withColumn(
        "final_agreement_number",
        F.when(
            F.col("direct_agreement_number").isNotNull(), F.col("direct_agreement_number")
        ).otherwise(F.col("parent_agreement_number")),
    ).drop("agr_id", "parent_id", "direct_agreement_number", "parent_agreement_number")

    return result


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
    
    missing_columns = [col for col in required_columns if col not in time_entry_df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns for time entry fact: {missing_columns}")
    
    # Start with all time entries
    fact_df = time_entry_df.select(
        # Keys
        F.sha2(F.col("id").cast("string"), 256).alias("TimeEntrySK"),
        F.col("id").alias("timeEntryId"),
        
        # Foreign keys
        F.col("memberId"),
        F.col("agreementId"),
        F.col("chargeToId"),
        F.col("chargeToType"),
        F.col("invoiceId"),
        F.col("ticketId"),
        F.col("projectId"),
        
        # Dimensions
        F.col("memberName"),
        F.col("workTypeId"),
        F.col("workTypeName"),
        F.col("workRoleId"),
        F.col("workRoleName"),
        F.col("billableOption"),
        F.col("status"),
        
        # Time attributes
        F.col("timeStart"),
        F.col("timeEnd"),
        F.date_format("timeStart", "yyyyMMdd").cast("int").alias("WorkDateSK"),
        
        # Metrics
        F.col("actualHours"),
        F.col("hourlyRate"),
        F.coalesce("hourlyCost", F.lit(0)).alias("hourlyCost"),
        
        # Calculate amounts
        (F.col("actualHours") * F.col("hourlyRate")).alias("potentialRevenue"),
        (F.col("actualHours") * F.coalesce("hourlyCost", F.lit(0))).alias("actualCost"),
        
        # Notes and metadata
        F.col("notes"),
        F.col("internalNotes"),
        F.col("addToDetailDescriptionFlag"),
        F.col("addToInternalAnalysisFlag"),
        F.col("addToResolutionFlag"),
    )
    
    # Calculate margin
    fact_df = fact_df.withColumn(
        "margin",
        F.col("potentialRevenue") - F.col("actualCost")
    ).withColumn(
        "marginPercentage",
        F.when(F.col("potentialRevenue") > 0,
            (F.col("margin") / F.col("potentialRevenue") * 100)
        ).otherwise(0)
    )
    
    # Add status flags
    fact_df = fact_df.withColumn(
        "isInvoiced",
        F.col("invoiceId").isNotNull()
    ).withColumn(
        "isBillable",
        F.col("billableOption") == "Billable"
    ).withColumn(
        "isNoCharge",
        F.col("billableOption") == "NoCharge"
    ).withColumn(
        "isDoNotBill",
        F.col("billableOption") == "DoNotBill"
    )
    
    # If agreements provided, resolve agreement types
    if agreement_df is not None:
        # Get agreement types with hierarchy resolution
        fact_df = resolve_agreement_hierarchy(
            fact_df,
            agreement_df,
            entity_agreement_col="agreementId",
            entity_type="time_entries"
        )
        
        # Add agreement type flags based on business rules
        fact_df = fact_df.withColumn(
            "isBillableWork",
            F.when(
                F.trim(F.col("agreement_type")).isin(["yÞjónusta", "yþjónusta"]),
                True
            ).otherwise(False)
        ).withColumn(
            "isTimapottur",
            F.when(
                F.trim(F.col("agreement_type")).isin(["Tímapottur", "Timapottur"]),
                True
            ).otherwise(False)
        ).withColumn(
            "isInternalWork",
            F.when(
                F.trim(F.col("agreement_type")).isin(["Innri verkefni"]),
                True
            ).otherwise(False)
        )
        
        # Calculate effective billing status combining agreement type and billableOption
        fact_df = fact_df.withColumn(
            "effectiveBillingStatus",
            F.when(F.col("isInvoiced"), "Invoiced")
            .when(F.col("isInternalWork"), "Internal")
            .when(F.col("isTimapottur"), "Prepaid")
            .when(F.col("isBillableWork") & F.col("isBillable"), "Billable")
            .when(F.col("isNoCharge"), "NoCharge")
            .when(F.col("isDoNotBill"), "DoNotBill")
            .otherwise("Unknown")
        )
    
    # Add utilization calculations
    fact_df = fact_df.withColumn(
        "utilizationType",
        F.when(F.col("isInternalWork"), "Internal")
        .when(F.col("isBillable") | F.col("isTimapottur"), "Billable")
        .otherwise("Non-Billable")
    )
    
    # Add date calculations for aging
    fact_df = fact_df.withColumn(
        "daysSinceWork",
        F.datediff(F.current_date(), F.col("timeStart"))
    ).withColumn(
        "isCurrentMonth",
        F.date_format("timeStart", "yyyy-MM") == F.date_format(F.current_date(), "yyyy-MM")
    )
    
    # Add invoice aging for billable but not invoiced
    fact_df = fact_df.withColumn(
        "daysUninvoiced",
        F.when(
            (F.col("effectiveBillingStatus") == "Billable") & (~F.col("isInvoiced")),
            F.col("daysSinceWork")
        ).otherwise(None)
    )
    
    return fact_df


def create_invoice_facts(
    spark: SparkSession,
    invoice_df: DataFrame,
    config: dict[str, Any],
    timeEntryDf: DataFrame | None = None,  # noqa: N803
    productItemDf: DataFrame | None = None,  # noqa: N803
    agreementDf: DataFrame | None = None,  # noqa: N803
) -> dict[str, DataFrame]:
    """Create all invoice fact tables based on configuration.

    For ConnectWise, invoice lines are created by joining time entries and products
    that have invoice IDs. Applies business rules like Tímapottur filtering.

    Args:
        spark: SparkSession
        invoice_df: Invoice header data
        config: Configuration dictionary
        timeEntryDf: Optional time entry data for creating invoice lines
        productItemDf: Optional product data for creating invoice lines
        agreementDf: Optional agreement data for hierarchy resolution

    Returns:
        Dictionary of fact DataFrames by name
    """
    facts = {}

    # Create invoice lines from time entries and products if provided
    if timeEntryDf is not None and productItemDf is not None:
        # Validate time entry columns
        time_required = [
            "id",
            "invoiceId",
            "notes",
            "actualHours",
            "hourlyRate",
            "agreementId",
            "workTypeId",
            "workRoleId",
            "memberId",
        ]
        missing = [col for col in time_required if col not in timeEntryDf.columns]
        if missing:
            logger.warning(f"Missing time entry columns: {missing}")

        # Create lines from time entries with invoice IDs
        time_based_lines = timeEntryDf.filter(F.col("invoiceId").isNotNull()).select(
            F.col("invoiceId").cast("int").alias("invoiceId"),
            F.monotonically_increasing_id().alias("lineNumber"),
            F.col("id").alias("timeEntryId"),
            F.lit(None).alias("productId"),
            F.col("notes").alias("description"),
            F.col("actualHours").alias("quantity"),
            F.col("hourlyRate").alias("price"),
            F.lit(None).alias("cost"),
            (F.col("actualHours") * F.col("hourlyRate")).alias("lineAmount"),
            F.col("agreementId"),
            F.col("workTypeId"),
            F.col("workRoleId"),
            F.col("memberId").alias("employeeId"),
            F.lit(None).alias("itemIdentifier"),
            F.concat_ws(" - ", F.col("workTypeName"), F.col("memberName")).alias("memo"),
            F.lit("Service").alias("productClass"),
        )

        # Create lines from products with invoice IDs
        product_columns = [
            F.col("invoiceId").cast("int").alias("invoiceId"),
            F.monotonically_increasing_id().alias("lineNumber"),
            F.lit(None).alias("timeEntryId"),
            F.col("id").alias("productId"),
            F.col("description"),
            F.col("quantity"),
            F.col("price"),
            F.col("cost"),
            (F.col("quantity") * F.col("price")).alias("lineAmount"),
            F.col("agreementId"),
            F.lit(None).alias("workTypeId"),
            F.lit(None).alias("workRoleId"),
            F.lit(None).alias("employeeId"),
        ]

        # Add itemIdentifier if available
        if "catalogItemIdentifier" in productItemDf.columns:
            product_columns.append(F.col("catalogItemIdentifier").alias("itemIdentifier"))
        else:
            product_columns.append(F.lit(None).alias("itemIdentifier"))

        product_columns.extend([F.col("description").alias("memo"), F.col("productClass")])

        product_based_lines = productItemDf.filter(F.col("invoiceId").isNotNull()).select(
            *product_columns
        )

        # Union to create all invoice lines
        invoice_line_df = time_based_lines.unionByName(
            product_based_lines, allowMissingColumns=True
        )

        # If agreements provided, resolve hierarchy and filter
        if agreementDf is not None:
            # Resolve agreement hierarchy for lines
            invoice_line_df = resolve_agreement_hierarchy(
                invoice_line_df,
                agreementDf,
                entity_agreement_col="agreementId",
                entity_type="invoice_lines",
            )

            # Flag Tímapottur agreement types for special billing treatment
            invoice_line_df = invoice_line_df.withColumn(
                "isTimapottur",
                F.when(
                    F.trim(F.col("agreement_type")).isin(["Tímapottur", "Timapottur"]),
                    True
                ).otherwise(False)
            )

        # Line-level facts
        line_facts = create_invoice_line_fact(spark, invoice_df, invoice_line_df, agreementDf)
        facts["fact_invoice_line"] = line_facts
    else:
        # No line data available
        line_facts = None
        logger.warning("No time entry or product data provided for invoice lines")

    # Header-level facts
    header_facts = create_invoice_header_fact(spark, invoice_df, line_facts)
    facts["fact_invoice_header"] = header_facts

    # Period-level facts for revenue recognition
    if config.get("enable_period_facts", True):
        period_facts = create_invoice_period_fact(spark, invoice_df, config)
        facts["fact_invoice_period"] = period_facts

    return facts


__all__ = [
    "add_surrogate_keys",
    "create_agreement_facts",
    "create_agreement_period_fact", 
    "create_agreement_summary_fact",
    "create_date_spine",
    "create_invoice_facts",
    "create_invoice_header_fact",
    "create_invoice_line_fact",
    "create_invoice_period_fact",
    "extract_agreement_number",
    "generate_date_dimension",
    "resolve_agreement_hierarchy",
]
