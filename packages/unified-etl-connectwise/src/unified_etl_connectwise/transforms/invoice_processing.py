"""
Invoice processing with proper temporal grain for ConnectWise PSA.

This module handles invoice fact creation at multiple grains:
1. Invoice line-item level (most detailed)
2. Invoice header level (document grain)
3. Invoice period level (temporal grain for revenue recognition)
"""

from datetime import datetime
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


def create_invoice_line_fact(
    spark: SparkSession,
    invoice_df: DataFrame,
    invoice_line_df: DataFrame,
) -> DataFrame:
    """
    Create invoice line-item fact table (most detailed grain).
    
    One row per invoice line item, enabling:
    - Product/service revenue analysis
    - Margin calculations
    - Resource utilization tracking
    
    Args:
        spark: SparkSession
        invoice_df: Invoice header data
        invoice_line_df: Invoice line item data
        
    Returns:
        DataFrame with line-item level facts
    """
    # Join invoice headers with lines
    line_facts = invoice_line_df.alias("lines").join(
        invoice_df.alias("inv"),
        F.col("lines.invoiceId") == F.col("inv.id"),
        "left"
    )

    # Select and transform fields
    line_facts = line_facts.select(
        # Keys
        F.sha2(F.concat_ws("_", "lines.id", "lines.invoiceId"), 256).alias("InvoiceLineSK"),
        F.sha2(F.col("lines.invoiceId").cast("string"), 256).alias("InvoiceSK"),
        F.coalesce("lines.agreementId", "inv.agreementId").alias("agreementId"),
        F.sha2(F.coalesce("lines.agreementId", "inv.agreementId").cast("string"), 256).alias("AgreementSK"),

        # Line details
        "lines.id",
        "lines.lineNumber",
        "lines.description",
        "lines.quantity",
        "lines.price",
        "lines.cost",
        "lines.productClass",
        "lines.itemId",

        # Calculations
        (F.col("lines.quantity") * F.col("lines.price")).alias("lineTotal"),
        (F.col("lines.quantity") * F.col("lines.cost")).alias("lineCost"),
        ((F.col("lines.quantity") * F.col("lines.price")) -
         (F.col("lines.quantity") * F.col("lines.cost"))).alias("lineMargin"),

        # Margin percentage
        F.when(
            F.col("lines.price") > 0,
            ((F.col("lines.price") - F.col("lines.cost")) / F.col("lines.price") * 100)
        ).otherwise(0).alias("marginPercentage"),

        # Invoice header info
        "inv.invoiceNumber",
        "inv.status",
        "inv.invoiceDate",
        "inv.dueDate",
        "inv.companyId",
        "inv.billToCompanyId",
        "inv.shipToCompanyId",

        # Dates for joining
        F.date_format("inv.invoiceDate", "yyyyMMdd").cast("int").alias("InvoiceDateSK"),
        F.date_format("inv.dueDate", "yyyyMMdd").cast("int").alias("DueDateSK"),

        # Type flags
        F.when(F.col("lines.productClass") == "Service", True).otherwise(False).alias("isService"),
        F.when(F.col("lines.productClass") == "Product", True).otherwise(False).alias("isProduct"),
        F.when(F.col("lines.productClass") == "Agreement", True).otherwise(False).alias("isAgreement"),
    )

    return line_facts


def create_invoice_header_fact(
    spark: SparkSession,
    invoice_df: DataFrame,
    line_facts_df: DataFrame | None = None,
) -> DataFrame:
    """
    Create invoice header fact table (document grain).
    
    One row per invoice, with aggregated line information.
    
    Args:
        spark: SparkSession
        invoice_df: Invoice header data
        line_facts_df: Optional pre-calculated line facts
        
    Returns:
        DataFrame with invoice-level facts
    """
    header_facts = invoice_df.select(
        # Keys
        F.sha2(F.col("id").cast("string"), 256).alias("InvoiceSK"),
        F.sha2(F.coalesce("agreementId", F.lit(-1)).cast("string"), 256).alias("AgreementSK"),

        # Invoice details
        "id",
        "invoiceNumber",
        "status",
        "type",
        "invoiceDate",
        "dueDate",
        "invoiceTotal",
        "subtotal",
        "taxTotal",
        "agreementId",
        "companyId",
        "billToCompanyId",

        # Date calculations
        F.datediff("dueDate", "invoiceDate").alias("paymentTermsDays"),
        F.when(
            (F.col("dueDate") < F.current_date()) &
            (F.col("status") != "Paid"),
            True
        ).otherwise(False).alias("isOverdue"),

        # Date keys
        F.date_format("invoiceDate", "yyyyMMdd").cast("int").alias("InvoiceDateSK"),
        F.date_format("dueDate", "yyyyMMdd").cast("int").alias("DueDateSK"),
    )

    # If line facts are available, aggregate them
    if line_facts_df is not None:
        line_agg = line_facts_df.groupBy("InvoiceSK").agg(
            F.count("*").alias("lineItemCount"),
            F.sum("lineTotal").alias("calculatedTotal"),
            F.sum("lineCost").alias("totalCost"),
            F.sum("lineMargin").alias("totalMargin"),
            F.avg("marginPercentage").alias("avgMarginPercentage"),
            F.sum(F.when(F.col("isService"), 1).otherwise(0)).alias("serviceLineCount"),
            F.sum(F.when(F.col("isProduct"), 1).otherwise(0)).alias("productLineCount"),
        )

        header_facts = header_facts.join(line_agg, on="InvoiceSK", how="left")

    return header_facts


def create_invoice_period_fact(
    spark: SparkSession,
    invoice_df: DataFrame,
    start_date: str = "2020-01-01",
    end_date: str | None = None,
    period_type: str = "month",
) -> DataFrame:
    """
    Create invoice period fact table (temporal grain for revenue recognition).
    
    This spreads invoice revenue across accounting periods based on
    service dates or recognition rules.
    
    Args:
        spark: SparkSession
        invoice_df: Invoice data with line items
        start_date: Start date for period generation
        end_date: End date (defaults to current date)
        period_type: Period granularity ('month' or 'day')
        
    Returns:
        DataFrame with period-level revenue facts
    """
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # For invoices with service periods, we need to spread revenue
    # For now, let's assign all revenue to invoice date period
    # In production, this would use service dates from time entries

    period_facts = invoice_df.select(
        "id",
        "invoiceNumber",
        "invoiceDate",
        "invoiceTotal",
        "agreementId",
        "companyId",
        "status",
        "type"
    )

    if period_type == "month":
        # Assign to month
        period_facts = period_facts.withColumn(
            "period_start",
            F.date_format("invoiceDate", "yyyy-MM-01")
        ).withColumn(
            "period_end",
            F.last_day("invoiceDate")
        ).withColumn(
            "year",
            F.year("invoiceDate")
        ).withColumn(
            "month",
            F.month("invoiceDate")
        ).withColumn(
            "quarter",
            F.quarter("invoiceDate")
        )
    else:
        # Daily grain
        period_facts = period_facts.withColumn(
            "period_start",
            F.col("invoiceDate")
        ).withColumn(
            "period_end",
            F.col("invoiceDate")
        )

    # Add period-specific calculations
    period_facts = period_facts.withColumn(
        "InvoicePeriodSK",
        F.sha2(F.concat_ws("_", "id", "period_start"), 256)
    ).withColumn(
        "InvoiceSK",
        F.sha2(F.col("id").cast("string"), 256)
    ).withColumn(
        "PeriodDateSK",
        F.date_format("period_start", "yyyyMMdd").cast("int")
    ).withColumn(
        "recognizedRevenue",
        # In a real implementation, this would be prorated based on service dates
        F.when(F.col("status") == "Paid", F.col("invoiceTotal")).otherwise(0)
    ).withColumn(
        "billedRevenue",
        F.col("invoiceTotal")
    )

    # Add running totals by company
    window_spec = Window.partitionBy("companyId").orderBy("period_start")

    period_facts = period_facts.withColumn(
        "cumulativeRevenue",
        F.sum("billedRevenue").over(
            window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )

    return period_facts


def create_invoice_facts(
    spark: SparkSession,
    invoice_df: DataFrame,
    invoice_line_df: DataFrame,
    config: dict[str, Any],
) -> dict[str, DataFrame]:
    """
    Create all invoice fact tables based on configuration.
    
    Args:
        spark: SparkSession
        invoice_df: Invoice header data
        invoice_line_df: Invoice line data
        config: Configuration dictionary
        
    Returns:
        Dictionary of fact DataFrames by name
    """
    facts = {}

    # Line-level facts (always created as base)
    line_facts = create_invoice_line_fact(spark, invoice_df, invoice_line_df)
    facts["fact_invoice_line"] = line_facts

    # Header-level facts
    header_facts = create_invoice_header_fact(spark, invoice_df, line_facts)
    facts["fact_invoice_header"] = header_facts

    # Period-level facts for revenue recognition
    if config.get("enable_period_facts", True):
        period_facts = create_invoice_period_fact(
            spark,
            invoice_df,
            start_date=config.get("period_start_date", "2020-01-01"),
            period_type=config.get("period_type", "month")
        )
        facts["fact_invoice_period"] = period_facts

    return facts
