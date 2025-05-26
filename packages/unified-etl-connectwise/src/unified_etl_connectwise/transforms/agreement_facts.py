"""
Agreement-specific fact transformations for ConnectWise PSA.

This module handles the creation of agreement facts at the proper grain
for financial analytics and reporting.
"""

from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window


def create_agreement_period_fact(
    spark: SparkSession,
    agreement_df: DataFrame,
    start_date: str = "2020-01-01",
    end_date: str | None = None,
) -> DataFrame:
    """
    Create agreement period fact table with monthly grain.
    
    This creates one row per agreement per month, allowing for:
    - Monthly Recurring Revenue (MRR) tracking
    - Agreement lifecycle analysis
    - Churn and retention metrics
    
    Args:
        spark: SparkSession
        agreement_df: Silver layer agreement data
        start_date: Start date for period generation
        end_date: End date (defaults to current date)
        
    Returns:
        DataFrame with monthly agreement facts
    """
    # Generate date spine (monthly periods)
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Create monthly date dimension
    date_spine = spark.sql(f"""
        SELECT 
            DATE_FORMAT(date_col, 'yyyy-MM-01') as period_start,
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

    # Prepare agreements with effective dates
    agreements_prep = agreement_df.select(
        "id",
        "name",
        "agreementStatus",
        "type",
        "company",
        "contact",
        "billCycleId",
        "billAmount",
        "applicationUnits",
        F.coalesce("startDate", "dateEntered").alias("effective_start"),
        F.coalesce("endDate", F.lit("2099-12-31")).alias("effective_end"),
        "cancelledFlag",
        "dateEntered",
        "lastUpdated"
    )

    # Cross join with date spine and filter to active periods
    period_facts = date_spine.crossJoin(agreements_prep).filter(
        (F.col("period_start") >= F.col("effective_start")) &
        (F.col("period_start") <= F.col("effective_end"))
    )

    # Calculate period-specific metrics
    period_facts = period_facts.withColumn(
        "is_active_period",
        F.when(
            (F.col("agreementStatus") == "Active") &
            (~F.col("cancelledFlag")),
            True
        ).otherwise(False)
    ).withColumn(
        "is_new_agreement",
        F.when(
            F.date_format("effective_start", "yyyy-MM") ==
            F.date_format("period_start", "yyyy-MM"),
            True
        ).otherwise(False)
    ).withColumn(
        "is_churned_agreement",
        F.when(
            (F.date_format("effective_end", "yyyy-MM") ==
             F.date_format("period_start", "yyyy-MM")) &
            (F.col("effective_end") < F.lit("2099-01-01")),
            True
        ).otherwise(False)
    ).withColumn(
        "monthly_revenue",
        F.when(
            F.col("is_active_period"),
            F.col("billAmount")
        ).otherwise(0)
    ).withColumn(
        "days_in_period",
        F.datediff(
            F.least("period_end", "effective_end"),
            F.greatest("period_start", "effective_start")
        ) + 1
    ).withColumn(
        "prorated_revenue",
        F.col("monthly_revenue") * F.col("days_in_period") /
        F.dayofmonth(F.last_day("period_start"))
    )

    # Add running totals and period-over-period calculations
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

    # Generate composite keys
    period_facts = period_facts.withColumn(
        "AgreementPeriodSK",
        F.sha2(F.concat_ws("_", "id", "period_start"), 256)
    ).withColumn(
        "AgreementSK",
        F.sha2(F.col("id").cast("string"), 256)
    ).withColumn(
        "DateSK",
        F.date_format("period_start", "yyyyMMdd").cast("int")
    )

    return period_facts


def create_agreement_summary_fact(
    spark: SparkSession,
    agreement_df: DataFrame,
    period_fact_df: DataFrame | None = None
) -> DataFrame:
    """
    Create agreement lifetime summary facts.
    
    This creates one row per agreement with lifetime metrics:
    - Total revenue
    - Average monthly revenue
    - Lifetime duration
    - Churn indicators
    
    Args:
        spark: SparkSession  
        agreement_df: Silver layer agreement data
        period_fact_df: Optional period facts for enhanced metrics
        
    Returns:
        DataFrame with agreement lifetime summaries
    """
    # Start with base agreement data
    summary_df = agreement_df.select(
        "id",
        "name",
        "agreementStatus",
        "type",
        "company",
        "contact",
        "billAmount",
        F.coalesce("startDate", "dateEntered").alias("start_date"),
        "endDate",
        "cancelledFlag",
        "dateEntered"
    )

    # Calculate lifetime metrics
    summary_df = summary_df.withColumn(
        "lifetime_days",
        F.when(
            F.col("endDate").isNotNull(),
            F.datediff("endDate", "start_date")
        ).otherwise(
            F.datediff(F.current_date(), "start_date")
        )
    ).withColumn(
        "lifetime_months",
        F.round(F.col("lifetime_days") / 30.44, 1)
    ).withColumn(
        "is_active",
        (F.col("agreementStatus") == "Active") &
        (~F.col("cancelledFlag")) &
        (F.col("endDate").isNull() | (F.col("endDate") > F.current_date()))
    ).withColumn(
        "estimated_lifetime_value",
        F.col("billAmount") * F.col("lifetime_months")
    )

    # If period facts are available, aggregate them for actual metrics
    if period_fact_df is not None:
        period_agg = period_fact_df.groupBy("id").agg(
            F.sum("monthly_revenue").alias("actual_total_revenue"),
            F.avg("monthly_revenue").alias("actual_avg_monthly_revenue"),
            F.count("*").alias("active_periods"),
            F.sum(F.col("is_new_agreement").cast("int")).alias("new_periods"),
            F.max("cumulative_revenue").alias("cumulative_lifetime_revenue")
        )

        # Join with summary
        summary_df = summary_df.join(period_agg, on="id", how="left")

    # Generate surrogate key
    summary_df = summary_df.withColumn(
        "AgreementSK",
        F.sha2(F.col("id").cast("string"), 256)
    )

    return summary_df
