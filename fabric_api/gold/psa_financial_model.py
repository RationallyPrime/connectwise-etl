"""
PSA Financial Model - Standalone with BC Integration Points.

This module creates a unified financial data model for PSA that:
1. Works independently for PSA reporting
2. Maps cleanly to BC financial structures
3. Maintains dimensional consistency
"""

import logging
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    lit,
    when,
)

logger = logging.getLogger(__name__)


def create_unified_line_items(
    time_entries: DataFrame,
    expense_entries: DataFrame,
    product_items: DataFrame,
    invoices: DataFrame,
    agreements: DataFrame,
) -> DataFrame:
    """
    Create a unified fact table for all PSA line items.
    
    This fact table serves as the foundation for both PSA reporting
    and BC integration.
    """
    
    # Standardize time entries
    time_lines = time_entries.select(
        col("id").alias("source_id"),
        lit("TIME").alias("line_type"),
        coalesce(col("invoiceId"), col("invoice_id")).alias("invoice_id"),
        col("agreementId").alias("agreement_id"),
        col("timeStart").alias("posting_date"),
        (col("actualHours") * col("hourlyRate")).alias("revenue"),
        col("actualHours").alias("quantity"),
        lit(0).alias("cost"),  # Time entries typically don't have direct cost
        col("memberId").alias("employee_id"),
        col("workTypeId").alias("work_type_id"),
        lit(None).alias("product_id"),
        col("notes").alias("description"),
        # BC mapping fields
        lit(None).alias("bc_job_no"),  # Will be populated from agreement
        lit(None).alias("bc_resource_no"),  # Will be mapped from employee
        lit("RESOURCE").alias("bc_line_type"),
    )
    
    # Standardize expense entries
    expense_lines = expense_entries.select(
        col("id").alias("source_id"),
        lit("EXPENSE").alias("line_type"),
        coalesce(col("invoiceId"), col("invoice_id")).alias("invoice_id"),
        col("agreementId").alias("agreement_id"),
        col("date").alias("posting_date"),
        col("amount").alias("revenue"),
        col("amount").alias("quantity"),
        col("amount").alias("cost"),  # For expenses, cost = amount
        col("memberId").alias("employee_id"),
        lit(None).alias("work_type_id"),
        lit(None).alias("product_id"),
        col("notes").alias("description"),
        # BC mapping fields
        lit(None).alias("bc_job_no"),
        lit(None).alias("bc_resource_no"),
        lit("EXPENSE").alias("bc_line_type"),
    )
    
    # Standardize product items
    product_lines = product_items.select(
        col("id").alias("source_id"),
        lit("PRODUCT").alias("line_type"),
        coalesce(col("invoiceId"), col("invoice_id")).alias("invoice_id"),
        col("agreementId").alias("agreement_id"),
        current_timestamp().alias("posting_date"),  # Products don't have specific dates
        (col("quantity") * col("price")).alias("revenue"),
        col("quantity"),
        col("cost"),
        lit(None).alias("employee_id"),
        lit(None).alias("work_type_id"),
        col("catalogItemId").alias("product_id"),
        col("description"),
        # BC mapping fields
        lit(None).alias("bc_job_no"),
        lit(None).alias("bc_resource_no"),
        lit("ITEM").alias("bc_line_type"),
    )
    
    # Union all line types
    all_lines = time_lines.unionByName(expense_lines).unionByName(product_lines)
    
    # Add invoice and agreement information
    enriched_lines = (
        all_lines
        .join(
            invoices.select("id", "invoiceNumber", "companyId", "statusName"),
            all_lines.invoice_id == invoices.id,
            "left"
        )
        .join(
            agreements.select("id", "agreementNumber", "parentAgreementId"),
            all_lines.agreement_id == agreements.id,
            "left"
        )
    )
    
    # Populate BC mapping fields
    final_lines = enriched_lines.withColumn(
        "bc_job_no", 
        coalesce(col("agreementNumber"), col("parentAgreementId"))
    ).withColumn(
        "line_item_key",
        concat(col("line_type"), lit("_"), col("source_id"))
    )
    
    return final_lines


def create_financial_summary(line_items: DataFrame) -> DataFrame:
    """
    Create financial summary by various dimensions.
    
    This can be used for both PSA dashboards and BC financial reporting.
    """
    return (
        line_items
        .groupBy(
            "agreement_id",
            "agreementNumber", 
            "companyId",
            "posting_date",
            "line_type"
        )
        .agg(
            sum("revenue").alias("total_revenue"),
            sum("cost").alias("total_cost"),
            sum("quantity").alias("total_quantity"),
            count("line_item_key").alias("line_count"),
            (sum("revenue") - sum("cost")).alias("gross_margin")
        )
    )


def create_bc_job_ledger_view(line_items: DataFrame) -> DataFrame:
    """
    Transform PSA line items into BC Job Ledger Entry format.
    
    This view can be used for BC integration or reporting.
    """
    return line_items.select(
        # BC Job Ledger Entry fields
        col("line_item_key").alias("EntryNo"),
        col("bc_job_no").alias("JobNo"),
        col("posting_date").alias("PostingDate"),
        col("invoiceNumber").alias("DocumentNo"),
        col("bc_line_type").alias("Type"),
        coalesce(col("bc_resource_no"), col("employee_id")).alias("No"),
        col("description").alias("Description"),
        col("quantity").alias("Quantity"),
        col("cost").alias("DirectUnitCostLCY"),
        col("revenue").alias("TotalPriceLCY"),
        col("work_type_id").alias("WorkTypeCode"),
        # Additional BC fields
        col("agreementNumber").alias("ExternalDocumentNo"),
        col("companyId").alias("$Company"),
    )


def create_bc_dimension_bridge(line_items: DataFrame, dimension_entries: DataFrame) -> DataFrame:
    """
    Create BC-compatible dimension bridge for financial reporting.
    
    Maps PSA dimensions to BC dimension framework.
    """
    # This would map PSA dimensions (department, project, etc.) to BC dimensions
    # Implementation depends on specific dimension mapping requirements
    pass


def run_psa_financial_gold(
    silver_path: str,
    gold_path: str,
    spark: SparkSession,
) -> dict[str, int]:
    """
    Run PSA financial model processing.
    
    Creates standalone PSA financial facts and BC-compatible views.
    """
    results = {}
    
    # Read silver tables
    time_entries = spark.table(f"{silver_path}.TimeEntry")
    expense_entries = spark.table(f"{silver_path}.ExpenseEntry")
    product_items = spark.table(f"{silver_path}.ProductItem")
    invoices = spark.table(f"{silver_path}.PostedInvoice")
    agreements = spark.table(f"{silver_path}.Agreement")
    
    # Create unified line items
    line_items = create_unified_line_items(
        time_entries, expense_entries, product_items, invoices, agreements
    )
    
    # Write main fact table
    line_items.write.mode("overwrite").saveAsTable(f"{gold_path}.fact_psa_line_items")
    results["fact_psa_line_items"] = line_items.count()
    
    # Create financial summary
    financial_summary = create_financial_summary(line_items)
    financial_summary.write.mode("overwrite").saveAsTable(f"{gold_path}.fact_financial_summary")
    results["fact_financial_summary"] = financial_summary.count()
    
    # Create BC-compatible views
    bc_job_ledger = create_bc_job_ledger_view(line_items)
    bc_job_ledger.write.mode("overwrite").saveAsTable(f"{gold_path}.vw_bc_job_ledger_entries")
    results["vw_bc_job_ledger_entries"] = bc_job_ledger.count()
    
    logger.info(f"PSA Financial Gold processing complete: {results}")
    return results