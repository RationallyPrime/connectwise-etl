"""
Gold layer processing for invoice data.

Implements the business logic from Wise AL for creating BI-ready invoice structures:
- Splits invoices into headers and lines
- Resolves agreement hierarchies
- Connects invoice lines to time entries, expenses, and products
- Applies business rules like Tímapottur filtering
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    count,
    countDistinct,
    get_json_object,
    lit,
    lower,
    max,
    sum,
    trim,
    when,
)

from ..core.spark_utils import get_spark_session
from ..storage.fabric_delta import write_to_delta

logger = logging.getLogger(__name__)


def extract_agreement_number(df: DataFrame, custom_fields_col: str = "customFields") -> DataFrame:
    """
    Extract agreement number from customFields JSON string.

    Args:
        df: DataFrame with customFields column
        custom_fields_col: Name of the custom fields column

    Returns:
        DataFrame with agreementNumber column added
    """
    # Parse JSON string and extract the first array element's value field
    return df.withColumn(
        "agreementNumber",
        when(
            col(custom_fields_col).isNotNull(),
            get_json_object(get_json_object(col(custom_fields_col), "$[0]"), "$.value"),
        ).otherwise(None),
    )


def resolve_agreement_hierarchy(
    df: DataFrame,
    agreements_df: DataFrame,
    entity_agreement_col: str = "agreementId",
    entity_type: str = "entity",
) -> DataFrame:
    """
    Resolve agreement hierarchy with parent fallback logic.

    Args:
        df: Entity DataFrame with agreement_id
        agreements_df: Agreements DataFrame with parent relationships
        entity_agreement_col: Name of the agreement ID column in entity
        entity_type: Type of entity for logging

    Returns:
        DataFrame with resolved agreement numbers and types
    """
    logger.info(f"Resolving agreement hierarchy for {entity_type}")

    # First, get agreement numbers from agreements table
    agreements_with_numbers = extract_agreement_number(agreements_df)

    # Join entity with agreements
    df_with_agreement = df.join(
        agreements_with_numbers.select(
            col("id").alias("agr_id"),
            col("agreementNumber").alias("direct_agreement_number"),
            col("parentAgreementId"),
            col("typeName").alias("agreement_type"),
        ),
        df[entity_agreement_col] == col("agr_id"),
        "left",
    )

    # If no direct agreement number, check parent
    df_with_parent = df_with_agreement.join(
        agreements_with_numbers.select(
            col("id").alias("parent_id"), col("agreementNumber").alias("parent_agreement_number")
        ),
        col("parentAgreementId") == col("parent_id"),
        "left",
    )

    # Final agreement number with fallback logic
    result = df_with_parent.withColumn(
        "final_agreement_number",
        when(col("direct_agreement_number").isNotNull(), col("direct_agreement_number")).otherwise(
            col("parent_agreement_number")
        ),
    ).drop("agr_id", "parent_id", "direct_agreement_number", "parent_agreement_number")

    return result


def create_invoice_headers_gold(
    silver_invoices: DataFrame, silver_agreements: DataFrame
) -> DataFrame:
    """
    Create gold layer invoice headers with business logic applied.

    Args:
        silver_invoices: Silver layer invoice data
        silver_agreements: Silver layer agreement data

    Returns:
        Gold layer invoice headers DataFrame
    """
    logger.info("Creating gold invoice headers")

    # Extract key fields from flattened structure - use camelCase consistently
    headers = silver_invoices.select(
        col("id").alias("invoiceId"),
        col("invoiceNumber"),
        col("type").alias("invoiceType"),
        col("statusName").alias("status"),
        col("companyId"),
        col("companyName"),
        col("billToCompanyId"),
        col("billToCompanyName"),
        col("date").alias("invoiceDate"),
        col("dueDate"),
        col("subtotal"),
        col("total"),
        col("salesTax"),
        col("agreementId"),
        col("projectId"),
        col("ticketId"),
        col("etlTimestamp"),
    ).dropDuplicates(["invoiceId"])

    # Resolve agreement hierarchy
    headers_with_agreements = resolve_agreement_hierarchy(
        headers,
        silver_agreements,
        entity_agreement_col="agreementId",
        entity_type="invoice_headers",
    )

    # Add business logic calculations
    headers_gold = headers_with_agreements.withColumn(
        "vat_percentage",
        when(
            col("salesTax").isNotNull() & (col("subtotal") > 0),
            (col("salesTax") / col("subtotal")) * 100,
        ).otherwise(0),
    ).withColumn(
        "is_agreement_invoice",
        when(lower(col("invoiceType")) == "agreement", True).otherwise(False),
    )

    return headers_gold


def create_invoice_lines_gold(
    silver_invoice_lines: DataFrame,
    silver_time_entries: DataFrame,
    silver_products: DataFrame,
    silver_agreements: DataFrame,
) -> DataFrame:
    """
    Create gold layer invoice lines with connections to time entries and products.

    Note: This is simplified - actual implementation would parse line items from invoice details.

    Args:
        silver_invoice_lines: Silver layer invoice line data
        silver_time_entries: Silver layer time entry data
        silver_products: Silver layer product data
        silver_agreements: Silver layer agreement data

    Returns:
        Gold layer invoice lines DataFrame
    """
    logger.info("Creating gold invoice lines")

    # Display column names for debugging purposes
    logger.info(f"Silver time entries columns: {silver_time_entries.columns}")

    # For actual implementation, we'd parse invoice detail arrays
    # This is a placeholder showing the structure

    # Display column names for debugging
    logger.info(f"Available columns in silver_time_entries: {', '.join(silver_time_entries.columns)}")

    # Example structure for Standard invoices (time-based)
    time_based_lines = silver_time_entries.filter(col("invoiceId").isNotNull()).select(
        col("invoiceId"),
        lit(10000).alias("lineNumber"),  # Would increment in real implementation
        col("id").alias("timeEntryId"),
        lit(None).alias("productId"),
        col("notes").alias("memo"),
        col("actualHours").alias("quantity"),
        (col("actualHours") * col("hourlyRate")).alias("lineAmount"),
        concat_ws(" - ", col("workTypeName"), col("memberName")).alias("description"),
        col("timeStart").alias("documentDate"),
        col("hourlyRate").alias("price"),
        lit(None).alias("cost"),
        col("agreementId"),
        col("memberId").alias("employeeId"),
        col("workRoleId"),
        col("workTypeId"),
    )

    # Resolve agreement hierarchy for time entries
    time_lines_with_agreements = resolve_agreement_hierarchy(
        time_based_lines,
        silver_agreements,
        entity_agreement_col="agreementId",
        entity_type="time_entry_lines",
    )

    # Log available columns after agreement hierarchy resolution
    logger.info(f"Available columns after time agreement resolution: {', '.join(time_lines_with_agreements.columns)}")

    # Filter out Tímapottur agreement types
    filtered_time_lines = time_lines_with_agreements.filter(
        ~trim(col("agreement_type")).isin(["Tímapottur", "Timapottur"])
    )

    # Display column names for debugging
    logger.info(f"Available columns in silver_products: {', '.join(silver_products.columns)}")

    # Example structure for Agreement invoices (product-based)
    product_based_lines = silver_products.filter(col("invoiceId").isNotNull()).select(
        col("invoiceId"),
        lit(10000).alias("lineNumber"),  # Would increment in real implementation
        lit(None).alias("timeEntryId"),
        col("id").alias("productId"),
        col("description").alias("memo"),
        col("quantity"),
        (col("quantity") * col("price")).alias("lineAmount"),
        col("description"),
        col("etlTimestamp").alias("documentDate"),  # Would use actual date
        col("price"),
        col("cost"),
        col("agreementId"),
        lit(None).alias("employeeId"),
        lit(None).alias("workRoleId"),
        lit(None).alias("workTypeId"),
        col("catalogItemId").alias("itemIdentifier"),
    )

    # Resolve agreement hierarchy for products
    product_lines_with_agreements = resolve_agreement_hierarchy(
        product_based_lines,
        silver_agreements,
        entity_agreement_col="agreementId",
        entity_type="product_lines",
    )

    # Log the columns we have after hierarchy resolution
    logger.info(f"Available columns after agreement resolution: {', '.join(product_lines_with_agreements.columns)}")

    # Apply special discount logic
    product_lines_with_discounts = product_lines_with_agreements.withColumn(
        "discountApplicable", when(col("itemIdentifier") != "SALE0000", True).otherwise(False)
    )

    # Union all line types
    all_lines = filtered_time_lines.unionByName(
        product_lines_with_discounts, allowMissingColumns=True
    )

    return all_lines


def create_expense_lines_gold(
    silver_expenses: DataFrame, silver_agreements: DataFrame, silver_invoices: DataFrame
) -> DataFrame:
    """
    Create gold layer expense lines linked to invoices.

    Args:
        silver_expenses: Silver layer expense data
        silver_agreements: Silver layer agreement data
        silver_invoices: Silver layer invoice data

    Returns:
        Gold layer expense lines DataFrame
    """
    logger.info("Creating gold expense lines")

    # Join expenses with invoices to get invoice numbers
    expenses_with_invoices = silver_expenses.join(
        silver_invoices.select("id", "invoiceNumber", "type"),
        silver_expenses["invoiceId"] == silver_invoices["id"],
        "left",
    )

    # Filter for Standard invoices only (Agreement invoices don't have expenses)
    standard_invoice_expenses = expenses_with_invoices.filter(lower(col("type")) != "agreement")

    # Structure expense lines
    expense_lines = standard_invoice_expenses.select(
        col("invoiceId"),
        col("invoiceNumber"),
        lit(10000).alias("lineNumber"),  # Would increment in real implementation
        col("typeName").alias("expenseType"),
        col("amount").alias("quantity"),
        col("amount").alias("lineAmount"),
        col("notes").alias("description"),
        col("date").alias("workDate"),
        col("memberIdentifier").alias("employee"),
        col("agreementId"),
        col("etlTimestamp"),
    )

    # Resolve agreement hierarchy
    expense_lines_with_agreements = resolve_agreement_hierarchy(
        expense_lines,
        silver_agreements,
        entity_agreement_col="agreementId",
        entity_type="expense_lines",
    )

    return expense_lines_with_agreements


def create_agreement_summary_gold(
    silver_agreements: DataFrame, gold_invoice_headers: DataFrame, gold_invoice_lines: DataFrame
) -> DataFrame:
    """
    Create agreement summary with aggregated invoice data.

    Args:
        silver_agreements: Silver layer agreement data
        gold_invoice_headers: Gold layer invoice headers
        gold_invoice_lines: Gold layer invoice lines

    Returns:
        Gold layer agreement summary DataFrame
    """
    logger.info("Creating gold agreement summary")

    # Extract agreement numbers with all required columns aliased
    agreements_with_numbers = extract_agreement_number(silver_agreements).select(
        col("id").alias("agr_id"),
        col("name").alias("agr_name"),
        col("agreementNumber").alias("agr_agreementNumber"),
        col("typeName").alias("agr_typeName"),
        col("companyName").alias("agr_companyName"),
        col("billToCompanyName").alias("agr_billToCompanyName"),
        col("startDate").alias("agr_startDate"),
        col("endDate").alias("agr_endDate"),
        col("agreementStatus").alias("agr_agreementStatus"),
        col("billAmount").alias("agr_billAmount"),
        col("etlTimestamp").alias("agr_etlTimestamp"),
    )
    
    # Add customer name (billToCompanyName or companyName)
    agreements_with_numbers = agreements_with_numbers.withColumn(
        "agr_customerName",
        when(col("agr_billToCompanyName").isNotNull(), col("agr_billToCompanyName"))
        .otherwise(col("agr_companyName"))
    )

    # Aggregate invoice data by agreement
    invoice_summary = gold_invoice_headers.groupBy("final_agreement_number").agg(
        count("invoiceId").alias("inv_invoiceCount"),
        sum("total").alias("inv_totalInvoiced"),
        max("invoiceDate").alias("inv_lastInvoiceDate"),
    )

    # Aggregate line data by agreement
    line_summary = gold_invoice_lines.groupBy("final_agreement_number").agg(
        count("lineNumber").alias("line_lineCount"),
        sum("lineAmount").alias("line_totalLineAmount"),
        countDistinct("timeEntryId").alias("line_timeEntryCount"),
        countDistinct("productId").alias("line_productCount"),
    )

    # Join with agreement data
    agreement_summary = (
        agreements_with_numbers.join(
            invoice_summary,
            agreements_with_numbers["agr_agreementNumber"] == invoice_summary["final_agreement_number"],
            "left",
        )
        .join(
            line_summary,
            agreements_with_numbers["agr_agreementNumber"] == line_summary["final_agreement_number"],
            "left",
        )
        .select(
            col("agr_id").alias("agreementId"),
            col("agr_name").alias("agreementName"),
            col("agr_agreementNumber").alias("agreementNumber"),
            col("agr_typeName").alias("agreementType"),
            col("agr_companyName").alias("companyName"),
            col("agr_customerName").alias("customerName"),
            col("agr_startDate").alias("startDate"),
            col("agr_endDate").alias("endDate"),
            col("agr_agreementStatus").alias("status"),
            col("agr_billAmount").alias("billAmount"),
            col("inv_invoiceCount").alias("invoiceCount"),
            col("inv_totalInvoiced").alias("totalInvoiced"),
            col("inv_lastInvoiceDate").alias("lastInvoiceDate"),
            col("line_lineCount").alias("lineCount"),
            col("line_timeEntryCount").alias("timeEntryCount"),
            col("line_productCount").alias("productCount"),
            col("agr_etlTimestamp").alias("etlTimestamp"),
        )
    )

    return agreement_summary


def run_gold_invoice_processing(
    silver_path: str = "/lakehouse/default/Tables/silver",
    gold_path: str = "/lakehouse/default/Tables/gold",
    spark: SparkSession | None = None,
) -> dict[str, int]:
    """
    Run the complete gold layer processing for invoice-related data.

    Args:
        silver_path: Base path for silver tables
        gold_path: Base path for gold tables
        spark: Spark session (created if None)

    Returns:
        Dictionary with row counts for each gold table
    """
    spark = spark or get_spark_session()
    results = {}

    # Read silver tables
    logger.info("Reading silver layer tables")
    silver_invoices = spark.table("silver.PostedInvoice")  # Using consistent camelCase table names
    silver_agreements = spark.table("silver.Agreement")
    silver_time_entries = spark.table("silver.TimeEntry")
    silver_products = spark.table("silver.ProductItem")
    silver_expenses = spark.table("silver.ExpenseEntry")

    # Log columns available in each table for debugging
    logger.info(f"TimeEntry columns: {', '.join(silver_time_entries.columns)}")
    logger.info(f"ProductItem columns: {', '.join(silver_products.columns)}")

    # Create gold invoice headers
    gold_headers = create_invoice_headers_gold(silver_invoices, silver_agreements)
    header_count = gold_headers.count()
    logger.info(f"Created {header_count} gold invoice headers")

    # Write gold headers
    write_to_delta(
        df=gold_headers,
        entity_name="invoice_headers",
        base_path=gold_path,
        mode="overwrite",
        add_timestamp=False,
    )
    results["invoice_headers"] = header_count

    # Create gold invoice lines (simplified - would need actual line parsing)
    gold_lines = create_invoice_lines_gold(
        silver_invoices.select("*"),  # Pass all columns, add proper handling in real implementation
        silver_time_entries,
        silver_products,
        silver_agreements,
    )
    line_count = gold_lines.count()
    logger.info(f"Created {line_count} gold invoice lines")

    # Write gold lines
    write_to_delta(
        df=gold_lines,
        entity_name="invoice_lines",
        base_path=gold_path,
        mode="overwrite",
        add_timestamp=False,
    )
    results["invoice_lines"] = line_count

    # Create gold expense lines
    gold_expenses = create_expense_lines_gold(silver_expenses, silver_agreements, silver_invoices)
    expense_count = gold_expenses.count()
    logger.info(f"Created {expense_count} gold expense lines")

    # Write gold expenses
    write_to_delta(
        df=gold_expenses,
        entity_name="expense_lines",
        base_path=gold_path,
        mode="overwrite",
        add_timestamp=False,
    )
    results["expense_lines"] = expense_count

    # Create agreement summary
    gold_agreements = create_agreement_summary_gold(silver_agreements, gold_headers, gold_lines)
    agreement_count = gold_agreements.count()
    logger.info(f"Created {agreement_count} gold agreement summaries")

    # Write gold agreements
    write_to_delta(
        df=gold_agreements,
        entity_name="agreement_summary",
        base_path=gold_path,
        mode="overwrite",
        add_timestamp=False,
    )
    results["agreement_summary"] = agreement_count

    return results
