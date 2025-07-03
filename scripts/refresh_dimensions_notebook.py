# Cell 1: Refresh Dimensions Only
# Run this in a Fabric notebook after installing the wheels

from unified_etl_connectwise.dimension_config import refresh_connectwise_dimensions
from unified_etl_core.dimensions import create_dimension_from_column, add_dimension_keys
import logging

logging.basicConfig(level=logging.INFO)

print("Starting dimension refresh process...")

# Step 1: Create dimensions from silver tables
print("Creating dimensions from silver tables...")
refresh_connectwise_dimensions(spark)

# Step 2: Create dimensions from gold calculated columns
print("Creating LineType dimension from gold fact table...")
line_type_dim = create_dimension_from_column(
    spark=spark,
    source_table="gold_cw_fact_invoice_line",
    column_name="LineType",
    dimension_name="line_type"
)
line_type_dim.write.mode("overwrite").format("delta").saveAsTable("gold.dim_line_type")
print(f"Created dim_line_type: {line_type_dim.count()} rows")

# Step 3: Add dimension keys to existing fact tables
print("Adding dimension keys to fact tables...")

# Time Entry Facts
print("Processing time entry facts...")
time_fact = spark.table("gold_cw_fact_timeentry")
time_mappings = [
    ("billableOption", "dim_billable_status", "billable_status_code", "billable_status_key"),
    ("status", "dim_time_status", "time_status_code", "time_status_key"),
    ("chargeToType", "dim_charge_type", "charge_type_code", "charge_type_key"),
    ("workTypeId", "dim_work_type", "work_type_code", "work_type_key"),
    ("workRoleId", "dim_work_role", "work_role_code", "work_role_key"),
    ("departmentId", "dim_department", "department_code", "department_key"),
    ("businessUnitId", "dim_business_unit", "business_unit_code", "business_unit_key"),
]
time_fact_with_keys = add_dimension_keys(time_fact, spark, time_mappings)
time_fact_with_keys.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_cw_fact_timeentry")
print("Updated time entry facts with dimension keys")

# Invoice Line Facts
print("Processing invoice line facts...")
invoice_fact = spark.table("gold_cw_fact_invoice_line")
invoice_mappings = [
    ("LineType", "dim_line_type", "line_type_code", "line_type_key"),
    ("productClass", "dim_product_class", "product_class_code", "product_class_key"),
    ("applyToType", "dim_invoice_apply_type", "invoice_apply_type_code", "invoice_apply_type_key"),
    ("status", "dim_invoice_status", "invoice_status_code", "invoice_status_key"),
]
invoice_fact_with_keys = add_dimension_keys(invoice_fact, spark, invoice_mappings)
invoice_fact_with_keys.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_cw_fact_invoice_line")
print("Updated invoice line facts with dimension keys")

# Expense Facts
print("Processing expense facts...")
expense_fact = spark.table("gold_cw_fact_expenseentry")
expense_mappings = [
    ("billableOption", "dim_expense_billable_status", "expense_billable_status_code", "expense_billable_status_key"),
    ("chargeToType", "dim_expense_charge_type", "expense_charge_type_code", "expense_charge_type_key"),
    ("status", "dim_expense_status", "expense_status_code", "expense_status_key"),
    ("classificationId", "dim_expense_classification", "expense_classification_code", "expense_classification_key"),
]
expense_fact_with_keys = add_dimension_keys(expense_fact, spark, expense_mappings)
expense_fact_with_keys.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("gold_cw_fact_expenseentry")
print("Updated expense facts with dimension keys")

print("Dimension refresh complete!")

# Show summary
dim_tables = spark.sql("SHOW TABLES IN gold LIKE 'dim_*'").collect()
print(f"\nCreated {len(dim_tables)} dimension tables:")
for table in dim_tables:
    count = spark.table(f"gold.{table.tableName}").count()
    print(f"  {table.tableName}: {count} rows")