# Invoice ID Column Fixes

## Summary
Fixed column name inconsistencies in the invoice processing gold layer.

## Issues Resolved

1. **Time Entries Invoice ID**:
   - Time entries table already had dynamic column detection
   - Supports both `invoiceId` and `invoice_id` column names

2. **Product Invoice ID**: 
   - Added dynamic column detection for product invoice IDs
   - Now checks for both `invoiceId` and `invoice_id` column names
   - Fixed hardcoded reference to `invoice_id`

3. **Expense Invoice ID**:
   - Expense entries already had dynamic column detection
   - Works with both column naming conventions

4. **Catalog Item Identifier**:
   - Added conditional check for `catalogItemIdentifier` column
   - Returns null if column doesn't exist to prevent errors

## Silver Table Names
- Confirmed that PSA tables (PostedInvoice, Agreement, TimeEntry, ProductItem, ExpenseEntry) keep their original names
- These are PSA-specific tables and don't follow the BC numeric suffix convention
- BC tables have numeric suffixes stripped when moved to Silver layer

## Code Changes

### Updated files:
- `/fabric_api/gold/invoice_processing.py`

### Key fixes:
```python
# Dynamic column detection for products
product_invoice_col = "invoiceId" if "invoiceId" in silver_products.columns else "invoice_id"

# Conditional catalog item identifier
col("catalogItemIdentifier").alias("itemIdentifier") if "catalogItemIdentifier" in silver_products.columns else lit(None).alias("itemIdentifier")
```

## Testing Required
1. Verify that the pipeline runs without column name errors
2. Check that all three invoice line types (time, product, expense) are created correctly
3. Ensure proper null handling when columns are missing