# BC Pipeline Fixes Summary

## Issues Fixed

1. **PSA Tables Exclusion**: 
   - Added logic to exclude PSA tables (Agreement, PostedInvoice, UnpostedInvoice, ExpenseEntry, TimeEntry, ProductItem) from BC processing
   - These tables don't have numeric suffixes and aren't part of Business Central

2. **Numeric Suffix Handling**:
   - Bronze BC tables have numeric suffixes (e.g., GLEntry17, Customer18)
   - Pipeline now strips these suffixes when creating Silver tables
   - Only processes tables with numeric suffixes as BC tables (excluding known PSA tables)

3. **Schema Mismatch Handling**:
   - Added `mergeSchema=true` option when writing to Silver layer
   - This handles schema evolution and mismatches between Bronze and Silver

4. **Table Discovery**:
   - Pipeline now discovers tables from Bronze layer automatically
   - Properly filters out broken tables (like VendorLedgerEntry25)
   - Handles tables that don't exist gracefully

5. **Model Mapping Updates**:
   - Updated model mapping to include all BC models without numeric suffixes
   - Added proper natural keys for all tables based on BC structure
   - Separated dimensions and facts properly

6. **Natural Key Handling**:
   - Made surrogate key generation more robust
   - Handles cases where natural keys don't exist in the dataframe
   - Uses only existing keys for surrogate key generation

## Key Changes

### bc_medallion_pipeline.py
- Updated model mapping to include all BC tables
- Added proper PSA table exclusion logic
- Enhanced Bronze to Silver processing with better error handling
- Added schema merge option for Silver writes
- Made natural key handling more flexible

### fabric_bc_pipeline_test.ipynb
- Updated to let pipeline discover tables automatically
- Added better error handling with full tracebacks
- Simplified initialization

## What the Pipeline Now Does

1. **Bronze → Silver**:
   - Discovers all Bronze tables automatically
   - Filters out PSA tables (no numeric suffix)
   - Filters out broken tables
   - Strips numeric suffixes from BC table names
   - Handles schema mismatches with merge option
   - Creates clean Silver tables

2. **Silver → Gold**:
   - Creates dimension tables with surrogate keys
   - Creates fact tables with dimension lookups
   - Builds dimension bridge for BC's dimension framework
   - Adds fiscal year and date dimensions

3. **Data Quality**:
   - Validates against Pydantic models where available
   - Removes duplicates based on natural keys
   - Handles missing columns gracefully
   - Adds processing timestamps

## Next Steps

1. Run the updated notebook with the new wheel file
2. Verify Silver tables are created without numeric suffixes
3. Check that PSA tables are properly excluded
4. Confirm dimension bridge and fact tables are created in Gold
5. Test incremental processing if needed