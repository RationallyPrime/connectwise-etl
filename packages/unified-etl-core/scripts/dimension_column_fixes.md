# ConnectWise Dimension Column Fixes

## Summary of Column Name Issues Found and Fixed

### 1. BusinessUnit Dimension (silver_cw_timeentry)
- **Error**: Column `businessUnitName` doesn't exist
- **Fix**: Changed to `businessGroupDesc` (the actual column name in the model)
- **Location**: dimension_config.py line 242

### 2. Missing Column Mappings Added to config.py

#### TimeEntry Entity:
- Added `department.id` → `departmentId`
- Added `department.identifier` → `departmentIdentifier`
- Added `department.name` → `departmentName`
- Added `workType.utilizationFlag` → `workTypeUtilizationFlag`
- Added `member.dailyCapacity` → `memberDailyCapacity`
- Added column types for `departmentId` and `businessUnitId`

#### Agreement Entity:
- Added `type.id` → `typeId`
- Added `type.name` → `typeName`
- Added `billCycle.name` → `billCycleName`
- Added column type for `typeId`

#### ExpenseEntry Entity:
- Added `classification.id` → `classificationId`
- Added `classification.name` → `classificationName`
- Added column type for `classificationId`

#### Invoice Entity:
- Added `status.isClosed` → `statusIsClosed`

## Columns That Should Now Be Available

### silver_cw_timeentry:
- `businessUnitId` (integer)
- `businessGroupDesc` (string) - NOT businessUnitName
- `departmentId` (integer)
- `departmentIdentifier` (string)
- `departmentName` (string)
- `workTypeUtilizationFlag` (boolean)
- `memberDailyCapacity` (decimal)

### silver_cw_agreement:
- `typeId` (integer)
- `typeName` (string)
- `billCycleName` (string)

### silver_cw_expenseentry:
- `classificationId` (integer)
- `classificationName` (string)

### silver_cw_invoice:
- `statusIsClosed` (boolean)

## Next Steps

1. Refresh the silver tables to include these newly mapped columns
2. Run the dimension refresh script to create the rich dimensions
3. Verify that all dimension tables are created successfully

## Notes

- The ConnectWise models use camelCase naming convention
- Some fields like `department` are complex reference objects that need to be flattened
- The `businessGroupDesc` field is a direct string field, not a reference object
- All reference objects typically have `id`, `identifier`, and `name` fields