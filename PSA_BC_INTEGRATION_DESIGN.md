# PSA-BC Integration Design

## Overview

This design creates a PSA financial data model that:
1. Works standalone for PSA reporting
2. Integrates seamlessly with BC when needed
3. Uses conformed dimensions that work for both systems

## Architecture

### Layer Structure

```
Silver Layer (Raw Data)
├── PSA Tables
│   ├── TimeEntry
│   ├── ExpenseEntry
│   ├── ProductItem
│   ├── PostedInvoice
│   └── Agreement
└── BC Tables (Optional)
    ├── Customer
    ├── Resource
    ├── Job
    └── WorkType

Gold Layer (Analytics)
├── Conformed Dimensions
│   ├── dim_customer (PSA + BC mapping)
│   ├── dim_employee (PSA + BC mapping)
│   ├── dim_agreement_job (PSA + BC mapping)
│   ├── dim_work_type (PSA + BC mapping)
│   └── dim_time (shared)
├── PSA Facts
│   ├── fact_psa_line_items (unified time/expense/product)
│   └── fact_financial_summary (aggregated)
└── BC Integration Views
    ├── vw_bc_job_ledger_entries
    ├── vw_bc_sales_invoice_lines
    └── vw_bc_dimension_entries
```

### Key Design Principles

1. **Unified Line Items**: All PSA transactions (time, expense, product) are unified into a single fact table
2. **Optional BC Mapping**: BC fields are optional - system works without BC data
3. **Conformed Dimensions**: Dimensions contain both PSA and BC identifiers
4. **Flexible Integration**: Can run standalone or with BC integration

## Implementation

### 1. Unified Fact Table

```python
fact_psa_line_items:
  # PSA Core Fields
  - line_item_key (unique)
  - line_type (TIME/EXPENSE/PRODUCT)
  - invoice_id
  - agreement_id
  - amount, quantity, cost
  - posting_date
  
  # Dimension Keys
  - customer_key
  - employee_key
  - product_key
  - work_type_key
  
  # BC Mapping Fields (Optional)
  - bc_job_no
  - bc_resource_no
  - bc_gl_account_no
```

### 2. Conformed Dimensions

Each dimension contains:
- PSA identifiers (required)
- BC identifiers (optional)
- Unified business attributes

Example - Customer Dimension:
```python
dim_customer:
  - customer_key (surrogate)
  - psa_company_id
  - bc_customer_no (optional)
  - customer_name
  - customer_type
  - is_integrated (flag)
```

### 3. BC Integration Views

Views transform PSA data into BC format:

```sql
CREATE VIEW vw_bc_job_ledger_entries AS
SELECT 
  line_item_key as EntryNo,
  bc_job_no as JobNo,
  posting_date as PostingDate,
  invoice_number as DocumentNo,
  -- ... other BC fields
FROM fact_psa_line_items
```

## Benefits

1. **Independence**: PSA reporting works without BC
2. **Integration**: Easy BC integration when needed
3. **Consistency**: Same dimensions for both systems
4. **Flexibility**: Can gradually add BC mappings
5. **Performance**: Pre-joined facts reduce query complexity

## Usage Examples

### PSA-Only Query
```sql
-- Revenue by line type
SELECT line_type, SUM(revenue) 
FROM fact_psa_line_items
GROUP BY line_type
```

### BC-Integrated Query
```sql
-- Job profitability (works with BC tools)
SELECT JobNo, SUM(TotalPriceLCY - DirectCostLCY)
FROM vw_bc_job_ledger_entries
GROUP BY JobNo
```

### Hybrid Analysis
```sql
-- Customer analysis with BC mapping info
SELECT 
  c.customer_name,
  c.bc_customer_no,
  SUM(f.revenue) as total_revenue
FROM fact_psa_line_items f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_integrated = TRUE
GROUP BY c.customer_name, c.bc_customer_no
```

## Migration Path

1. **Phase 1**: Build PSA-only model
2. **Phase 2**: Add BC dimension mappings
3. **Phase 3**: Create BC views
4. **Phase 4**: Full integration

This approach ensures PSA can operate independently while providing a clear path to BC integration.