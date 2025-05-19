# Unified Lakehouse Architecture - PSA and BC Integration

## Overview

This document describes the complete multi-schema lakehouse architecture that integrates PSA (Professional Services Automation) and BC (Business Central) data pipelines. The architecture allows PSA to function standalone while seamlessly integrating with BC for comprehensive financial reporting.

## Architecture Components

### 1. Schema Structure

```
Lakehouse Root
├── bronze/                 # Raw data layer
│   ├── psa/               # PSA raw data from ConnectWise
│   │   ├── Agreement
│   │   ├── TimeEntry
│   │   ├── ExpenseEntry
│   │   ├── ProductItem
│   │   └── PostedInvoice
│   └── bc/                # BC raw data from Business Central
│       ├── Customer18
│       ├── GLEntry17
│       ├── Job167
│       └── ... (other BC tables with numeric suffixes)
│
├── silver/                # Cleaned and validated data
│   ├── psa/              # Flattened, validated PSA data
│   │   ├── Agreement
│   │   ├── TimeEntry
│   │   └── ...
│   └── bc/               # Validated BC data (numeric suffixes removed)
│       ├── Customer
│       ├── GLEntry
│       └── ...
│
└── gold/                  # Analytics-ready data
    ├── conformed/        # Shared dimensions across systems
    │   ├── dim_customer
    │   ├── dim_employee
    │   ├── dim_agreement_job
    │   ├── dim_work_type
    │   └── dim_time
    ├── psa/              # PSA-specific facts
    │   ├── fact_psa_line_items
    │   └── fact_financial_summary
    ├── bc/               # BC-specific facts and dimensions
    │   ├── dim_GLAccount
    │   ├── dim_DimensionBridge
    │   ├── fact_GLEntry
    │   └── ...
    └── integrated/       # Cross-system views
        ├── vw_unified_financial
        ├── vw_bc_job_ledger
        ├── vw_consolidated_revenue
        └── vw_employee_utilization
```

### 2. Pipeline Components

#### 2.1 Unified Lakehouse Orchestrator
**File**: `fabric_api/unified_lakehouse_orchestrator.py`

The main orchestrator that manages both PSA and BC pipelines:
- Creates and manages schema structure
- Coordinates bronze → silver → gold transformations
- Handles conformed dimension creation
- Creates integrated views

Key Methods:
- `run_full_pipeline()`: Executes complete pipeline for both systems
- `run_psa_bronze_to_silver()`: PSA-specific transformations
- `run_bc_bronze_to_silver()`: BC-specific transformations
- `run_conformed_dimensions()`: Creates shared dimensions
- `create_integrated_views()`: Creates cross-system views

#### 2.2 PSA Pipeline (Enhanced)
**File**: `fabric_api/pipeline_psa_enhanced.py`

Schema-aware PSA pipeline:
- Extracts from ConnectWise API to `bronze.psa`
- Transforms and validates to `silver.psa`
- Creates PSA-specific facts in `gold.psa`

#### 2.3 BC Pipeline (Enhanced)
**File**: `fabric_api/bc_medallion_pipeline_enhanced.py`

Schema-aware BC pipeline:
- Reads from BC API/export to `bronze.bc`
- Handles numeric suffix stripping in silver layer
- Creates BC-specific dimensions and facts in `gold.bc`

#### 2.4 Conformed Dimensions
**File**: `fabric_api/gold/conformed_dimensions_enhanced.py`

Creates dimensions that work across both systems:
- Customer dimension (PSA companies ↔ BC customers)
- Employee dimension (PSA members ↔ BC resources)
- Agreement/Job dimension (PSA agreements ↔ BC jobs)
- Work type dimension
- Time dimension (shared calendar)

#### 2.5 Integrated Views
**File**: `fabric_api/gold/integrated_views.py`

SQL views that combine PSA and BC data:
- Unified financial view
- BC job ledger view (PSA data in BC format)
- Consolidated revenue view
- Employee utilization view

## Implementation Guide

### 1. Initial Setup

```python
from fabric_api.unified_lakehouse_orchestrator import UnifiedLakehouseOrchestrator

# Initialize orchestrator
orchestrator = UnifiedLakehouseOrchestrator(spark)

# Schemas are automatically created
```

### 2. Running Individual Components

```python
# PSA only
psa_results = orchestrator.run_psa_bronze_to_silver()
psa_gold = orchestrator.run_psa_gold_processing()

# BC only
bc_results = orchestrator.run_bc_bronze_to_silver()
bc_gold = orchestrator.run_bc_gold_processing()

# Conformed dimensions
conformed = orchestrator.run_conformed_dimensions(include_bc_data=True)

# Integrated views
views = orchestrator.create_integrated_views()
```

### 3. Running Full Pipeline

```python
# Complete pipeline execution
results = orchestrator.run_full_pipeline(
    run_psa=True,
    run_bc=True,
    create_integrated=True,
    incremental=False,
    min_year=2023
)
```

### 4. Daily Incremental Updates

```python
# PSA daily update
psa_pipeline = PSAPipeline(spark)
daily_results = psa_pipeline.run_daily_pipeline(days_back=1)

# BC incremental update
bc_pipeline = BCMedallionPipelineEnhanced(spark)
bc_results = bc_pipeline.run_pipeline(incremental=True)
```

## Query Examples

### 1. Unified Financial Analysis

```sql
SELECT 
    customer_name,
    fiscal_year,
    SUM(total_amount) as revenue,
    SUM(CASE WHEN source_system = 'PSA' THEN total_amount ELSE 0 END) as psa_revenue,
    SUM(CASE WHEN source_system = 'BC' THEN total_amount ELSE 0 END) as bc_revenue
FROM gold.integrated.vw_unified_financial
WHERE fiscal_year = 2024
GROUP BY customer_name, fiscal_year
```

### 2. Employee Utilization

```sql
SELECT 
    employee_name,
    fiscal_quarter,
    SUM(total_hours) as hours,
    AVG(utilization_rate) as utilization
FROM gold.integrated.vw_employee_utilization
WHERE fiscal_year = 2024
GROUP BY employee_name, fiscal_quarter
```

### 3. Agreement/Job Performance

```sql
SELECT 
    agreement_name,
    job_code,
    SUM(psa_revenue) as psa_revenue,
    SUM(bc_amount) as bc_revenue,
    SUM(total_margin) as margin
FROM gold.integrated.vw_consolidated_revenue
WHERE has_bc_integration = 1
GROUP BY agreement_name, job_code
```

## Key Features

### 1. Schema Separation
- Clear separation between PSA and BC data
- Allows independent processing and updates
- Maintains data lineage and governance

### 2. Conformed Dimensions
- Shared dimensional model across systems
- Enables unified reporting
- Maintains mappings between system identifiers

### 3. Flexible Integration
- PSA can run standalone
- BC integration is optional
- Views provide unified interface when both available

### 4. Incremental Processing
- Supports both full and incremental loads
- Date-based filtering for recent data
- Efficient updates for large datasets

### 5. Error Handling
- Graceful handling of missing tables
- Continues processing on partial failures
- Comprehensive logging and status reporting

## Deployment Considerations

### 1. Microsoft Fabric Environment
- Designed for Fabric Spark runtime
- Uses Fabric lakehouse storage
- Leverages managed Spark sessions

### 2. Data Volumes
- Partitioning by year/company for facts
- Incremental processing for large datasets
- Schema evolution support

### 3. Security
- Credential management via Key Vault
- Row-level security on views
- Audit logging for compliance

### 4. Performance
- Optimized joins using surrogate keys
- Partition pruning for queries
- Caching for frequently accessed dimensions

## Maintenance

### 1. Adding New PSA Entities
1. Add to `psa_entities` list in orchestrator
2. Ensure model exists in `connectwise_models`
3. Update conformed dimensions if needed

### 2. Adding New BC Tables
1. Tables automatically discovered from bronze.bc
2. Add to dimension/fact lists in BC pipeline
3. Update dimension mappings if needed

### 3. Schema Changes
- Use `mergeSchema` option for silver writes
- Update Pydantic models for validation
- Regenerate models if API changes

## Troubleshooting

### Common Issues

1. **Missing Tables**
   - Check bronze layer for data
   - Verify API extraction succeeded
   - Review logs for errors

2. **Schema Mismatches**
   - Enable schema merge in silver
   - Check for column name changes
   - Update field mappings

3. **Performance Issues**
   - Check partition pruning
   - Optimize join strategies
   - Consider materialized views

### Debugging Tools

```python
# Check schema contents
spark.sql("SHOW TABLES IN bronze.psa").show()

# Verify row counts
spark.table("silver.psa.TimeEntry").count()

# Review integrated data
spark.sql("SELECT * FROM gold.integrated.vw_unified_financial LIMIT 10").show()
```

## Future Enhancements

1. **Real-time Integration**
   - Streaming from PSA webhooks
   - Change data capture for BC
   - Near real-time dashboards

2. **Advanced Analytics**
   - Predictive project profitability
   - Resource optimization models
   - Customer churn analysis

3. **Extended Integration**
   - Additional PSA systems
   - Other ERP connections
   - BI tool integrations

## Conclusion

This unified lakehouse architecture provides a robust foundation for integrating PSA and BC data while maintaining system independence. The design supports both standalone PSA operations and comprehensive financial reporting when integrated with BC.