# PSA-BC Integration Implementation Plan

## Phase 1: CDM Model Generation and Validation

### 1.1 Generate BC Models from CDM Schemas
```bash
cd /home/rationallyprime/CascadeProjects/PSA
python fabric_api/generate_cdm_models.py BC_ETL-main/BC_ETL-main/cdm_manifests/
```

**Verification:** 
- fabric_api/bc_models/models.py exists
- All 21 BC models are generated
- Models inherit from SparkModel
- Test with: `python -c "from fabric_api.bc_models import GLEntry"`

### 1.2 Validate Generated Models
```python
# Test schema generation
from fabric_api.bc_models import GLEntry, Customer, SalesInvoiceHeader
GLEntry.model_spark_schema()
Customer.model_spark_schema()
SalesInvoiceHeader.model_spark_schema()
```

**Verification:**
- No import errors
- Spark schemas generated without exceptions
- Field types match CDM definitions

## Phase 2: Bronze Data Validation

### 2.1 Read Bronze BC Tables
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("BC_Integration").getOrCreate()

# Test critical tables
gl_bronze = spark.read.table("bronze.GLEntry17")
customer_bronze = spark.read.table("bronze.Customer18")
invoice_bronze = spark.read.table("bronze.SalesInvoiceHeader112")
```

**Verification:**
- Tables exist in bronze layer
- Row counts > 0
- Schema matches CDM structure

### 2.2 Validate Data with CDM Models
```python
from fabric_api.bc_models import GLEntry

# Test model validation
gl_records = gl_bronze.limit(10).toPandas()
for _, row in gl_records.iterrows():
    GLEntry(**row.to_dict())
```

**Verification:**
- No validation errors
- All required fields present
- Data types correctly parsed

## Phase 3: Silver Transformation Pipeline

### 3.1 Create BC Silver Transformer
```python
# fabric_api/bc_transformer.py
from fabric_api.transform.dataframe_utils import flatten_all_nested_structures
from fabric_api.storage.fabric_delta import write_to_delta

def transform_bc_entity(entity_name: str, bronze_path: str, silver_path: str):
    bronze_df = spark.read.format("delta").load(bronze_path)
    
    # Flatten
    flat_df = flatten_all_nested_structures(bronze_df)
    
    # Column mapping based on entity
    column_config = BC_COLUMN_CONFIG.get(entity_name, {})
    if column_config.get("keep"):
        flat_df = flat_df.select(*column_config["keep"])
    
    # Write to silver
    write_to_delta(flat_df, silver_path, mode="overwrite")
```

**Verification:**
- Function processes without errors
- Silver tables created
- Column names standardized (snake_case)

### 3.2 Define Entity Configuration
```python
# fabric_api/bc_config.py
BC_COLUMN_CONFIG = {
    "GLEntry": {
        "keep": ["EntryNo", "GLAccountNo", "PostingDate", "Amount", "DimensionSetID"],
        "incremental_key": "EntryNo"
    },
    "Customer": {
        "keep": ["No", "Name", "Address", "City", "VATRegistrationNo"],
        "incremental_key": "No"
    }
}
```

**Verification:**
- Configuration covers all critical entities
- Keys match actual bronze column names

## Phase 4: Gold Layer Integration

### 4.1 Create Unified Customer Dimension
```python
# fabric_api/gold/dim_customer_unified.py
def create_unified_customer_dim():
    # PSA customers
    psa_customers = spark.read.table("silver.Company")
    
    # BC customers
    bc_customers = spark.read.table("silver.Customer")
    
    # Join on VAT registration or manual mapping
    unified = psa_customers.alias("psa").join(
        bc_customers.alias("bc"),
        F.col("psa.taxIdentifier") == F.col("bc.VATRegistrationNo"),
        "full_outer"
    )
    
    # Create unified structure
    unified_dim = unified.select(
        F.coalesce(F.col("psa.id"), F.col("bc.No")).alias("customer_id"),
        F.coalesce(F.col("psa.name"), F.col("bc.Name")).alias("customer_name"),
        F.col("psa.id").alias("psa_company_id"),
        F.col("bc.No").alias("bc_customer_no")
    )
    
    write_to_delta(unified_dim, "/lakehouse/default/Tables/gold/dim_customer_unified")
```

**Verification:**
- Dimension table created
- PSA and BC customers linked
- No duplicate customers

### 4.2 Create Project Financial Fact
```python
# fabric_api/gold/fact_project_financials.py
def create_project_financial_fact():
    # PSA time entries
    time_entries = spark.read.table("silver.TimeEntry")
    
    # BC job ledger entries
    job_entries = spark.read.table("silver.JobLedgerEntry")
    
    # BC GL entries
    gl_entries = spark.read.table("silver.GLEntry")
    
    # Join on project/job codes
    fact_data = time_entries.alias("psa").join(
        job_entries.alias("bc"),
        F.col("psa.project.id") == F.col("bc.JobNo"),
        "left"
    ).join(
        gl_entries.alias("gl"),
        F.col("bc.DimensionSetID") == F.col("gl.DimensionSetID"),
        "left"
    )
    
    # Aggregate financials
    project_financials = fact_data.groupBy(
        "psa.project.id",
        "bc.JobNo"
    ).agg(
        F.sum("psa.hoursBilled").alias("total_hours"),
        F.sum("psa.hourlyRate").alias("psa_revenue"),
        F.sum("gl.Amount").alias("bc_revenue"),
        F.sum("psa.hourlyCost").alias("psa_cost")
    )
    
    write_to_delta(project_financials, "/lakehouse/default/Tables/gold/fact_project_financials")
```

**Verification:**
- Fact table created
- PSA time linked to BC jobs
- Revenue/cost calculations correct

## Phase 5: Data Quality and Testing

### 5.1 Implement Data Quality Checks
```python
# fabric_api/dq_checks.py
def validate_unified_customers():
    unified = spark.read.table("gold.dim_customer_unified")
    
    # Check for orphaned records
    orphaned_psa = unified.filter(F.col("bc_customer_no").isNull()).count()
    orphaned_bc = unified.filter(F.col("psa_company_id").isNull()).count()
    
    assert orphaned_psa < unified.count() * 0.1, "Too many unmatched PSA customers"
    assert orphaned_bc < unified.count() * 0.1, "Too many unmatched BC customers"
```

**Verification:**
- DQ checks pass
- Orphaned record percentage acceptable
- No data integrity issues

### 5.2 Create Reconciliation Reports
```python
# fabric_api/reconciliation.py
def create_revenue_reconciliation():
    recon = spark.sql("""
        SELECT 
            JobNo,
            SUM(psa_revenue) as psa_total,
            SUM(bc_revenue) as bc_total,
            SUM(bc_revenue) - SUM(psa_revenue) as variance
        FROM gold.fact_project_financials
        GROUP BY JobNo
        HAVING variance > 100
    """)
    
    write_to_delta(recon, "/lakehouse/default/Tables/gold/revenue_reconciliation")
```

**Verification:**
- Reconciliation report generated
- Variances identified
- Thresholds appropriate

## Phase 6: Pipeline Orchestration

### 6.1 Create Master Pipeline
```python
# fabric_api/bc_pipeline.py
def run_bc_integration_pipeline(start_date: str = None):
    # Bronze to Silver
    bc_entities = ["GLEntry", "Customer", "Vendor", "Job", "SalesInvoiceHeader"]
    for entity in bc_entities:
        transform_bc_entity(entity, f"bronze.{entity}", f"silver.{entity}")
    
    # Silver to Gold
    create_unified_customer_dim()
    create_project_financial_fact()
    
    # DQ and Reconciliation
    validate_unified_customers()
    create_revenue_reconciliation()
```

**Verification:**
- Pipeline completes without errors
- All tables updated
- Timestamps reflect execution

### 6.2 Schedule and Monitor
```python
# Create Fabric notebook for scheduling
# Cell 1: Install package
%pip install /lakehouse/default/Files/fabric_api-0.1.0-py3-none-any.whl

# Cell 2: Run pipeline
from fabric_api.bc_pipeline import run_bc_integration_pipeline
run_bc_integration_pipeline()
```

**Verification:**
- Notebook executes successfully
- Schedule configured in Fabric
- Monitoring alerts set up

## Phase 7: Performance Optimization

### 7.1 Implement Incremental Processing
```python
# Update transformer for incremental loads
def transform_bc_entity_incremental(entity_name: str, watermark_col: str):
    last_watermark = get_last_watermark(entity_name)
    
    bronze_df = spark.read.format("delta").load(f"bronze.{entity_name}")
    new_data = bronze_df.filter(F.col(watermark_col) > last_watermark)
    
    if new_data.count() > 0:
        processed_df = flatten_all_nested_structures(new_data)
        write_to_delta(processed_df, f"silver.{entity_name}", mode="append")
        update_watermark(entity_name, new_data.agg(F.max(watermark_col)).collect()[0][0])
```

**Verification:**
- Incremental loads process only new data
- Watermarks correctly maintained
- Performance improvement measured

### 7.2 Optimize Gold Layer Queries
```python
# Add partitioning and Z-ordering
project_financials.write.format("delta") \
    .partitionBy("year", "month") \
    .option("optimizeWrite", "true") \
    .mode("overwrite") \
    .save("/lakehouse/default/Tables/gold/fact_project_financials")

# Z-order for common queries
spark.sql("OPTIMIZE gold.fact_project_financials ZORDER BY (JobNo, customer_id)")
```

**Verification:**
- Query performance improved
- Partition pruning effective
- Z-ordering reduces shuffle

## Phase 8: Documentation and Maintenance

### 8.1 Document Data Lineage
```python
# fabric_api/lineage.py
DATA_LINEAGE = {
    "gold.dim_customer_unified": {
        "sources": ["silver.Company", "silver.Customer"],
        "join_keys": ["taxIdentifier", "VATRegistrationNo"]
    },
    "gold.fact_project_financials": {
        "sources": ["silver.TimeEntry", "silver.JobLedgerEntry", "silver.GLEntry"],
        "join_keys": ["project.id", "JobNo", "DimensionSetID"]
    }
}
```

**Verification:**
- Lineage documented for all gold tables
- Dependencies clear
- Impact analysis possible

### 8.2 Create Health Check Dashboard
```python
# fabric_api/health_checks.py
def generate_health_metrics():
    metrics = {}
    
    # Table freshness
    for table in ["silver.GLEntry", "gold.fact_project_financials"]:
        df = spark.table(table)
        metrics[f"{table}_freshness"] = df.agg(F.max("SystemModifiedAt")).collect()[0][0]
    
    # Record counts
    metrics["customer_match_rate"] = calculate_customer_match_rate()
    metrics["revenue_variance"] = calculate_revenue_variance()
    
    write_to_delta(spark.createDataFrame([metrics]), "/lakehouse/default/Tables/integration_health_metrics")
```

**Verification:**
- Health metrics table populated
- Dashboard created in Fabric
- Alerts configured for anomalies