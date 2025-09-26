# ConnectWise Manage ETL for Microsoft Fabric

A specialized ETL pipeline for extracting ConnectWise Manage PSA data into Microsoft Fabric Lakehouse, designed for Icelandic IT service companies with sophisticated agreement classification and billing logic.

## 🎯 What This Actually Does

This ETL system extracts data from **ConnectWise Manage API v3.0** (specifically the 2025.1 version) and transforms it through a medallion architecture (Bronze → Silver → Gold) for analytics in Power BI. It's tailored for Icelandic business operations with built-in support for local agreement types and billing patterns.

### Core Entities Extracted
- **Agreements** - Service contracts with Icelandic classification (Tímapottur, yÞjónusta, etc.)
- **Time Entries** - Labor tracking with billability classification
- **Companies** - Customer and vendor records
- **Members** - Staff/technician data
- **Expenses** - Reimbursable cost tracking
- **Products** - Catalog items and sales
- **Invoices** - Billing records (posted and unposted)

## 🚀 Installation & Setup

### Prerequisites
- Microsoft Fabric workspace with Data Engineering experience
- ConnectWise Manage API credentials
- Python 3.11+ environment

### Installation in Fabric Notebook
```python
# Upload the wheel to your lakehouse Files section, then:
%pip install /lakehouse/default/Files/connectwise_etl-1.0.0-py3-none-any.whl
```

### Required Environment Variables
```python
import os
os.environ['CW_AUTH_USERNAME'] = 'your_connectwise_username'
os.environ['CW_AUTH_PASSWORD'] = 'your_connectwise_password'
os.environ['CW_CLIENTID'] = 'your_client_id'

# Optional - defaults to Icelandic deployment
os.environ['CW_BASE_URL'] = 'https://your.connectwise.com/v4_6_release/apis/3.0'
```

## 📊 Usage

### Basic Pipeline Execution
```python
from connectwise_etl import run_etl_pipeline

# Incremental update (recommended for daily runs)
run_etl_pipeline(
    spark=spark,  # Fabric's global spark session
    layers=["bronze", "silver", "gold"],
    mode="incremental",
    lookback_days=7  # Fetch last 7 days of changes
)

# Full refresh (initial load or recovery)
run_etl_pipeline(
    spark=spark,
    layers=["bronze", "silver", "gold"],
    mode="full",
    lookback_days=30  # Used for initial filtering even in full mode
)
```

### Processing Specific Layers
```python
# Just update Bronze layer (raw extraction)
run_etl_pipeline(spark, layers=["bronze"], mode="incremental")

# Transform existing Bronze data to Silver/Gold
run_etl_pipeline(spark, layers=["silver", "gold"], mode="incremental")
```

## 🏗️ Architecture

### Medallion Layers

#### Bronze Layer (Raw Extraction)
- Direct API extraction with minimal transformation
- Preserves nested JSON structures from ConnectWise
- Adds `etlTimestamp` for tracking extraction time
- Uses MERGE operations for incremental updates

#### Silver Layer (Cleansed Data)
- Flattens nested structures up to 3 levels deep
- Resolves camelCase field name conflicts
- Adds ETL metadata (`_etl_processed_at`, `_etl_source`, `_etl_batch_id`)
- Implements SCD Type 1 (overwrite) for changes

#### Gold Layer (Business Model)
- Star schema with facts and dimensions
- YAML-driven dimension generation
- Icelandic business rule application
- Optimized for Power BI consumption

### Incremental Processing Strategy

**Recent Fix**: The system now uses `lastUpdated` instead of `dateEntered` to catch both new AND modified records. Each entity has optimized lookback periods:

| Entity | Lookback Days | Reason |
|--------|--------------|---------|
| Agreement | 90 | Contracts change infrequently |
| TimeEntry | 30 | Recent work tracking |
| ExpenseEntry | 30 | Current expense tracking |
| Invoice | 60 | Billing cycle coverage |
| PostedInvoice | 90 | Posted invoices rarely change |
| ProductItem | 180 | Stable product catalog |
| UnpostedInvoice | 7 | Active work in progress |

## 🇮🇸 Icelandic Business Logic

### Agreement Type Classification
The system automatically classifies agreements based on Icelandic naming patterns:

- **Tímapottur** → Prepaid hour buckets (excluded from invoicing)
- **yÞjónusta** → Billable services
- **Innri verkefni** → Internal projects (non-billable)
- **Rekstrarþjónusta/Alrekstur** → Operations support
- **Hugbúnaðarþjónusta** → Software services
- **Office 365** → Microsoft licensing

### Why This Matters
The classification ensures proper revenue recognition and prevents double-billing of prepaid hours. It also captures ALL work (including internal projects) to avoid the "$18M missing revenue" problem.

## 📁 Data Model

### Fact Tables
```
fact_timeentry      # Time tracking with utilization metrics
fact_expenseentry   # Expense tracking and reimbursables  
fact_productitem    # Product sales and inventory
```

### Key Dimensions
```
dimcompany          # Customer/vendor master
dimmember           # Staff directory
dimagreementtype    # Agreement classifications (Icelandic)
dimbillablestatus   # Billable/Non-billable/No Charge
dimworktype         # Work categorization
dimdepartment       # Organizational structure
```

## 🔧 Advanced Features

### Direct API Extraction
```python
from connectwise_etl.client import ConnectWiseClient

client = ConnectWiseClient()
df = client.extract(
    endpoint="/time/entries",
    conditions="lastUpdated>=[2024-01-01]",  # Note: uses lastUpdated not dateEntered!
    page_size=1000
)
```

### Custom Agreement Processing
```python
from connectwise_etl.agreement_utils import classify_agreement_type

# The system handles complex agreement hierarchies and custom field parsing
agreement_type, billing_behavior = classify_agreement_type(agreement_name)
```

## 🧪 Testing

```bash
# Run unit tests
./scripts/run_tests.sh

# Run with integration tests (requires ConnectWise credentials)
./scripts/run_tests.sh --integration

# Generate coverage report
./scripts/run_tests.sh --coverage
```

## ⚠️ Known Limitations

1. **Models are auto-generated** - Don't modify files in `models/` as they're regenerated from OpenAPI schema
2. **Icelandic character handling** - Regex patterns handle Þ, ð, á, é, í, ó, ú, ý, þ, æ, ö
3. **Memory considerations** - Large initial loads may require smaller `page_size` values
4. **Schema evolution** - Use `overwriteSchema=true` for structural changes
5. **Member entity** - May lack `etlTimestamp` if loaded separately; falls back to full scan

## 📦 Technical Stack

- **PySpark 3.5.5+** - Distributed processing engine
- **SparkDantic 1.1.1+** - Pydantic models for Spark DataFrames
- **Pydantic 2.11.4+** - Data validation from OpenAPI schema
- **PyYAML 6.0.0+** - YAML-driven configuration
- **Requests 2.32.0+** - HTTP client with retry logic

## 🔄 Recent Improvements

### Incremental Processing Fix (2024)
- Fixed critical bug using `dateEntered` instead of `lastUpdated`
- Added entity-specific timestamp field detection
- Implemented proper MERGE operations in Gold layer
- Added fallback for tables without timestamp columns

### Configuration Simplification
- Replaced "config monster" with YAML-driven approach
- Consolidated duplicate configuration files
- Improved error handling with structured exceptions

## 📝 License

MIT License - See LICENSE file

## 📞 Support

**GitHub Issues**: [github.com/RationallyPrime/connectwise-etl](https://github.com/RationallyPrime/connectwise-etl/issues)  
**Author**: Hákon Freyr Gunnarsson (hakonf@wise.is)

---

*Built for production use at Þekking Tristan hf, processing ConnectWise data for Icelandic IT service operations.*