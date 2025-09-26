# ConnectWise ETL for Microsoft Fabric

A production-ready ETL pipeline for extracting ConnectWise PSA data into Microsoft Fabric's Lakehouse using the medallion architecture (Bronze → Silver → Gold).

## 🚀 Quick Start

### For Microsoft Fabric Users

1. **Upload the wheel to your Fabric workspace:**
   ```python
   # In a Fabric notebook
   %pip install dist/connectwise_etl-1.0.0-py3-none-any.whl
   ```

2. **Set your ConnectWise credentials as environment variables:**
   ```python
   import os
   os.environ['CW_AUTH_USERNAME'] = 'your_username'
   os.environ['CW_AUTH_PASSWORD'] = 'your_password'
   os.environ['CW_CLIENTID'] = 'your_client_id'
   os.environ['CW_BASE_URL'] = 'https://your.connectwise.com/v4_6_release/apis/3.0'
   ```

3. **Run the ETL pipeline:**
   ```python
   from connectwise_etl import run_etl_pipeline
   
   # Full load (first time)
   run_etl_pipeline(
       spark=spark,
       layers=["bronze", "silver", "gold"],
       mode="full"
   )
   
   # Incremental updates (daily runs)
   run_etl_pipeline(
       spark=spark,
       layers=["bronze", "silver", "gold"],
       mode="incremental",
       lookback_days=7  # Only fetch last 7 days
   )
   ```

## 🎯 Key Features

### Smart Incremental Processing
- **Properly handles different timestamp fields** per entity type:
  - TimeEntry uses `dateEntered` (new records only)
  - Agreement uses `startDate` or `endDate`
  - Company uses `dateAcquired`
  - Invoice/ExpenseEntry use `date`
- **Efficient Gold layer updates** using MERGE instead of table recreation
- **Automatic fallback** for tables without timestamp columns

### Medallion Architecture
- **Bronze**: Raw data from ConnectWise API with minimal transformation
- **Silver**: Cleaned, flattened, deduplicated data
- **Gold**: Business-ready dimensional model with facts and dimensions

### Built for Scale
- Handles large ConnectWise datasets efficiently
- Parallel extraction from multiple endpoints
- Automatic retry logic with exponential backoff
- Structured error handling and logging

## 📊 Data Model

### Supported Entities
- **Time Entries** - Labor tracking and billing
- **Agreements** - Service contracts and SLAs  
- **Companies** - Client and vendor records
- **Expenses** - Expense tracking
- **Products** - Product catalog items
- **Invoices** - Billing records
- **Members** - Staff/technician records

### Gold Layer Schema
```
Facts:
├── fact_timeentry      # Time tracking measures
├── fact_expenseentry   # Expense measures
└── fact_productitem    # Product sales measures

Dimensions:
├── dimcompany          # Company/client dimension
├── dimmember          # Staff dimension
├── dimagreementtype   # Agreement classifications
├── dimbillablestatus  # Billable/non-billable
├── dimworktype        # Work classifications
└── dimdepartment      # Organizational structure
```

## ⚙️ Configuration

### Environment Variables
```bash
# Required
CW_AUTH_USERNAME=your_connectwise_username
CW_AUTH_PASSWORD=your_connectwise_password  
CW_CLIENTID=your_client_id

# Optional (defaults shown)
CW_BASE_URL=https://your.connectwise.com/v4_6_release/apis/3.0
```

### Incremental Lookback Periods
Different entities have optimized lookback windows:
- **TimeEntry**: 30 days (frequent updates)
- **Agreement**: 90 days (less frequent changes)
- **Invoice**: 60 days (billing cycle considerations)
- **ProductItem**: 180 days (stable catalog)

## 🔧 Advanced Usage

### Extract Specific Entities Only
```python
from connectwise_etl.client import ConnectWiseClient

client = ConnectWiseClient()
df = client.extract(
    endpoint="/time/entries",
    conditions="dateEntered>=[2024-01-01]",
    page_size=1000
)
```

### Custom Transformations
```python
from connectwise_etl.transforms import create_time_entry_fact

# Load your data
time_entries = spark.table("silver.silver_cw_timeentry")
agreements = spark.table("silver.silver_cw_agreement")

# Create custom fact table
fact_df = create_time_entry_fact(
    spark=spark,
    time_entry_df=time_entries,
    agreement_df=agreements
)
```

### Run Specific Layers
```python
# Just refresh Bronze layer
run_etl_pipeline(spark, layers=["bronze"], mode="incremental")

# Update Silver and Gold only
run_etl_pipeline(spark, layers=["silver", "gold"], mode="incremental")
```

## 🧪 Testing

Run the test suite:
```bash
# Unit tests only
./scripts/run_tests.sh

# Include integration tests (requires ConnectWise credentials)
./scripts/run_tests.sh --integration

# Generate coverage report
./scripts/run_tests.sh --coverage
```

## 🏗️ Architecture Details

### Incremental Processing Flow
1. **API Extraction** → Uses source system dates (dateEntered, lastUpdated)
2. **Bronze Layer** → Stores with etlTimestamp for tracking
3. **Silver Layer** → Propagates changes based on etlTimestamp
4. **Gold Layer** → MERGE operations preserve history

### Error Handling
- Structured error types with specific handling per error class
- Automatic retries for transient failures
- Detailed logging with Logfire integration
- Graceful degradation for missing optional data

## 📦 Project Structure
```
connectwise-etl/
├── src/connectwise_etl/
│   ├── client.py         # ConnectWise API client
│   ├── incremental.py    # Incremental processing logic
│   ├── transforms.py     # Business transformations
│   ├── main.py          # Main pipeline orchestration
│   └── models/          # Pydantic models for validation
├── tests/               # Pytest test suite
├── configs/            # Generation configuration
└── scripts/            # Utility scripts
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests to ensure nothing breaks
4. Submit a pull request

## 📄 License

MIT License - See LICENSE file for details

## 🙋 Support

For issues or questions:
- GitHub Issues: [github.com/RationallyPrime/connectwise-etl/issues](https://github.com/RationallyPrime/connectwise-etl/issues)
- Email: hakonf@wise.is

## 🔄 Recent Updates

### v1.0.0 (2024)
- Fixed incremental processing logic for correct timestamp handling
- Optimized Gold layer to use MERGE instead of full table recreation
- Added comprehensive test suite with pytest
- Cleaned up configuration files and project structure
- Improved error handling and logging