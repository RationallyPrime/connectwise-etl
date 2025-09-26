# ConnectWise Manage ETL for Microsoft Fabric

ETL pipeline for extracting ConnectWise Manage PSA data into Microsoft Fabric Lakehouse, with Icelandic agreement classification and billing logic.

## Installation

```bash
# In Fabric notebook:
%pip install /lakehouse/default/Files/connectwise_etl-1.0.0-py3-none-any.whl
```

## Configuration

Set environment variables:
```python
os.environ['CW_AUTH_USERNAME'] = 'your_username'
os.environ['CW_AUTH_PASSWORD'] = 'your_password'
os.environ['CW_CLIENTID'] = 'your_client_id'
```

## Usage

```python
from connectwise_etl import run_etl_pipeline

# Incremental update
run_etl_pipeline(
    spark=spark,
    layers=["bronze", "silver", "gold"],
    mode="incremental",
    lookback_days=7
)

# Full refresh
run_etl_pipeline(
    spark=spark,
    layers=["bronze", "silver", "gold"],
    mode="full",
    lookback_days=30
)
```

## Architecture

**Medallion Layers:**
- **Bronze**: Raw API extraction with etlTimestamp
- **Silver**: Flattened, cleansed data with ETL metadata
- **Gold**: Star schema optimized for Power BI

**Entities Processed:**
- Agreements (with Icelandic classification)
- Time Entries
- Companies
- Members
- Expenses
- Products
- Invoices (posted/unposted)

## Icelandic Agreement Types

- **Tímapottur**: Prepaid hour buckets (non-billable)
- **yÞjónusta**: Billable services
- **Innri verkefni**: Internal projects
- **Rekstrarþjónusta**: Operations support
- **Hugbúnaðarþjónusta**: Software services

## Testing

```bash
./scripts/run_tests.sh
./scripts/run_tests.sh --integration  # Requires credentials
./scripts/run_tests.sh --coverage
```

## Model Generation

Models are auto-generated from OpenAPI schema:
```bash
uv run datamodel-codegen \
    --input PSA_OpenAPI_schema.json \
    --output src/connectwise_etl/models/ \
    --input-file-type openapi \
    --base-class sparkdantic.SparkModel
```

## License

MIT