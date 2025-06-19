# Unified ETL ConnectWise

ConnectWise PSA adapter for the unified ETL framework, providing specialized extraction and transformation capabilities with Icelandic business logic.

## Overview

`unified-etl-connectwise` extends the core ETL framework with:

- **ConnectWise API client** with field selection and pagination
- **Pydantic models** generated from OpenAPI specification  
- **Business logic** for Icelandic agreement types (Tímapottur, yÞjónusta)
- **Fact table creators** for time tracking, invoicing, and agreements
- **Dimension configurations** for ConnectWise-specific enums

## Installation

```bash
# Requires unified-etl-core
pip install unified-etl-connectwise

# For development with core
pip install -e "../unified-etl-core[azure]"
pip install -e "."
```

## Architecture

```
ConnectWise API → Bronze (Validated) → Silver (Transformed) → Gold (Facts)
      ↓               ↓                     ↓                    ↓
   Paginated      TimeEntry            Flattened           fact_time_entry
   Field Select   Agreement           Type Casted          fact_invoice_line
   Retry Logic    Invoice             Normalized           fact_agreement_period
```

## Key Components

### 1. ConnectWise Client (`client.py`)

Unified API client with high-level extraction:

```python
from unified_etl_connectwise import ConnectWiseClient

# Initialize with environment variables
client = ConnectWiseClient()

# Extract with automatic pagination and validation
time_entries_df = client.extract(
    endpoint="/time/entries",
    model=TimeEntry,
    page_size=1000,
    conditions=["dateEntered > '2024-01-01'"]
)

# Extract all supported entities
all_data = client.extract_all()
# Returns dict: {"agreement": df, "time_entry": df, ...}
```

### 2. API Field Selection (`api_utils.py`)

Automatic field generation from Pydantic models:

```python
from unified_etl_connectwise.api_utils import get_fields_for_api_call

# Generate ConnectWise field string
fields = get_fields_for_api_call(TimeEntry)
# Returns: "id,notes,dateEntered,member/id,member/name,..."

# Build query conditions
from unified_etl_connectwise.api_utils import build_condition_string

condition = build_condition_string(
    field="status/name",
    value="Open",
    operator="eq"
)
# Returns: "status/name='Open'"
```

### 3. Agreement Business Logic (`agreement_utils.py`)

Icelandic-specific agreement handling:

```python
from unified_etl_connectwise.agreement_utils import (
    normalize_agreement_type,
    add_agreement_flags,
    filter_billable_time_entries
)

# Normalize agreement types
df = normalize_agreement_type(df)
# Maps: "yÞjónusta" → "billable_service"
#       "Tímapottur" → "prepaid_hours"
#       "Innri verkefni" → "internal_project"

# Add business flags
df = add_agreement_flags(df)
# Adds: isBillableWork, isTimapottur, isInternalWork, etc.

# Filter per Business Central logic
billable_df = filter_billable_time_entries(
    time_entry_df=time_entries,
    agreement_df=agreements
)
# Excludes Tímapottur entries from billable work
```

### 4. Fact Table Transformations (`transforms.py`)

Create dimensional fact tables:

```python
from unified_etl_connectwise import create_time_entry_fact

# Time entry fact - captures ALL work
fact_df = create_time_entry_fact(
    spark=spark,
    time_entry_df=time_entries,
    agreement_df=agreements,
    member_df=members,
    config={
        "include_internal": True,
        "add_cost_metrics": True
    }
)

# Invoice line fact - excludes Tímapottur
from unified_etl_connectwise import create_invoice_line_fact

invoice_fact = create_invoice_line_fact(
    spark=spark,
    invoice_df=invoices,
    product_df=products,
    agreement_df=agreements
)

# Agreement period fact - monthly MRR snapshots
from unified_etl_connectwise import create_agreement_period_fact

mrr_fact = create_agreement_period_fact(
    spark=spark,
    agreement_df=agreements,
    period_start="2023-01-01"
)
```

### 5. Dimension Configuration (`dimension_config.py`)

Pre-configured ConnectWise dimensions:

```python
from unified_etl_connectwise.dimension_config import (
    CONNECTWISE_DIMENSIONS,
    refresh_connectwise_dimensions
)

# Create all ConnectWise dimensions
dimensions = refresh_connectwise_dimensions(
    spark=spark,
    source_schema="silver"
)

# Individual dimension configs available:
print(CONNECTWISE_DIMENSIONS["time_entry"])
# ["billableOption", "chargeToType", "status", "workType", ...]
```

## Configuration

### Environment Variables

Required for ConnectWise API authentication:

```bash
# Option 1: Username/Password (Member auth)
CW_AUTH_USERNAME=your_username
CW_AUTH_PASSWORD=your_password
CW_CLIENTID=your_client_id

# Option 2: Company/Public/Private keys (API auth)
CW_COMPANY=your_company
CW_PUBLIC_KEY=your_public_key
CW_PRIVATE_KEY=your_private_key
CW_CLIENTID=your_client_id

# Optional
CW_BASE_URL=https://api-na.myconnectwise.net  # Override default
```

### Model Generation

Models are generated from ConnectWise OpenAPI schema:

```bash
# Regenerate models (from package root)
datamodel-codegen \
    --input PSA_OpenAPI_schema.json \
    --output src/unified_etl_connectwise/models/models.py \
    --input-file-type openapi \
    --output-file-type pydantic_v2 \
    --base-class sparkdantic.SparkModel \
    --snake-case-field false \
    --field-constraints
```

### Silver Layer Configuration (`config.py`)

```python
SILVER_CONFIG = {
    "agreement": {
        "column_mappings": {
            "company.id": "company_id",
            "company.name": "company_name",
            "parentAgreement.id": "parent_agreement_id"
        },
        "type_conversions": {
            "startDate": "date",
            "endDate": "date",
            "dateCancelled": "date"
        },
        "scd_type": 2  # Track history
    },
    "time_entry": {
        "column_mappings": {
            "member.id": "member_id",
            "member.name": "member_name",
            "agreement.id": "agreement_id"
        },
        "type_conversions": {
            "dateEntered": "timestamp",
            "actualHours": "decimal(10,2)"
        }
    }
}
```

## Business Logic Implementation

### Tímapottur (Prepaid Hours) Handling

The system implements special logic for Tímapottur agreements:

1. **Detection**: Agreements with names containing "Tímapottur :" (with space and colon)
2. **Exclusion**: Time entries under Tímapottur agreements are excluded from invoice line facts
3. **Tracking**: Separate tracking for prepaid hour consumption analysis

```python
# Example: Tímapottur detection pattern
AGREEMENT_TYPE_PATTERNS = {
    r"Tímapottur\s*:?": (AgreementType.PREPAID_HOURS, "prepaid"),
    r"yÞjónusta": (AgreementType.BILLABLE_SERVICE, "billable"),
    r"Innri verkefni": (AgreementType.INTERNAL_PROJECT, "internal")
}
```

### Agreement Hierarchy

Handles parent-child agreement relationships:

```python
# Agreement number extraction from customFields
def extract_agreement_number(customFields_json: str) -> str:
    """Extract 'Samningsnúmer' from customFields JSON."""
    
# Hierarchy resolution with parent fallback
def resolve_agreement_hierarchy(
    df: DataFrame,
    parent_df: DataFrame | None = None
) -> DataFrame:
    """Resolve agreement data using parent fallback."""
```

### Billing Status Calculation

Complex billing status based on multiple factors:

```python
def calculate_effective_billing_status(df: DataFrame) -> DataFrame:
    """
    Combines:
    - invoiceId (presence indicates billed)
    - agreement.type (Tímapottur = prepaid)
    - billableOption (Billable/DoNotBill/NoCharge)
    """
```

## Integration with Core

The package integrates seamlessly with unified-etl-core:

```python
# Use core dimensions
from unified_etl_core.dimensions import create_dimension_from_column

# Create ConnectWise-specific dimension
status_dim = create_dimension_from_column(
    spark=spark,
    source_table="silver.time_entries",
    column_name="chargeToType",
    dimension_name="dim_charge_type"
)

# Use core date utilities
from unified_etl_core.date_utils import add_date_key

df = add_date_key(df, "dateEntered")
```

## Complete Pipeline Example

```python
from pyspark.sql import SparkSession
from unified_etl_connectwise import ConnectWiseClient
from unified_etl_connectwise import (
    create_time_entry_fact,
    create_invoice_line_fact,
    create_agreement_period_fact
)
from unified_etl_connectwise.dimension_config import refresh_connectwise_dimensions

# Initialize
spark = SparkSession.builder.appName("ConnectWise ETL").getOrCreate()
client = ConnectWiseClient()

# Extract data
agreements = client.extract("/finance/agreements", model=Agreement)
time_entries = client.extract("/time/entries", model=TimeEntry)
invoices = client.extract("/finance/invoices", model=Invoice)

# Create dimensions
dimensions = refresh_connectwise_dimensions(spark, "silver")

# Create facts
time_fact = create_time_entry_fact(
    spark=spark,
    time_entry_df=time_entries,
    agreement_df=agreements
)

invoice_fact = create_invoice_line_fact(
    spark=spark,
    invoice_df=invoices,
    agreement_df=agreements
)

mrr_fact = create_agreement_period_fact(
    spark=spark,
    agreement_df=agreements,
    period_start="2023-01-01"
)

# Write to gold layer
time_fact.write.mode("overwrite").saveAsTable("gold.fact_time_entry")
invoice_fact.write.mode("overwrite").saveAsTable("gold.fact_invoice_line")
mrr_fact.write.mode("overwrite").saveAsTable("gold.fact_agreement_period")
```

## Performance Optimization

1. **Field Selection**: Only request needed fields from API
   ```python
   fields = get_fields_for_api_call(TimeEntry, exclude=["_info", "customFields"])
   ```

2. **Batch Processing**: Use appropriate page sizes
   ```python
   df = client.extract("/time/entries", page_size=1000)  # Default: 1000
   ```

3. **Condition Filtering**: Filter at API level when possible
   ```python
   df = client.extract(
       "/time/entries",
       conditions=["dateEntered > '2024-01-01'", "status/name = 'Open'"]
   )
   ```

4. **Spark Operations**: Leverage distributed processing for transformations

## Error Handling

The client provides structured error handling:

```python
try:
    df = client.extract("/time/entries")
except ApiError as e:
    logger.error(f"API error: {e.status_code} - {e.message}")
except ValidationError as e:
    logger.error(f"Validation failed: {e}")
    # Access error records
    for error in e.error_records:
        print(f"Record {error.record_id}: {error.errors}")
```

## Testing

```bash
# Run tests
pytest tests/

# Integration tests (requires API credentials)
pytest tests/integration/ -v

# Type checking
pyright src/

# Linting
ruff check src/
```

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify environment variables are set
   - Check client ID is correct
   - Ensure API user has proper permissions

2. **Field Selection Errors**
   - Some fields may not be available in all API versions
   - Use `--debug` flag to see actual API requests

3. **Timeout Issues**
   - Increase timeout: `client = ConnectWiseClient(timeout=120)`
   - Reduce page size for slow endpoints

4. **Memory Issues**
   - Use iterative processing for large datasets
   - Enable Spark adaptive query execution

### Debug Mode

```python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)

# Client with debug info
client = ConnectWiseClient(debug=True)
```

## License

MIT License - see LICENSE file for details.