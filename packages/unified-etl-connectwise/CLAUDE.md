# CLAUDE.md - unified-etl-connectwise

This file provides guidance to Claude Code when working with the unified-etl-connectwise package.

## Package Overview

The unified-etl-connectwise package is the ConnectWise PSA adapter implementing business logic specific to ConnectWise data. It handles API limitations with intelligent field selection and provides ConnectWise-specific transformations.

## Key Business Logic

### Agreement Types (Icelandic)

- **yÞjónusta**: Billable service agreements
- **Tímapottur**: Prepaid hours (special handling - excluded from invoices)
- **Innri verkefni**: Internal projects (non-billable)
- **Rekstrarþjónusta/Alrekstur**: Operations/maintenance
- **Hugbúnaðarþjónusta/Office 365**: Software service agreements

### Critical Patterns

1. **Tímapottur Detection**: Must match exact pattern with space/colon
   ```python
   r"Tímapottur\s*:?"  # Matches "Tímapottur :" in data
   ```

2. **Cost Recovery**: Comprehensive fact_time_entry captures ALL work
   - ✅ Successfully captured internal projects ($18M previously missing - NOW FOUND)
   - Includes non-billable work
   - Tracks Tímapottur consumption

## Core Components

### API Client (`client.py`)
- Unified API client with field selection & pagination
- Handles ConnectWise API limitations
- Automatic retry logic with exponential backoff
- Field generation from Pydantic models

### API Utils (`api_utils.py`)
- Field generation from Pydantic models
- Recursive field extraction for nested structures
- ConnectWise-specific field handling

### Agreement Utils (`agreement_utils.py`)
- Icelandic business logic for agreement types
- Tímapottur detection and handling
- Agreement type classification

### Transforms (`transforms.py`)
- Business-specific fact creators:
  - `create_time_entry_fact`: Comprehensive time tracking including internal work
  - `create_invoice_line_fact`: Revenue tracking with proper agreement linkage
  - `create_agreement_period_fact`: Contract period analysis

### Configuration (`config.py`)
- Silver layer entity configurations
- Field mappings for ConnectWise entities
- Business key definitions

### Models (`models/models.py`)
- Auto-generated from OpenAPI schema
- Inherit from SparkModel for Spark compatibility
- CamelCase field names preserved

## Model Regeneration

```bash
# Regenerate from OpenAPI schema
python scripts/regenerate_models_v2.py \
    PSA_OpenAPI_schema.json \
    packages/unified-etl-connectwise/src/unified_etl_connectwise/models/models.py
```

## Environment Variables

Required for API access:
- `CW_COMPANY`: ConnectWise company identifier
- `CW_PUBLIC_KEY`: API public key
- `CW_PRIVATE_KEY`: API private key
- `CW_CLIENTID`: Client ID for API access

## Field Selection Strategy

ConnectWise API has field limitations per request. The client automatically:
1. Generates all available fields from Pydantic models
2. Chunks fields to stay under API limits
3. Makes multiple requests if needed
4. Merges results transparently

## Business Rules

### Time Entry Processing
- Capture ALL time entries (billable and non-billable)
- Link to agreements via AgreementId
- Detect Tímapottur hours for special handling
- Calculate internal vs external hours

### Invoice Processing
- Link invoice lines to agreements
- Exclude Tímapottur from standard invoicing
- Track revenue by agreement type

### Agreement Period Processing
- Track contract periods and renewals
- Monitor agreement utilization
- Calculate period-over-period changes

## Testing Considerations

- Mock ConnectWise API responses
- Test Icelandic text handling (UTF-8)
- Verify Tímapottur regex matching
- Validate agreement type classification