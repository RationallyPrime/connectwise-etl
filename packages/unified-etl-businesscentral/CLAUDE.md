# CLAUDE.md - unified-etl-businesscentral

This file provides guidance to Claude Code when working with the unified-etl-businesscentral package.

## Package Overview

The unified-etl-businesscentral package is the Microsoft Business Central adapter (currently in progress). It implements BC-specific dimensional modeling and transformations.

## Core Components

### Gold Transforms (`transforms/gold.py`)
BC-specific dimensional modeling including:
- Account hierarchy generation
- Dimension bridges for multi-dimensional analysis
- Item attribute extraction and normalization

## Model Generation

```bash
# Regenerate from CDM manifest
python scripts/regenerate_models_v2.py \
    BC_CDM_manifest.json \
    packages/unified-etl-businesscentral/src/unified_etl_businesscentral/models/ \
    --format cdm
```

## Environment Variables

Required for API access:
- `BC_TENANT_ID`: Azure AD tenant ID
- `BC_CLIENT_ID`: Application (client) ID
- `BC_CLIENT_SECRET`: Client secret for authentication

## Implementation Status

  **In Progress**: This package is currently being developed. Key areas:
- API client implementation
- Bronze layer data extraction
- Silver layer transformations
- Gold layer dimensional modeling

## Business Central Specifics

### Data Model
- Uses Common Data Model (CDM) format
- Complex nested structures requiring flattening
- Multiple company support

### Dimensional Modeling
- Chart of Accounts hierarchy
- Item categorization
- Customer/Vendor dimensions
- Multi-currency support

## Integration Patterns

### Account Hierarchy
- Recursive parent-child relationships
- Level-based hierarchy flattening
- Account type classifications

### Dimension Bridges
- Many-to-many dimension handling
- Bridge tables for complex relationships
- Aggregate-aware design

## Testing Considerations

- CDM manifest parsing
- OAuth2 authentication flow
- Multi-company data segregation
- Currency conversion accuracy