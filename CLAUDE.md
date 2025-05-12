# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The ConnectWise PSA to Microsoft Fabric Integration project extracts company data from ConnectWise PSA API and loads it directly into Microsoft Fabric OneLake. It handles relationship endpoint permission issues by using filtered queries to standard endpoints instead of direct relationship endpoints.

## Implementation Plan

We're following a structured modernization plan with distinct phases. Each phase will be implemented sequentially, with code review and commits after each.

### Phase 1: SparkModel Schema Compatibility (Pydantic + SparkDantic)

**Goal:** Ensure all Pydantic models inherit from `sparkdantic.SparkModel` and function correctly in Microsoft Fabric Spark Runtime.

**Tasks:**
1. Verify all models in `fabric_api/connectwise_models/` properly inherit from SparkModel
2. Test Spark schema conversion with `model_spark_schema()` method
3. Implement fallback mechanism for nested fields that cause schema issues
4. Update any model generation code if needed

**Files to focus on:**
- `fabric_api/connectwise_models/*.py`
- `fabric_api/generate_models_from_json.py`
- `fabric_api/bronze_loader.py`

### Phase 2: Schema-Driven Field Selection Utility

**Goal:** Use Pydantic models to dynamically determine which fields to request from the API.

**Tasks:**
1. Enhance `get_fields_for_api_call()` in `api_utils.py`
2. Apply this utility across all extract modules
3. Support appropriate nesting for fields if needed
4. Test field selection with all main entity types

**Files to focus on:**
- `fabric_api/api_utils.py`
- `fabric_api/extract/*.py`

### Phase 3: Modular ETL Workflow

**Goal:** Separate ETL stages into modular, testable units.

**Tasks:**
1. Standardize extract functions to return raw JSON data
2. Create consistent validation layer using Pydantic models
3. Isolate Delta loading logic
4. Build clear orchestration structure

**Files to focus on:**
- `fabric_api/bronze_loader.py`
- `fabric_api/pipeline.py`
- Possibly extraction functions in `fabric_api/extract/*.py`

### Phase 4: OneLake-Focused Data Structure

**Goal:** Align output with Microsoft Fabric OneLake conventions.

**Tasks:**
1. Configure direct Delta writes to OneLake paths
2. Implement consistent table naming and partitioning
3. Ensure Fabric Spark compatibility
4. Remove any unnecessary data movement steps

**Files to focus on:**
- `fabric_api/bronze_loader.py`
- `fabric_api/pipeline.py`

## Development Setup

### Environment Setup

The project uses Poetry for dependency management:

```bash
# Install Poetry if needed
pip install poetry

# Install dependencies
poetry install

# Install optional Azure dependencies
poetry install --extras azure
```

### Python Requirements

- Python ≥3.10
- Key dependencies: requests, pandas, pyarrow, pydantic, pyspark, sparkdantic

## Testing

Run tests with pytest:

```bash
# Run all tests
pytest

# Run a specific test file
pytest test_api.py

# Run a specific test case
pytest test_api.py::test_client_construction
```

## Code Quality Tools

### Type Checking

The project uses pyright for type checking:

```bash
# Run type checker
pyright
```

### Linting

The project uses ruff for linting:

```bash
# Run linter
ruff .
```

## Build and Deployment

### Building the Package

Build a wheel distribution for deployment to Fabric:

```bash
# Using pip build
python -m pip install build
python -m build --wheel

# This will create a .whl file in the dist/ directory
```

### Deploying to Microsoft Fabric

1. Create a Lakehouse in your workspace
2. Add secrets to your workspace Key Vault:
   - `CW_COMPANY`
   - `CW_PUBLIC_KEY`
   - `CW_PRIVATE_KEY`
   - `CW_CLIENTID`
3. Upload the wheel file to your lakehouse
4. Create a Notebook and attach your Lakehouse
5. Install the wheel in the first cell:
   ```python
   %pip install /lakehouse/Files/dist/fabric_api-0.1.0-py3-none-any.whl
   ```
6. Run the ETL code in the next cell

## Architecture

### Core Components

- **Client Module** (`client.py`): Provides a resilient ConnectWise API client with retry logic
- **Extract Modules** (`extract/*.py`):
  - Fetch different entity types (invoices, agreements, time, expenses, products)
  - Handle relationships between entities
- **Transform Module** (`transform.py`): Processes data and writes to OneLake
- **Pipeline Module** (`pipeline.py`): Orchestrates the extraction and loading process
- **Bronze Loader** (`bronze_loader.py`): Loads data into Delta tables

### Data Flow

1. **Extract**: Pull company data from ConnectWise PSA API
2. **Transform**: Convert to DataFrame and add extraction timestamp
3. **Load**: Write directly to OneLake via abfss:// URLs

### Key Features

- Secure credential handling via Fabric Key Vault
- Direct writing to OneLake
- Delta loading optimizations
- Resilient API client with retry logic
- Configurable date filters for data extraction

## Running the Pipeline

The pipeline can be run using the `run_daily` function in `pipeline.py`:

```python
from fabric_api.pipeline import run_daily

# Run with default parameters (last 30 days)
run_daily()

# Run with custom parameters
run_daily(
    start_date="2025-04-01",
    end_date="2025-04-30",
    max_pages=100,
    lakehouse_root="/lakehouse/default/Tables/connectwise",
    mode="append"
)
```

## JSON Export Tool

A production-ready export script is included to extract invoice data to JSON files:

```bash
# Basic usage (exports last 30 days of invoices)
python production_export.py

# Specify date range
python production_export.py --start_date 2025-04-01 --end_date 2025-04-30

# Specify output directory
python production_export.py --output_dir invoice_data_april
```