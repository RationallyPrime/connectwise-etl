# How the ETL Moves Data

A walkthrough of the ConnectWise ETL pipeline for people who know what APIs and data warehouses are but aren't excited about reading Spark documentation.

---

## The Problem

ConnectWise Manage is a PSA (Professional Services Automation) platform. It tracks agreements, time entries, expenses, invoices, products, team members, and companies. All the operational data a managed services provider runs on.

The data lives behind a REST API. If you want to build dashboards, calculate margins, or answer "which agreement types are most profitable this quarter?", you need to get that data out of the API and into an analytical environment where you can actually query it.

The naive approach: call the API, dump JSON into a table, write SQL against it. This works until it doesn't — nested objects break your queries, schema changes corrupt your tables, you can't tell fresh data from stale data, and everything is coupled to ConnectWise's specific API quirks.

**What if the extraction framework was generic enough to work with *any* API, the data flowed through validated, typed layers, and the dimensional model built itself from configuration?**

That's what this ETL builds.

---

## The Medallion

The pipeline uses a **medallion architecture** — three layers of increasing refinement, each with a clear responsibility:

```
API Response (raw JSON)
        │
        ▼
┌─────────────────────────────┐
│  BRONZE — Validated Raw     │
│                             │
│  Pydantic validates every   │
│  record against SparkDantic │
│  models. Invalid records    │
│  are logged, not loaded.    │
│  Original structure kept.   │
│  etlTimestamp + etlEntity   │
│  metadata stamped on.       │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│  SILVER — Cleaned & Flat    │
│                             │
│  Nested structs flattened   │
│  (company.name → companyName│
│  up to 3 levels deep).     │
│  ETL metadata added:        │
│  _etl_processed_at,         │
│  _etl_source, _etl_batch_id │
│  No re-validation — Bronze  │
│  already handled that.      │
└──────────────┬──────────────┘
               │
               ▼
┌─────────────────────────────┐
│  GOLD — Dimensional Model   │
│                             │
│  Star schema: fact tables   │
│  with surrogate keys,       │
│  dimension tables from YAML │
│  definitions. Business      │
│  metrics calculated:        │
│  margin, revenue, cost.     │
└─────────────────────────────┘
```

Why three layers instead of one? Because each layer serves a different audience and fails independently:

- **Bronze** fails if the API changes its response shape. Fix: update the Pydantic model.
- **Silver** fails if a new nested field type appears. Fix: adjust the flattener.
- **Gold** fails if business logic changes (a new agreement type, a new billing status). Fix: update the YAML schema.

No single failure corrupts the entire pipeline. Bronze is always a clean, validated snapshot of what the API returned.

---

## The Models: Pydantic Meets Spark

ConnectWise's API has an OpenAPI specification. From that spec, we auto-generate **SparkDantic models** — Pydantic v2 classes that also know how to produce Apache Spark schemas:

```
OpenAPI spec (PSA_OpenAPI_schema_patched.json)
        │
        │  datamodel-code-generator (DMCG)
        │  with --base-class sparkdantic.SparkModel
        ▼
┌─────────────────────────────────────────┐
│  class Agreement(SparkModel):           │
│      id: int | None                     │
│      name: str | None                   │
│      type: AgreementTypeReference | None│
│      company: CompanyReference | None   │
│      ...                                │
│                                         │
│  Agreement.model_validate(raw_dict)     │  ← Pydantic validation
│  Agreement.model_spark_schema()         │  ← Spark StructType
└─────────────────────────────────────────┘
```

One model definition serves three purposes:
1. **Validation** — every API record is parsed through Pydantic. Datetimes get parsed, types get coerced, invalid records get caught.
2. **Schema generation** — SparkDantic produces the Spark StructType for DataFrame creation. No manual schema maintenance.
3. **Field selection** — the model's fields drive the API `fields` parameter, so we only request what we can validate.

---

## The Generic Fetch Layer

The original client was hardwired to ConnectWise — its auth, its pagination, its URL structure. That meant adding any other API source required writing a new client from scratch.

The `etl_core.fetch` module replaces this with **declarative endpoint configuration**:

```python
EndpointConfig(
    base_url="https://eu.myconnectwise.net/v4_6_release/apis/3.0",
    path="/finance/agreements",
    entity_name="agreement",
    auth=BasicAuth(username_env="CW_AUTH_USERNAME", password_env="CW_AUTH_PASSWORD"),
    pagination=PageNumberPagination(page_size=1000),
    headers={"clientId": client_id, "Accept": "application/vnd.connectwise.com+json"},
)
```

Auth and pagination are **discriminated unions** — add a new strategy by adding a new variant:

| Auth Type | How It Works |
|-----------|-------------|
| `BasicAuth` | Base64-encodes username:password from env vars into Authorization header |
| `BearerAuth` | Reads a token from an env var, sends as `Authorization: Bearer <token>` |
| `ApiKeyAuth` | Reads a key from an env var, sends in a named header (e.g. `X-API-Key`) |

| Pagination Strategy | How It Works |
|---------------------|-------------|
| `PageNumberPagination` | `page=1&pageSize=100`, increment page until empty (ConnectWise, DRF) |
| `OffsetLimitPagination` | `skip=0&limit=100`, increment offset (many REST APIs) |
| `CursorPagination` | Follow a cursor token from the response body (Stripe, Slack) |

The `HttpxFetcher` reads an `EndpointConfig`, resolves credentials from environment, and paginates according to strategy. To add a new API source, you write a config — not a client.

---

## The Plugin Architecture

The ETL framework is **protocol-based** — every integration point is a Python Protocol (structural interface), not a base class:

```
ETLRunner (orchestrator)
    │
    ├── IntegrationPluginProtocol ──► ConnectWisePlugin
    │       │
    │       ├── ModelRegistryProtocol ──► ConnectWiseRegistry
    │       │     "What entities exist? What model validates them?"
    │       │
    │       ├── DataFetcherProtocol ──► ConnectWiseFetcher
    │       │     "How do I get raw records from the source?"
    │       │
    │       └── Processors
    │             ├── BronzeProcessorProtocol
    │             ├── SilverProcessorProtocol
    │             └── GoldProcessorProtocol
    │
    └── (Future: JiraPlugin, ServiceTitanPlugin, ...)
```

Adding a new data source means implementing three things:
1. A **registry** — which entities exist, what Pydantic model validates each one, what endpoint fetches it
2. A **fetcher** — how to get raw records (usually just building `EndpointConfig` objects)
3. **Processors** — any source-specific transform logic for each medallion layer

The core framework handles orchestration, validation, incremental merge logic, and Spark DataFrame creation. The plugin handles "what's unique about this particular API."

---

## Gold Layer: YAML-Driven Dimensional Modeling

The Gold layer builds a star schema — fact tables surrounded by dimension tables. Rather than hardcoding dimension definitions, they're declared in YAML:

```yaml
# connectwise-dimensions.yaml
dimensions:
  - table_name: dimBillableStatus
    source_table: silver.silver_cw_timeentry
    natural_key: billableOption
    columns:
      - name: billableOption
        type: string
      - name: description
        type: string
```

The dimension builder reads this YAML, queries the Silver table, groups by natural key, assigns surrogate keys via window functions, and writes the dimension table. Adding a new dimension is a YAML change, not a code change.

Fact tables are more specialized — they compute derived metrics:

| Fact | Key Metrics |
|------|------------|
| **Time Entry** | potentialRevenue = actualHours x hourlyRate, actualCost = actualHours x hourlyCost, margin, marginPercentage |
| **Expense Entry** | amount, billable flag, classification |
| **Product Item** | price, cost, margin = price - cost, marginPercentage |
| **Invoice Line** | lineAmount = quantity x price, line type (service/product/agreement) |

Each fact table joins to its dimensions via surrogate keys, producing a proper star schema that any BI tool can query directly.

---

## Icelandic Agreement Types

This is a domain-specific detail, but it illustrates why the Silver-to-Gold transform isn't purely mechanical.

ConnectWise agreements in this deployment use Icelandic names for agreement types:

| Icelandic | Meaning | Business Rule |
|-----------|---------|---------------|
| **yThjonusta** | Billable service | Normal billing |
| **Timapottur** | Prepaid hours pool | Excluded from invoices (hours deducted from pool) |
| **Innri verkefni** | Internal projects | Not customer-facing |
| **Rekstrarþjonusta** | Operations/maintenance | Recurring |
| **Hugbunadarþjonusta** | Software service | License-based |

The Gold layer classifies time entries by matching against these patterns (`r"Timapottur\s*:?"`) to determine billing treatment. A time entry against a Timapottur agreement gets different margin calculations than one against a billable service agreement. This logic lives in the ConnectWise Gold processor — it's domain-specific, not framework-level.

---

## Incremental Processing

Full reloads are fine for small datasets. ConnectWise has years of data.

Each layer supports **incremental processing** using watermarks:

1. **Bronze**: Fetches only records where `lastUpdated > [last_run_date]`. Merges into the Bronze table using MERGE (update if exists, insert if new).
2. **Silver**: Reads only changed Bronze records since last Silver run. Applies SCD Type 1 merge (overwrite with latest).
3. **Gold**: Re-evaluates dimensions and facts from current Silver state.

The `IncrementalHandler` tracks watermarks and builds the merge SQL. The fetcher adds the date condition to the API call automatically when mode is `"incremental"`.

---

## The Full Picture

```
┌─────────────────────────────────────────────────────────────────────┐
│  ConnectWise Manage API (REST)                                      │
│  /finance/agreements · /time/entries · /expense/entries              │
│  /procurement/products · /finance/invoices · /system/members        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                    EndpointConfig + HttpxFetcher
                    (auth, pagination, field selection)
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER                                                       │
│                                                                     │
│  SparkDantic model validation (Pydantic v2 + Spark schema)          │
│  Invalid records logged · etlTimestamp/etlEntity metadata           │
│  Incremental: MERGE on id                                           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER                                                       │
│                                                                     │
│  Recursive struct flattening (company.name → companyName)           │
│  ETL metadata (_etl_processed_at, _etl_source, _etl_batch_id)      │
│  SCD Type 1 merge                                                   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    ▼                     ▼
┌─────────────────────────┐  ┌────────────────────────────────────────┐
│  DIMENSIONS (YAML)      │  │  FACTS (code)                          │
│                         │  │                                        │
│  dimBillableStatus      │  │  gold_cw_fact_timeentry                │
│  dimTimeEntryStatus     │  │  gold_cw_fact_expenseentry             │
│  dimAgreementType       │  │  gold_cw_fact_productitem              │
│  dimMember              │  │  gold_cw_fact_invoiceline              │
│  dimCompany             │  │                                        │
│  ...                    │  │  Surrogate keys, date keys, derived    │
│                         │  │  metrics (margin, revenue, cost)       │
│  Surrogate keys via     │  │  Dimension FK joins                    │
│  window functions       │  │  Icelandic agreement classification    │
└────────────┬────────────┘  └───────────────────┬────────────────────┘
             │                                   │
             └──────────────┬────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Microsoft Fabric Lakehouse                                         │
│                                                                     │
│  Star schema queryable by Power BI, Synapse SQL, Notebooks          │
│  "What's our margin by agreement type this quarter?"                │
│  "Which engineers have the highest billable utilization?"           │
│  "How do prepaid hours pools trend against actual consumption?"     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Why This Matters

**For the MSP:** Operational data locked in ConnectWise becomes a queryable star schema in Fabric. Dashboards that previously required manual data exports now refresh automatically. Margin calculations that lived in spreadsheets now live in a governed, versioned pipeline.

**For extensibility:** The protocol-based plugin architecture and generic fetch layer mean adding a new data source (Jira, ServiceTitan, HubSpot) is a configuration exercise, not a rewrite. Define the endpoints, point at an OpenAPI spec for model generation, write YAML for dimensions — done.

**For correctness:** Every record is validated through Pydantic before it touches a table. Schema changes in the API surface as validation errors in Bronze, not corrupt data in Gold. The medallion layers fail independently, so a bad transform in Gold never corrupts the validated data in Bronze.

The traditional approach to PSA analytics is: export CSVs, paste into Excel, build pivot tables, email them around. This pipeline replaces that with a typed, validated, incrementally-refreshed dimensional model that updates itself. The data engineers sleep better. The finance team gets fresh numbers. Everyone wins.
