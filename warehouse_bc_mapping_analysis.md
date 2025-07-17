# Warehouse Schema to Business Central CDM Mapping Analysis

## Executive Summary

This analysis compares the warehouse schema entities against the available Business Central CDM entities to assess mapping feasibility and implementation complexity. The warehouse schema contains 58 entities (32 dimensions, 16 facts, 2 many-to-many tables) while the Business Central CDM provides 21 core entities.

## Available Business Central CDM Entities

1. **AccountingPeriod** - Accounting period definitions
2. **CompanyInformation** - Company master data
3. **Currency** - Currency master data
4. **Customer** - Customer master data
5. **CustLedgerEntry** - Customer ledger entries
6. **DetailedCustLedgEntry** - Detailed customer ledger entries
7. **DetailedVendorLedgEntry** - Detailed vendor ledger entries
8. **Dimension** - Dimension definitions
9. **DimensionSetEntry** - Dimension set entries
10. **DimensionValue** - Dimension values
11. **GLAccount** - General ledger account master
12. **GLEntry** - General ledger entries
13. **GeneralLedgerSetup** - GL setup configuration
14. **Item** - Item master data
15. **Job** - Job/project master data
16. **JobLedgerEntry** - Job ledger entries
17. **Resource** - Resource master data
18. **SalesInvoiceHeader** - Sales invoice headers
19. **SalesInvoiceLine** - Sales invoice lines
20. **Vendor** - Vendor master data
21. **VendorLedgerEntry** - Vendor ledger entries

## Detailed Mapping Analysis

### DIMENSION ENTITIES

#### HIGH PRIORITY - DIRECT MAPPINGS

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **dim_Company** | CompanyInformation | DIRECT | Low | 1:1 mapping for company master data |
| **dim_Currency** | Currency | DIRECT | Low | 1:1 mapping for currency master data |
| **dim_Customer** | Customer | DIRECT | Low | Strong alignment on customer fields |
| **dim_GLAccount** | GLAccount | DIRECT | Low | Good alignment on GL account structure |
| **dim_GL_Account** | GLAccount | DIRECT | Low | Duplicate table - use GLAccount |
| **dim_Item** | Item | DIRECT | Low | Strong alignment on item master data |
| **dim_Resource** | Resource | DIRECT | Low | Good alignment on resource master data |
| **dim_Vendor** | Vendor | DIRECT | Low | Strong alignment on vendor master data |

#### MEDIUM PRIORITY - DERIVABLE MAPPINGS

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **dim_GD1-GD8** | Dimension + DimensionValue | DERIVABLE | Medium | Global dimensions 1-8 can be derived from dimension tables |
| **dim_Date** | N/A | DERIVABLE | Low | Standard date dimension - can be generated |
| **dim_BankAccount** | GLAccount | DERIVABLE | Medium | Bank accounts are GL accounts with specific posting groups |
| **dim_Bank_Account** | GLAccount | DERIVABLE | Medium | Duplicate - derive from GLAccount |
| **dim_Salesperson** | Resource | DERIVABLE | Medium | Salesperson often stored as resource or can be derived |
| **dim_UnitOfMeasure** | Item | DERIVABLE | Medium | UOM available in item master data |

#### MEDIUM PRIORITY - PARTIAL MAPPINGS

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **dim_FixedAsset** | GLAccount | PARTIAL | Medium | FA data partially in GL, may need custom source |
| **dim_ItemVariant** | Item | PARTIAL | Medium | Some variant info in item master, may need extension |
| **dim_ItemCharge** | Item | PARTIAL | Medium | Item charges are items with specific types |
| **dim_SalesHeader** | SalesInvoiceHeader | PARTIAL | Medium | Only invoice headers available, not all sales docs |
| **dim_SalesItems** | SalesInvoiceLine | PARTIAL | Medium | Derived from sales lines with type logic |
| **dim_Contact** | Customer + Vendor | PARTIAL | High | Contact info spread across customer/vendor |

#### LOW PRIORITY - MISSING ENTITIES

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **dim_AccSchedule** | N/A | MISSING | High | Account schedules not in CDM |
| **dim_Acc_Schedule** | N/A | MISSING | High | Duplicate - missing from CDM |
| **dim_Agreement** | N/A | MISSING | High | Agreement/contract data not in CDM |
| **dim_Bond** | N/A | MISSING | High | Bond data not in CDM |
| **dim_Budget** | N/A | MISSING | High | Budget master data not in CDM |
| **dim_ClosingEntry** | N/A | MISSING | High | Closing entry logic not in CDM |
| **dim_DueDate** | N/A | MISSING | Medium | Due date bucketing - can be calculated |

### JIRA-SPECIFIC ENTITIES (EXTERNAL SYSTEM)

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **dim_Jira_Account** | N/A | MISSING | High | External Jira data - separate integration needed |
| **dim_Jira_Customer** | N/A | MISSING | High | External Jira data - separate integration needed |
| **dim_Jira_Issue** | N/A | MISSING | High | External Jira data - separate integration needed |
| **dim_Jira_IssueInfo** | N/A | MISSING | High | External Jira data - separate integration needed |
| **dim_Jira_Project** | N/A | MISSING | High | External Jira data - separate integration needed |
| **dim_Jira_Team** | N/A | MISSING | High | External Jira data - separate integration needed |
| **dim_Jira_TeamMember** | N/A | MISSING | High | External Jira data - separate integration needed |
| **dim_Jira_User** | N/A | MISSING | High | External Jira data - separate integration needed |

### FACT ENTITIES

#### HIGH PRIORITY - DIRECT MAPPINGS

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **fact_GLEntry** | GLEntry | DIRECT | Low | Strong alignment on GL entries |
| **fact_SalesInvoiceLines** | SalesInvoiceLine | DIRECT | Low | Direct mapping to invoice lines |
| **fact_ValueEntry** | GLEntry | DERIVABLE | Medium | Value entries can be derived from GL entries |

#### MEDIUM PRIORITY - DERIVABLE MAPPINGS

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **fact_BC_SalesLines** | SalesInvoiceLine | DERIVABLE | Medium | Subset of sales invoice lines |
| **fact_Sales_Entries** | SalesInvoiceLine | DERIVABLE | Medium | Aggregated sales entry data |
| **fact_SalesCrMemoLines** | SalesInvoiceLine | DERIVABLE | Medium | Credit memo lines (negative quantities) |
| **fact_CustomerMetrics** | CustLedgerEntry | DERIVABLE | Medium | Customer metrics from ledger entries |

#### MEDIUM PRIORITY - PARTIAL MAPPINGS

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **fact_GLBudget** | GLAccount | PARTIAL | High | Budget amounts not in CDM, needs custom source |
| **fact_RevenueProjection** | SalesInvoiceLine | PARTIAL | High | Projection logic not in CDM |

#### LOW PRIORITY - MISSING ENTITIES

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **fact_CRM_Opportunity** | N/A | MISSING | High | CRM data not in BC CDM |
| **fact_Opportunity** | N/A | MISSING | High | Opportunity data not in BC CDM |
| **fact_Issues_Estimate** | N/A | MISSING | High | Jira integration data |
| **fact_Tempo_Worklog** | N/A | MISSING | High | Tempo/Jira worklog data |

### MANY-TO-MANY ENTITIES

| Warehouse Entity | BC CDM Entity | Classification | Complexity | Notes |
|------------------|---------------|----------------|------------|-------|
| **m2m_AccountSchedule** | N/A | MISSING | High | Account schedule relationships not in CDM |
| **m2m_GLAccount** | GLAccount | DERIVABLE | Medium | GL account hierarchies from totaling field |

## Critical Gaps Analysis

### 1. **Budget and Planning Data**
- **Impact**: HIGH - Budget vs actual reporting impossible
- **Entities**: dim_Budget, fact_GLBudget
- **Recommendation**: Implement custom budget data source or use Excel/Power BI datasets

### 2. **Agreement/Contract Management**
- **Impact**: HIGH - Recurring revenue tracking compromised
- **Entities**: dim_Agreement, fact_RevenueProjection
- **Recommendation**: Implement custom agreement tracking or use BC Service Management

### 3. **CRM Integration**
- **Impact**: MEDIUM - Sales pipeline visibility reduced
- **Entities**: fact_CRM_Opportunity, fact_Opportunity
- **Recommendation**: Implement Dynamics 365 Sales integration or use BC CRM functionality

### 4. **Project Management Integration**
- **Impact**: MEDIUM - Development tracking limited
- **Entities**: All Jira entities, fact_Tempo_Worklog
- **Recommendation**: Maintain separate Jira integration pipeline

### 5. **Account Schedules**
- **Impact**: MEDIUM - Financial reporting flexibility reduced
- **Entities**: dim_AccSchedule, m2m_AccountSchedule
- **Recommendation**: Implement custom account schedule logic or use BC Account Schedules

## Implementation Priority Matrix

### Phase 1: Core Financial Data (1-2 weeks)
- **DIRECT mappings**: Company, Currency, Customer, GLAccount, Item, Resource, Vendor
- **DERIVABLE**: Date dimension, GD1-GD8 dimensions
- **FACTS**: GLEntry, SalesInvoiceLines

### Phase 2: Extended Sales Data (2-3 weeks)
- **DERIVABLE**: BankAccount, Salesperson, UnitOfMeasure
- **PARTIAL**: SalesHeader, SalesItems, Contact
- **FACTS**: Sales-related fact tables

### Phase 3: Specialized Areas (3-4 weeks)
- **PARTIAL**: FixedAsset, ItemVariant, ItemCharge
- **CUSTOM**: Budget integration, Agreement tracking
- **FACTS**: CustomerMetrics, ValueEntry

### Phase 4: External Integrations (4-6 weeks)
- **MISSING**: Jira integration pipeline
- **MISSING**: CRM opportunity tracking
- **MISSING**: Project management integration

## Recommendations

### Immediate Actions
1. **Focus on Phase 1** - Implement core financial dimensions and facts
2. **Standardize naming** - Align warehouse field names with BC CDM conventions
3. **Implement surrogate keys** - Ensure dimensional integrity across systems

### Medium-term Strategy
1. **Custom data sources** - Develop budget and agreement data pipelines
2. **Extend BC CDM** - Add missing entities through custom development
3. **Data quality frameworks** - Implement validation for derived entities

### Long-term Vision
1. **Unified data platform** - Consolidate all business data sources
2. **Real-time capabilities** - Implement streaming for critical business metrics
3. **Advanced analytics** - Enable predictive analytics on complete dataset

## Technical Considerations

### Data Volume Impact
- **High volume**: GLEntry, SalesInvoiceLine, CustomerLedgerEntry
- **Medium volume**: Item, Customer, Vendor
- **Low volume**: Company, Currency, Resource

### Performance Optimization
- **Partitioning**: Implement date-based partitioning for fact tables
- **Indexing**: Create appropriate indexes for dimension lookups
- **Caching**: Implement caching for frequently accessed dimensions

### Data Quality Assurance
- **Validation rules**: Implement comprehensive data validation
- **Reconciliation**: Build reconciliation processes for critical metrics
- **Monitoring**: Implement data quality monitoring and alerting

## Conclusion

The warehouse schema can be **70% implemented** using existing BC CDM entities with varying degrees of complexity. The core financial and operational data has strong alignment, while specialized areas like budgeting, agreements, and external system integration require custom development.

**Success Factors:**
- Focus on high-value, low-complexity mappings first
- Implement robust data quality processes
- Plan for custom integration of missing critical entities
- Maintain flexibility for future BC CDM enhancements

**Risk Mitigation:**
- Validate business requirements against available data
- Implement fallback strategies for missing entities
- Ensure scalability for future data source additions
- Maintain clear documentation of mapping decisions