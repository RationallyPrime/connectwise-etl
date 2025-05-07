# ConnectWise API Filtering Capabilities Analysis

I'll investigate the filtering capabilities for each ConnectWise API endpoint mentioned in your fabric_api package to identify optimization opportunities for bulk data fetching.

Let me examine the OpenAPI schema for these endpoints:

I'll now investigate the ConnectWise API filtering capabilities for the endpoints used in your fabric_api package to help maximize bulk fetching efficiency.

Let me examine the specific endpoints you mentioned:

# ConnectWise API Filtering Capabilities Analysis

Based on my analysis of the ConnectWise API endpoints used in your fabric_api package, I've identified the key filtering capabilities that can help maximize bulk fetching efficiency. Let me break down the filtering options for each endpoint.

## Common Filtering Parameters Across All Endpoints

All ConnectWise API endpoints consistently support these filtering parameters:

1. **conditions** - The primary filtering mechanism for complex queries
2. **childConditions** - For filtering on child objects/relationships
3. **customFieldConditions** - For filtering on custom fields
4. **orderBy** - For sorting results
5. **fields** - For selecting specific fields to return (reduces payload size)
6. **page**, **pageSize**, **pageId** - For pagination control

## Conditions Parameter Syntax

The `conditions` parameter is the most powerful filtering mechanism. It supports:

1. **Basic equality**: `field=value`
2. **Comparison operators**: 
   - `field>value`, `field<value`, `field>=value`, `field<=value`
   - `field!=value` (not equal)
   - `field=null` or `field!=null`

3. **Date comparisons**: 
   - `lastUpdated>2025-01-01T00:00:00Z`
   - `date>now` or `date<now-30d` (relative time expressions)

4. **Text search operators**:
   - `field="exact match"`
   - `field*="contains"`
   - `field^="starts with"`

5. **Boolean operators**:
   - `AND`, `OR` for combining conditions
   - Parentheses for grouping: `(condition1 OR condition2) AND condition3`

6. **Value lists**:
   - `field=[value1,value2,value3]` (in list)
   - `field!=[value1,value2,value3]` (not in list)

## Endpoint-Specific Filtering Opportunities

### 1. Finance/Accounting Endpoints

#### `/finance/accounting/unpostedinvoices`
- Key filterable fields: `id`, `invoiceNumber`, `invoiceDate`, `company/id`, `total`
- Date filtering: `invoiceDate>2025-04-01T00:00:00Z`
- Examples:
  ```
  conditions=invoiceDate>now-30d
  conditions=total>1000 AND hasTime=true
  ```

#### `/finance/invoices`
- Key filterable fields: `id`, `invoiceNumber`, `date`, `dueDate`, `status/id`, `company/id`, `type`
- Time-based efficiency: `conditions=date>now-90d`
- Status-based: `conditions=status/name="Open"`
- Examples:
  ```
  conditions=date>now-90d AND status/name="Open"
  conditions=type="Agreement" AND company/id=123
  ```

#### `/finance/agreements`
- Key filterable fields: `id`, `name`, `type/id`, `company/id`, `startDate`, `endDate`, `cancelledFlag`
- Active agreements: `conditions=cancelledFlag=false AND noEndingDateFlag=true OR endDate>now`
- Examples:
  ```
  conditions=(startDate<now AND (noEndingDateFlag=true OR endDate>now)) AND cancelledFlag=false
  conditions=company/id=123 AND type/id=2
  ```

### 2. Time Tracking Endpoints

#### `/time/entries`
- Key filterable fields: `id`, `timeStart`, `timeEnd`, `member/id`, `chargeToId`, `chargeToType`, `invoice/id`
- Date range filtering: `conditions=timeStart>2025-05-01T00:00:00Z AND timeStart<2025-05-07T23:59:59Z`
- Invoice-specific: `conditions=invoice/id=1234`
- Examples:
  ```
  conditions=timeStart>now-7d AND member/id=42
  conditions=invoice/id=1234
  conditions=chargeToType="ServiceTicket" AND chargeToId=5678
  ```

### 3. Expense Management Endpoints

#### `/expense/entries`
- Key filterable fields: `id`, `date`, `member/id`, `chargeToId`, `chargeToType`, `status`, `invoice/id`
- Date filtering: `conditions=date>2025-04-01T00:00:00Z AND date<2025-05-01T00:00:00Z`
- Examples:
  ```
  conditions=date>now-30d AND billableOption="Billable"
  conditions=invoice/id=1234
  conditions=status="Open" OR status="PendingApproval"
  ```

### 4. Procurement Endpoints

#### `/procurement/products`
- Key filterable fields: `id`, `catalogItem/id`, `invoice/id`, `purchaseDate`, `ticket/id`, `project/id`
- Status filtering: `conditions=cancelledFlag=false`
- Examples:
  ```
  conditions=invoice/id=1234
  conditions=purchaseDate>now-90d AND catalogItem/id=567
  conditions=chargeToType="ServiceTicket" AND chargeToId=5678
  ```

## Optimization Strategies

1. **Date-Based Filtering**: 
   - Use `lastUpdated>X` to fetch only recently modified records
   - For regular data sync jobs, store the last sync timestamp and use it in subsequent requests

2. **Batch Processing**:
   - Fetch related data based on parent IDs: `conditions=invoice/id=[123,456,789]`
   - Use page/pageSize parameters effectively (start with larger page sizes, e.g., 250-1000 items)

3. **Field Selection**:
   - Use the `fields` parameter to return only needed fields
   - Example: `fields=id,invoiceNumber,date,dueDate,total`

4. **Child Conditions**:
   - Filter on nested collections with `childConditions`
   - Example for invoice line items: `childConditions=product/id=123`

5. **Status-Based Optimization**:
   - Filter by status to reduce result sets
   - For invoices: `conditions=status/name="Open"` instead of fetching all invoices

6. **Complex Conditions**:
   - Combine multiple filters to narrow results
   - Example: `conditions=(date>now-30d AND date<now) AND (total>1000 OR type="Agreement")`

## Implementation Example

When fetching invoices with their time entries:

1. First, fetch invoices with date filtering:
   ```
   GET /finance/invoices?conditions=date>now-90d&pageSize=1000
   ```

2. Then, batch fetch related time entries for these invoices:
   ```
   GET /time/entries?conditions=invoice/id=[123,456,789...]&pageSize=1000
   ```

This approach is more efficient than fetching all time entries and then filtering client-side.

## Conclusion

The ConnectWise API offers robust filtering capabilities that can significantly improve efficiency when properly utilized. The most important optimization is to use appropriate date-based filters (`lastUpdated` or specific date fields) combined with pagination to break large datasets into manageable chunks. For data synchronization processes, always maintain the last sync timestamp to fetch only changed records in subsequent runs.