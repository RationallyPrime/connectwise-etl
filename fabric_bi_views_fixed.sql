-- ============================================
-- Microsoft Fabric BI Views 
-- Run these in the SQL Analytics Endpoint (not notebook)
-- Fixed table names with gold_cw_ prefix
-- ============================================

-- Monthly Revenue Dashboard View
CREATE VIEW mv_monthly_revenue_dashboard AS
SELECT 
    year as revenue_year,
    month as revenue_month,
    quarter as revenue_quarter,
    SUM(monthly_revenue) as total_mrr,
    SUM(prorated_revenue) as total_prorated_revenue,
    COUNT(DISTINCT id) as active_agreements,
    AVG(monthly_revenue) as avg_agreement_value,
    SUM(CASE WHEN is_new_agreement = 1 THEN 1 ELSE 0 END) as new_agreements,
    SUM(CASE WHEN is_churned_agreement = 1 THEN 1 ELSE 0 END) as churned_agreements,
    SUM(revenue_change) as net_revenue_change
FROM gold_cw_agreement_period_facts
WHERE is_active_period = 1
GROUP BY year, month, quarter;
GO

-- Customer Revenue Analysis View  
CREATE VIEW mv_customer_revenue_analysis AS
SELECT 
    companyId,
    companyName,
    SUM(estimated_lifetime_value) as estimated_lifetime_value,
    SUM(actual_total_revenue) as actual_lifetime_value,
    SUM(cumulative_lifetime_revenue) as cumulative_revenue,
    COUNT(DISTINCT id) as agreement_count,
    AVG(actual_avg_monthly_revenue) as avg_monthly_revenue,
    MAX(endDate) as last_agreement_end,
    SUM(lifetime_days) as total_relationship_days,
    CASE 
        WHEN SUM(actual_total_revenue) > 100000 THEN 'Enterprise'
        WHEN SUM(actual_total_revenue) > 25000 THEN 'Mid-Market' 
        WHEN SUM(actual_total_revenue) > 5000 THEN 'SMB'
        ELSE 'Starter'
    END as customer_segment
FROM gold_cw_agreement_summary_facts
WHERE cancelledFlag = 0
GROUP BY companyId, companyName;
GO

-- Resource Utilization View
CREATE VIEW mv_resource_utilization AS
SELECT 
    memberIdentifier,
    memberName,
    YEAR(timeStart) as work_year,
    MONTH(timeStart) as work_month,
    agreementType,
    SUM(actualHours) as total_hours_worked,
    SUM(hoursBilled) as total_billable_hours,
    SUM(invoiceHours) as total_invoiced_hours,
    SUM(extendedInvoiceAmount) as total_revenue_generated,
    ROUND(SUM(hoursBilled) * 100.0 / NULLIF(SUM(actualHours), 0), 2) as utilization_percentage,
    ROUND(SUM(extendedInvoiceAmount) / NULLIF(SUM(hoursBilled), 0), 2) as avg_hourly_rate,
    COUNT(DISTINCT agreementId) as agreements_worked,
    COUNT(DISTINCT companyId) as companies_served
FROM gold_cw_time_entry_facts
WHERE timeStart >= DATEADD(year, -2, GETDATE())
GROUP BY memberIdentifier, memberName, YEAR(timeStart), MONTH(timeStart), agreementType;
GO

-- Agreement Performance View
CREATE VIEW mv_agreement_performance AS
SELECT 
    ap.id as agreement_id,
    ap.name as agreement_name,
    ap.typeName as agreement_type,
    ap.companyId,
    COALESCE(ash.companyName, 'Unknown') as company_name,
    ap.agreementStatus,
    SUM(ap.monthly_revenue) as total_monthly_revenue,
    SUM(ap.prorated_revenue) as total_prorated_revenue,
    COUNT(*) as active_periods,
    MIN(ap.period_start) as first_revenue_period,
    MAX(ap.period_end) as last_revenue_period,
    AVG(ap.monthly_revenue) as avg_monthly_revenue,
    COALESCE(ash.estimated_lifetime_value, 0) as estimated_lifetime_value,
    COALESCE(ash.actual_total_revenue, 0) as actual_total_revenue,
    CASE 
        WHEN ap.agreementStatus = 'Active' AND ap.cancelledFlag = 0 THEN 'Active'
        WHEN ap.cancelledFlag = 1 THEN 'Cancelled'
        ELSE 'Inactive'
    END as current_status
FROM gold_cw_agreement_period_facts ap
LEFT JOIN gold_cw_agreement_summary_facts ash ON ap.id = ash.id
WHERE ap.is_active_period = 1
GROUP BY 
    ap.id, ap.name, ap.typeName, ap.companyId, ash.companyName, 
    ap.agreementStatus, ash.estimated_lifetime_value, ash.actual_total_revenue, 
    ap.cancelledFlag;
GO

-- Revenue Mix Analysis View
CREATE VIEW mv_revenue_mix_analysis AS
SELECT 
    YEAR(date) as revenue_year,
    CASE 
        WHEN MONTH(date) IN (1,2,3) THEN 1
        WHEN MONTH(date) IN (4,5,6) THEN 2
        WHEN MONTH(date) IN (7,8,9) THEN 3
        ELSE 4
    END as revenue_quarter,
    MONTH(date) as revenue_month,
    productClass,
    CASE 
        WHEN isService = 1 THEN 'Service'
        WHEN isProduct = 1 THEN 'Product' 
        ELSE 'Other'
    END as revenue_type,
    agreement_type,
    SUM(lineAmount) as revenue_amount,
    SUM(lineCost) as total_cost,
    SUM(lineMargin) as total_margin,
    AVG(marginPercentage) as avg_margin_percentage,
    COUNT(*) as transaction_count,
    COUNT(DISTINCT agreementId) as unique_agreements,
    COUNT(DISTINCT companyId) as unique_customers,
    SUM(quantity) as total_quantity
FROM gold_cw_invoice_line_facts
WHERE date >= DATEADD(year, -3, GETDATE())
GROUP BY 
    YEAR(date), MONTH(date), 
    productClass, isService, isProduct, agreement_type;
GO

-- Invoice Performance Dashboard View
CREATE VIEW mv_invoice_performance_dashboard AS
SELECT 
    YEAR(date) as invoice_year,
    MONTH(date) as invoice_month,
    statusName,
    type as invoice_type,
    COUNT(*) as invoice_count,
    SUM(total) as total_invoice_amount,
    SUM(subtotal) as total_subtotal,
    SUM(salesTax) as total_sales_tax,
    AVG(total) as avg_invoice_amount,
    SUM(totalMargin) as total_margin,
    AVG(avgMarginPercentage) as avg_margin_percentage,
    SUM(CASE WHEN isOverdue = 1 THEN 1 ELSE 0 END) as overdue_count,
    SUM(CASE WHEN isOverdue = 1 THEN total ELSE 0 END) as overdue_amount,
    AVG(paymentTermsDays) as avg_payment_terms,
    COUNT(DISTINCT companyId) as unique_customers
FROM gold_cw_invoice_header_facts
WHERE date >= DATEADD(year, -2, GETDATE())
GROUP BY YEAR(date), MONTH(date), statusName, type;
GO

-- Time Tracking Efficiency View
CREATE VIEW mv_time_tracking_efficiency AS
SELECT 
    memberIdentifier,
    memberName,
    YEAR(timeStart) as work_year,
    MONTH(timeStart) as work_month,
    workTypeName,
    billableOption,
    SUM(actualHours) as hours_logged,
    SUM(hoursBilled) as hours_billed,
    SUM(invoiceHours) as hours_invoiced,
    SUM(extendedInvoiceAmount) as revenue_generated,
    ROUND(AVG(hourlyRate), 2) as avg_hourly_rate,
    COUNT(DISTINCT ticketId) as tickets_worked,
    COUNT(DISTINCT projectId) as projects_worked,
    ROUND(SUM(hoursBilled) / NULLIF(SUM(actualHours), 0) * 100, 2) as billing_efficiency,
    ROUND(SUM(invoiceHours) / NULLIF(SUM(hoursBilled), 0) * 100, 2) as invoice_efficiency
FROM gold_cw_time_entry_facts
WHERE timeStart >= DATEADD(year, -1, GETDATE())
  AND actualHours > 0
GROUP BY 
    memberIdentifier, memberName, YEAR(timeStart), MONTH(timeStart), 
    workTypeName, billableOption;
GO

-- Customer Health Score View
CREATE VIEW mv_customer_health_score AS
WITH customer_metrics AS (
    SELECT 
        companyId,
        companyName,
        COUNT(DISTINCT id) as total_agreements,
        SUM(actual_total_revenue) as lifetime_revenue,
        AVG(actual_avg_monthly_revenue) as avg_monthly_revenue,
        SUM(CASE WHEN cancelledFlag = 0 THEN 1 ELSE 0 END) as active_agreements,
        MAX(endDate) as last_agreement_date,
        AVG(lifetime_days) as avg_agreement_duration
    FROM gold_cw_agreement_summary_facts
    GROUP BY companyId, companyName
)
SELECT 
    cm.*,
    CASE 
        WHEN cm.active_agreements = 0 THEN 'At Risk'
        WHEN cm.last_agreement_date < DATEADD(month, -6, GETDATE()) THEN 'At Risk'
        WHEN cm.avg_monthly_revenue > 10000 AND cm.active_agreements >= 2 THEN 'Healthy'
        WHEN cm.avg_monthly_revenue > 5000 AND cm.active_agreements >= 1 THEN 'Stable'
        ELSE 'Needs Attention'
    END as health_score,
    DATEDIFF(day, cm.last_agreement_date, GETDATE()) as days_since_last_agreement,
    ROUND(cm.lifetime_revenue / NULLIF(cm.total_agreements, 0), 2) as revenue_per_agreement
FROM customer_metrics cm;
GO