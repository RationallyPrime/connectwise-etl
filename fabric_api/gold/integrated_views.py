"""
Integrated Views for PSA and BC Cross-System Analytics.

These views provide a unified interface for querying across both PSA and BC data,
enabling comprehensive financial reporting and analysis.
"""

import logging
from typing import Optional

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class IntegratedViewBuilder:
    """Builder for integrated views that combine PSA and BC data."""
    
    def __init__(self, spark: SparkSession):
        """Initialize the view builder."""
        self.spark = spark
        self.schemas = {
            "gold_psa": "gold.psa",
            "gold_bc": "gold.bc",
            "gold_conformed": "gold.conformed",
            "gold_integrated": "gold.integrated"
        }
    
    def create_unified_financial_view(self) -> None:
        """
        Create a unified financial view combining PSA and BC transactions.
        
        This view provides a comprehensive financial picture across both systems.
        """
        logger.info("Creating unified financial view")
        
        view_sql = f"""
        CREATE OR REPLACE VIEW {self.schemas['gold_integrated']}.vw_unified_financial AS
        WITH psa_transactions AS (
            SELECT 
                -- Dimension keys
                psa.customer_key,
                psa.employee_key,
                psa.agreement_key,
                psa.work_type_key,
                -- Date
                psa.posting_date,
                t.date_key,
                t.fiscal_year,
                t.fiscal_quarter,
                t.month,
                -- PSA specific
                psa.line_type as transaction_type,
                psa.invoice_id,
                psa.invoiceNumber,
                -- Amounts
                psa.revenue as amount,
                psa.cost,
                psa.quantity,
                psa.revenue - psa.cost as margin,
                -- Identifiers
                'PSA' as source_system,
                psa.line_item_key as source_key,
                psa.description
            FROM {self.schemas['gold_psa']}.fact_psa_line_items psa
            JOIN {self.schemas['gold_conformed']}.dim_time t ON psa.posting_date = t.date
        ),
        bc_transactions AS (
            SELECT
                -- Map BC to conformed dimensions
                c.customer_key,
                NULL as employee_key, -- BC GL entries don't have employees
                aj.agreement_key,
                NULL as work_type_key,
                -- Date
                gl.PostingDate as posting_date,
                t.date_key,
                t.fiscal_year,
                t.fiscal_quarter,
                t.month,
                -- BC specific
                'GL_ENTRY' as transaction_type,
                NULL as invoice_id,
                gl.DocumentNo as invoiceNumber,
                -- Amounts
                gl.Amount as amount,
                0 as cost,
                0 as quantity,
                gl.Amount as margin,
                -- Identifiers
                'BC' as source_system,
                CAST(gl.EntryNo as STRING) as source_key,
                gl.Description
            FROM {self.schemas['gold_bc']}.fact_GLEntry gl
            JOIN {self.schemas['gold_conformed']}.dim_time t ON gl.PostingDate = t.date
            LEFT JOIN {self.schemas['gold_conformed']}.dim_customer c 
                ON gl.CustomerNo = c.bc_customer_no AND gl."$Company" = c.company_code
            LEFT JOIN {self.schemas['gold_conformed']}.dim_agreement_job aj
                ON gl.JobNo = aj.bc_job_no
        )
        SELECT 
            -- Dimensions
            COALESCE(p.customer_key, b.customer_key) as customer_key,
            c.customer_name,
            c.customer_code,
            COALESCE(p.employee_key, b.employee_key) as employee_key,
            e.employee_name,
            e.employee_code,
            COALESCE(p.agreement_key, b.agreement_key) as agreement_key,
            aj.agreement_name,
            aj.job_code,
            p.work_type_key,
            wt.work_type_name,
            -- Time
            COALESCE(p.date_key, b.date_key) as date_key,
            COALESCE(p.posting_date, b.posting_date) as posting_date,
            COALESCE(p.fiscal_year, b.fiscal_year) as fiscal_year,
            COALESCE(p.fiscal_quarter, b.fiscal_quarter) as fiscal_quarter,
            COALESCE(p.month, b.month) as month,
            -- Transaction details  
            COALESCE(p.transaction_type, b.transaction_type) as transaction_type,
            COALESCE(p.invoice_id, b.invoice_id) as invoice_id,
            COALESCE(p.invoiceNumber, b.invoiceNumber) as invoice_number,
            -- Amounts
            COALESCE(p.amount, 0) + COALESCE(b.amount, 0) as total_amount,
            COALESCE(p.cost, 0) + COALESCE(b.cost, 0) as total_cost,
            COALESCE(p.margin, 0) + COALESCE(b.margin, 0) as total_margin,
            p.quantity,
            -- System indicators
            COALESCE(p.source_system, b.source_system) as source_system,
            COALESCE(p.source_key, b.source_key) as source_key,
            COALESCE(p.description, b.description) as description,
            -- Flags
            CASE WHEN p.source_key IS NOT NULL THEN 1 ELSE 0 END as has_psa_data,
            CASE WHEN b.source_key IS NOT NULL THEN 1 ELSE 0 END as has_bc_data
        FROM psa_transactions p
        FULL OUTER JOIN bc_transactions b 
            ON p.posting_date = b.posting_date 
            AND p.agreement_key = b.agreement_key
            AND p.customer_key = b.customer_key
        LEFT JOIN {self.schemas['gold_conformed']}.dim_customer c ON COALESCE(p.customer_key, b.customer_key) = c.customer_key
        LEFT JOIN {self.schemas['gold_conformed']}.dim_employee e ON COALESCE(p.employee_key, b.employee_key) = e.employee_key
        LEFT JOIN {self.schemas['gold_conformed']}.dim_agreement_job aj ON COALESCE(p.agreement_key, b.agreement_key) = aj.agreement_key
        LEFT JOIN {self.schemas['gold_conformed']}.dim_work_type wt ON p.work_type_key = wt.work_type_key
        """
        
        self.spark.sql(view_sql)
        logger.info("Created unified financial view")
    
    def create_bc_job_ledger_view(self) -> None:
        """
        Create a BC-compatible job ledger view from PSA data.
        
        This allows PSA time and expense data to be viewed in BC job ledger format.
        """
        logger.info("Creating BC job ledger view")
        
        view_sql = f"""
        CREATE OR REPLACE VIEW {self.schemas['gold_integrated']}.vw_bc_job_ledger AS
        SELECT
            -- BC Job Ledger Entry fields
            psa.line_item_key as EntryNo,
            aj.bc_job_no as JobNo,
            psa.posting_date as PostingDate,
            psa.invoiceNumber as DocumentNo,
            CASE 
                WHEN psa.line_type = 'TIME' THEN 'Resource'
                WHEN psa.line_type = 'EXPENSE' THEN 'G/L Account'
                WHEN psa.line_type = 'PRODUCT' THEN 'Item'
                ELSE psa.line_type
            END as Type,
            COALESCE(e.bc_resource_no, e.psa_member_id) as No,
            psa.description as Description,
            psa.quantity as Quantity,
            psa.cost as DirectUnitCostLCY,
            psa.revenue as TotalPriceLCY,
            psa.revenue - psa.cost as TotalCostLCY,
            wt.bc_work_type_code as WorkTypeCode,
            -- Additional fields
            aj.agreement_number as ExternalDocumentNo,
            c.psa_company_id as Company,
            'PSA' as SourceSystem,
            -- BC posting groups (if mapped)
            e.bc_resource_no as ResourceNo,
            NULL as ItemNo, -- Would need item mapping
            NULL as GLAccountNo -- Would need GL mapping
        FROM {self.schemas['gold_psa']}.fact_psa_line_items psa
        JOIN {self.schemas['gold_conformed']}.dim_agreement_job aj ON psa.agreement_key = aj.agreement_key
        JOIN {self.schemas['gold_conformed']}.dim_customer c ON psa.customer_key = c.customer_key
        LEFT JOIN {self.schemas['gold_conformed']}.dim_employee e ON psa.employee_key = e.employee_key
        LEFT JOIN {self.schemas['gold_conformed']}.dim_work_type wt ON psa.work_type_key = wt.work_type_key
        WHERE aj.bc_job_no IS NOT NULL -- Only include records that map to BC jobs
        """
        
        self.spark.sql(view_sql)
        logger.info("Created BC job ledger view")
    
    def create_consolidated_revenue_view(self) -> None:
        """
        Create a consolidated revenue view across both systems.
        
        This view provides high-level revenue analytics by customer, agreement, and period.
        """
        logger.info("Creating consolidated revenue view")
        
        view_sql = f"""
        CREATE OR REPLACE VIEW {self.schemas['gold_integrated']}.vw_consolidated_revenue AS
        SELECT
            -- Dimensions
            c.customer_key,
            c.customer_name,
            c.customer_code,
            aj.agreement_key,
            aj.agreement_name,
            aj.job_code,
            t.fiscal_year,
            t.fiscal_quarter,
            t.month,
            t.month_name,
            -- PSA Metrics
            SUM(CASE WHEN source = 'PSA' THEN revenue ELSE 0 END) as psa_revenue,
            SUM(CASE WHEN source = 'PSA' THEN cost ELSE 0 END) as psa_cost,
            SUM(CASE WHEN source = 'PSA' THEN revenue - cost ELSE 0 END) as psa_margin,
            SUM(CASE WHEN source = 'PSA' AND line_type = 'TIME' THEN revenue ELSE 0 END) as psa_time_revenue,
            SUM(CASE WHEN source = 'PSA' AND line_type = 'EXPENSE' THEN revenue ELSE 0 END) as psa_expense_revenue,
            SUM(CASE WHEN source = 'PSA' AND line_type = 'PRODUCT' THEN revenue ELSE 0 END) as psa_product_revenue,
            -- BC Metrics (when available)
            SUM(CASE WHEN source = 'BC' THEN amount ELSE 0 END) as bc_amount,
            -- Combined Metrics
            SUM(revenue) as total_revenue,
            SUM(cost) as total_cost,
            SUM(revenue - cost) as total_margin,
            -- Utilization metrics
            SUM(CASE WHEN source = 'PSA' AND line_type = 'TIME' THEN quantity ELSE 0 END) as total_hours,
            COUNT(DISTINCT CASE WHEN source = 'PSA' THEN invoice_id END) as psa_invoice_count,
            -- Integration status
            MAX(CASE WHEN aj.is_integrated THEN 1 ELSE 0 END) as has_bc_integration
        FROM (
            -- PSA Data
            SELECT 
                'PSA' as source,
                customer_key,
                agreement_key,
                posting_date,
                line_type,
                invoice_id,
                revenue,
                cost,
                quantity,
                0 as amount
            FROM {self.schemas['gold_psa']}.fact_psa_line_items
            
            UNION ALL
            
            -- BC Data (when integrated)
            SELECT
                'BC' as source,
                c.customer_key,
                aj.agreement_key,
                gl.PostingDate as posting_date,
                'GL_ENTRY' as line_type,
                NULL as invoice_id,
                0 as revenue,
                0 as cost,
                0 as quantity,
                gl.Amount as amount
            FROM {self.schemas['gold_bc']}.fact_GLEntry gl
            LEFT JOIN {self.schemas['gold_conformed']}.dim_customer c 
                ON gl.CustomerNo = c.bc_customer_no
            LEFT JOIN {self.schemas['gold_conformed']}.dim_agreement_job aj
                ON gl.JobNo = aj.bc_job_no
            WHERE c.customer_key IS NOT NULL OR aj.agreement_key IS NOT NULL
        ) combined
        JOIN {self.schemas['gold_conformed']}.dim_time t ON combined.posting_date = t.date
        JOIN {self.schemas['gold_conformed']}.dim_customer c ON combined.customer_key = c.customer_key
        JOIN {self.schemas['gold_conformed']}.dim_agreement_job aj ON combined.agreement_key = aj.agreement_key
        GROUP BY
            c.customer_key,
            c.customer_name,
            c.customer_code,
            aj.agreement_key,
            aj.agreement_name,
            aj.job_code,
            t.fiscal_year,
            t.fiscal_quarter,
            t.month,
            t.month_name
        """
        
        self.spark.sql(view_sql)
        logger.info("Created consolidated revenue view")
    
    def create_employee_utilization_view(self) -> None:
        """
        Create an employee utilization view combining PSA and BC resource data.
        
        This view helps analyze resource utilization across both systems.
        """
        logger.info("Creating employee utilization view")
        
        view_sql = f"""
        CREATE OR REPLACE VIEW {self.schemas['gold_integrated']}.vw_employee_utilization AS
        SELECT
            e.employee_key,
            e.employee_name,
            e.employee_code,
            e.bc_resource_no,
            t.fiscal_year,
            t.fiscal_quarter,
            t.month,
            t.month_name,
            -- PSA Time Metrics
            SUM(CASE WHEN psa.line_type = 'TIME' THEN psa.quantity ELSE 0 END) as psa_hours,
            SUM(CASE WHEN psa.line_type = 'TIME' THEN psa.revenue ELSE 0 END) as psa_revenue,
            COUNT(DISTINCT CASE WHEN psa.line_type = 'TIME' THEN psa.invoice_id END) as psa_invoices,
            COUNT(DISTINCT CASE WHEN psa.line_type = 'TIME' THEN psa.agreement_key END) as psa_projects,
            -- Average rates
            AVG(CASE WHEN psa.line_type = 'TIME' AND psa.quantity > 0 
                THEN psa.revenue / psa.quantity ELSE NULL END) as avg_hourly_rate,
            -- BC Resource Metrics (if available)
            SUM(CASE WHEN jr.Type = 'Resource' THEN jr.Quantity ELSE 0 END) as bc_hours,
            SUM(CASE WHEN jr.Type = 'Resource' THEN jr.TotalPriceLCY ELSE 0 END) as bc_revenue,
            -- Combined metrics
            SUM(CASE WHEN psa.line_type = 'TIME' THEN psa.quantity ELSE 0 END) + 
            SUM(CASE WHEN jr.Type = 'Resource' THEN jr.Quantity ELSE 0 END) as total_hours,
            -- Utilization (assumes 160 hours per month standard)
            (SUM(CASE WHEN psa.line_type = 'TIME' THEN psa.quantity ELSE 0 END) + 
             SUM(CASE WHEN jr.Type = 'Resource' THEN jr.Quantity ELSE 0 END)) / 160.0 as utilization_rate
        FROM {self.schemas['gold_conformed']}.dim_employee e
        CROSS JOIN {self.schemas['gold_conformed']}.dim_time t
        LEFT JOIN {self.schemas['gold_psa']}.fact_psa_line_items psa 
            ON e.employee_key = psa.employee_key AND psa.posting_date = t.date
        LEFT JOIN {self.schemas['gold_bc']}.fact_JobLedgerEntry jr
            ON e.bc_resource_no = jr.No AND jr.PostingDate = t.date AND jr.Type = 'Resource'
        WHERE t.date >= '2020-01-01' -- Limit historical data
        GROUP BY
            e.employee_key,
            e.employee_name,
            e.employee_code,
            e.bc_resource_no,
            t.fiscal_year,
            t.fiscal_quarter,
            t.month,
            t.month_name
        HAVING total_hours > 0 -- Only show months with activity
        """
        
        self.spark.sql(view_sql)
        logger.info("Created employee utilization view")
    
    def create_all_views(self) -> dict[str, str]:
        """Create all integrated views."""
        results = {}
        
        try:
            # Ensure integrated schema exists
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.schemas['gold_integrated']}")
            
            # Create views
            self.create_unified_financial_view()
            results["vw_unified_financial"] = "created"
            
            self.create_bc_job_ledger_view()
            results["vw_bc_job_ledger"] = "created"
            
            self.create_consolidated_revenue_view()
            results["vw_consolidated_revenue"] = "created"
            
            self.create_employee_utilization_view()
            results["vw_employee_utilization"] = "created"
            
            logger.info(f"Successfully created all integrated views: {results}")
            
        except Exception as e:
            logger.error(f"Error creating integrated views: {e}")
            results["error"] = str(e)
            raise
        
        return results


# Example usage:
"""
from fabric_api.gold.integrated_views import IntegratedViewBuilder

# Create view builder
builder = IntegratedViewBuilder(spark)

# Create all views
results = builder.create_all_views()

# Query the views
df = spark.sql("SELECT * FROM gold.integrated.vw_unified_financial WHERE fiscal_year = 2024")
df.show()
"""