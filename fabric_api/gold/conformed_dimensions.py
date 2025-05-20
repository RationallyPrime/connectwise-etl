"""
Conformed Dimensions for PSA and BC Integration.

These dimensions work for both PSA reporting and BC financial integration.
"""

import logging

from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    date_format,
    dayofmonth,
    dayofweek,
    lit,
    month,
    quarter,
    row_number,
    weekofyear,
    when,
    year,
)
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)


def create_customer_dimension(
    psa_companies: DataFrame,
    psa_invoices: Optional[DataFrame] = None,
    bc_customers: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Create a conformed customer dimension.

    Combines PSA company data with optional BC customer mappings.
    """

    # Get unique customers from PSA
    psa_customers = psa_companies.select(
        col("id").alias("psa_company_id"),
        col("identifier").alias("psa_identifier"),
        col("name").alias("customer_name"),
        col("type").alias("customer_type"),
    ).distinct()

    # If BC customer data is available, join on identifier
    if bc_customers:
        customers_with_bc = psa_customers.join(
            bc_customers.select(
                col("No").alias("bc_customer_no"),
                col("Name").alias("bc_customer_name"),
                col("CustomerPostingGroup").alias("bc_posting_group"),
                col("SocialSecurityNo").alias("bc_identifier"),
            ),
            psa_customers.psa_identifier == bc_customers.bc_identifier,
            "left",
        )
    else:
        customers_with_bc = psa_customers.withColumn("bc_customer_no", lit(None))

    # Generate surrogate key
    window = Window.orderBy("psa_company_id")
    customer_dim = (
        customers_with_bc.withColumn("customer_key", row_number().over(window))
        .withColumn("customer_code", coalesce(col("bc_customer_no"), col("psa_identifier")))
        .withColumn("is_integrated", when(col("bc_customer_no").isNotNull(), True).otherwise(False))
    )

    return customer_dim


def create_employee_dimension(
    psa_members: DataFrame,
    bc_resources: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Create a conformed employee/resource dimension.
    """

    employees = psa_members.select(
        col("id").alias("psa_member_id"),
        col("identifier").alias("psa_identifier"),
        col("name").alias("employee_name"),
        col("departmentId").alias("department_id"),
        col("workRoleId").alias("work_role_id"),
    ).distinct()

    # If BC resource data is available, join
    if bc_resources:
        employees_with_bc = employees.join(
            bc_resources.select(
                col("No").alias("bc_resource_no"),
                col("Name").alias("bc_resource_name"),
                col("PTE Manage Resource Id").alias("bc_psa_id"),
            ),
            employees.psa_member_id == bc_resources.bc_psa_id,
            "left",
        )
    else:
        employees_with_bc = employees.withColumn("bc_resource_no", lit(None))

    # Generate surrogate key
    window = Window.orderBy("psa_member_id")
    employee_dim = employees_with_bc.withColumn(
        "employee_key", row_number().over(window)
    ).withColumn("employee_code", coalesce(col("bc_resource_no"), col("psa_identifier")))

    return employee_dim


def create_agreement_dimension(
    psa_agreements: DataFrame,
    bc_jobs: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Create agreement/job dimension.

    PSA Agreements map to BC Jobs.
    """

    agreements = psa_agreements.select(
        col("id").alias("agreement_id"),
        col("agreementNumber").alias("agreement_number"),
        col("name").alias("agreement_name"),
        col("typeName").alias("agreement_type"),
        col("parentAgreementId").alias("parent_agreement_id"),
        col("startDate"),
        col("endDate"),
        col("agreementStatus").alias("status"),
    )

    # If BC job data is available, join on agreement number
    if bc_jobs:
        agreements_with_bc = agreements.join(
            bc_jobs.select(
                col("No").alias("bc_job_no"),
                col("Description").alias("bc_job_description"),
                col("Status").alias("bc_job_status"),
            ),
            agreements.agreement_number == bc_jobs.bc_job_no,
            "left",
        )
    else:
        agreements_with_bc = agreements.withColumn("bc_job_no", col("agreement_number"))

    # Generate surrogate key
    window = Window.orderBy("agreement_id")
    agreement_dim = agreements_with_bc.withColumn(
        "agreement_key", row_number().over(window)
    ).withColumn("job_code", coalesce(col("bc_job_no"), col("agreement_number")))

    return agreement_dim


def create_work_type_dimension(
    psa_work_types: DataFrame,
    bc_work_types: Optional[DataFrame] = None,
) -> DataFrame:
    """
    Create work type dimension for labor categorization.
    """

    work_types = psa_work_types.select(
        col("id").alias("work_type_id"),
        col("name").alias("work_type_name"),
        col("billFlag").alias("is_billable"),
        col("utilizationFlag").alias("affects_utilization"),
    ).distinct()

    # If BC work types available, join on name pattern
    if bc_work_types:
        work_types_with_bc = work_types.join(
            bc_work_types.select(
                col("Code").alias("bc_work_type_code"),
                col("Description").alias("bc_work_type_desc"),
                col("PTE Manage Work Type Name").alias("bc_psa_name"),
            ),
            work_types.work_type_name == bc_work_types.bc_psa_name,
            "left",
        )
    else:
        work_types_with_bc = work_types.withColumn("bc_work_type_code", lit(None))

    # Generate surrogate key
    window = Window.orderBy("work_type_id")
    work_type_dim = work_types_with_bc.withColumn("work_type_key", row_number().over(window))

    return work_type_dim


def create_time_dimension(
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
    spark: Optional[SparkSession] = None,
) -> DataFrame:
    """
    Create a standard time dimension that works for both PSA and BC.
    """

    # Ensure SparkSession is provided
    if spark is None:
        raise ValueError("SparkSession must be provided")
    # This could reuse the BC date dimension creation logic
    # but ensure it has fields needed for both systems
    from datetime import datetime, timedelta

    dates = []
    current = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        dates.append((current,))
        current += timedelta(days=1)

    date_df = spark.createDataFrame(dates, ["date"])

    # Add attributes useful for both systems
    date_dim = (
        date_df.withColumn("date_key", date_format(col("date"), "yyyyMMdd").cast("integer"))
        .withColumn("year", year(col("date")))
        .withColumn("month", month(col("date")))
        .withColumn("quarter", quarter(col("date")))
        .withColumn("week", weekofyear(col("date")))
        .withColumn("day_of_month", dayofmonth(col("date")))
        .withColumn("day_of_week", dayofweek(col("date")))
        .withColumn("month_name", date_format(col("date"), "MMMM"))
        .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), True).otherwise(False))
        # Fiscal year (assuming Jan 1 start for simplicity)
        .withColumn("fiscal_year", col("year"))
        .withColumn("fiscal_quarter", concat(lit("FY"), col("year"), lit("-Q"), col("quarter")))
    )

    return date_dim


def run_conformed_dimensions(
    silver_path: str,
    gold_path: str,
    spark: SparkSession,
    include_bc_data: bool = False,
) -> dict[str, int]:
    """
    Create all conformed dimensions for PSA/BC integration.
    """
    results = {}

    # Read PSA silver tables
    psa_agreements = spark.table(f"{silver_path}.Agreement")
    psa_time_entries = spark.table(f"{silver_path}.TimeEntry")

    # Extract unique dimension data from fact tables
    psa_companies = (
        spark.table(f"{silver_path}.PostedInvoice").select("companyId", "companyName").distinct()
    )
    psa_members = (
        spark.table(f"{silver_path}.TimeEntry")
        .select("memberId", "memberName", "memberIdentifier")
        .distinct()
    )
    psa_work_types = (
        spark.table(f"{silver_path}.TimeEntry").select("workTypeId", "workTypeName").distinct()
    )

    # Optionally read BC dimension tables if available
    bc_customers = None
    bc_resources = None
    bc_jobs = None
    bc_work_types = None

    if include_bc_data:
        # These would come from BC silver layer
        try:
            bc_customers = spark.table("bc_silver.Customer")
            bc_resources = spark.table("bc_silver.Resource")
            bc_jobs = spark.table("bc_silver.Job")
            bc_work_types = spark.table("bc_silver.WorkType")
        except:
            logger.info("BC dimension tables not available, creating PSA-only dimensions")

    # Create dimensions
    customer_dim = create_customer_dimension(psa_companies, None, bc_customers)
    customer_dim.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_customer")
    results["dim_customer"] = customer_dim.count()

    employee_dim = create_employee_dimension(psa_members, bc_resources)
    employee_dim.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_employee")
    results["dim_employee"] = employee_dim.count()

    agreement_dim = create_agreement_dimension(psa_agreements, bc_jobs)
    agreement_dim.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_agreement_job")
    results["dim_agreement_job"] = agreement_dim.count()

    work_type_dim = create_work_type_dimension(psa_work_types, bc_work_types)
    work_type_dim.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_work_type")
    results["dim_work_type"] = work_type_dim.count()

    # Create time dimension
    time_dim = create_time_dimension(spark=spark)
    time_dim.write.mode("overwrite").saveAsTable(f"{gold_path}.dim_time")
    results["dim_time"] = time_dim.count()

    logger.info(f"Conformed dimensions created: {results}")
    return results
