"""
Enhanced Conformed Dimensions for PSA and BC Integration.

This version uses the new schema structure and creates dimensions in gold.conformed.
These dimensions work for both PSA reporting and BC financial integration.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat,
    current_timestamp,
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


class ConformedDimensionBuilder:
    """Builder for conformed dimensions that work across PSA and BC."""

    def __init__(self, spark: SparkSession):
        """Initialize the dimension builder."""
        self.spark = spark
        self.schemas = {
            "silver_psa": "silver.psa",
            "silver_bc": "silver.bc",
            "gold_conformed": "gold.conformed",
        }

    def create_customer_dimension(self, include_bc_data: bool = True) -> DataFrame:
        """
        Create a conformed customer dimension.

        Combines PSA company data with optional BC customer mappings.
        """
        logger.info("Creating conformed customer dimension")

        # Get PSA customer data from invoices (contains company info)
        try:
            psa_invoices = self.spark.table(f"{self.schemas['silver_psa']}.PostedInvoice")

            # Extract unique companies from invoices
            psa_customers = psa_invoices.select(
                col("companyId").alias("psa_company_id"),
                col("companyIdentifier").alias("psa_identifier"),
                col("companyName").alias("customer_name"),
                lit("Company").alias("customer_type"),
            ).distinct()
        except Exception as e:
            logger.error(f"Error reading PSA invoice data: {e}")
            raise

        # If BC data is available, join on identifier
        if include_bc_data:
            try:
                bc_customers = self.spark.table(f"{self.schemas['silver_bc']}.Customer")
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
            except Exception as e:
                logger.warning(f"BC customer data not available: {e}")
                customers_with_bc = psa_customers.withColumn("bc_customer_no", lit(None))
        else:
            customers_with_bc = psa_customers.withColumn("bc_customer_no", lit(None))

        # Generate surrogate key
        window = Window.orderBy("psa_company_id")
        customer_dim = (
            customers_with_bc.withColumn("customer_key", row_number().over(window))
            .withColumn("customer_code", coalesce(col("bc_customer_no"), col("psa_identifier")))
            .withColumn(
                "is_integrated", when(col("bc_customer_no").isNotNull(), True).otherwise(False)
            )
            .withColumn("processed_timestamp", current_timestamp())
        )

        return customer_dim

    def create_employee_dimension(self, include_bc_data: bool = True) -> DataFrame:
        """Create a conformed employee/resource dimension."""
        logger.info("Creating conformed employee dimension")

        # Get PSA member data from time entries
        try:
            psa_time_entries = self.spark.table(f"{self.schemas['silver_psa']}.TimeEntry")

            # Extract unique members
            psa_employees = psa_time_entries.select(
                col("memberId").alias("psa_member_id"),
                col("memberIdentifier").alias("psa_identifier"),
                col("memberName").alias("employee_name"),
                lit(None).alias("department_id"),  # Not in time entries
                lit(None).alias("work_role_id"),  # Not in time entries
            ).distinct()
        except Exception as e:
            logger.error(f"Error reading PSA time entry data: {e}")
            raise

        # If BC data is available, join on resource
        if include_bc_data:
            try:
                bc_resources = self.spark.table(f"{self.schemas['silver_bc']}.Resource")
                employees_with_bc = psa_employees.join(
                    bc_resources.select(
                        col("No").alias("bc_resource_no"),
                        col("Name").alias("bc_resource_name"),
                        col("PTE Manage Resource Id").alias("bc_psa_id"),
                    ),
                    psa_employees.psa_member_id == bc_resources.bc_psa_id,
                    "left",
                )
            except Exception as e:
                logger.warning(f"BC resource data not available: {e}")
                employees_with_bc = psa_employees.withColumn("bc_resource_no", lit(None))
        else:
            employees_with_bc = psa_employees.withColumn("bc_resource_no", lit(None))

        # Generate surrogate key
        window = Window.orderBy("psa_member_id")
        employee_dim = (
            employees_with_bc.withColumn("employee_key", row_number().over(window))
            .withColumn("employee_code", coalesce(col("bc_resource_no"), col("psa_identifier")))
            .withColumn(
                "is_integrated", when(col("bc_resource_no").isNotNull(), True).otherwise(False)
            )
            .withColumn("processed_timestamp", current_timestamp())
        )

        return employee_dim

    def create_agreement_dimension(self, include_bc_data: bool = True) -> DataFrame:
        """Create agreement/job dimension."""
        logger.info("Creating conformed agreement/job dimension")

        # Get PSA agreement data
        try:
            psa_agreements = self.spark.table(f"{self.schemas['silver_psa']}.Agreement")

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
        except Exception as e:
            logger.error(f"Error reading PSA agreement data: {e}")
            raise

        # If BC data is available, join on job
        if include_bc_data:
            try:
                bc_jobs = self.spark.table(f"{self.schemas['silver_bc']}.Job")
                agreements_with_bc = agreements.join(
                    bc_jobs.select(
                        col("No").alias("bc_job_no"),
                        col("Description").alias("bc_job_description"),
                        col("Status").alias("bc_job_status"),
                    ),
                    agreements.agreement_number == bc_jobs.bc_job_no,
                    "left",
                )
            except Exception as e:
                logger.warning(f"BC job data not available: {e}")
                agreements_with_bc = agreements.withColumn("bc_job_no", col("agreement_number"))
        else:
            agreements_with_bc = agreements.withColumn("bc_job_no", col("agreement_number"))

        # Generate surrogate key
        window = Window.orderBy("agreement_id")
        agreement_dim = (
            agreements_with_bc.withColumn("agreement_key", row_number().over(window))
            .withColumn("job_code", coalesce(col("bc_job_no"), col("agreement_number")))
            .withColumn("is_integrated", when(col("bc_job_no").isNotNull(), True).otherwise(False))
            .withColumn("processed_timestamp", current_timestamp())
        )

        return agreement_dim

    def create_work_type_dimension(self, include_bc_data: bool = True) -> DataFrame:
        """Create work type dimension for labor categorization."""
        logger.info("Creating conformed work type dimension")

        # Get PSA work type data from time entries
        try:
            psa_time_entries = self.spark.table(f"{self.schemas['silver_psa']}.TimeEntry")

            # Extract unique work types
            psa_work_types = psa_time_entries.select(
                col("workTypeId").alias("work_type_id"),
                col("workTypeName").alias("work_type_name"),
                lit(True).alias("is_billable"),  # Assume billable by default
                lit(True).alias("affects_utilization"),
            ).distinct()
        except Exception as e:
            logger.error(f"Error reading PSA time entry data: {e}")
            raise

        # If BC data is available, try to join
        if include_bc_data:
            try:
                # BC might have work types in a custom table
                # For now, we'll just add BC fields as null
                work_types_with_bc = psa_work_types.withColumn("bc_work_type_code", lit(None))
            except Exception as e:
                logger.warning(f"BC work type data not available: {e}")
                work_types_with_bc = psa_work_types.withColumn("bc_work_type_code", lit(None))
        else:
            work_types_with_bc = psa_work_types.withColumn("bc_work_type_code", lit(None))

        # Generate surrogate key
        window = Window.orderBy("work_type_id")
        work_type_dim = (
            work_types_with_bc.withColumn("work_type_key", row_number().over(window))
            .withColumn(
                "is_integrated", when(col("bc_work_type_code").isNotNull(), True).otherwise(False)
            )
            .withColumn("processed_timestamp", current_timestamp())
        )

        return work_type_dim

    def create_time_dimension(
        self, start_date: str = "2020-01-01", end_date: str = "2030-12-31"
    ) -> DataFrame:
        """Create a standard time dimension that works for both PSA and BC."""
        logger.info(f"Creating time dimension from {start_date} to {end_date}")

        # Generate date range
        from datetime import datetime, timedelta

        dates = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        while current <= end:
            dates.append((current,))
            current += timedelta(days=1)

        date_df = self.spark.createDataFrame(dates, ["date"])

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
            .withColumn("processed_timestamp", current_timestamp())
        )

        return date_dim

    def run_all_dimensions(self, include_bc_data: bool = True) -> dict[str, int]:
        """Create all conformed dimensions."""
        results = {}

        try:
            # Create customer dimension
            customer_dim = self.create_customer_dimension(include_bc_data)
            customer_table = f"{self.schemas['gold_conformed']}.dim_customer"
            customer_dim.write.mode("overwrite").saveAsTable(customer_table)
            results["dim_customer"] = customer_dim.count()
            logger.info(f"Created customer dimension with {results['dim_customer']} rows")

            # Create employee dimension
            employee_dim = self.create_employee_dimension(include_bc_data)
            employee_table = f"{self.schemas['gold_conformed']}.dim_employee"
            employee_dim.write.mode("overwrite").saveAsTable(employee_table)
            results["dim_employee"] = employee_dim.count()
            logger.info(f"Created employee dimension with {results['dim_employee']} rows")

            # Create agreement/job dimension
            agreement_dim = self.create_agreement_dimension(include_bc_data)
            agreement_table = f"{self.schemas['gold_conformed']}.dim_agreement_job"
            agreement_dim.write.mode("overwrite").saveAsTable(agreement_table)
            results["dim_agreement_job"] = agreement_dim.count()
            logger.info(f"Created agreement dimension with {results['dim_agreement_job']} rows")

            # Create work type dimension
            work_type_dim = self.create_work_type_dimension(include_bc_data)
            work_type_table = f"{self.schemas['gold_conformed']}.dim_work_type"
            work_type_dim.write.mode("overwrite").saveAsTable(work_type_table)
            results["dim_work_type"] = work_type_dim.count()
            logger.info(f"Created work type dimension with {results['dim_work_type']} rows")

            # Create time dimension
            time_dim = self.create_time_dimension()
            time_table = f"{self.schemas['gold_conformed']}.dim_time"
            time_dim.write.mode("overwrite").saveAsTable(time_table)
            results["dim_time"] = time_dim.count()
            logger.info(f"Created time dimension with {results['dim_time']} rows")

        except Exception as e:
            logger.error(f"Error creating conformed dimensions: {e}")
            raise

        return results


# Example usage:
"""
from fabric_api.gold.conformed_dimensions_enhanced import ConformedDimensionBuilder

# Create dimension builder
builder = ConformedDimensionBuilder(spark)

# Run all dimensions
results = builder.run_all_dimensions(include_bc_data=True)

# Or create individual dimensions
customer_dim = builder.create_customer_dimension()
"""
