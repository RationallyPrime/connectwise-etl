"""Integration tests using test data to validate the hoursBilled change."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

from unified_etl_connectwise import (
    create_time_entry_fact,
    create_invoice_line_fact,
)
from unified_etl_connectwise.agreement_utils import (
    extract_agreement_number,
    resolve_agreement_hierarchy,
)


@pytest.fixture(scope="session")
def spark():
    """Create a local Spark session for testing."""
    return (
        SparkSession.builder
        .appName("ConnectWise_ETL_Test")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def create_test_time_entries_df(spark):
    """Create a test DataFrame that mimics silver layer time entries."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("chargeToType", StringType(), True),
        StructField("chargeToId", IntegerType(), True),
        StructField("memberId", IntegerType(), True),
        StructField("memberName", StringType(), True),
        StructField("workTypeId", IntegerType(), True),
        StructField("workTypeName", StringType(), True),
        StructField("workRoleId", IntegerType(), True),
        StructField("workRoleName", StringType(), True),
        StructField("timeStart", TimestampType(), True),
        StructField("timeEnd", TimestampType(), True),
        StructField("actualHours", DoubleType(), True),
        StructField("hoursBilled", DoubleType(), True),
        StructField("hoursDeduct", DoubleType(), True),
        StructField("billableOption", StringType(), True),
        StructField("hourlyRate", DoubleType(), True),
        StructField("hourlyCost", DoubleType(), True),
        StructField("agreementId", IntegerType(), True),
        StructField("agreementName", StringType(), True),
        StructField("agreementType", StringType(), True),
        StructField("invoiceId", IntegerType(), True),
        StructField("invoiceIdentifier", StringType(), True),
        StructField("companyId", IntegerType(), True),
        StructField("companyIdentifier", StringType(), True),
        StructField("companyName", StringType(), True),
        StructField("ticketId", IntegerType(), True),
        StructField("ticketSummary", StringType(), True),
        StructField("ticketBoard", StringType(), True),
        StructField("ticketType", StringType(), True),
        StructField("projectId", IntegerType(), True),
        StructField("projectName", StringType(), True),
        StructField("notes", StringType(), True),
        StructField("status", StringType(), True),
        StructField("locationId", IntegerType(), True),
        StructField("departmentId", IntegerType(), True),
        StructField("businessUnitId", IntegerType(), True),
        StructField("timeSheetId", IntegerType(), True),
    ])
    
    base_date = datetime(2024, 1, 15)
    
    data = [
        # Billable time - full hours billed
        (1, "ServiceTicket", 100, 1, "John Doe", 1, "Remote Support", 1, "Engineer",
         base_date, base_date + timedelta(hours=2), 
         2.0, 2.0, 0.0,  # actualHours, hoursBilled, hoursDeduct
         "Billable", 150.0, 75.0,
         1, "Test Service Agreement", "Yþjónusta",
         1001, "INV-2024-001",
         1, "COMP001", "Test Company 1",
         100, "Fix server issue", "Service Board", "Break/Fix",
         None, None, "Fixed customer issue", "Open",
         None, None, None, None),
        
        # Billable time - reduced hours billed
        (2, "ServiceTicket", 101, 1, "John Doe", 1, "Remote Support", 1, "Engineer",
         base_date + timedelta(days=1), base_date + timedelta(days=1, hours=4),
         4.0, 3.5, 0.5,  # actualHours, hoursBilled, hoursDeduct
         "Billable", 150.0, 75.0,
         None, None, None,
         1002, "INV-2024-002",
         1, "COMP001", "Test Company 1",
         101, "Training session", "Service Board", "Training",
         None, None, "Included 30min training time", "Open",
         None, None, None, None),
        
        # Prepaid hours (Tímapottur) - no direct billing
        (3, "ServiceTicket", 102, 2, "Jane Smith", 1, "Remote Support", 1, "Engineer",
         base_date + timedelta(days=2), base_date + timedelta(days=2, hours=3),
         3.0, 0.0, 0.0,  # hoursBilled is 0 for prepaid
         "DoNotBill", 150.0, 80.0,
         2, "Prepaid Hours Block", "Tímapottur",
         None, None,
         1, "COMP001", "Test Company 1",
         102, "Monthly maintenance", "Service Board", "Maintenance",
         None, None, "Covered by prepaid hours", "Open",
         None, None, None, None),
        
        # Internal project - no billing
        (4, "Project", 200, 3, "Bob Johnson", 2, "Development", 2, "Developer",
         base_date + timedelta(days=3), base_date + timedelta(days=3, hours=8),
         8.0, 0.0, 0.0,  # No billing for internal
         "DoNotBill", 0.0, 90.0,
         3, "Internal Project", "Innri verkefni",
         None, None,
         2, "COMP002", "Internal Company",
         None, None, None, None,
         200, "Internal Tool Development",
         "Internal development work", "Open",
         None, None, None, None),
        
        # No charge work
        (5, "ServiceTicket", 103, 1, "John Doe", 1, "Remote Support", 1, "Engineer",
         base_date + timedelta(days=4), base_date + timedelta(days=4, hours=1),
         1.0, 0.0, 0.0,
         "NoCharge", 150.0, 75.0,
         1, "Test Service Agreement", "Yþjónusta",
         None, None,
         1, "COMP001", "Test Company 1",
         103, "Warranty work", "Service Board", "Warranty",
         None, None, "No charge - warranty", "Open",
         None, None, None, None),
    ]
    
    return spark.createDataFrame(data, schema)


def create_test_agreements_df(spark):
    """Create a test DataFrame of agreements."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("typeId", IntegerType(), True),
        StructField("typeName", StringType(), True),
        StructField("agreementStatus", StringType(), True),
        StructField("companyId", IntegerType(), True),
        StructField("companyName", StringType(), True),
        StructField("parentAgreementId", IntegerType(), True),
        StructField("parentAgreementName", StringType(), True),
        StructField("applicationUnits", StringType(), True),
        StructField("applicationLimit", DoubleType(), True),
        StructField("billTime", StringType(), True),
        StructField("startDate", DateType(), True),
        StructField("endDate", DateType(), True),
        StructField("customFields", ArrayType(
            StructType([
                StructField("id", IntegerType(), True),
                StructField("caption", StringType(), True),
                StructField("value", StringType(), True),
            ])
        ), True),
    ])
    
    data = [
        (1, "Test Service Agreement", 1, "Yþjónusta", "Active",
         1, "Test Company 1", None, None,
         "Amount", None, "Billable",
         datetime(2024, 1, 1).date(), datetime(2024, 12, 31).date(),
         [{"id": 1, "caption": "Agreement Number", "value": "AGR-2024-001"}]),
        
        (2, "Prepaid Hours Block", 2, "Tímapottur", "Active",
         1, "Test Company 1", None, None,
         "Hours", 100.0, "DoNotBill",
         datetime(2024, 1, 1).date(), datetime(2024, 12, 31).date(),
         [{"id": 1, "caption": "Agreement Number", "value": "AGR-2024-002"}]),
        
        (3, "Internal Project", 3, "Innri verkefni", "Active",
         2, "Internal Company", None, None,
         "Amount", None, "DoNotBill",
         datetime(2024, 1, 1).date(), None,
         [{"id": 1, "caption": "Agreement Number", "value": "INT-2024-001"}]),
    ]
    
    return spark.createDataFrame(data, schema)


def create_test_invoices_df(spark):
    """Create a test DataFrame of invoices."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("identifier", StringType(), True),
        StructField("date", DateType(), True),
        StructField("dueDate", DateType(), True),
        StructField("statusName", StringType(), True),
        StructField("companyId", IntegerType(), True),
        StructField("companyName", StringType(), True),
        StructField("applyToType", StringType(), True),
        StructField("applyToId", IntegerType(), True),
        StructField("agreementType", StringType(), True),
        StructField("total", DoubleType(), True),
    ])
    
    data = [
        (1001, "INV-2024-001", datetime(2024, 1, 31).date(), datetime(2024, 2, 15).date(),
         "Sent", 1, "Test Company 1", "Agreement", 1, "Yþjónusta", 300.0),
        
        (1002, "INV-2024-002", datetime(2024, 1, 31).date(), datetime(2024, 2, 15).date(),
         "Sent", 1, "Test Company 1", "Services", None, None, 525.0),
    ]
    
    return spark.createDataFrame(data, schema)


def test_time_entry_fact_calculations(spark):
    """Test time entry fact calculations."""
    # Create test data
    time_df = create_test_time_entries_df(spark)
    agreement_df = create_test_agreements_df(spark)
    
    # Create fact table
    fact_df = create_time_entry_fact(spark, time_df, agreement_df)
    
    # Collect results for testing
    results = fact_df.collect()
    
    # Test 1: Billable time with full hours
    billable_full = [r for r in results if r['timeEntryId'] == 1][0]
    assert billable_full['actualHours'] == 2.0
    assert billable_full['hoursBilled'] == 2.0
    assert billable_full['potentialRevenue'] == 300.0  # 2 * 150
    assert billable_full['utilizationType'] == 'Billable'
    
    # Test 2: Billable time with reduced hours
    billable_reduced = [r for r in results if r['timeEntryId'] == 2][0]
    assert billable_reduced['actualHours'] == 4.0
    assert billable_reduced['hoursBilled'] == 3.5
    assert billable_reduced['hoursDeduct'] == 0.5
    assert billable_reduced['potentialRevenue'] == 600.0  # 4 * 150 (still uses actualHours)
    
    # Test 3: Prepaid hours (Tímapottur)
    prepaid = [r for r in results if r['timeEntryId'] == 3][0]
    assert prepaid['actualHours'] == 3.0
    assert prepaid['hoursBilled'] == 0.0
    assert prepaid['billableOption'] == 'DoNotBill'
    assert prepaid['utilizationType'] == 'Non-Billable'
    
    # Test 4: Internal project
    internal = [r for r in results if r['timeEntryId'] == 4][0]
    assert internal['actualHours'] == 8.0
    assert internal['hourlyRate'] == 0.0
    assert internal['utilizationType'] == 'Internal'
    assert 'agreement_type_normalized' in internal
    
    # Test 5: No charge work
    no_charge = [r for r in results if r['timeEntryId'] == 5][0]
    assert no_charge['billableOption'] == 'NoCharge'
    assert no_charge['utilizationType'] == 'No Charge'


def test_invoice_line_fact_hours_billed(spark):
    """Test that invoice lines use hoursBilled for lineAmount calculation."""
    # Create test data
    time_df = create_test_time_entries_df(spark)
    invoice_df = create_test_invoices_df(spark)
    agreement_df = create_test_agreements_df(spark)
    
    # Create invoice line fact
    fact_df = create_invoice_line_fact(
        spark,
        invoice_df,
        time_df,
        None,  # No products for this test
        agreement_df
    )
    
    # Get the lines
    lines = fact_df.collect()
    
    # Test 1: Full hours billed line
    line1 = [l for l in lines if l['timeEntryId'] == 1][0]
    assert line1['quantity'] == 2.0  # actualHours
    assert line1['price'] == 150.0   # hourlyRate
    assert line1['lineAmount'] == 300.0  # Should be hoursBilled (2.0) * rate (150)
    
    # Test 2: Reduced hours billed line (CRITICAL TEST)
    line2 = [l for l in lines if l['timeEntryId'] == 2][0]
    assert line2['quantity'] == 4.0  # actualHours
    assert line2['price'] == 150.0   # hourlyRate
    assert line2['lineAmount'] == 525.0  # Should be hoursBilled (3.5) * rate (150)
    
    # Verify it's NOT using actualHours
    wrong_amount = line2['quantity'] * line2['price']  # 4 * 150 = 600
    assert line2['lineAmount'] != wrong_amount, "ERROR: lineAmount is using actualHours!"
    
    # Test 3: Agreement type on invoice lines
    assert line1.get('applyToType') == 'Agreement'
    assert 'agreement_type_final' in line1


def test_agreement_utils(spark):
    """Test agreement utility functions."""
    # Test extract_agreement_number
    agreement_df = create_test_agreements_df(spark)
    with_numbers = extract_agreement_number(agreement_df)
    
    numbers = with_numbers.select("id", "agreementNumber").collect()
    agr1 = [n for n in numbers if n['id'] == 1][0]
    assert agr1['agreementNumber'] == "AGR-2024-001"
    
    # Test agreement type normalization
    time_df = create_test_time_entries_df(spark)
    resolved = resolve_agreement_hierarchy(time_df, agreement_df, "agreementId", "test")
    
    # Check normalized types
    norm_data = resolved.select("id", "agreement_type_normalized").collect()
    
    # Time entry 1 should have billable_service
    te1 = [d for d in norm_data if d['id'] == 1][0]
    assert te1['agreement_type_normalized'] == 'billable_service'
    
    # Time entry 3 should have prepaid_hours
    te3 = [d for d in norm_data if d['id'] == 3][0]
    assert te3['agreement_type_normalized'] == 'prepaid_hours'
    
    # Time entry 4 should have internal_project
    te4 = [d for d in norm_data if d['id'] == 4][0]
    assert te4['agreement_type_normalized'] == 'internal_project'


def test_data_validation(spark):
    """Test data validation scenarios."""
    time_df = create_test_time_entries_df(spark)
    
    # Check no negative hours
    negative = time_df.filter(
        (F.col("actualHours") < 0) | (F.col("hoursBilled") < 0)
    ).count()
    assert negative == 0
    
    # Check hoursBilled <= actualHours
    over_billed = time_df.filter(
        (F.col("hoursBilled") > F.col("actualHours") + 0.01) &
        (F.col("hoursBilled").isNotNull())
    ).count()
    assert over_billed == 0
    
    # Check billable entries have rates
    billable_no_rate = time_df.filter(
        (F.col("billableOption") == "Billable") &
        ((F.col("hourlyRate").isNull()) | (F.col("hourlyRate") == 0))
    ).count()
    assert billable_no_rate == 0