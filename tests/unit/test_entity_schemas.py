#!/usr/bin/env python
"""
Test script that generates sample data for each entity based on the schema models
and saves the responses as pretty-printed JSON files for review.
"""

import json
import logging
import os
import random
from datetime import datetime, timedelta

# Use the main entity models from fabric_api.connectwise_models

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def generate_random_id():
    """Generate a random ID between 1000 and 9999"""
    return random.randint(1000, 9999)


def generate_random_date(days_back=365):
    """Generate a random date within the last year"""
    days = random.randint(0, days_back)
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_random_decimal(min_val=10.0, max_val=1000.0):
    """Generate a random decimal value"""
    return round(random.uniform(min_val, max_val), 2)


def create_reference():
    """Create a generic reference object"""
    return {
        "id": generate_random_id(),
        "name": f"Sample-{generate_random_id()}",
        "_info": {"additional_properties": {}},
    }


def generate_company_reference():
    """Generate a company reference"""
    ref = create_reference()
    ref["identifier"] = f"COMP-{ref['id']}"
    return ref


def generate_invoice_reference():
    """Generate an invoice reference"""
    ref = create_reference()
    ref["identifier"] = f"INV-{ref['id']}"
    ref["billing_type"] = random.choice(["Agreement", "Time", "Expense"])
    ref["apply_to_type"] = "Agreement"
    ref["invoice_date"] = generate_random_date()
    ref["charge_firm_flag"] = random.choice([True, False])
    return ref


def generate_info_object():
    """Generate an InfoObject"""
    return {
        "additional_properties": {
            "lastUpdated": generate_random_date(),
            "updatedBy": f"user{generate_random_id()}",
        }
    }


def generate_agreement():
    """Generate a sample Agreement"""
    return {
        "id": generate_random_id(),
        "name": f"Agreement-{generate_random_id()}",
        "type": {
            "id": generate_random_id(),
            "name": random.choice(["Recurring Service", "Block Time", "Managed Services"]),
            "_info": generate_info_object(),
        },
        "company": generate_company_reference(),
        "contact": create_reference(),
        "site": create_reference(),
        "location_id": generate_random_id(),
        "start_date": generate_random_date(),
        "end_date": generate_random_date(days_back=10),
        "no_ending_date_flag": random.choice([True, False]),
        "canceled_flag": False,
        "bill_time": random.choice(["Billable", "DoNotBill", "NoCharge"]),
        "billable_expense": random.choice(["Billable", "DoNotBill", "NoCharge"]),
        "billable_product": random.choice(["Billable", "DoNotBill", "NoCharge"]),
        "billing_cycle": {
            "id": generate_random_id(),
            "name": random.choice(["Monthly", "Quarterly", "Annual"]),
            "_info": generate_info_object(),
        },
        "billing_terms": {
            "id": generate_random_id(),
            "name": random.choice(["Net 30", "Due on Receipt", "Net 15"]),
            "_info": generate_info_object(),
        },
        "work_type": create_reference(),
        "work_role": create_reference(),
        "agreement_type": random.choice(["Standard", "CompanyWide"]),
        "_info": generate_info_object(),
        "custom_fields": [],
    }


def generate_posted_invoice():
    """Generate a sample Posted Invoice"""
    return {
        "id": generate_random_id(),
        "invoice_number": f"INV{generate_random_id()}",
        "invoice_date": generate_random_date(),
        "status": random.choice(["Paid", "Open"]),
        "type": random.choice(["Agreement", "Time", "Expense", "Product"]),
        "company": generate_company_reference(),
        "billing_site_id": generate_random_id(),
        "billing_site": create_reference(),
        "billing_terms": create_reference(),
        "due_date": generate_random_date(),
        "shipped_date": generate_random_date(),
        "total": generate_random_decimal(100, 5000),
        "payment_total": generate_random_decimal(0, 5000),
        "balance": generate_random_decimal(0, 1000),
        "sales_tax": generate_random_decimal(0, 100),
        "tax_code": create_reference(),
        "service_total": generate_random_decimal(50, 2000),
        "expense_total": generate_random_decimal(0, 500),
        "product_total": generate_random_decimal(0, 800),
        "agreement_id": generate_random_id(),
        "agreement": create_reference(),
        "currency": {
            "id": generate_random_id(),
            "symbol": "$",
            "currency_code": "USD",
            "decimal_separator": ".",
            "number_of_decimals": 2,
            "thousands_separator": ",",
            "_info": generate_info_object(),
        },
        "_info": generate_info_object(),
        "custom_fields": [],
    }


def generate_unposted_invoice():
    """Generate a sample Unposted Invoice"""
    return {
        "id": generate_random_id(),
        "invoice_number": f"DRAFT-{generate_random_id()}",
        "type": random.choice(["Agreement", "Time", "Expense", "Product"]),
        "company": generate_company_reference(),
        "tax_code": create_reference(),
        "location_id": generate_random_id(),
        "business_unit_id": generate_random_id(),
        "date": generate_random_date(),
        "department_id": generate_random_id(),
        "status": "Draft",
        "billing_terms_id": generate_random_id(),
        "billing_terms": create_reference(),
        "amount": generate_random_decimal(100, 3000),
        "_info": generate_info_object(),
        "custom_fields": [],
    }


def generate_time_entry():
    """Generate a sample Time Entry"""
    return {
        "id": generate_random_id(),
        "company": generate_company_reference(),
        "charge_to_id": generate_random_id(),
        "charge_to_type": random.choice(["ServiceTicket", "ProjectTicket", "Activity"]),
        "member": {
            "id": generate_random_id(),
            "identifier": f"user{generate_random_id()}",
            "name": f"User {generate_random_id()}",
            "daily_capacity": 8.0,
            "_info": generate_info_object(),
        },
        "location_id": generate_random_id(),
        "business_unit_id": generate_random_id(),
        "work_type": create_reference(),
        "work_role": create_reference(),
        "agreement": create_reference(),
        "time_start": generate_random_date(),
        "time_end": generate_random_date(),
        "hours_deduct": generate_random_decimal(0, 8),
        "actual_hours": generate_random_decimal(0.5, 8),
        "billable_option": random.choice(["Billable", "DoNotBill", "NoCharge"]),
        "notes": f"Sample time entry notes for ticket #{generate_random_id()}",
        "internal_notes": "Internal notes for time entry",
        "hourly_rate": generate_random_decimal(50, 200),
        "hours": generate_random_decimal(0.5, 8),
        "total": generate_random_decimal(50, 1000),
        "status": random.choice(["Approved", "PendingApproval"]),
        "date_entered": generate_random_date(),
        "entered_by": f"user{generate_random_id()}",
        "_info": generate_info_object(),
        "custom_fields": [],
    }


def generate_expense_entry():
    """Generate a sample Expense Entry"""
    return {
        "id": generate_random_id(),
        "company": generate_company_reference(),
        "charge_to_id": generate_random_id(),
        "charge_to_type": random.choice(["ServiceTicket", "ProjectTicket", "Activity"]),
        "type": {
            "id": generate_random_id(),
            "name": random.choice(["Mileage", "Meals", "Lodging", "Travel"]),
            "_info": generate_info_object(),
        },
        "member": {
            "id": generate_random_id(),
            "identifier": f"user{generate_random_id()}",
            "name": f"User {generate_random_id()}",
            "_info": generate_info_object(),
        },
        "location_id": generate_random_id(),
        "business_unit_id": generate_random_id(),
        "agreement": create_reference(),
        "work_role": create_reference(),
        "work_type": create_reference(),
        "date": generate_random_date(),
        "amount": generate_random_decimal(10, 500),
        "billable_option": random.choice(["Billable", "DoNotBill", "NoCharge"]),
        "billable_amount": generate_random_decimal(10, 500),
        "reimbursable": random.choice([True, False]),
        "status": random.choice(["Open", "PendingApproval", "Approved"]),
        "entered_date": generate_random_date(),
        "entered_by": f"user{generate_random_id()}",
        "invoice_id": generate_random_id(),
        "line_no": random.randint(1, 10),
        "expense_type": random.choice(["Meals", "Mileage", "Travel", "Lodging"]),
        "_info": generate_info_object(),
        "custom_fields": [],
    }


def generate_product_item():
    """Generate a sample Product Item"""
    return {
        "id": generate_random_id(),
        "catalog_item": {
            "id": generate_random_id(),
            "identifier": f"PROD-{generate_random_id()}",
            "description": f"Sample product {generate_random_id()}",
            "_info": generate_info_object(),
        },
        "description": f"Sample product description for item #{generate_random_id()}",
        "quantity": random.randint(1, 10),
        "price": generate_random_decimal(50, 300),
        "cost": generate_random_decimal(25, 200),
        "ext_price": generate_random_decimal(50, 3000),
        "ext_cost": generate_random_decimal(25, 2000),
        "discount": generate_random_decimal(0, 10),
        "margin": generate_random_decimal(10, 50),
        "billable_option": random.choice(["Billable", "DoNotBill", "NoCharge"]),
        "agreement": create_reference(),
        "location_id": generate_random_id(),
        "business_unit_id": generate_random_id(),
        "taxable_flag": random.choice([True, False]),
        "dropship_flag": random.choice([True, False]),
        "special_order_flag": random.choice([True, False]),
        "phase_product_flag": random.choice([True, False]),
        "cancelled_flag": False,
        "customer_description": f"Customer facing description for product #{generate_random_id()}",
        "internal_notes": "Internal notes for product",
        "ticket": create_reference(),
        "project": create_reference(),
        "invoice": generate_invoice_reference(),
        "tax_code": create_reference(),
        "_info": generate_info_object(),
        "custom_fields": [],
    }


def generate_entity_samples(output_dir: str = "entity_samples"):
    """
    Generate sample data for each entity and save to JSON files.

    Args:
        output_dir: Directory to save output files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Define entities and their generator functions
    entities = {
        "Agreement": generate_agreement,
        "PostedInvoice": generate_posted_invoice,
        "UnpostedInvoice": generate_unposted_invoice,
        "TimeEntry": generate_time_entry,
        "ExpenseEntry": generate_expense_entry,
        "ProductItem": generate_product_item,
    }

    for entity_name, generate_func in entities.items():
        logger.info(f"Generating sample for {entity_name}...")

        try:
            # Generate sample data
            sample_data = generate_func()

            # Save to file
            file_path = os.path.join(output_dir, f"{entity_name}.json")
            with open(file_path, "w", encoding="utf-8") as f:
                # Pretty print with 4 space indentation
                json.dump(sample_data, f, indent=4)

            logger.info(f"✅ Saved {entity_name} sample to {file_path}")

        except Exception as e:
            logger.error(f"❌ Error generating {entity_name}: {e!s}")

    logger.info(f"Completed generating samples for {len(entities)} entities")


def main():
    """Main function to run the script."""
    logger.info("Starting entity sample generation...")
    generate_entity_samples()
    logger.info("Done!")


if __name__ == "__main__":
    main()
