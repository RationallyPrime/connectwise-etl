#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script to generate Pydantic models from JSON samples using datamodel-code-generator
and update schemas.py directly.
"""

import os
import subprocess
import logging
import shutil
import tempfile

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def generate_models():
    """
    Generate models from JSON files in a package structure with each model in its own file.
    """
    # Directories
    samples_dir = "entity_samples"
    models_dir = "connectwise_models"
    
    # Create package directory
    os.makedirs(models_dir, exist_ok=True)
    
    # Generate models for each JSON file
    for filename in sorted(os.listdir(samples_dir)):
        if filename.endswith(".json"):
            entity_name = os.path.splitext(filename)[0]
            input_path = os.path.join(samples_dir, filename)
            
            # Convert entity name to snake_case for the file name
            snake_case_name = ''.join(['_' + c.lower() if c.isupper() else c for c in entity_name]).lstrip('_')
            if snake_case_name.startswith("posted_"):
                snake_case_name = "invoice"  # Special case for PostedInvoice
                
            output_path = os.path.join(models_dir, f"{snake_case_name}.py")
            
            logger.info(f"Generating model for {entity_name} in {output_path}...")
            
            # Command to run datamodel-code-generator
            cmd = [
                "datamodel-codegen",
                "--input", input_path,
                "--input-file-type", "json",
                "--output", output_path,
                "--class-name", entity_name,
                "--base-class", "sparkdantic.SparkModel",
                "--field-constraints"
            ]
            
            try:
                # Run the command
                result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                logger.info(f"✅ Successfully generated model for {entity_name}")
                
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Error generating model for {entity_name}: {e}")
                logger.error(f"Command output: {e.stderr}")
    
    # Create __init__.py file that only exports the main models
    init_path = os.path.join(models_dir, "__init__.py")
    logger.info(f"Creating package __init__.py at {init_path}")
    
    with open(init_path, 'w', encoding='utf-8') as f:
        f.write('''#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ConnectWise API models generated from actual API responses.
"""

from typing import List
from sparkdantic import SparkModel

# Import only the main models to avoid namespace collisions
from .agreement import Agreement
from .expense_entry import ExpenseEntry
from .invoice import Invoice
from .product_item import ProductItem
from .time_entry import TimeEntry
from .unposted_invoice import UnpostedInvoice

__all__ = [
    "Agreement",
    "ExpenseEntry",
    "Invoice",
    "PostedInvoice", # Alias for Invoice
    "ProductItem",
    "TimeEntry",
    "UnpostedInvoice",
    # List container models
    "AgreementList",
    "InvoiceList",
    "UnpostedInvoiceList",
    "TimeEntryList",
    "ExpenseEntryList",
    "ProductItemList",
]

# For backward compatibility
PostedInvoice = Invoice

# List container models for API responses
class AgreementList(SparkModel):
    """List of agreements response"""
    items: List[Agreement]

class InvoiceList(SparkModel):
    """List of invoices response"""
    items: List[Invoice]

class UnpostedInvoiceList(SparkModel):
    """List of unposted invoices response"""
    items: List[UnpostedInvoice]

class TimeEntryList(SparkModel):
    """List of time entries response"""
    items: List[TimeEntry]

class ExpenseEntryList(SparkModel):
    """List of expense entries response"""
    items: List[ExpenseEntry]

class ProductItemList(SparkModel):
    """List of product items response"""
    items: List[ProductItem]
''')
        
    # Back up the original schemas file if it exists
    schemas_file = "fabric_api/schemas.py"
    if os.path.exists(schemas_file):
        backup_file = f"{schemas_file}.bak"
        logger.info(f"Backing up original schemas file to {backup_file}")
        shutil.copy2(schemas_file, backup_file)
        
        # We could delete it, but better to leave that as a manual step
        logger.warning(f"The old schemas.py is now obsolete. You should delete it after updating all imports.")
    
    logger.info(f"Model generation complete. All models are in the {models_dir} package.")
    logger.info(f"Use 'from connectwise_models import Agreement, PostedInvoice, etc.' in your code.")
    logger.info(f"You'll need to update all files that previously imported from fabric_api.schemas.")



if __name__ == "__main__":
    generate_models()
