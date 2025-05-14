#!/usr/bin/env python
"""
Utility script to regenerate models from a subset of the OpenAPI schema.

This script:
1. Creates a subset schema with only the models we need and their dependencies
2. Generates Pydantic models from the subset schema
3. Ensures reference models are properly defined and imported

Usage:
    python regenerate_models.py [--entities ENTITY1 ENTITY2 ...]

Example:
    python regenerate_models.py
    python regenerate_models.py --entities Agreement TimeEntry
"""
import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
from typing import Dict, List, Set, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Default entity configurations
DEFAULT_ENTITIES = {
    "Agreement": {"output": "agreement.py", "schema_path": "Agreement"},
    "TimeEntry": {"output": "time_entry.py", "schema_path": "TimeEntry"},
    "ExpenseEntry": {"output": "expense_entry.py", "schema_path": "ExpenseEntry"},
    "Invoice": {"output": "invoice.py", "schema_path": "Invoice"},
    "UnpostedInvoice": {"output": "unposted_invoice.py", "schema_path": "UnpostedInvoice"},
    "ProductItem": {"output": "product_item.py", "schema_path": "ProductItem"},
}

# Reference models to always include
REFERENCE_MODELS = [
    "ActivityReference",
    "AgreementReference",
    "AgreementTypeReference",
    "BatchReference",
]

def load_schema(schema_path: str) -> Dict[str, Any]:
    """Load an OpenAPI schema from a file path."""
    with open(schema_path, "r") as f:
        return json.load(f)

def find_references(schema_obj: Dict[str, Any], refs: Set[str]) -> Set[str]:
    """Find all $ref references in a schema object."""
    if isinstance(schema_obj, dict):
        for k, v in schema_obj.items():
            if k == "$ref" and isinstance(v, str) and v.startswith("#/components/schemas/"):
                # Extract the model name from the reference
                ref_name = v.split("/")[-1]
                refs.add(ref_name)
            elif isinstance(v, (dict, list)):
                refs = refs.union(find_references(v, refs))
    elif isinstance(schema_obj, list):
        for item in schema_obj:
            if isinstance(item, (dict, list)):
                refs = refs.union(find_references(item, refs))
    return refs

def extract_model_with_dependencies(
    schema: Dict[str, Any], model_names: List[str]
) -> Dict[str, Any]:
    """
    Extract specified models and their dependencies from an OpenAPI schema.
    
    Args:
        schema: The full OpenAPI schema
        model_names: List of model names to extract
        
    Returns:
        A new schema containing only the specified models and their dependencies
    """
    # Create a new schema with the same structure
    subset_schema = {
        "openapi": schema.get("openapi", "3.0.0"),
        "info": schema.get("info", {"title": "Subset Schema", "version": "1.0.0"}),
        "paths": {},
        "components": {"schemas": {}}
    }
    
    # Start with the specified models
    models_to_include = set(model_names)
    processed_models = set()
    
    # Process models and their dependencies
    while models_to_include.difference(processed_models):
        # Get next model to process
        model_name = next(iter(models_to_include.difference(processed_models)))
        
        # Skip if model doesn't exist in the original schema
        if model_name not in schema.get("components", {}).get("schemas", {}):
            logger.warning(f"Model '{model_name}' not found in the schema!")
            processed_models.add(model_name)
            continue
        
        # Add the model to the subset schema
        model_schema = schema["components"]["schemas"][model_name]
        subset_schema["components"]["schemas"][model_name] = model_schema
        
        # Find references in this model
        refs = find_references(model_schema, set())
        models_to_include.update(refs)
        
        # Mark this model as processed
        processed_models.add(model_name)
    
    # Log the models included in the subset schema
    included_models = set(subset_schema["components"]["schemas"].keys())
    logger.info(f"Generated subset schema with {len(included_models)} models")
    
    return subset_schema

def create_init_file(output_dir: str, entity_names: List[str]) -> None:
    """Create the __init__.py file with reference models and imports."""
    init_path = os.path.join(output_dir, "__init__.py")
    
    with open(init_path, 'w', encoding='utf-8') as f:
        f.write('"""ConnectWise API models generated from OpenAPI schema.\n\n')
        f.write('Compatible with Pydantic v2 and SparkDantic for Spark schema generation.\n')
        f.write('"""\n\n')
        f.write('from typing import Any, Dict, List, Optional\n')
        f.write('from pydantic import Field\n')
        f.write('from sparkdantic import SparkModel\n\n')
        
        # Define reference models
        f.write('# Base reference model\n')
        f.write('class ActivityReference(SparkModel):\n')
        f.write('    """Base reference model for ConnectWise entities."""\n')
        f.write('    id: int | None = None\n')
        f.write('    name: str | None = None\n')
        f.write('    _info: dict[str, str] | None = None\n\n')
        
        f.write('class AgreementReference(ActivityReference):\n')
        f.write('    """Reference model for Agreement entities."""\n')
        f.write('    type: str | None = None\n')
        f.write('    chargeFirmFlag: bool | None = None\n\n')
        
        f.write('class AgreementTypeReference(ActivityReference):\n')
        f.write('    """Reference model for AgreementType entities."""\n')
        f.write('    pass\n\n')
        
        f.write('class BatchReference(AgreementTypeReference):\n')
        f.write('    """Reference model for Batch entities."""\n')
        f.write('    pass\n\n')
        
        # Now import entities
        f.write('# Entity imports\n')
        for entity_name in entity_names:
            if entity_name == "Agreement":
                f.write('from .agreement import Agreement\n')
            elif entity_name == "TimeEntry":
                f.write('from .time_entry import TimeEntry\n')
            elif entity_name == "ExpenseEntry":
                f.write('from .expense_entry import ExpenseEntry\n')
            elif entity_name == "Invoice":
                f.write('from .invoice import Invoice\n')
                # Special case for PostedInvoice (alias of Invoice)
                f.write('# PostedInvoice is an alias of Invoice\n')
                f.write('PostedInvoice = Invoice\n')
            elif entity_name == "UnpostedInvoice":
                f.write('from .unposted_invoice import UnpostedInvoice\n')
            elif entity_name == "ProductItem":
                f.write('from .product_item import ProductItem\n')
        
        f.write('\n')
        
        # __all__ list
        f.write('__all__ = [\n')
        f.write('    # Reference models\n')
        for ref_model in REFERENCE_MODELS:
            f.write(f'    "{ref_model}",\n')
        
        f.write('    # Entity models\n')
        for entity_name in entity_names:
            f.write(f'    "{entity_name}",\n')
            if entity_name == "Invoice":
                f.write('    "PostedInvoice",\n')
        
        f.write(']\n')
    
    logger.info(f"✅ Created __init__.py at {init_path}")

def generate_models(entities: Dict[str, Dict[str, str]], schema_file: str, output_dir: str) -> None:
    """Generate models using datamodel-code-generator from pyproject.toml config."""
    # Load the schema
    logger.info(f"Loading schema from {schema_file}")
    schema = load_schema(schema_file)
    
    # Add reference models and entity models to the list of models to include
    models_to_include = list(entities.keys()) + REFERENCE_MODELS
    
    # Create subset schema
    logger.info(f"Creating subset schema with {len(models_to_include)} models...")
    subset_schema = extract_model_with_dependencies(schema, models_to_include)
    
    # Write debug schema for inspection
    debug_dir = "debug_output"
    os.makedirs(debug_dir, exist_ok=True)
    with open(os.path.join(debug_dir, "temp_schema.json"), 'w') as f:
        json.dump(subset_schema, f, indent=2)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Write subset schema to temp file and generate models
    with tempfile.NamedTemporaryFile(suffix=".json", mode="w") as tmp:
        json.dump(subset_schema, tmp)
        tmp.flush()
        
        # Generate models for each entity
        for entity_name, config in entities.items():
            logger.info(f"Generating {entity_name}...")
            
            output_path = os.path.join(output_dir, config["output"])
            cmd = [
                "datamodel-codegen",
                "--input", tmp.name,
                "--output", output_path,
                "--class-name", entity_name,
            ]
            
            # Run the command
            try:
                result = subprocess.run(cmd, check=True, capture_output=True, text=True)
                logger.info(f"✅ Generated {entity_name}")
                
                # Fix imports if needed
                with open(output_path, 'r') as f:
                    content = f.read()
                
                # Add __init__ import for reference models if needed
                for ref_model in REFERENCE_MODELS:
                    if f"class {entity_name}({ref_model})" in content or f"({ref_model}):" in content:
                        if f"from .__init__ import {ref_model}" not in content:
                            content = content.replace("from pydantic import Field", f"from pydantic import Field\nfrom .__init__ import {ref_model}")
                
                # Write back changes
                with open(output_path, 'w') as f:
                    f.write(content)
                
            except subprocess.CalledProcessError as e:
                logger.error(f"❌ Error generating {entity_name}: {e}")
                logger.error(f"Command output: {e.stderr}")
    
    # Create __init__.py file
    create_init_file(output_dir, list(entities.keys()))
    
    logger.info("✅ Model generation complete!")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Generate Pydantic models from OpenAPI schema")
    parser.add_argument(
        "--schema", 
        default="PSA_OpenAPI_schema.json",
        help="Path to the OpenAPI schema"
    )
    parser.add_argument(
        "--output-dir",
        default="fabric_api/connectwise_models",
        help="Directory to output the generated models"
    )
    parser.add_argument(
        "--entities",
        nargs="+",
        help="List of entities to generate models for"
    )
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Get entities to generate
    if args.entities:
        entities = {}
        for entity_name in args.entities:
            if entity_name in DEFAULT_ENTITIES:
                entities[entity_name] = DEFAULT_ENTITIES[entity_name]
            else:
                logger.warning(f"Unknown entity: {entity_name}, using default configuration")
                entities[entity_name] = {"output": f"{entity_name.lower()}.py", "schema_path": entity_name}
    else:
        entities = DEFAULT_ENTITIES
    
    # Generate models
    generate_models(entities, args.schema, args.output_dir)

if __name__ == "__main__":
    main()