#!/usr/bin/env python
"""
Orchestrator script to regenerate Pydantic models for both PSA (ConnectWise) and BC (Business Central).

This script:
1. Calls the PSA model generator (`fabric_api.generators.generate_psa_models`)
2. Calls the BC model generator (`fabric_api.generators.generate_bc_models`)

Usage:
    python regenerate_models.py [--psa-schema PATH] [--psa-output-dir DIR] [--psa-entities ENTITY1 ...] \
                                [--bc-schema-dir DIR] [--bc-output-models FILE] [--bc-output-init FILE] \
                                [--generate-psa] [--generate-bc]

Defaults:
- Generates both PSA and BC models.
- Uses default schema paths and output locations defined in the respective generator scripts.
"""

import argparse
import logging # Using standard logging for this root script
import pathlib

# Import the generator functions
from fabric_api.generators.generate_psa_models import generate_psa_models, DEFAULT_PSA_OPENAPI_SCHEMA_PATH, DEFAULT_PSA_OUTPUT_DIR
from fabric_api.generators.generate_bc_models import generate_bc_models, DEFAULT_BC_CDM_SCHEMA_DIR, DEFAULT_BC_OUTPUT_MODELS_FILE, DEFAULT_BC_OUTPUT_INIT_FILE

# Configure basic logging for the orchestrator script
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Default PSA entities to include if --psa-entities is not provided
# These are the keys from the original DEFAULT_ENTITIES in the old script
DEFAULT_PSA_ENTITIES_TO_GENERATE = [
    "Agreement", "TimeEntry", "ExpenseEntry", "Invoice", "ProductItem", # UnpostedInvoice was there but Invoice is primary
]
# PSA Reference models that are commonly used (from old script's REFERENCE_MODELS)
DEFAULT_PSA_REFERENCE_MODELS = [
    "ActivityReference", "AgreementReference", "AgreementTypeReference", "BatchReference",
]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Orchestrator to generate Pydantic models for PSA and BC.")
    
    # PSA specific arguments
    parser.add_argument(
        "--psa-schema", 
        default=str(pathlib.Path(DEFAULT_PSA_OPENAPI_SCHEMA_PATH).resolve()), # Resolve to make it absolute
        help=f"Path to the PSA OpenAPI schema. Default: {DEFAULT_PSA_OPENAPI_SCHEMA_PATH}"
    )
    parser.add_argument(
        "--psa-output-dir",
        default=str(pathlib.Path(DEFAULT_PSA_OUTPUT_DIR).resolve()),
        help=f"Directory to output the generated PSA models. Default: {DEFAULT_PSA_OUTPUT_DIR}"
    )
    parser.add_argument(
        "--psa-entities", 
        nargs="+", 
        default=DEFAULT_PSA_ENTITIES_TO_GENERATE,
        help=f"List of PSA entities to generate models for. Default: {' '.join(DEFAULT_PSA_ENTITIES_TO_GENERATE)}"
    )
    # TODO: Add argument for PSA reference models if needed, for now use default

    # BC specific arguments
    parser.add_argument(
        "--bc-schema-dir",
        default=str(pathlib.Path(DEFAULT_BC_CDM_SCHEMA_DIR).resolve()),
        help=f"Directory containing BC CDM schema files. Default: {DEFAULT_BC_CDM_SCHEMA_DIR}"
    )
    parser.add_argument(
        "--bc-output-models",
        default=str(pathlib.Path(DEFAULT_BC_OUTPUT_MODELS_FILE).resolve()),
        help=f"Output file for generated BC models. Default: {DEFAULT_BC_OUTPUT_MODELS_FILE}"
    )
    parser.add_argument(
        "--bc-output-init",
        default=str(pathlib.Path(DEFAULT_BC_OUTPUT_INIT_FILE).resolve()),
        help=f"Output __init__.py file for BC models. Default: {DEFAULT_BC_OUTPUT_INIT_FILE}"
    )

    # Control flags
    parser.add_argument(
        "--generate-psa", action=argparse.BooleanOptionalAction, default=True, help="Generate PSA models."
    )
    parser.add_argument(
        "--generate-bc", action=argparse.BooleanOptionalAction, default=True, help="Generate BC models."
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    psa_generated_successfully = False
    if args.generate_psa:
        logger.info("--- Starting PSA Model Generation ---")
        # Note: generate_psa_models expects entities_to_include and reference_models_to_include.
        # The args.psa_entities covers the main entities. We'll use the default for reference models.
        psa_generated_successfully = generate_psa_models(
            openapi_schema_path_str=args.psa_schema,
            output_dir_str=args.psa_output_dir,
            entities_to_include=args.psa_entities, # Parsed from CLI
            reference_models_to_include=DEFAULT_PSA_REFERENCE_MODELS # Using hardcoded default for now
        )
        if psa_generated_successfully:
            logger.info("--- PSA Model Generation Completed Successfully ---")
        else:
            logger.error("--- PSA Model Generation Failed ---")
    else:
        logger.info("Skipping PSA model generation as per --no-generate-psa flag.")

    bc_generated_successfully = False
    if args.generate_bc:
        logger.info("--- Starting BC Model Generation ---")
        bc_generated_successfully = generate_bc_models(
            input_dir_str=args.bc_schema_dir,
            output_models_file_str=args.bc_output_models,
            output_init_file_str=args.bc_output_init
        )
        if bc_generated_successfully:
            logger.info("--- BC Model Generation Completed Successfully ---")
        else:
            logger.error("--- BC Model Generation Failed ---")
    else:
        logger.info("Skipping BC model generation as per --no-generate-bc flag.")

    if (args.generate_psa and not psa_generated_successfully) or \
       (args.generate_bc and not bc_generated_successfully):
        logger.error("One or more model generation processes failed.")
        exit(1)
    
    logger.info("All requested model generation processes completed.")


if __name__ == "__main__":
    main()
