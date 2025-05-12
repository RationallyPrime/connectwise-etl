#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Test script to fetch one instance of each entity from ConnectWise API
and save the responses as pretty-printed JSON files for review.
This will help validate Pydantic models against real API responses.
"""

import json
import os
import logging
from typing import Dict, Any

# Set credentials
os.environ["CW_AUTH_USERNAME"] = "thekking+yemGyHDPdJ1hpuqx"
os.environ["CW_AUTH_PASSWORD"] = "yMqpe26Jcu55FbQk"
os.environ["CW_CLIENTID"] = "c7ea92d2-eaf5-4bfb-a09c-58d7f9dd7b81" 

from fabric_api.client import ConnectWiseClient
from fabric_api.bronze_loader import ENTITY_CONFIG

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_entity_samples(output_dir: str = "entity_samples", limit: int = 1):
    """
    Fetch one valid instance of each entity and save to JSON files.
    
    Args:
        output_dir: Directory to save output files
        limit: Number of records to fetch for each entity
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize ConnectWise client
    logger.info("Initializing ConnectWise client...")
    client = ConnectWiseClient()
    
    results = {}
    
    # Iterate through each entity in ENTITY_CONFIG
    for entity_name, config in ENTITY_CONFIG.items():
        logger.info(f"Fetching {entity_name}...")
        fetch_func = config["fetch_func"]
        
        try:
            # Fetch data with limit
            raw_data = fetch_func(
                client=client,
                page_size=limit,
                max_pages=1,
                conditions=None
            )
            
            # If we have data, take the first item
            if raw_data and len(raw_data) > 0:
                sample_item = raw_data[0]
                results[entity_name] = sample_item
                
                # Save to file
                file_path = os.path.join(output_dir, f"{entity_name}.json")
                with open(file_path, 'w', encoding='utf-8') as f:
                    # Pretty print with 4 space indentation
                    json.dump(sample_item, f, indent=4, default=str)
                    
                logger.info(f"✅ Saved {entity_name} sample to {file_path}")
            else:
                logger.warning(f"⚠️ No {entity_name} data found")
        except Exception as e:
            logger.error(f"❌ Error fetching {entity_name}: {str(e)}")
    
    logger.info(f"Completed fetching samples for {len(results)} entities")
    return results

def main():
    """Main function to run the script."""
    logger.info("Starting entity sample fetch...")
    fetch_entity_samples()
    logger.info("Done!")

if __name__ == "__main__":
    main()
