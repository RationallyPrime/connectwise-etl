"""
Quick runner for BC Gold layer processing.

Usage:
    python -m unified_etl_businesscentral.run_gold \
        --bronze-path /path/to/bronze \
        --silver-path /path/to/silver \
        --gold-path /path/to/gold
"""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import SparkSession

from .orchestrate import orchestrate_bc_gold_layer


def main():
    """Main entry point for BC gold layer processing."""
    parser = argparse.ArgumentParser(description="Run BC Gold Layer Processing")
    parser.add_argument("--bronze-path", required=True, help="Path to bronze layer")
    parser.add_argument("--silver-path", required=True, help="Path to silver layer")
    parser.add_argument("--gold-path", required=True, help="Path to gold layer")
    parser.add_argument("--batch-id", help="Batch ID (defaults to timestamp)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Generate batch ID if not provided
    batch_id = args.batch_id or f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logging.info(f"Starting BC Gold Layer processing with batch_id: {batch_id}")
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("BC_Gold_Layer") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    try:
        # Run orchestration
        stats = orchestrate_bc_gold_layer(
            spark=spark,
            bronze_path=args.bronze_path,
            silver_path=args.silver_path,
            gold_path=args.gold_path,
            batch_id=batch_id
        )
        
        # Print summary
        print("\n=== BC Gold Layer Processing Complete ===")
        print(f"\nDimensions created:")
        for dim, count in stats["dimensions_created"].items():
            print(f"  {dim}: {count:,} rows")
        
        print(f"\nFacts created:")
        for fact, count in stats["facts_created"].items():
            print(f"  {fact}: {count:,} rows")
        
        if stats["errors"]:
            print(f"\nErrors encountered:")
            for error in stats["errors"]:
                print(f"  - {error}")
            sys.exit(1)
        else:
            print("\nAll processing completed successfully!")
            
    except Exception as e:
        logging.error(f"Fatal error in gold layer processing: {e}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()