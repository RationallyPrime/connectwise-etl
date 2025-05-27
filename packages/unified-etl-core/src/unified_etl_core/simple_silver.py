"""Simple Silver processing that actually works."""
import logging
import sys
from typing import Dict, Any

from pyspark.sql import functions as F


def process_bronze_to_silver(
    entity_name: str,
    bronze_table_name: str,
    lakehouse_root: str
) -> None:
    """
    Process Bronze data to Silver with basic transformations.
    
    Args:
        entity_name: Name of entity (for Silver table naming)
        bronze_table_name: Name of bronze table 
        lakehouse_root: Root path for tables
    """
    # Get Fabric global spark session
    spark = sys.modules['__main__'].spark
    
    # Read Bronze data
    bronze_path = f"{lakehouse_root}bronze/{bronze_table_name}"
    print(f"Reading from: {bronze_path}")
    
    try:
        bronze_df = spark.read.format("delta").load(bronze_path)
        record_count = bronze_df.count()
        print(f"✅ Read {record_count} records from Bronze")
    except Exception as e:
        print(f"❌ Failed to read Bronze table: {e}")
        raise
    
    # Basic Silver transformations
    silver_df = (bronze_df
                 .withColumn("_etl_processed_at", F.current_timestamp())
                 .withColumn("_etl_source", F.lit("connectwise")))
    
    # Write to Silver
    silver_path = f"{lakehouse_root}silver/silver_cw_{entity_name}"
    print(f"Writing to: {silver_path}")
    
    try:
        (silver_df.write
         .mode("overwrite")
         .format("delta")
         .option("mergeSchema", "true")
         .save(silver_path))
        print(f"✅ Wrote {record_count} records to Silver")
    except Exception as e:
        print(f"❌ Failed to write Silver table: {e}")
        raise