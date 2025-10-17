"""ConnectWise ETL entry point using the new protocol-based architecture."""

from pyspark.sql import SparkSession

from etl_core.config import LakehouseConfig
from etl_core.runner import ETLRunner
from etl_core.utils.logging import setup_logging

from .plugin import ConnectWisePlugin


def run_connectwise_etl(
    spark: SparkSession,
    catalog: str = "main",
    schema: str = "connectwise",
    mode: str = "full",
    layers: list[str] | None = None,
    entities: list[str] | None = None,
    lookback_days: int = 7,
    page_size: int = 1000,
) -> dict:
    """
    Run ConnectWise ETL pipeline using the protocol-based architecture.

    Args:
        spark: Active SparkSession.
        catalog: Lakehouse catalog name.
        schema: Lakehouse schema/database name.
        mode: Load mode (full, incremental, append).
        layers: Layers to run (bronze, silver, gold). All if None.
        entities: Entities to process (all if None).
        lookback_days: Days to look back for incremental loads.
        page_size: API pagination size.

    Returns:
        Dictionary with results from each layer.

    Example:
        >>> from pyspark.sql import SparkSession
        >>> spark = SparkSession.builder.appName("ConnectWise ETL").getOrCreate()
        >>> results = run_connectwise_etl(
        ...     spark=spark,
        ...     catalog="main",
        ...     schema="connectwise",
        ...     mode="incremental",
        ...     layers=["bronze", "silver", "gold"],
        ...     lookback_days=7
        ... )
        >>> print(f"Bronze: {results['bronze'].entities_processed}")
        >>> print(f"Silver: {results['silver'].entities_processed}")
        >>> print(f"Gold: {results['gold'].dimensions_created}")
    """
    # Setup logging
    setup_logging(service_name="connectwise-etl", environment="production")

    # Create lakehouse config
    lakehouse_config = LakehouseConfig(
        catalog=catalog,
        bronze_schema=f"{schema}_bronze",
        silver_schema=f"{schema}_silver",
        gold_schema=f"{schema}_gold",
    )

    # Initialize runner
    runner = ETLRunner(spark=spark, lakehouse_config=lakehouse_config)

    # Register ConnectWise plugin
    plugin = ConnectWisePlugin()
    runner.register_plugin(plugin)

    # Run pipeline
    results = runner.run_full_pipeline(
        plugin_name="connectwise",
        mode=mode,
        entities=entities,
        layers=layers,
        lookback_days=lookback_days,
        page_size=page_size,
    )

    return results


def main():
    """Entry point for Fabric notebook or script execution."""
    # This assumes SparkSession is already available (Fabric context)
    # For local testing, create SparkSession explicitly
    try:
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active SparkSession found")
    except Exception as e:
        raise RuntimeError(
            "This script requires an active SparkSession. "
            "Run in a Databricks/Fabric notebook or create SparkSession explicitly."
        ) from e

    # Run full pipeline with default settings
    results = run_connectwise_etl(
        spark=spark,
        catalog="main",
        schema="connectwise",
        mode="full",
        layers=["bronze", "silver", "gold"],
    )

    print("\n=== ConnectWise ETL Pipeline Complete ===")
    print(f"Bronze: Processed {len(results['bronze'].entities_processed)} entities")
    print(f"Silver: Processed {len(results['silver'].entities_processed)} entities")
    print(f"Gold: Created {len(results['gold'].dimensions_created)} dimensions")
    print(f"Gold: Created {len(results['gold'].facts_created)} facts")


if __name__ == "__main__":
    main()
