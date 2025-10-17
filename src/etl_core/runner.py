"""ETL pipeline orchestrator - wires plugins and executes medallion layers."""

import time
import uuid
from typing import TYPE_CHECKING

from etl_core.config import LakehouseConfig, RuntimeContext
from etl_core.domain.protocols import (
    BronzeResult,
    GoldResult,
    IntegrationPluginProtocol,
    SilverResult,
)
from etl_core.infrastructure.validator import PydanticDataValidator
from etl_core.utils.errors import ConfigError
from etl_core.utils.incremental import IncrementalHandler
from etl_core.utils.logging import get_logger, update_log_context

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class ETLRunner:
    """
    ETL pipeline orchestrator implementing hexagonal architecture.

    Responsibilities:
    - Load and initialize integration plugins
    - Inject dependencies (validator, incremental handler)
    - Execute medallion layers (Bronze → Silver → Gold)
    - Coordinate across plugins and layers
    """

    def __init__(self, spark: "SparkSession", lakehouse_config: LakehouseConfig):
        """
        Initialize ETL runner.

        Args:
            spark: Active SparkSession.
            lakehouse_config: Lakehouse configuration (catalog, schema, etc.).
        """
        # Note: Type annotations ensure these are not None
        # but we validate at runtime for explicit error messages

        self.spark = spark
        self.lakehouse_config = lakehouse_config

        # Initialize core infrastructure (shared across all plugins)
        self.validator = PydanticDataValidator()
        self.incremental_handler = IncrementalHandler(spark)

        # Plugins will be registered here
        self.plugins: dict[str, IntegrationPluginProtocol] = {}

    def register_plugin(self, plugin: IntegrationPluginProtocol) -> None:
        """
        Register an integration plugin.

        Args:
            plugin: Integration plugin implementing IntegrationPluginProtocol.
        """
        plugin_name = plugin.name
        if plugin_name in self.plugins:
            logger.warning(f"Plugin {plugin_name} already registered, replacing")

        self.plugins[plugin_name] = plugin
        logger.info(f"Registered plugin: {plugin_name}")

    def run_bronze(
        self,
        plugin_name: str,
        mode: str = "full",
        entities: list[str] | None = None,
        **extra_args,
    ) -> BronzeResult:
        """
        Execute bronze layer extraction and validation.

        Args:
            plugin_name: Name of the plugin to run.
            mode: Load mode (full, incremental, append).
            entities: Optional list of entities to process (all if None).
            **extra_args: Additional parameters (page_size, lookback_days, etc.).

        Returns:
            BronzeResult with metrics.
        """
        plugin = self._get_plugin(plugin_name)

        # Generate batch ID for this run
        batch_id = f"{plugin_name}_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        # Initialize plugin components
        registry = plugin.initialize_registry()
        fetcher = plugin.initialize_fetcher()
        processors = plugin.initialize_processors(
            fetcher=fetcher,
            validator=self.validator,
            registry=registry,
            incremental_handler=self.incremental_handler,
        )

        # Create runtime context
        context = RuntimeContext(
            spark=self.spark,
            lakehouse_config=self.lakehouse_config,
            batch_id=batch_id,
            mode=mode,
            entities=entities,
            extra_args=extra_args,
        )

        # Log context
        update_log_context("plugin", plugin_name)
        update_log_context("batch_id", batch_id)
        update_log_context("mode", mode)

        logger.info(
            f"Starting bronze layer for {plugin_name}",
            mode=mode,
            entities=entities or "all",
        )

        # Execute bronze processor
        result = processors.bronze.process(context)

        logger.info(
            f"Bronze layer complete for {plugin_name}",
            entities_processed=len(result.entities_processed),
            tables_written=len(result.tables_written),
            duration=f"{result.duration_seconds:.2f}s",
        )

        return result

    def run_silver(
        self,
        plugin_name: str,
        mode: str = "full",
        entities: list[str] | None = None,
        **extra_args,
    ) -> SilverResult:
        """
        Execute silver layer transformations.

        Args:
            plugin_name: Name of the plugin to run.
            mode: Load mode (full, incremental, append).
            entities: Optional list of entities to process (all if None).
            **extra_args: Additional parameters.

        Returns:
            SilverResult with metrics.
        """
        plugin = self._get_plugin(plugin_name)

        # Generate batch ID
        batch_id = f"{plugin_name}_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        # Initialize plugin components
        registry = plugin.initialize_registry()
        fetcher = plugin.initialize_fetcher()
        processors = plugin.initialize_processors(
            fetcher=fetcher,
            validator=self.validator,
            registry=registry,
            incremental_handler=self.incremental_handler,
        )

        # Create runtime context
        context = RuntimeContext(
            spark=self.spark,
            lakehouse_config=self.lakehouse_config,
            batch_id=batch_id,
            mode=mode,
            entities=entities,
            extra_args=extra_args,
        )

        # Log context
        update_log_context("plugin", plugin_name)
        update_log_context("batch_id", batch_id)
        update_log_context("mode", mode)

        logger.info(
            f"Starting silver layer for {plugin_name}",
            mode=mode,
            entities=entities or "all",
        )

        # Execute silver processor
        result = processors.silver.process(context)

        logger.info(
            f"Silver layer complete for {plugin_name}",
            entities_processed=len(result.entities_processed),
            tables_written=len(result.tables_written),
            duration=f"{result.duration_seconds:.2f}s",
        )

        return result

    def run_gold(
        self,
        plugin_name: str,
        entities: list[str] | None = None,
        **extra_args,
    ) -> GoldResult:
        """
        Execute gold layer dimensional modeling.

        Args:
            plugin_name: Name of the plugin to run.
            entities: Optional list of entities to process (all if None).
            **extra_args: Additional parameters (dimension_schema_path, etc.).

        Returns:
            GoldResult with metrics.
        """
        plugin = self._get_plugin(plugin_name)

        # Generate batch ID
        batch_id = f"{plugin_name}_{int(time.time())}_{uuid.uuid4().hex[:8]}"

        # Initialize plugin components
        registry = plugin.initialize_registry()
        fetcher = plugin.initialize_fetcher()
        processors = plugin.initialize_processors(
            fetcher=fetcher,
            validator=self.validator,
            registry=registry,
            incremental_handler=self.incremental_handler,
        )

        # Create runtime context (mode doesn't matter for gold)
        context = RuntimeContext(
            spark=self.spark,
            lakehouse_config=self.lakehouse_config,
            batch_id=batch_id,
            mode="full",
            entities=entities,
            extra_args=extra_args,
        )

        # Log context
        update_log_context("plugin", plugin_name)
        update_log_context("batch_id", batch_id)

        logger.info(f"Starting gold layer for {plugin_name}", entities=entities or "all")

        # Execute gold processor
        result = processors.gold.process(context, **extra_args)

        logger.info(
            f"Gold layer complete for {plugin_name}",
            dimensions_created=len(result.dimensions_created),
            facts_created=len(result.facts_created),
            duration=f"{result.duration_seconds:.2f}s",
        )

        return result

    def run_full_pipeline(
        self,
        plugin_name: str,
        mode: str = "full",
        entities: list[str] | None = None,
        layers: list[str] | None = None,
        **extra_args,
    ) -> dict:
        """
        Execute complete ETL pipeline (Bronze → Silver → Gold).

        Args:
            plugin_name: Name of the plugin to run.
            mode: Load mode (full, incremental, append).
            entities: Optional list of entities to process (all if None).
            layers: Optional list of layers to run (bronze, silver, gold). All if None.
            **extra_args: Additional parameters passed to processors.

        Returns:
            Dictionary with results from each layer.
        """
        if layers is None:
            layers = ["bronze", "silver", "gold"]

        logger.info(
            f"Starting full pipeline for {plugin_name}",
            mode=mode,
            layers=layers,
            entities=entities or "all",
        )

        results = {}

        if "bronze" in layers:
            results["bronze"] = self.run_bronze(plugin_name, mode, entities, **extra_args)

        if "silver" in layers:
            results["silver"] = self.run_silver(plugin_name, mode, entities, **extra_args)

        if "gold" in layers:
            results["gold"] = self.run_gold(plugin_name, entities, **extra_args)

        logger.info(f"Full pipeline complete for {plugin_name}")
        return results

    def _get_plugin(self, plugin_name: str) -> IntegrationPluginProtocol:
        """Get registered plugin or raise error."""
        if plugin_name not in self.plugins:
            raise ConfigError(
                f"Plugin '{plugin_name}' not registered. "
                f"Available: {list(self.plugins.keys())}"
            )
        return self.plugins[plugin_name]
