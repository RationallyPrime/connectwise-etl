"""ConnectWise processor implementations."""

import time
from pathlib import Path

import pyspark.sql.functions as F

from etl_core.config import RuntimeContext
from etl_core.domain.protocols import (
    BronzeResult,
    DataFetcherProtocol,
    DataValidatorProtocol,
    GoldResult,
    ModelRegistryProtocol,
    SilverResult,
)
from etl_core.utils.errors import BronzeProcessingError, GoldProcessingError, SilverProcessingError
from etl_core.utils.incremental import IncrementalHandler
from etl_core.utils.logging import get_logger, update_log_context

logger = get_logger(__name__)


class ConnectWiseBronzeProcessor:
    """Bronze layer processor for ConnectWise."""

    def __init__(
        self,
        fetcher: DataFetcherProtocol,
        validator: DataValidatorProtocol,
        registry: ModelRegistryProtocol,
    ):
        self.fetcher = fetcher
        self.validator = validator
        self.registry = registry

    def process(self, context: RuntimeContext) -> BronzeResult:
        """
        Execute bronze layer extraction and validation.

        Args:
            context: Runtime context.

        Returns:
            BronzeResult with metrics.
        """
        start_time = time.time()
        entities_processed: dict[str, int] = {}
        tables_written: list[str] = []
        total_validation_errors = 0

        update_log_context("layer", "bronze")
        update_log_context("integration", "connectwise")

        # Get entities to process (all if not specified)
        entities = context.entities or self.registry.list_entities()

        for entity_name in entities:
            try:
                update_log_context("entity", entity_name)
                logger.info(f"Processing bronze for {entity_name}")

                # Get model and config
                model_class = self.registry.get_model(entity_name)
                entity_config = self.registry.get_entity_config(entity_name)

                # Fetch raw data
                raw_data = self.fetcher.fetch_raw(
                    entity_name=entity_name,
                    mode=context.mode,
                    config=entity_config,
                    **context.extra_args,
                )

                # Validate and create DataFrame
                validation_result = self.validator.validate_and_create_dataframe(
                    raw_data=raw_data,
                    model_class=model_class,
                    spark=context.spark,
                    entity_name=entity_name,
                )

                # Add ETL metadata
                bronze_df = validation_result.dataframe
                bronze_df = bronze_df.withColumn("etl_timestamp", F.current_timestamp())
                bronze_df = bronze_df.withColumn("etl_entity", F.lit(entity_name))
                bronze_df = bronze_df.withColumn("etl_batch_id", F.lit(context.batch_id))

                # Write to bronze table
                table_name = context.table_name("bronze", entity_name)
                bronze_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                    table_name
                )

                entities_processed[entity_name] = validation_result.valid_count
                tables_written.append(table_name)
                total_validation_errors += validation_result.invalid_count

                logger.info(
                    f"Bronze complete for {entity_name}",
                    valid=validation_result.valid_count,
                    invalid=validation_result.invalid_count,
                )

            except Exception as e:
                logger.error(f"Bronze processing failed for {entity_name}: {e}")
                raise BronzeProcessingError(
                    f"Failed to process bronze for {entity_name}",
                    details={
                        "source": "ConnectWiseBronzeProcessor",
                        "operation": "process",
                        "entity_name": entity_name,
                        "layer": "bronze",
                        "batch_id": context.batch_id,
                    },
                ) from e

        duration = time.time() - start_time
        return BronzeResult(
            entities_processed=entities_processed,
            tables_written=tables_written,
            validation_errors=total_validation_errors,
            duration_seconds=duration,
            batch_id=context.batch_id,
        )


class ConnectWiseSilverProcessor:
    """Silver layer processor for ConnectWise."""

    def __init__(
        self,
        registry: ModelRegistryProtocol,
        incremental_handler: IncrementalHandler,
    ):
        self.registry = registry
        self.incremental = incremental_handler

    def process(self, context: RuntimeContext) -> SilverResult:
        """
        Execute silver layer transformations.

        Args:
            context: Runtime context.

        Returns:
            SilverResult with metrics.
        """
        start_time = time.time()
        entities_processed: dict[str, int] = {}
        tables_written: list[str] = []
        total_merged = 0
        total_inserted = 0
        total_updated = 0

        update_log_context("layer", "silver")

        entities = context.entities or self.registry.list_entities()

        for entity_name in entities:
            try:
                update_log_context("entity", entity_name)
                logger.info(f"Processing silver for {entity_name}")

                # Get model for validation
                model_class = self.registry.get_model(entity_name)

                # Read from bronze
                bronze_table = context.table_name("bronze", entity_name)
                bronze_df = context.spark.table(bronze_table)

                # Flatten nested struct columns completely (no arbitrary depth limit)
                from etl_core.utils.transforms import flatten_dataframe

                silver_df = flatten_dataframe(bronze_df, model_class=model_class)

                # Add ETL metadata after flattening
                silver_df = silver_df.withColumn("_etl_processed_at", F.current_timestamp())
                silver_df = silver_df.withColumn("_etl_source", F.lit("connectwise"))
                silver_df = silver_df.withColumn("_etl_batch_id", F.lit(context.batch_id))

                # Write to silver
                silver_table = context.table_name("silver", entity_name)
                silver_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                    silver_table
                )

                record_count = silver_df.count()
                entities_processed[entity_name] = record_count
                tables_written.append(silver_table)
                total_merged += record_count

                logger.info(f"Silver complete for {entity_name}", records=record_count)

            except Exception as e:
                logger.error(f"Silver processing failed for {entity_name}: {e}")
                raise SilverProcessingError(
                    f"Failed to process silver for {entity_name}",
                    details={
                        "source": "ConnectWiseSilverProcessor",
                        "operation": "process",
                        "entity_name": entity_name,
                        "layer": "silver",
                    },
                ) from e

        duration = time.time() - start_time
        return SilverResult(
            entities_processed=entities_processed,
            tables_written=tables_written,
            records_merged=total_merged,
            records_inserted=total_inserted,
            records_updated=total_updated,
            duration_seconds=duration,
            batch_id=context.batch_id,
        )


class ConnectWiseGoldProcessor:
    """Gold layer processor for ConnectWise - dimensional structure only."""

    def __init__(self, registry: ModelRegistryProtocol):
        self.registry = registry

    def process(
        self,
        context: RuntimeContext,
        dimension_schema_path: Path | None = None,
        fact_schema_path: Path | None = None,
    ) -> GoldResult:
        """
        Execute gold layer dimensional modeling.

        Creates dimensional structure (dims + facts with FK relationships).
        Business logic lives in Data Warehouse SQL views, not here.

        Args:
            context: Runtime context.
            dimension_schema_path: Path to YAML dimension definitions.
            fact_schema_path: Path to YAML fact definitions (optional).

        Returns:
            GoldResult with metrics.
        """
        start_time = time.time()
        update_log_context("layer", "gold")

        dimensions_created = []
        facts_created = []
        total_dimension_records = 0
        total_fact_records = 0

        try:
            logger.info("Processing gold layer - dimensional structure only")

            # Set default schema path if not provided
            if dimension_schema_path is None:
                dimension_schema_path = Path(__file__).parent.parent / "schemas"

            # Create all dimensions from YAML (dedup + surrogate keys)
            from ..yaml_dimensions import create_all_dimensions_yaml

            dimensions = create_all_dimensions_yaml(
                spark=context.spark,
                schema_dir=dimension_schema_path,
            )

            for dim_name, dim_df in dimensions.items():
                dimensions_created.append(dim_name)
                total_dimension_records += dim_df.count()

            logger.info(
                f"Created {len(dimensions_created)} dimensions",
                dimensions=dimensions_created,
                total_records=total_dimension_records,
            )

            # Load silver tables for fact creation
            entities = context.entities or self.registry.list_entities()
            available_tables = {}
            for entity_name in entities:
                silver_table = context.table_name("silver", entity_name)
                try:
                    available_tables[entity_name] = context.spark.table(silver_table)
                except Exception:
                    logger.warning(f"Silver table {silver_table} not found, skipping")

            # Create simple facts (silver + dimension FK joins)
            self._create_simple_facts(context, available_tables, facts_created)

            # Create invoice lines fact (fusion dance of time/product/invoice)
            if all(k in available_tables for k in ["invoice", "timeentry"]):
                invoice_fact_count = self._create_invoice_lines_fact(
                    context, available_tables
                )
                facts_created.append(context.table_name("gold", "fact_invoiceline"))
                total_fact_records += invoice_fact_count

            logger.info(
                "Gold processing complete",
                dimensions=len(dimensions_created),
                facts=len(facts_created),
                dimension_records=total_dimension_records,
                fact_records=total_fact_records,
            )

        except Exception as e:
            logger.error(f"Gold processing failed: {e}")
            raise GoldProcessingError(
                "Failed to process gold layer",
                details={
                    "source": "ConnectWiseGoldProcessor",
                    "operation": "process",
                    "layer": "gold",
                },
            ) from e

        duration = time.time() - start_time
        return GoldResult(
            dimensions_created=dimensions_created,
            facts_created=facts_created,
            total_dimension_records=total_dimension_records,
            total_fact_records=total_fact_records,
            duration_seconds=duration,
            batch_id=context.batch_id,
        )

    def _create_simple_facts(self, context: RuntimeContext, available_tables: dict, facts_created: list) -> None:
        """Create simple facts that are just silver + dimension FK joins."""
        # Simple facts that don't need structural transformation
        simple_fact_entities = ["timeentry", "expenseentry", "agreement"]

        for entity_name in simple_fact_entities:
            if entity_name not in available_tables:
                continue

            silver_df = available_tables[entity_name]

            # Add surrogate key
            fact_df = silver_df.withColumn(
                f"{entity_name}SK",
                F.sha2(F.col("id").cast("string"), 256)
            )

            # TODO: Add dimension FK joins from YAML mappings

            # Write fact table
            fact_table = context.table_name("gold", f"fact_{entity_name}")
            fact_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                fact_table
            )

            facts_created.append(fact_table)
            logger.info(f"Created simple fact {fact_table} with {fact_df.count()} records")

    def _create_invoice_lines_fact(
        self, context: RuntimeContext, available_tables: dict
    ) -> int:
        """
        Create invoice lines fact via fusion dance.

        Structural transformation: time entries + product items → unified invoice lines.
        NO calculated columns - that's Data Warehouse layer work.
        """
        logger.info("Creating invoice lines fact via fusion dance")

        # Time entries as invoice lines
        time_df = available_tables["timeentry"]
        time_lines = time_df.filter(F.col("invoiceId").isNotNull()).select(
            F.col("invoiceId").cast("int"),
            F.monotonically_increasing_id().alias("lineNumber"),
            F.col("id").alias("timeEntryId"),
            F.lit(None).cast("int").alias("productId"),
            F.col("notes").alias("description"),
            F.col("actualHours").alias("quantity"),
            F.col("hourlyRate").alias("price"),
            F.coalesce("hourlyCost", F.lit(0)).alias("cost"),
            F.col("agreementId"),
            F.col("memberId"),
            F.lit("TIME").alias("lineType"),
        )

        # Product items as invoice lines
        product_lines = None
        if "productitem" in available_tables:
            product_df = available_tables["productitem"]
            product_lines = product_df.filter(F.col("invoiceId").isNotNull()).select(
                F.col("invoiceId").cast("int"),
                F.monotonically_increasing_id().alias("lineNumber"),
                F.lit(None).cast("int").alias("timeEntryId"),
                F.col("id").alias("productId"),
                F.col("description"),
                F.col("quantity"),
                F.col("price"),
                F.coalesce("cost", F.lit(0)).alias("cost"),
                F.col("agreementId"),
                F.lit(None).cast("int").alias("memberId"),
                F.lit("PRODUCT").alias("lineType"),
            )

        # Fusion dance: union the lines
        if product_lines is not None:
            fact_df = time_lines.unionByName(product_lines, allowMissingColumns=True)
        else:
            fact_df = time_lines

        # Add surrogate key
        fact_df = fact_df.withColumn(
            "invoiceLineSK",
            F.sha2(
                F.concat_ws(
                    "|",
                    F.col("invoiceId").cast("string"),
                    F.col("lineNumber").cast("string"),
                ),
                256,
            ),
        )

        # TODO: Add dimension FK joins from invoice header

        # Write fact table
        fact_table = context.table_name("gold", "fact_invoiceline")
        fact_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            fact_table
        )

        fact_count = fact_df.count()
        logger.info(f"Created invoice lines fact with {fact_count} records")
        return fact_count
