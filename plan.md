  PSA Repository Restructuring Plan

  Core Principles and Insights

  1. Spark Session Management: Microsoft Fabric notebooks automatically create and manage
   Spark sessions. Our library should use SparkSession.getActiveSession() first, falling
  back to creation only when needed.
  2. Path Handling: We need a unified solution that works in local environments and
  Microsoft Fabric, handling both local paths and ABFSS URLs consistently.
  3. Generic Extraction: All entities share the same API pattern and can be extracted
  with a single reusable framework that leverages Pydantic models.
  4. Nested Structure Handling: We need consistent flattening utilities for complex
  nested structs and arrays when converting to Spark DataFrames.

  1. Unified Core Infrastructure

  A. Path Handling Module (fabric_api/core/path_utils.py)

  class PathManager:
      def __init__(self, base_path=None, is_fabric=None):
          self.base_path = base_path or self._detect_default_path()
          self.is_fabric = is_fabric if is_fabric is not None else
  self._detect_fabric_environment()

      def _detect_fabric_environment(self):
          """Detect if running in Microsoft Fabric environment"""
          import os
          # Check for Fabric environment indicators
          return bool(os.getenv("FABRIC_STORAGE_ACCOUNT") or
                     "fabric" in
  str(SparkSession.getActiveSession().sparkContext.getConf().get("spark.app.name", "")))

      def get_entity_path(self, entity_name, table_type="delta"):
          """Build path for entity data with environment awareness"""
          normalized_name = f"cw_{entity_name.lower().replace(' ', '_')}"

          if self.is_fabric:
              # Handle ABFSS paths for Fabric
              storage_account = os.getenv("FABRIC_STORAGE_ACCOUNT", "onelake")
              return f"abfss://lakehouse@{storage_account}.dfs.fabric.microsoft.com/Table
  s/{normalized_name}"

          # Local path handling
          return os.path.join(self.base_path, normalized_name)

  B. Spark Session Management (fabric_api/core/spark_utils.py)

  def get_spark_session(app_name="ConnectWise_ETL", configs=None):
      """Get existing Spark session from Fabric notebook or create optimized one"""
      # Try to get active session first (for Fabric notebooks)
      try:
          existing_session = SparkSession.getActiveSession()
          if existing_session:
              logger.info("Using existing Spark session from Fabric notebook")
              return existing_session
      except Exception as e:
          logger.debug(f"Error getting active session: {str(e)}")

      # Create a new session with optimized configs if needed
      logger.info("Creating new Spark session")
      builder = SparkSession.builder.appName(app_name)

      # Apply Fabric-optimized configs
      default_configs = {
          "spark.sql.adaptive.enabled": "true",
          "spark.sql.adaptive.coalescePartitions.enabled": "true",
          "spark.sql.catalog.spark_catalog":
  "org.apache.spark.sql.delta.catalog.DeltaCatalog",
          "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
          "spark.databricks.delta.schema.autoMerge.enabled": "true"
      }

      # Apply all configs
      all_configs = {**default_configs, **(configs or {})}
      for key, value in all_configs.items():
          builder = builder.config(key, value)

      return builder.getOrCreate()

  C. Configuration (fabric_api/core/config.py)

  class EntityConfig:
      """Centralized entity configuration"""
      ENTITIES = {
          "Agreement": {
              "endpoint": "/finance/agreements",
              "model_class": "Agreement",
              "partition_cols": [],
              "description": "ConnectWise agreements"
          },
          "TimeEntry": {
              "endpoint": "/time/entries",
              "model_class": "TimeEntry",
              "partition_cols": [],
              "description": "ConnectWise time entries"
          },
          # Other entities...
      }

      @classmethod
      def get_entity_config(cls, entity_name):
          """Get configuration for an entity"""
          if entity_name not in cls.ENTITIES:
              raise ValueError(f"Unknown entity: {entity_name}")
          return cls.ENTITIES[entity_name]

  2. Generic Extract and Transform Framework

  A. Generic Extractor (fabric_api/extract/generic.py)

  class EntityExtractor:
      """Generic extractor for any ConnectWise entity type"""

      def __init__(self, client, entity_name):
          self.client = client
          self.entity_name = entity_name
          self.config = EntityConfig.get_entity_config(entity_name)
          self.model_class = self._get_model_class()

      def _get_model_class(self):
          """Dynamically import the model class"""
          from fabric_api.connectwise_models import Agreement, TimeEntry, ExpenseEntry,
  ProductItem, PostedInvoice, UnpostedInvoice
          return globals()[self.config["model_class"]]

      def extract(self, conditions=None, page_size=100, max_pages=None):
          """Extract entity data with dynamic field selection"""
          # Get fields based on model structure
          fields = get_fields_for_api_call(self.model_class)

          # Fetch raw data
          endpoint = self.config["endpoint"]
          raw_data = self.client.paginate(
              endpoint=endpoint,
              fields=fields,
              page_size=page_size,
              max_pages=max_pages,
              conditions=conditions
          )

          return raw_data

      def validate(self, raw_data):
          """Validate raw data against model schema"""
          from fabric_api.extract._common import validate_batch
          return validate_batch(raw_data, self.model_class)

  B. Nested Structure Flattening (fabric_api/transform/struct_utils.py)

  def flatten_nested_structs(df, max_depth=3, delimiter="_", handle_arrays="json"):
      """
      Recursively flatten nested struct fields in a DataFrame

      Args:
          df: Input DataFrame with nested structures
          max_depth: Maximum recursion depth
          delimiter: Character to use between field names
          handle_arrays: How to handle arrays ("json", "explode", or "skip")

      Returns:
          Flattened DataFrame
      """
      from pyspark.sql.types import StructType, ArrayType
      from pyspark.sql.functions import col, to_json, from_json, schema_of_json,
  explode_outer

      # Process each field in the schema
      fields = []
      nested_fields = []
      array_fields = []

      for field_name in df.schema.names:
          field_type = df.schema[field_name].dataType

          # Handle regular fields
          if not isinstance(field_type, (StructType, ArrayType)):
              fields.append(field_name)

          # Handle nested structs
          elif isinstance(field_type, StructType):
              nested_fields.append(field_name)

          # Handle arrays
          elif isinstance(field_type, ArrayType):
              array_fields.append(field_name)

      # Start with non-nested fields
      result = df.select([col(f) for f in fields])

      # Process nested structs
      if nested_fields and max_depth > 0:
          for nested_field in nested_fields:
              # Get all the subfields
              nested_schema = df.schema[nested_field].dataType
              for nested_name in nested_schema.names:
                  field_path = f"{nested_field}.{nested_name}"
                  alias = f"{nested_field}{delimiter}{nested_name}"
                  result = result.withColumn(alias, col(field_path))

      # Process arrays based on strategy
      if array_fields:
          for array_field in array_fields:
              if handle_arrays == "json":
                  # Convert arrays to JSON strings
                  result = result.withColumn(array_field, to_json(col(array_field)))
              elif handle_arrays == "explode":
                  # Explode arrays (creates one row per array element)
                  result = result.withColumn(array_field,
  explode_outer(col(array_field)))

      # If we processed nested fields and still have depth, recursively flatten more
      if nested_fields and max_depth > 1:
          result = flatten_nested_structs(
              result,
              max_depth=max_depth-1,
              delimiter=delimiter,
              handle_arrays=handle_arrays
          )

      return result

  3. Delta Storage Module

  A. Unified Delta Writer (fabric_api/storage/delta.py)

  def write_to_delta(
      df,
      entity_name=None,
      path=None,
      mode="append",
      partition_cols=None,
      spark=None,
      flatten_nested=True,
      flatten_depth=3
  ):
      """
      Write DataFrame to Delta with uniform handling for Fabric/non-Fabric environments

      Args:
          df: DataFrame to write
          entity_name: Name of entity (used to derive path if not provided)
          path: Explicit path to write to (overrides entity_name)
          mode: Write mode (append, overwrite)
          partition_cols: Columns to partition by
          spark: SparkSession (obtained from active session if None)
          flatten_nested: Whether to flatten nested structures
          flatten_depth: Depth for flattening

      Returns:
          Tuple of (path, row_count)
      """
      from fabric_api.core.spark_utils import get_spark_session
      from fabric_api.core.path_utils import PathManager
      from fabric_api.transform.struct_utils import flatten_nested_structs

      # Get SparkSession
      spark = spark or get_spark_session()

      # Determine path
      if not path and not entity_name:
          raise ValueError("Either path or entity_name must be provided")

      if not path:
          path_manager = PathManager()
          path = path_manager.get_entity_path(entity_name)

      # Handle empty DataFrame
      if df.isEmpty():
          logger.warning(f"DataFrame is empty, skipping write to {path}")
          return path, 0

      # Flatten nested structures if needed
      if flatten_nested:
          df = flatten_nested_structs(df, max_depth=flatten_depth)

      # Add metadata columns
      from pyspark.sql.functions import current_timestamp, lit
      df = df.withColumn("etl_timestamp", current_timestamp())
      if entity_name:
          df = df.withColumn("etl_entity", lit(entity_name))

      # Configure writer
      writer = df.write.format("delta").mode(mode)

      # Add partitioning if specified
      if partition_cols and len(partition_cols) > 0:
          writer = writer.partitionBy(*partition_cols)

      # Add optimized options
      writer = writer.option("mergeSchema", "true")
      writer = writer.option("delta.autoOptimize.optimizeWrite", "true")
      writer = writer.option("delta.autoOptimize.autoCompact", "true")

      # Write data
      row_count = df.count()
      logger.info(f"Writing {row_count} rows to {path}")
      writer.save(path)

      # Register table if in Fabric
      try:
          table_name = path.split("/")[-1]
          spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {table_name}
          USING DELTA
          LOCATION '{path}'
          """)
          logger.info(f"Registered table {table_name}")
      except Exception as e:
          logger.debug(f"Could not register table: {e}")

      return path, row_count

  4. Streamlined Orchestration

  A. ETL Pipeline (fabric_api/pipeline.py)

  def process_entity(
      entity_name,
      client=None,
      spark=None,
      conditions=None,
      page_size=100,
      max_pages=None,
      mode="append",
      output_path=None
  ):
      """
      Process a single entity through the complete ETL pipeline

      Args:
          entity_name: Name of the entity to process
          client: ConnectWiseClient (created if None)
          spark: SparkSession (uses active session if None)
          conditions: Filter conditions for API
          page_size: Records per page
          max_pages: Maximum pages to fetch
          mode: Write mode for Delta
          output_path: Custom output path (uses default if None)

      Returns:
          Dictionary with processing results
      """
      from fabric_api.core.spark_utils import get_spark_session
      from fabric_api.client import ConnectWiseClient
      from fabric_api.extract.generic import EntityExtractor
      from fabric_api.storage.delta import write_to_delta
      from fabric_api.core.config import EntityConfig

      # Get dependencies
      spark = spark or get_spark_session()
      client = client or ConnectWiseClient()

      # Get entity config
      config = EntityConfig.get_entity_config(entity_name)

      # 1. Extract raw data
      extractor = EntityExtractor(client, entity_name)
      raw_data = extractor.extract(
          conditions=conditions,
          page_size=page_size,
          max_pages=max_pages
      )

      # 2. Validate against model
      valid_models, errors = extractor.validate(raw_data)

      # 3. Create DataFrame
      if valid_models:
          # Create dict data from models
          dict_data = [model.model_dump() for model in valid_models]

          # Create DataFrame
          model_class = extractor.model_class
          schema = model_class.model_spark_schema() if hasattr(model_class,
  "model_spark_schema") else None

          if schema:
              df = spark.createDataFrame(dict_data, schema)
          else:
              df = spark.createDataFrame(dict_data)
      else:
          # Create empty DataFrame with schema if possible
          model_class = extractor.model_class
          if hasattr(model_class, "model_spark_schema"):
              schema = model_class.model_spark_schema()
              df = spark.createDataFrame([], schema)
          else:
              # Truly empty DataFrame as last resort
              df = spark.createDataFrame([])

      # 4. Write to Delta
      path, row_count = write_to_delta(
          df=df,
          entity_name=entity_name,
          path=output_path,
          mode=mode,
          partition_cols=config.get("partition_cols", []),
          spark=spark
      )

      # 5. Write errors if any
      if errors:
          errors_df = spark.createDataFrame(errors)
          write_to_delta(
              df=errors_df,
              entity_name="validation_errors",
              mode="append",
              spark=spark
          )

      # Return results
      return {
          "entity": entity_name,
          "extracted": len(raw_data),
          "validated": len(valid_models),
          "errors": len(errors),
          "rows_written": row_count,
          "path": path
      }

  def run_etl(
      entity_names=None,
      start_date=None,
      end_date=None,
      lakehouse_root=None,
      mode="append",
      max_pages=None
  ):
      """
      Run ETL process for multiple entities

      Args:
          entity_names: List of entities to process (all if None)
          start_date: Start date for filtering (YYYY-MM-DD)
          end_date: End date for filtering (YYYY-MM-DD)
          lakehouse_root: Root path for output
          mode: Write mode
          max_pages: Maximum pages to fetch per entity

      Returns:
          Dictionary with results by entity
      """
      from fabric_api.core.config import EntityConfig
      from fabric_api.client import ConnectWiseClient
      import datetime

      # Set defaults
      if entity_names is None:
          entity_names = list(EntityConfig.ENTITIES.keys())

      # Create client
      client = ConnectWiseClient()

      # Create date conditions if specified
      conditions = None
      if start_date or end_date:
          today = datetime.date.today()
          start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date() if start_date
   else today - datetime.timedelta(days=30)
          end = datetime.datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else
  today

          conditions = f"lastUpdated>=[{start}] AND lastUpdated<[{end}]"

      # Process each entity
      results = {}
      for entity_name in entity_names:
          results[entity_name] = process_entity(
              entity_name=entity_name,
              client=client,
              conditions=conditions,
              max_pages=max_pages,
              mode=mode,
              output_path=lakehouse_root
          )

      return results

  Implementation Plan

  1. Phase 1: Core Infrastructure
    - Create core modules (path_utils, spark_utils, config)
    - Remove redundant code from existing modules
    - Create comprehensive tests for core functionality
  2. Phase 2: Generic Extraction Framework
    - Implement generic extractor
    - Update or replace entity-specific extractors
    - Add field selection utility based on models
    - Test with all entity types
  3. Phase 3: Struct Flattening and Transformation
    - Implement struct flattening utilities
    - Test with complex nested data structures
    - Address schema evolution issues
  4. Phase 4: Unified Storage Layer
    - Create unified Delta writer
    - Implement environment-aware path handling
    - Test in both local and Fabric environments
  5. Phase 5: Streamlined Orchestration
    - Implement simplified pipeline module
    - Add incremental load functionality
    - Create simplified notebook examples
    - End-to-end testing

  By following this implementation plan, we'll achieve:
  - A streamlined, modular architecture with clear responsibilities
  - Reusable utilities for common tasks
  - Proper handling of nested structures
  - Consistent behavior across environments
  - Significantly reduced code redundancy