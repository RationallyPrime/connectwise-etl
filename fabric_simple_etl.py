# Simple Business Central ETL Pipeline for Fabric
# Cell 1: Install packages
%pip install unified_etl_core-1.0.0-py3-none-any.whl
%pip install unified_etl_businesscentral-1.0.0-py3-none-any.whl

# Cell 2: Configuration
from datetime import datetime
from unified_etl_core.config.models import ETLConfig, LayerConfig, IntegrationConfig, SparkConfig, TableNamingConvention
from unified_etl_core.main import run_etl_pipeline
from pyspark.sql import SparkSession

# Simple config
batch_id = f"bc_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
spark = SparkSession.builder.appName("BC_ETL").getOrCreate()

config = ETLConfig(
    bronze=LayerConfig(catalog="LH", schema="bronze", prefix="", naming_convention=TableNamingConvention.CAMELCASE),
    silver=LayerConfig(catalog="LH", schema="silver", prefix="", naming_convention=TableNamingConvention.CAMELCASE),
    gold=LayerConfig(catalog="LH", schema="gold", prefix="", naming_convention=TableNamingConvention.CAMELCASE),
    integrations={"businesscentral": IntegrationConfig(name="businesscentral", abbreviation="bc", base_url="https://api.businesscentral.dynamics.com/", enabled=True)},
    spark=SparkConfig(app_name="bc_etl", session_type="fabric", config_overrides={}),
    fail_on_error=True,
    audit_columns=True
)

# Cell 3: Run ETL Pipeline
result = run_etl_pipeline(
    config=config,
    spark=spark,
    integrations=["businesscentral"],
    layers=["gold"],  # Only gold since bronze/silver already exist
    mode="full",
    batch_id=batch_id,
    lookback_days=0
)

print(f"✅ ETL Pipeline Complete: {result}")