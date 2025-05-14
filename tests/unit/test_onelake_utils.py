#!/usr/bin/env python
"""
Test the OneLake utilities for Microsoft Fabric integration.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from fabric_api.onelake_utils import (
    build_abfss_path,
    create_database_if_not_exists,
    direct_etl_to_onelake,
    get_partition_columns,
    get_table_name,
    get_table_path,
    prepare_dataframe_for_onelake,
    write_to_onelake,
)


class TestOneLakeUtils(unittest.TestCase):
    """Tests for OneLake utilities."""

    def setUp(self):
        """Set up test environment."""
        # Set up environment variables
        os.environ["FABRIC_STORAGE_ACCOUNT"] = "teststorageaccount"
        os.environ["FABRIC_TENANT_ID"] = "testtenantid"

        # Mock SparkSession
        self.spark = MagicMock()

    def test_build_abfss_path(self):
        """Test building ABFSS paths."""
        # Test with relative path
        relative_path = "/Tables/connectwise/agreement"
        abfss_path = build_abfss_path(relative_path)
        expected = "abfss://lakehouse@teststorageaccount.dfs.fabric.microsoft.com/Tables/connectwise/agreement"
        self.assertEqual(abfss_path, expected)

        # Test with path that already has abfss://
        already_abfss = "abfss://lakehouse@storage.dfs.fabric.microsoft.com/path"
        self.assertEqual(build_abfss_path(already_abfss), already_abfss)

    def test_get_table_path(self):
        """Test getting table paths."""
        # Test with default table type (delta)
        path = get_table_path("Agreement")
        self.assertTrue("/Tables/connectwise/agreement" in path)

        # Test with parquet table type
        parquet_path = get_table_path("TimeEntry", table_type="parquet")
        self.assertTrue("/Files/connectwise/time_entry" in parquet_path)

    def test_get_table_name(self):
        """Test getting table names."""
        # Test with CamelCase entity name
        name = get_table_name("Agreement")
        self.assertEqual(name, "connectwise.agreement")

        # Test with snake_case entity name
        name2 = get_table_name("time_entry")
        self.assertEqual(name2, "connectwise.time_entry")

    def test_get_partition_columns(self):
        """Test getting partition columns."""
        # Test agreement partitioning
        agreement_partitions = get_partition_columns("agreement")
        self.assertIn("agreementType", agreement_partitions)

        # Test invoice partitioning
        invoice_partitions = get_partition_columns("posted_invoice")
        self.assertIn("date", invoice_partitions)

    @patch('fabric_api.onelake_utils.to_date')
    @patch('fabric_api.onelake_utils.date_format')
    @patch('fabric_api.onelake_utils.col')
    def test_prepare_dataframe_for_onelake(self, mock_col, mock_date_format, mock_to_date):
        """Test preparing DataFrame for OneLake."""
        # Create mock DataFrame
        df = MagicMock()
        df.isEmpty.return_value = False
        df.columns = ["date", "id", "name"]
        df.withColumn.return_value = df  # Return self for chaining

        # Mock the column function
        mock_col.return_value = "date_column"
        mock_to_date.return_value = "date_column_to_date"
        mock_date_format.return_value = "date_column_formatted"

        # Test with date columns
        prepare_dataframe_for_onelake(df, "posted_invoice")

        # Verify withColumn was called with timestamp
        self.assertTrue(df.withColumn.called)

    @patch('fabric_api.onelake_utils.prepare_dataframe_for_onelake')
    def test_write_to_onelake(self, mock_prepare):
        """Test writing to OneLake."""
        # Create mock DataFrame and SparkSession
        df = MagicMock()
        df.isEmpty.return_value = False
        df.cache.return_value = df
        df.count.return_value = 10
        df.sparkSession = self.spark

        # Mock prepare_dataframe_for_onelake
        mock_prepare.return_value = df

        # Mock SQL execution
        self.spark.sql.return_value = MagicMock()

        # Test write_to_onelake with SQL table creation
        table_path, table_name, row_count = write_to_onelake(
            df=df,
            entity_name="Agreement",
            spark=self.spark,
            mode="append",
            create_table=True
        )

        # Verify SQL was executed for table creation
        self.assertTrue(self.spark.sql.called)
        self.assertEqual(row_count, 10)

    @patch('fabric_api.onelake_utils.write_to_onelake')
    def test_direct_etl_to_onelake(self, mock_write):
        """Test direct ETL to OneLake."""
        # Mock data and model class
        client_data = [
            {"id": 1, "name": "Agreement 1", "type": "Type A"},
            {"id": 2, "name": "Agreement 2", "type": "Type B"},
            {"id": 3, "name": "Invalid", "type": None}  # This will fail validation
        ]

        # Mock model validation and dumping
        model_class = MagicMock()
        model_instance = MagicMock()
        model_class.model_validate.side_effect = [
            model_instance,  # First record valid
            model_instance,  # Second record valid
            Exception("Invalid type")  # Third record invalid
        ]
        model_instance.model_dump.return_value = {"id": 1, "name": "Agreement", "type": "Type"}
        model_class.model_spark_schema.return_value = "mock_schema"

        # Mock DataFrame creation
        self.spark.createDataFrame.return_value = MagicMock()

        # Mock write_to_onelake
        mock_write.return_value = ("/path", "table", 2)

        # Test direct ETL
        result = direct_etl_to_onelake(
            client_data=client_data,
            entity_name="Agreement",
            spark=self.spark,
            model_class=model_class,
            mode="append"
        )

        # Verify results
        self.assertEqual(result[1], 2)  # 2 rows written
        self.assertEqual(result[2], 1)  # 1 error

    def test_create_database_if_not_exists(self):
        """Test database creation."""
        # Mock SQL execution and result
        mock_row = MagicMock()
        mock_row.databaseName = "other_db"
        self.spark.sql.return_value.collect.return_value = [mock_row]

        # Test database creation when it doesn't exist
        create_database_if_not_exists(self.spark)

        # Verify SQL was executed to create database
        self.spark.sql.assert_any_call("SHOW DATABASES")
        self.spark.sql.assert_any_call("CREATE DATABASE IF NOT EXISTS connectwise")


if __name__ == "__main__":
    unittest.main()
