#!/usr/bin/env python
"""
Tests for the Fabric integration functions in bronze_loader.py.
"""

import os
from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from fabric_api.bronze_loader import (
    add_fabric_metadata,
    ensure_fabric_path,
    register_table_metadata,
)


class TestFabricIntegration:
    """Test suite for Fabric integration functions."""

    def setup_method(self):
        """Set up test fixtures before each test method."""
        # Create a mock SparkSession for testing
        self.spark = MagicMock(spec=SparkSession)

        # Create a DataFrame mock without using spec to allow _jdf attribute
        self.df = MagicMock()

        # Mock the SQL context and collect methods
        sql_context = MagicMock()
        sql_result = MagicMock()
        sql_result.collect.return_value = [[MagicMock()]]
        sql_context.sql.return_value = sql_result

        # Set up DataFrame mock with chained attributes
        _jdf = MagicMock()
        sql_context_obj = MagicMock()
        sql_context_obj.sql = sql_context.sql
        _jdf.sqlContext.return_value = sql_context_obj
        self.df._jdf = _jdf

    def test_ensure_fabric_path_with_absolute_path(self):
        """Test ensure_fabric_path with an absolute path."""
        # When an absolute path is provided
        path = "/some/absolute/path"
        result = ensure_fabric_path(path)

        # Then the path should be returned unchanged
        assert result == path

    def test_ensure_fabric_path_with_relative_path(self):
        """Test ensure_fabric_path with a relative path."""
        # When a relative path is provided
        path = "my_table"
        result = ensure_fabric_path(path)

        # Then it should be converted to a Lakehouse path
        assert result == "/lakehouse/default/Tables/my_table"

    def test_ensure_fabric_path_with_lakehouse_path(self):
        """Test ensure_fabric_path with a Lakehouse path."""
        # When a Lakehouse path is provided
        path = "/lakehouse/default/Tables/my_table"

        # And no Fabric storage account is set
        with patch.dict(os.environ, {}, clear=True):
            result = ensure_fabric_path(path)

            # Then the path should be returned unchanged
            assert result == path

        # When a Fabric storage account is set
        with patch.dict(os.environ, {"FABRIC_STORAGE_ACCOUNT": "teststorage"}, clear=True):
            result = ensure_fabric_path(path)

            # Then it should be converted to an ABFSS URL
            assert (
                result
                == "abfss://lakehouse@teststorage.dfs.fabric.microsoft.com/lakehouse/default/Tables/my_table"
            )

    def test_ensure_fabric_path_with_abfss_path(self):
        """Test ensure_fabric_path with an ABFSS path."""
        # When an ABFSS path is provided
        path = "abfss://lakehouse@storage.dfs.fabric.microsoft.com/path"
        result = ensure_fabric_path(path)

        # Then the path should be returned unchanged
        assert result == path

    @patch("fabric_api.bronze_loader.datetime")
    def test_add_fabric_metadata(self, mock_datetime):
        """Test add_fabric_metadata adds the correct columns."""
        # Given a DataFrame with column tracking
        df = MagicMock()
        column_names = []

        # Setup a side effect to track column names
        def track_column(name, value):
            column_names.append(name)
            return df

        df.withColumn.side_effect = track_column

        # When we add Fabric metadata
        result = add_fabric_metadata(df, "TestEntity", "Test description")

        # Then the DataFrame should have all metadata columns added
        assert "etl_entity_name" in column_names
        assert "etl_entity_type" in column_names
        assert "etl_timestamp" in column_names
        assert "etl_version" in column_names

        # And the result should be the modified DataFrame
        assert result == df

    @patch("fabric_api.bronze_loader.log")
    def test_register_table_metadata_in_fabric(self, mock_log):
        """Test register_table_metadata when running in Fabric."""
        # Given a SparkSession that appears to be running in Fabric
        spark = MagicMock()
        conf = MagicMock()
        conf.get.return_value = "fabric_test"
        spark.sparkContext.getConf.return_value = conf

        # When we register table metadata
        table_path = "/lakehouse/default/Tables/test_table"
        register_table_metadata(spark, table_path, "TestEntity", "Test description")

        # Then SQL should be executed (checking for at least one call)
        assert spark.sql.called

        # Verify the CREATE TABLE statement was executed
        create_table_called = False
        for call in spark.sql.call_args_list:
            sql_statement = call[0][0]
            if "CREATE TABLE IF NOT EXISTS" in sql_statement:
                create_table_called = True
                assert "test_table" in sql_statement
                assert "Test description" in sql_statement

        assert create_table_called, "CREATE TABLE statement was not executed"

        # And a success message should be logged
        mock_log.info.assert_called_with("Registered table test_table in Fabric Lakehouse")

    @patch("fabric_api.bronze_loader.log")
    def test_register_table_metadata_not_in_fabric(self, mock_log):
        """Test register_table_metadata when not running in Fabric."""
        # Given a SparkSession that doesn't appear to be running in Fabric
        spark = self.spark
        conf = MagicMock()
        conf.get.return_value = "local_test"
        spark.sparkContext.getConf.return_value = conf

        # When we register table metadata
        table_path = "/some/local/path/test_table"
        register_table_metadata(spark, table_path, "TestEntity", "Test description")

        # Then no SQL should be executed
        spark.sql.assert_not_called()

        # And a debug message should be logged
        mock_log.debug.assert_called_with(
            "Not running in Fabric, skipping table metadata registration"
        )
