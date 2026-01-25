"""Unit tests for session mode components."""

from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.databricks.session import (
    DatabricksSessionHandle,
    SessionCursorWrapper,
)


class TestSessionCursorWrapper:
    """Tests for SessionCursorWrapper."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = MagicMock()
        return spark

    @pytest.fixture
    def cursor(self, mock_spark):
        """Create a SessionCursorWrapper with mock SparkSession."""
        return SessionCursorWrapper(mock_spark)

    def test_execute_cleans_sql(self, cursor, mock_spark):
        """Test that execute cleans SQL (strips whitespace and trailing semicolon)."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        result = cursor.execute("  SELECT 1;  ")

        mock_spark.sql.assert_called_once_with("SELECT 1")
        assert result is cursor

    def test_execute_with_bindings(self, cursor, mock_spark):
        """Test that execute handles bindings with proper SQL literal formatting."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT %s, %s", (1, "test"))

        # String values should be properly quoted
        mock_spark.sql.assert_called_once_with("SELECT 1, 'test'")

    def test_fetchall_returns_tuples(self, cursor, mock_spark):
        """Test that fetchall returns list of tuples."""
        mock_df = MagicMock()
        mock_row1 = MagicMock()
        mock_row1.__iter__ = lambda self: iter([1, "a"])
        mock_row2 = MagicMock()
        mock_row2.__iter__ = lambda self: iter([2, "b"])
        mock_df.collect.return_value = [mock_row1, mock_row2]
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT * FROM test")
        result = cursor.fetchall()

        assert result == [(1, "a"), (2, "b")]

    def test_fetchone_returns_single_tuple(self, cursor, mock_spark):
        """Test that fetchone returns a single tuple."""
        mock_df = MagicMock()
        mock_row = MagicMock()
        mock_row.__iter__ = lambda self: iter([1, "a"])
        mock_df.collect.return_value = [mock_row]
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT * FROM test")
        result = cursor.fetchone()

        assert result == (1, "a")

    def test_fetchone_returns_none_when_empty(self, cursor, mock_spark):
        """Test that fetchone returns None when no rows."""
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT * FROM test")
        result = cursor.fetchone()

        assert result is None

    def test_fetchmany_returns_limited_rows(self, cursor, mock_spark):
        """Test that fetchmany returns limited number of rows."""
        mock_df = MagicMock()
        mock_rows = [MagicMock() for _ in range(5)]
        for i, row in enumerate(mock_rows):
            row.__iter__ = (lambda i: lambda self: iter([i]))(i)
        mock_df.collect.return_value = mock_rows
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT * FROM test")
        result = cursor.fetchmany(2)

        assert len(result) == 2
        assert result == [(0, ), (1, )]

    def test_description_returns_column_info(self, cursor, mock_spark):
        """Test that description returns column metadata."""
        mock_df = MagicMock()
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field1.dataType.simpleString.return_value = "int"
        mock_field1.nullable = False

        mock_field2 = MagicMock()
        mock_field2.name = "name"
        mock_field2.dataType.simpleString.return_value = "string"
        mock_field2.nullable = True

        mock_df.schema.fields = [mock_field1, mock_field2]
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT * FROM test")
        desc = cursor.description

        assert len(desc) == 2
        assert desc[0][0] == "id"
        assert desc[0][1] == "int"
        assert desc[0][6] is False
        assert desc[1][0] == "name"
        assert desc[1][1] == "string"
        assert desc[1][6] is True

    def test_description_returns_none_before_execute(self, cursor):
        """Test that description returns None before execute."""
        assert cursor.description is None

    def test_get_response_returns_adapter_response(self, cursor, mock_spark):
        """Test that get_response returns an AdapterResponse."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT 1")
        response = cursor.get_response()

        assert response._message == "OK"
        assert response.query_id == "session-query"

    def test_close_sets_open_to_false(self, cursor):
        """Test that close sets open to False."""
        assert cursor.open is True
        cursor.close()
        assert cursor.open is False

    def test_context_manager(self, mock_spark):
        """Test that cursor works as context manager."""
        with SessionCursorWrapper(mock_spark) as cursor:
            assert cursor.open is True
        assert cursor.open is False

    def test_execute_with_string_special_characters(self, cursor, mock_spark):
        """Test that execute properly quotes strings with special characters."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("INSERT INTO test VALUES (%s, %s)",
                       ("k. A.", "O'Brien"))

        # Special characters should be properly quoted and escaped
        mock_spark.sql.assert_called_once_with(
            "INSERT INTO test VALUES ('k. A.', 'O''Brien')")

    def test_execute_with_null_value(self, cursor, mock_spark):
        """Test that execute handles NULL values correctly."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("INSERT INTO test VALUES (%s, %s)", (1, None))

        # NULL should be formatted as SQL NULL
        mock_spark.sql.assert_called_once_with(
            "INSERT INTO test VALUES (1, NULL)")

    def test_execute_with_boolean_values(self, cursor, mock_spark):
        """Test that execute handles boolean values correctly."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("INSERT INTO test VALUES (%s, %s)", (True, False))

        # Booleans should be formatted as TRUE/FALSE
        mock_spark.sql.assert_called_once_with(
            "INSERT INTO test VALUES (TRUE, FALSE)")

    def test_execute_with_numeric_values(self, cursor, mock_spark):
        """Test that execute handles numeric values correctly."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("INSERT INTO test VALUES (%s, %s, %s)", (42, 3.14, 100))

        # Numbers should remain unquoted
        mock_spark.sql.assert_called_once_with(
            "INSERT INTO test VALUES (42, 3.14, 100)")

    def test_execute_with_comma_in_string(self, cursor, mock_spark):
        """Test that execute properly handles strings containing commas."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("INSERT INTO test VALUES (%s)",
                       ("Freie und Hansestadt Hamburg", ))

        # String with spaces and special characters should be properly quoted
        mock_spark.sql.assert_called_once_with(
            "INSERT INTO test VALUES ('Freie und Hansestadt Hamburg')")

    def test_execute_with_parentheses_in_string(self, cursor, mock_spark):
        """Test that execute properly handles strings containing parentheses."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor.execute("INSERT INTO test VALUES (%s)",
                       ("Haus in Planung (projektiert)", ))

        # String with parentheses should be properly quoted
        mock_spark.sql.assert_called_once_with(
            "INSERT INTO test VALUES ('Haus in Planung (projektiert)')")


class TestDatabricksSessionHandle:
    """Tests for DatabricksSessionHandle."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock SparkSession."""
        spark = MagicMock()
        spark.sparkContext.applicationId = "app-123"
        spark.conf.get.return_value = "14.3.x-scala2.12"
        return spark

    @pytest.fixture
    def handle(self, mock_spark):
        """Create a DatabricksSessionHandle with mock SparkSession."""
        return DatabricksSessionHandle(mock_spark)

    def test_session_id_returns_application_id(self, handle):
        """Test that session_id returns the Spark application ID."""
        assert handle.session_id == "app-123"

    def test_dbr_version_extracts_version(self, handle, mock_spark):
        """Test that dbr_version extracts version from Spark config."""
        mock_spark.conf.get.return_value = "14.3.x-scala2.12"

        version = handle.dbr_version

        assert version == (14, 3)

    def test_dbr_version_caches_result(self, handle, mock_spark):
        """Test that dbr_version caches the result."""
        mock_spark.conf.get.return_value = "14.3.x-scala2.12"

        _ = handle.dbr_version
        _ = handle.dbr_version

        # Should only call conf.get once due to caching
        assert mock_spark.conf.get.call_count == 1

    def test_execute_returns_cursor(self, handle, mock_spark):
        """Test that execute returns a SessionCursorWrapper."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor = handle.execute("SELECT 1")

        assert isinstance(cursor, SessionCursorWrapper)

    def test_execute_closes_previous_cursor(self, handle, mock_spark):
        """Test that execute closes any previous cursor."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        cursor1 = handle.execute("SELECT 1")
        assert cursor1.open is True

        cursor2 = handle.execute("SELECT 2")
        assert cursor1.open is False
        assert cursor2.open is True

    def test_close_sets_open_to_false(self, handle):
        """Test that close sets open to False."""
        assert handle.open is True
        handle.close()
        assert handle.open is False

    def test_list_schemas_executes_show_schemas(self, handle, mock_spark):
        """Test that list_schemas executes SHOW SCHEMAS."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        handle.list_schemas("my_catalog")

        mock_spark.sql.assert_called_with("SHOW SCHEMAS IN my_catalog")

    def test_list_schemas_with_pattern(self, handle, mock_spark):
        """Test that list_schemas includes LIKE pattern."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        handle.list_schemas("my_catalog", "my_schema")

        mock_spark.sql.assert_called_with(
            "SHOW SCHEMAS IN my_catalog LIKE 'my_schema'")

    def test_list_tables_executes_show_tables(self, handle, mock_spark):
        """Test that list_tables executes SHOW TABLES."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        handle.list_tables("my_catalog", "my_schema")

        mock_spark.sql.assert_called_with(
            "SHOW TABLES IN my_catalog.my_schema")

    def test_create_gets_existing_spark_session(self):
        """Test that create finds an existing SparkSession via SparkContext."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_sc = MagicMock()

        with patch.dict(
                "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock()
            },
        ):
            import sys

            # Mock SparkContext._active_spark_context to return a context
            sys.modules["pyspark"].SparkContext._active_spark_context = mock_sc
            # Mock SparkSession constructor to return our mock session
            sys.modules["pyspark.sql"].SparkSession.return_value = mock_spark

            handle = DatabricksSessionHandle.create()

            # Should have created SparkSession from SparkContext
            sys.modules["pyspark.sql"].SparkSession.assert_called_once_with(
                mock_sc)
            assert handle.session_id == "app-456"

    def test_create_sets_catalog(self):
        """Test that create sets the catalog."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_sc = MagicMock()

        with patch.dict(
                "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock()
            },
        ):
            import sys

            sys.modules["pyspark"].SparkContext._active_spark_context = mock_sc
            sys.modules["pyspark.sql"].SparkSession.return_value = mock_spark

            DatabricksSessionHandle.create(catalog="my_catalog")

            mock_spark.catalog.setCurrentCatalog.assert_called_once_with(
                "my_catalog")

    def test_create_does_not_set_schema(self):
        """Test that create does NOT set the schema/database.

        dbt uses the schema from the profile as a base/prefix for generated schema names
        (e.g., "dbt" -> "dbt_seeds", "dbt_bronze" via generate_schema_name macro).
        dbt creates these schemas during execution via CREATE SCHEMA IF NOT EXISTS,
        and uses fully qualified names (catalog.schema.table) for all operations.
        Setting the current database would fail if the schema doesn't exist yet.
        """
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_sc = MagicMock()

        with patch.dict(
                "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock()
            },
        ):
            import sys

            sys.modules["pyspark"].SparkContext._active_spark_context = mock_sc
            sys.modules["pyspark.sql"].SparkSession.return_value = mock_spark

            DatabricksSessionHandle.create(schema="my_schema")

            # Schema should NOT be set - this is the fix for SCHEMA_NOT_FOUND error
            mock_spark.catalog.setCurrentDatabase.assert_not_called()

    def test_create_sets_session_properties(self):
        """Test that create sets session properties."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_sc = MagicMock()

        with patch.dict(
                "sys.modules",
            {
                "pyspark": MagicMock(),
                "pyspark.sql": MagicMock()
            },
        ):
            import sys

            sys.modules["pyspark"].SparkContext._active_spark_context = mock_sc
            sys.modules["pyspark.sql"].SparkSession.return_value = mock_spark

            DatabricksSessionHandle.create(session_properties={
                "key1": "value1",
                "key2": 123
            })

            mock_spark.conf.set.assert_any_call("key1", "value1")
            mock_spark.conf.set.assert_any_call("key2", "123")
