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

    def test_execute_with_bindings_renders_into_sql(self, cursor, mock_spark):
        """Test that execute renders bindings into the SQL string."""
        mock_spark.sql.return_value = MagicMock()

        cursor.execute("INSERT INTO t VALUES (%s, %s, %s)", (42, "hello", None))

        mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (42, 'hello', NULL)")

    def test_execute_with_bindings_escapes_quotes(self, cursor, mock_spark):
        """Test that string bindings with single quotes are properly escaped."""
        mock_spark.sql.return_value = MagicMock()

        cursor.execute("INSERT INTO t VALUES (%s)", ("O'Brien",))

        mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES ('O''Brien')")

    def test_execute_with_bindings_handles_bool_before_int(self, cursor, mock_spark):
        """Test that booleans render as TRUE/FALSE, not Python's str(True)='True'."""
        mock_spark.sql.return_value = MagicMock()

        cursor.execute("INSERT INTO t VALUES (%s, %s)", (True, False))

        mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (TRUE, FALSE)")

    def test_execute_with_bindings_preserves_literal_percent(self, cursor, mock_spark):
        """Test that literal % in SQL is preserved when bindings are present."""
        mock_spark.sql.return_value = MagicMock()

        cursor.execute("SELECT * FROM t WHERE name LIKE '%abc%' AND id = %s", (42,))

        mock_spark.sql.assert_called_once_with(
            "SELECT * FROM t WHERE name LIKE '%abc%' AND id = 42"
        )

    def test_execute_with_bindings_count_mismatch_raises(self, cursor, mock_spark):
        """Test that mismatched binding count raises an error."""
        from dbt_common.exceptions import DbtRuntimeError

        with pytest.raises(DbtRuntimeError, match="mismatch"):
            cursor.execute("SELECT %s, %s", (42,))

    def test_execute_with_bindings_handles_decimal(self, cursor, mock_spark):
        """Test that Decimal values are converted to float, matching translate_bindings."""
        from decimal import Decimal

        mock_spark.sql.return_value = MagicMock()

        cursor.execute("INSERT INTO t VALUES (%s)", (Decimal("0.73"),))

        mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (0.73)")

    def test_execute_with_bindings_handles_float(self, cursor, mock_spark):
        """Test that float values render as bare numeric literals."""
        mock_spark.sql.return_value = MagicMock()

        cursor.execute("INSERT INTO t VALUES (%s)", (3.14,))

        mock_spark.sql.assert_called_once_with("INSERT INTO t VALUES (3.14)")

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
        assert result == [(0,), (1,)]

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

    def test_fetchmany_returns_empty_when_no_rows(self, cursor, mock_spark):
        """fetchmany returns empty list when no rows exist."""
        mock_df = MagicMock()
        mock_df.collect.return_value = []
        mock_spark.sql.return_value = mock_df

        cursor.execute("SELECT 1")
        result = cursor.fetchmany(5)

        assert result == []

    def test_cancel_sets_open_to_false(self, cursor):
        """cancel() sets open to False."""
        assert cursor.open is True
        cursor.cancel()
        assert cursor.open is False


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

        mock_spark.sql.assert_called_with("SHOW SCHEMAS IN `my_catalog`")

    def test_list_schemas_with_pattern(self, handle, mock_spark):
        """Test that list_schemas includes LIKE pattern."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        handle.list_schemas("my_catalog", "my_schema")

        mock_spark.sql.assert_called_with("SHOW SCHEMAS IN `my_catalog` LIKE 'my\\_schema'")

    def test_list_tables_executes_show_tables(self, handle, mock_spark):
        """Test that list_tables executes SHOW TABLES."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df

        handle.list_tables("my_catalog", "my_schema")

        mock_spark.sql.assert_called_with("SHOW TABLES IN `my_catalog`.`my_schema`")

    def test_create_gets_or_creates_spark_session(self):
        """Test that create uses getOrCreate."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        with patch.dict(
            "sys.modules",
            {"pyspark": MagicMock(), "pyspark.sql": MagicMock()},
        ):
            import sys

            sys.modules["pyspark.sql"].SparkSession.builder = mock_builder

            handle = DatabricksSessionHandle.create()

            mock_builder.getOrCreate.assert_called_once()
            assert handle.session_id == "app-456"

    def test_create_sets_catalog(self):
        """Test that create sets the catalog."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        with patch.dict(
            "sys.modules",
            {"pyspark": MagicMock(), "pyspark.sql": MagicMock()},
        ):
            import sys

            sys.modules["pyspark.sql"].SparkSession.builder = mock_builder

            DatabricksSessionHandle.create(catalog="my_catalog")

            mock_spark.catalog.setCurrentCatalog.assert_called_once_with("my_catalog")

    def test_create_sets_schema(self):
        """Test that create sets the schema."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        with patch.dict(
            "sys.modules",
            {"pyspark": MagicMock(), "pyspark.sql": MagicMock()},
        ):
            import sys

            sys.modules["pyspark.sql"].SparkSession.builder = mock_builder

            DatabricksSessionHandle.create(schema="my_schema")

            mock_spark.catalog.setCurrentDatabase.assert_called_once_with("my_schema")

    def test_create_sets_session_properties(self):
        """Test that create sets session properties."""
        mock_spark = MagicMock()
        mock_spark.sparkContext.applicationId = "app-456"
        mock_builder = MagicMock()
        mock_builder.getOrCreate.return_value = mock_spark

        with patch.dict(
            "sys.modules",
            {"pyspark": MagicMock(), "pyspark.sql": MagicMock()},
        ):
            import sys

            sys.modules["pyspark.sql"].SparkSession.builder = mock_builder

            DatabricksSessionHandle.create(session_properties={"key1": "value1", "key2": 123})

            mock_spark.conf.set.assert_any_call("key1", "value1")
            mock_spark.conf.set.assert_any_call("key2", "123")

    def test_dbr_version_fallback_on_empty_string(self, handle, mock_spark):
        """dbr_version returns (maxsize, maxsize) when version string is empty."""
        import sys

        mock_spark.conf.get.return_value = ""
        handle._dbr_version = None  # reset cached value
        version = handle.dbr_version
        assert version == (sys.maxsize, sys.maxsize)

    def test_dbr_version_fallback_on_exception(self, handle, mock_spark):
        """dbr_version returns (maxsize, maxsize) when conf.get raises."""
        import sys

        mock_spark.conf.get.side_effect = Exception("conf unavailable")
        handle._dbr_version = None  # reset cached value
        version = handle.dbr_version
        assert version == (sys.maxsize, sys.maxsize)

    def test_session_id_fallback_on_exception(self, handle, mock_spark):
        """session_id returns 'session-unknown' when sparkContext raises."""
        type(mock_spark).sparkContext = property(
            lambda self: (_ for _ in ()).throw(Exception("no spark context"))
        )
        result = handle.session_id
        assert result == "session-unknown"

    def test_execute_raises_on_closed_handle(self, handle):
        """execute raises DbtRuntimeError when handle is closed."""
        from dbt_common.exceptions import DbtRuntimeError

        handle.close()
        with pytest.raises(DbtRuntimeError, match="closed session handle"):
            handle.execute("SELECT 1")

    def test_cancel_closes_cursor_and_sets_open_false(self, handle, mock_spark):
        """cancel() closes active cursor and sets open=False."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df
        cursor = handle.execute("SELECT 1")
        assert cursor.open is True

        handle.cancel()

        assert handle.open is False
        assert cursor.open is False

    def test_close_closes_cursor_and_sets_open_false(self, handle, mock_spark):
        """close() closes active cursor and sets open=False."""
        mock_df = MagicMock()
        mock_spark.sql.return_value = mock_df
        cursor = handle.execute("SELECT 1")

        handle.close()

        assert handle.open is False
        assert cursor.open is False

    def test_rollback_is_noop(self, handle):
        """rollback() does not raise."""
        handle.rollback()  # should not raise

    def test_del_does_not_raise(self, handle):
        """__del__ does not raise even with open cursor."""
        mock_df = MagicMock()
        handle._spark.sql.return_value = mock_df
        handle.execute("SELECT 1")
        handle.__del__()  # should not raise


class TestBuildSparkSchema:
    """Tests for DatabricksSessionHandle._build_spark_schema."""

    @pytest.fixture
    def handle(self):
        spark = MagicMock()
        spark.sparkContext.applicationId = "app-test"
        spark.conf.get.return_value = "14.3.x-scala2.12"
        return DatabricksSessionHandle(spark)

    def test_string_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "string"})
        assert len(schema.fields) == 1
        assert schema.fields[0].name == "col1"
        assert schema.fields[0].dataType.simpleString() == "string"

    def test_varchar_maps_to_string(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "varchar"})
        assert schema.fields[0].dataType.simpleString() == "string"

    def test_bigint_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "bigint"})
        assert schema.fields[0].dataType.simpleString() == "bigint"

    def test_long_maps_to_bigint(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "long"})
        assert schema.fields[0].dataType.simpleString() == "bigint"

    def test_int_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "int"})
        assert schema.fields[0].dataType.simpleString() == "int"

    def test_integer_maps_to_int(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "integer"})
        assert schema.fields[0].dataType.simpleString() == "int"

    def test_double_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "double"})
        assert schema.fields[0].dataType.simpleString() == "double"

    def test_float_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "float"})
        assert schema.fields[0].dataType.simpleString() == "float"

    def test_boolean_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "boolean"})
        assert schema.fields[0].dataType.simpleString() == "boolean"

    def test_date_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "date"})
        assert schema.fields[0].dataType.simpleString() == "date"

    def test_timestamp_type(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "timestamp"})
        assert schema.fields[0].dataType.simpleString() == "timestamp"

    def test_decimal_with_precision_scale(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "decimal(10,2)"})
        assert schema.fields[0].dataType.simpleString() == "decimal(10,2)"

    def test_decimal_without_params(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "decimal"})
        assert schema.fields[0].dataType.simpleString() == "decimal(10,0)"

    def test_unknown_type_falls_back_to_string(self, handle):
        schema = handle._build_spark_schema(["col1"], {"col1": "somecustomtype"})
        assert schema.fields[0].dataType.simpleString() == "string"

    def test_multiple_columns(self, handle):
        schema = handle._build_spark_schema(
            ["id", "name", "active"],
            {"id": "bigint", "name": "string", "active": "boolean"},
        )
        assert len(schema.fields) == 3
        assert schema.fields[0].name == "id"
        assert schema.fields[0].dataType.simpleString() == "bigint"
        assert schema.fields[1].name == "name"
        assert schema.fields[1].dataType.simpleString() == "string"
        assert schema.fields[2].name == "active"
        assert schema.fields[2].dataType.simpleString() == "boolean"
