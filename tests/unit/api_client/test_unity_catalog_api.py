from unittest.mock import Mock

import pytest
from databricks.sdk.service.catalog import ColumnInfo, TableInfo, TableType

from dbt.adapters.databricks.api_client import UnityCatalogApi


class TestUnityCatalogApi:
    @pytest.fixture
    def mock_workspace_client(self):
        mock_client = Mock()
        mock_client.tables = Mock()
        return mock_client

    @pytest.fixture
    def unity_catalog_api(self, mock_workspace_client):
        return UnityCatalogApi(mock_workspace_client)

    def test_list_tables(self, unity_catalog_api, mock_workspace_client):
        """Test list_tables calls SDK list method."""
        mock_table1 = Mock(spec=TableInfo)
        mock_table2 = Mock(spec=TableInfo)
        mock_workspace_client.tables.list.return_value = iter([mock_table1, mock_table2])

        result = unity_catalog_api.list_tables("catalog", "schema")

        assert len(result) == 2
        assert result[0] == mock_table1
        assert result[1] == mock_table2
        mock_workspace_client.tables.list.assert_called_once_with(
            catalog_name="catalog", schema_name="schema"
        )

    def test_get_table__caches_result(self, unity_catalog_api, mock_workspace_client):
        """Test get_table caches results per thread."""
        mock_table = TableInfo(
            name="test_table",
            table_type=TableType.MANAGED,
            columns=[
                ColumnInfo(name="col1", type_text="string"),
                ColumnInfo(name="col2", type_text="int"),
            ],
        )
        mock_workspace_client.tables.get.return_value = mock_table

        # First call should hit the SDK
        result1 = unity_catalog_api.get_table("catalog.schema.test_table")
        assert result1 == mock_table
        assert mock_workspace_client.tables.get.call_count == 1

        # Second call should return cached result
        result2 = unity_catalog_api.get_table("catalog.schema.test_table")
        assert result2 == mock_table
        assert mock_workspace_client.tables.get.call_count == 1  # Still 1, not 2

    def test_get_table__different_tables_not_cached(
        self, unity_catalog_api, mock_workspace_client
    ):
        """Test get_table fetches different tables separately."""
        mock_table1 = TableInfo(name="table1", table_type=TableType.MANAGED)
        mock_table2 = TableInfo(name="table2", table_type=TableType.MANAGED)

        def get_side_effect(full_name):
            if full_name == "catalog.schema.table1":
                return mock_table1
            else:
                return mock_table2

        mock_workspace_client.tables.get.side_effect = get_side_effect

        result1 = unity_catalog_api.get_table("catalog.schema.table1")
        result2 = unity_catalog_api.get_table("catalog.schema.table2")

        assert result1 == mock_table1
        assert result2 == mock_table2
        assert mock_workspace_client.tables.get.call_count == 2

    def test_clear_thread_cache__clears_cache(self, unity_catalog_api, mock_workspace_client):
        """Test clear_thread_cache clears the cache."""
        mock_table = TableInfo(name="test_table", table_type=TableType.MANAGED)
        mock_workspace_client.tables.get.return_value = mock_table

        # First call
        unity_catalog_api.get_table("catalog.schema.test_table")
        assert mock_workspace_client.tables.get.call_count == 1

        # Clear cache
        unity_catalog_api.clear_thread_cache()

        # Next call should hit SDK again
        unity_catalog_api.get_table("catalog.schema.test_table")
        assert mock_workspace_client.tables.get.call_count == 2

    def test_clear_thread_cache__no_cache_initialized(self, unity_catalog_api):
        """Test clear_thread_cache handles case where cache not yet initialized."""
        # Should not raise an error
        unity_catalog_api.clear_thread_cache()


class TestColumnInfoMapping:
    """Test mapping of SDK ColumnInfo to DatabricksColumn."""

    def test_map_column_info_simple_type(self):
        """Test mapping simple column type."""
        from dbt.adapters.databricks.utils import map_column_info_to_databricks_column

        column_info = ColumnInfo(
            name="id", type_text="bigint", type_json='{"name":"long"}', comment="User ID"
        )

        result = map_column_info_to_databricks_column(column_info)

        assert result.column == "id"
        assert result.dtype == "long"  # Raw type from JSON
        assert result.data_type == "bigint"  # Translated via translate_type()
        assert result.comment == "User ID"

    def test_map_column_info_complex_type_with_json(self):
        """Test mapping complex nested struct type using type_json."""
        from dbt.adapters.databricks.utils import map_column_info_to_databricks_column

        # Complex nested struct that would be truncated in type_text
        type_json = """{
            "name": "struct",
            "fields": [
                {"name": "id", "type": {"name": "long"}},
                {"name": "address", "type": {
                    "name": "struct",
                    "fields": [
                        {"name": "street", "type": {"name": "string"}},
                        {"name": "city", "type": {"name": "string"}},
                        {"name": "coordinates", "type": {
                            "name": "struct",
                            "fields": [
                                {"name": "lat", "type": {"name": "double"}},
                                {"name": "lon", "type": {"name": "double"}}
                            ]
                        }}
                    ]
                }}
            ]
        }"""

        column_info = ColumnInfo(
            name="user_data",
            type_text="struct<id:bigint,address:struct<...>>",  # Truncated
            type_json=type_json,
            comment="User data",
        )

        result = map_column_info_to_databricks_column(column_info)

        assert result.column == "user_data"
        # Should get full type from JSON, not truncated type_text
        expected = "struct<id:long,address:struct<street:string,city:string,coordinates:struct<lat:double,lon:double>>>"
        assert result.dtype == expected
        assert result.comment == "User data"

    def test_map_column_info_fallback_to_type_text(self):
        """Test fallback to type_text when type_json is not available."""
        from dbt.adapters.databricks.utils import map_column_info_to_databricks_column

        column_info = ColumnInfo(name="name", type_text="string", type_json=None)

        result = map_column_info_to_databricks_column(column_info)

        assert result.column == "name"
        assert result.dtype == "string"

    def test_map_column_info_fallback_on_json_parse_error(self):
        """Test fallback to type_text when type_json parsing fails."""
        from dbt.adapters.databricks.utils import map_column_info_to_databricks_column

        column_info = ColumnInfo(
            name="data", type_text="string", type_json="invalid json"
        )

        result = map_column_info_to_databricks_column(column_info)

        assert result.column == "data"
        assert result.dtype == "string"  # Falls back to type_text
