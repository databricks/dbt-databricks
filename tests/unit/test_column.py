import pytest

from dbt.adapters.databricks.column import DatabricksColumn


class TestSparkColumn:
    def test_convert_table_stats_with_no_statistics(self):
        assert DatabricksColumn.convert_table_stats(None) == {}

    def test_convert_table_stats_with_bytes(self):
        assert DatabricksColumn.convert_table_stats("123456789 bytes") == {
            "stats:bytes:description": "",
            "stats:bytes:include": True,
            "stats:bytes:label": "bytes",
            "stats:bytes:value": 123456789,
        }

    def test_convert_table_stats_with_bytes_and_rows(self):
        assert DatabricksColumn.convert_table_stats("1234567890 bytes, 12345678 rows") == {
            "stats:bytes:description": "",
            "stats:bytes:include": True,
            "stats:bytes:label": "bytes",
            "stats:bytes:value": 1234567890,
            "stats:rows:description": "",
            "stats:rows:include": True,
            "stats:rows:label": "rows",
            "stats:rows:value": 12345678,
        }


class TestColumnStatics:
    @pytest.mark.parametrize(
        "column, expected",
        [
            ({"name": "foo", "quote": True}, "`foo`"),
            ({"name": "foo", "quote": False}, "foo"),
            ({"name": "foo"}, "foo"),
        ],
    )
    def test_get_name(self, column, expected):
        assert DatabricksColumn.get_name(column) == expected

    @pytest.mark.parametrize(
        "columns, expected",
        [
            ([], ""),
            ([DatabricksColumn("foo", "string")], "`foo`"),
            ([DatabricksColumn("foo", "string"), DatabricksColumn("bar", "int")], "`foo`, `bar`"),
        ],
    )
    def test_format_remove_column_list(self, columns, expected):
        assert DatabricksColumn.format_remove_column_list(columns) == expected

    @pytest.mark.parametrize(
        "columns, expected",
        [
            ([], ""),
            ([DatabricksColumn("foo", "string")], "`foo` string"),
            (
                [DatabricksColumn("foo", "string"), DatabricksColumn("bar", "int")],
                "`foo` string, `bar` int",
            ),
        ],
    )
    def test_format_add_column_list(self, columns, expected):
        assert DatabricksColumn.format_add_column_list(columns) == expected
