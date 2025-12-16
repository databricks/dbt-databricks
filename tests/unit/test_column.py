import json

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


class TestRenderForCreate:
    @pytest.fixture
    def column(self):
        return DatabricksColumn("`id`", "INT")

    def test_render_for_create__base(self, column):
        assert column.render_for_create() == "`id` INT"

    def test_render_for_create__not_null(self, column):
        column.not_null = True
        assert column.render_for_create() == "`id` INT NOT NULL"

    def test_render_for_create__comment(self, column):
        column.comment = "this is a column"
        assert column.render_for_create() == "`id` INT COMMENT 'this is a column'"

    def test_render_for_create__escaping(self, column):
        column.comment = "this is a 'column'"
        assert column.render_for_create() == "`id` INT COMMENT 'this is a \\'column\\''"


class TestColumnStatics:
    @pytest.mark.parametrize(
        "columns, expected",
        [
            ([], ""),
            ([DatabricksColumn("`foo`", "string")], "`foo`"),
            (
                [DatabricksColumn("`foo`", "string"), DatabricksColumn("`bar`", "int")],
                "`foo`, `bar`",
            ),
        ],
    )
    def test_format_remove_column_list(self, columns, expected):
        assert DatabricksColumn.format_remove_column_list(columns) == expected

    @pytest.mark.parametrize(
        "columns, expected",
        [
            ([], ""),
            ([DatabricksColumn("`foo`", "string")], "`foo` string"),
            (
                [DatabricksColumn("`foo`", "string"), DatabricksColumn("`bar`", "int")],
                "`foo` string, `bar` int",
            ),
        ],
    )
    def test_format_add_column_list(self, columns, expected):
        assert DatabricksColumn.format_add_column_list(columns) == expected


# Tests are based on possible JSON output from "DESCRIBE EXTENDED <table> AS JSON"
# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table#json-formatted-output
class TestDatabricksColumn:
    def test_from_json_metadata_happy_path(self):
        """Test from_json_metadata with a variety of column types"""
        json_metadata = json.dumps(
            {
                "columns": [
                    {"name": "id", "type": {"name": "bigint"}, "comment": "Primary key"},
                    {"name": "name", "type": {"name": "string"}, "comment": "User name"},
                    {
                        "name": "nested_data",
                        "type": {
                            "name": "struct",
                            "fields": [
                                {"name": "field1", "type": {"name": "string"}},
                                {"name": "field2", "type": {"name": "int"}},
                            ],
                        },
                        "comment": None,
                    },
                ]
            }
        )

        result = DatabricksColumn.from_json_metadata(json_metadata)

        assert len(result) == 3
        assert isinstance(result[0], DatabricksColumn)
        assert result[0].column == "id"
        assert result[0].dtype == "bigint"
        assert result[0].comment == "Primary key"

        assert result[1].column == "name"
        assert result[1].dtype == "string"
        assert result[1].comment == "User name"

        assert result[2].column == "nested_data"
        assert result[2].dtype == "struct<field1:string,field2:int>"
        assert result[2].comment is None

    def test_from_json_metadata_with_varchar_and_char(self):
        """Test from_json_metadata properly handles varchar and char with length"""
        json_metadata = json.dumps(
            {
                "columns": [
                    {"name": "id", "type": {"name": "bigint"}, "comment": "Primary key"},
                    {
                        "name": "code",
                        "type": {"name": "char", "length": 5},
                        "comment": "Fixed code",
                    },
                    {
                        "name": "description",
                        "type": {"name": "varchar", "length": 100},
                        "comment": "Variable description",
                    },
                ]
            }
        )

        result = DatabricksColumn.from_json_metadata(json_metadata)

        assert len(result) == 3
        assert result[0].column == "id"
        assert result[0].dtype == "bigint"

        assert result[1].column == "code"
        assert result[1].dtype == "char(5)"
        assert result[1].comment == "Fixed code"

        assert result[2].column == "description"
        assert result[2].dtype == "varchar(100)"
        assert result[2].comment == "Variable description"

    def test_from_json_metadata_empty_columns(self):
        """Test from_json_metadata with empty columns list"""
        json_metadata = json.dumps({"columns": []})

        result = DatabricksColumn.from_json_metadata(json_metadata)

        assert len(result) == 0

    def test_from_json_metadata_no_columns_key(self):
        """Test from_json_metadata when columns key is missing"""
        json_metadata = json.dumps({})

        result = DatabricksColumn.from_json_metadata(json_metadata)

        assert len(result) == 0

    def test_parse_type_from_json_struct(self):
        """Test _parse_type_from_json with struct type"""
        type_info = {
            "name": "struct",
            "fields": [
                {"name": "field1", "type": {"name": "string"}},
                {"name": "field2", "type": {"name": "int"}},
            ],
        }

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "struct<field1:string,field2:int>"

    def test_parse_type_from_json_array(self):
        """Test _parse_type_from_json with array type"""
        type_info = {"name": "array", "element_type": {"name": "string"}}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "array<string>"

    def test_parse_type_from_json_map(self):
        """Test _parse_type_from_json with map type"""
        type_info = {
            "name": "map",
            "key_type": {"name": "string"},
            "value_type": {"name": "int"},
        }

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "map<string,int>"

    def test_parse_type_from_json_map_nested(self):
        """Test _parse_type_from_json with nested map type"""
        type_info = {
            "name": "map",
            "key_type": {"name": "string"},
            "value_type": {"name": "array", "element_type": {"name": "double"}},
        }

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "map<string,array<double>>"

    def test_parse_type_from_json_decimal_with_precision_scale(self):
        """Test _parse_type_from_json with decimal type having precision and scale"""
        type_info = {"name": "decimal", "precision": 10, "scale": 2}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "decimal(10, 2)"

    def test_parse_type_from_json_decimal_with_precision_only(self):
        """Test _parse_type_from_json with decimal type having only precision"""
        type_info = {"name": "decimal", "precision": 10}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "decimal(10)"

    def test_parse_type_from_json_decimal(self):
        """Test _parse_type_from_json with decimal type without precision and scale"""
        type_info = {"name": "decimal"}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "decimal"

    def test_parse_type_from_json_string_with_collation(self):
        type_info = {"name": "string", "collation": "UTF8_LCASE"}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "string COLLATE UTF8_LCASE"

    def test_parse_type_from_json_string_without_collation(self):
        type_info = {"name": "string"}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "string"

    def test_parse_type_from_json_varchar_with_length(self):
        type_info = {"name": "varchar", "length": 10}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "varchar(10)"

    def test_parse_type_from_json_varchar_without_length(self):
        type_info = {"name": "varchar"}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "varchar"

    def test_parse_type_from_json_char_with_length(self):
        type_info = {"name": "char", "length": 10}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "char(10)"

    def test_parse_type_from_json_char_without_length(self):
        type_info = {"name": "char"}

        result = DatabricksColumn._parse_type_from_json(type_info)
        assert result == "char"

    def test_parse_type_from_json_primitive_types(self):
        """Test _parse_type_from_json with various primitive types"""
        primitive_types = [
            "int",
            "bigint",
            "smallint",
            "tinyint",
            "double",
            "float",
            "boolean",
            "date",
            "timestamp",
            "binary",
        ]

        for type_name in primitive_types:
            type_info = {"name": type_name}
            result = DatabricksColumn._parse_type_from_json(type_info)
            assert result == type_name
