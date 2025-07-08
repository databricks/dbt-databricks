import json

from dbt.adapters.databricks.behaviors.columns import GetColumnsByDescribe
from dbt.adapters.databricks.column import DatabricksColumn


# Tests are based on possible JSON output from "DESCRIBE EXTENDED <table> AS JSON"
# https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table#json-formatted-output
class TestGetColumnsByDescribe:
    def test_parse_columns_from_json_happy_path(self):
        """Test _parse_columns_from_json with a variety of column types"""
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

        result = GetColumnsByDescribe._parse_columns_from_json(json_metadata)

        assert len(result) == 3
        assert isinstance(result[0], DatabricksColumn)
        assert result[0].column == "id"
        assert result[0].dtype == "bigint"
        assert result[0].comment == "Primary key"

        assert result[1].column == "name"
        assert result[1].dtype == "string"
        assert result[1].comment == "User name"

        assert result[2].column == "nested_data"
        assert result[2].dtype == "struct<field1: string, field2: int>"
        assert result[2].comment is None

    def test_parse_type_struct(self):
        """Test _parse_type with struct type"""
        type_info = {
            "name": "struct",
            "fields": [
                {"name": "field1", "type": {"name": "string"}},
                {"name": "field2", "type": {"name": "int"}},
                {
                    "name": "nested",
                    "type": {
                        "name": "struct",
                        "fields": [{"name": "inner", "type": {"name": "double"}}],
                    },
                },
            ],
        }

        result = GetColumnsByDescribe._parse_type(type_info)
        expected = "struct<field1: string, field2: int, nested: struct<inner: double>>"
        assert result == expected

    def test_parse_type_array(self):
        """Test _parse_type with array type"""
        type_info = {"name": "array", "element_type": {"name": "string"}}

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "array<string>"

    def test_parse_type_array_nested(self):
        """Test _parse_type with nested array type"""
        type_info = {
            "name": "array",
            "element_type": {
                "name": "struct",
                "fields": [{"name": "item", "type": {"name": "int"}}],
            },
        }

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "array<struct<item: int>>"

    def test_parse_type_map(self):
        """Test _parse_type with map type"""
        type_info = {
            "name": "map",
            "key_type": {"name": "string"},
            "value_type": {"name": "int"},
        }

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "map<string, int>"

    def test_parse_type_map_nested(self):
        """Test _parse_type with nested map type"""
        type_info = {
            "name": "map",
            "key_type": {"name": "string"},
            "value_type": {"name": "array", "element_type": {"name": "double"}},
        }

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "map<string, array<double>>"

    def test_parse_type_decimal_with_precision_scale(self):
        """Test _parse_type with decimal type having precision and scale"""
        type_info = {"name": "decimal", "precision": 10, "scale": 2}

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "decimal(10, 2)"

    def test_parse_type_decimal_with_precision_only(self):
        """Test _parse_type with decimal type having only precision"""
        type_info = {"name": "decimal", "precision": 10}

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "decimal(10)"

    def test_parse_type_decimal(self):
        """Test _parse_type with decimal type without precision and scale"""
        type_info = {"name": "decimal"}

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "decimal"

    def test_parse_type_string_with_collation(self):
        type_info = {"name": "string", "collation": "UTF8_BINARY"}

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "string COLLATE UTF8_BINARY"

    def test_parse_type_string_without_collation(self):
        type_info = {"name": "string"}

        result = GetColumnsByDescribe._parse_type(type_info)
        assert result == "string"

    def test_parse_type_varchar(self):
        type_info = {"name": "varchar", "length": 10}

        result = GetColumnsByDescribe._parse_type(type_info)
        # varchar is just a string in Databricks
        assert result == "string"

    def test_parse_type_char(self):
        type_info = {"name": "char", "length": 10}

        result = GetColumnsByDescribe._parse_type(type_info)
        # char is just a string in Databricks
        assert result == "string"

    def test_parse_type_primitive_types(self):
        """Test _parse_type with various primitive types"""
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
            result = GetColumnsByDescribe._parse_type(type_info)
            assert result == type_name
