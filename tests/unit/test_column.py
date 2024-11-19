import pytest
from dbt.adapters.databricks import DatabricksColumn
from dbt_common.contracts.constraints import ColumnLevelConstraint, ConstraintType


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


class TestAddConstraint:
    @pytest.fixture
    def column(self):
        return DatabricksColumn("id", "LONG")

    def test_add_constraint__not_null(self, column):
        column.add_constraint(ColumnLevelConstraint(type=ConstraintType.not_null))
        assert column.not_null is True
        assert column.constraints == []

    def test_add_constraint__other_constraint(self, column):
        constraint = ColumnLevelConstraint(type=ConstraintType.custom)
        column.add_constraint(constraint)
        assert column.not_null is False
        assert column.constraints == [constraint]


class TestEnrich:
    @pytest.fixture
    def column(self):
        return DatabricksColumn("id", "INT")

    @pytest.fixture
    def model_column(self):
        return {
            "data_type": "LONG",
            "description": "this is a column",
            "constraints": [
                {"type": "not_null"},
                {"type": "primary_key", "name": "foo"},
            ],
        }

    def test_enrich(self, column, model_column):
        enriched_column = column.enrich(model_column)
        assert enriched_column.data_type == "bigint"
        assert enriched_column.comment == "this is a column"
        assert enriched_column.not_null is True
        assert enriched_column.constraints == [
            ColumnLevelConstraint(type=ConstraintType.primary_key, name="foo")
        ]


class TestRenderForCreate:
    @pytest.fixture
    def column(self):
        return DatabricksColumn("id", "INT")

    def test_render_for_create__base(self, column):
        assert column.render_for_create() == "id INT"

    def test_render_for_create__not_null(self, column):
        column.not_null = True
        assert column.render_for_create() == "id INT NOT NULL"

    def test_render_for_create__comment(self, column):
        column.comment = "this is a column"
        assert column.render_for_create() == "id INT COMMENT 'this is a column'"

    def test_render_for_create__constraints(self, column):
        column.constraints = [
            ColumnLevelConstraint(type=ConstraintType.primary_key),
        ]
        assert column.render_for_create() == "id INT PRIMARY KEY"

    def test_render_for_create__everything(self, column):
        column.not_null = True
        column.comment = "this is a column"
        column.constraints = [
            ColumnLevelConstraint(type=ConstraintType.primary_key),
            ColumnLevelConstraint(type=ConstraintType.custom, expression="foo"),
        ]
        assert column.render_for_create() == (
            "id INT NOT NULL COMMENT 'this is a column' " "PRIMARY KEY foo"
        )

    def test_render_for_create__escaping(self, column):
        column.comment = "this is a 'column'"
        assert column.render_for_create() == "id INT COMMENT 'this is a \\'column\\''"
