import pytest
from dbt.adapters.databricks import DatabricksColumn
from dbt_common.contracts.constraints import ColumnLevelConstraint, ConstraintType

from dbt.adapters.databricks.impl import DatabricksAdapter


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
        assert column.create_constraints == []
        assert column.alter_constraints == []

    def test_add_constraint__check(self, column):
        constraint = ColumnLevelConstraint(type=ConstraintType.check)
        column.add_constraint(constraint)
        assert column.not_null is False
        assert column.create_constraints == []
        assert column.alter_constraints == [constraint]

    def test_add_constraint__other_constraint(self, column):
        constraint = ColumnLevelConstraint(type=ConstraintType.custom)
        column.add_constraint(constraint)
        assert column.not_null is False
        assert column.create_constraints == [constraint]
        assert column.alter_constraints == []


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
                {"type": "check", "name": "foo"},
            ],
        }

    def test_enrich__data_type(self, column, model_column):
        enriched_column = column.enrich(
            model_column, lambda x: DatabricksAdapter._get_column_constraint(x, "id")
        )
        assert enriched_column.data_type == "bigint"

    def test_enrich__description(self, column, model_column):
        enriched_column = column.enrich(
            model_column, lambda x: DatabricksAdapter._get_column_constraint(x, "id")
        )
        assert enriched_column.comment == "this is a column"

    def test_enrich__constraints(self, column, model_column):
        enriched_column = column.enrich(
            model_column, lambda x: DatabricksAdapter._get_column_constraint(x, "id")
        )
        assert enriched_column.not_null is True
        assert enriched_column.create_constraints == []
        assert enriched_column.alter_constraints == [
            ColumnLevelConstraint(type=ConstraintType.check, name="foo")
        ]


class TestRenderForCreate:
    @pytest.fixture
    def column(self):
        return DatabricksColumn("id", "INT")

    @pytest.fixture
    def render_func(self):
        return lambda x: str(x.type)

    def test_render_for_create__base(self, column, render_func):
        assert column.render_for_create(render_func) == "id INT"

    def test_render_for_create__not_null(self, column, render_func):
        column.not_null = True
        assert column.render_for_create(render_func) == "id INT NOT NULL"

    def test_render_for_create__comment(self, column, render_func):
        column.comment = "this is a column"
        assert column.render_for_create(render_func) == "id INT COMMENT 'this is a column'"

    def test_render_for_create__constraints(self, column, render_func):
        column.create_constraints = [
            ColumnLevelConstraint(type=ConstraintType.primary_key),
            ColumnLevelConstraint(type=ConstraintType.unique),
        ]
        assert (
            column.render_for_create(render_func)
            == "id INT ConstraintType.primary_key ConstraintType.unique"
        )

    def test_render_for_create__everything(self, column, render_func):
        column.not_null = True
        column.comment = "this is a column"
        column.create_constraints = [
            ColumnLevelConstraint(type=ConstraintType.primary_key),
            ColumnLevelConstraint(type=ConstraintType.unique),
        ]
        assert column.render_for_create(render_func) == (
            "id INT NOT NULL COMMENT 'this is a column' "
            "ConstraintType.primary_key ConstraintType.unique"
        )

    def test_render_for_create__escaping(self, column, render_func):
        column.comment = "this is a 'column'"
        assert column.render_for_create(render_func) == "id INT COMMENT 'this is a \\'column\\''"


class TestAdapterRenderColumnForCreate:
    @pytest.fixture
    def column(self):
        c = DatabricksColumn("id", "INT")
        c.create_constraints = [ColumnLevelConstraint(type=ConstraintType.primary_key, name="foo")]
        return c

    def test_render_column_for_create(self, column):
        assert DatabricksAdapter.render_column_for_create(column) == "id INT"
