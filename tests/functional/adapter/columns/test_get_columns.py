import pytest
from dbt.tests import util

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.relation import DatabricksRelation
from tests.functional.adapter.columns import fixtures
from tests.functional.adapter.fixtures import MaterializationV2Mixin


class ColumnsInRelation:
    @pytest.fixture(scope="class")
    def models(self):
        return {"base_model.sql": fixtures.base_model, "schema.yml": fixtures.schema}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        # debug uses different rules for managing project flags than run
        util.run_dbt(["debug", "--connection"])

        util.run_dbt(["run"])

    @pytest.fixture(scope="class")
    def expected_columns(self):
        return [
            DatabricksColumn(
                column="struct_col",
                dtype=(
                    "struct<col1:string,col2:int,col3:string,"
                    "col4:string,col5:string,col6:array<int>>"
                ),
            ),
            DatabricksColumn(column="str_col", dtype="string"),
        ]

    def test_columns_in_relation(self, project, expected_columns):
        my_relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="base_model",
            type=DatabricksRelation.Table,
        )

        with project.adapter.connection_named("_test"):
            actual_columns = project.adapter.get_columns_in_relation(my_relation)
        assert actual_columns == expected_columns


class TestColumnsInRelation(ColumnsInRelation):
    pass


class TestVarcharCharTypePreservation(MaterializationV2Mixin):
    """Test that varchar and char types preserve their length constraints with mat v2."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "varchar_char_model.sql": fixtures.varchar_char_model,
            "schema.yml": fixtures.varchar_char_schema,
        }

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        util.run_dbt(["debug", "--connection"])
        util.run_dbt(["run"])

    @pytest.fixture(scope="class")
    def expected_columns(self):
        return [
            DatabricksColumn(column="varchar_col", dtype="varchar(50)"),
            DatabricksColumn(column="char_col", dtype="char(10)"),
            DatabricksColumn(column="string_col", dtype="string"),
        ]

    def test_varchar_char_columns(self, project, expected_columns):
        my_relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="varchar_char_model",
            type=DatabricksRelation.Table,
        )

        with project.adapter.connection_named("_test"):
            actual_columns = project.adapter.get_columns_in_relation(my_relation)
        assert actual_columns == expected_columns


class TestColumnTypePredicates:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "typed_columns.sql": fixtures.typed_columns_model,
            "column_type_predicates.sql": fixtures.column_type_predicates_model,
        }

    def test_column_type_predicates(self, project):
        util.run_dbt(["run"])

        rows = project.run_sql(
            "select column_name, is_string, is_number, is_integer, is_float, is_numeric"
            " from {database}.{schema}.column_type_predicates",
            fetch="all",
        )
        actual = {row[0]: tuple(row[1:]) for row in rows}

        string = (True, False, False, False, False)
        integer = (False, True, True, False, False)
        floating = (False, True, False, True, False)
        decimal = (False, True, False, False, True)
        other = (False, False, False, False, False)
        assert actual == {
            "string_col": string,
            "varchar_col": string,
            "char_col": string,
            "tinyint_col": integer,
            "smallint_col": integer,
            "int_col": integer,
            "bigint_col": integer,
            "float_col": floating,
            "double_col": floating,
            "decimal_col": decimal,
            "decimal_38_0_col": decimal,
            "boolean_col": other,
            "date_col": other,
            "timestamp_col": other,
            "array_col": other,
            "struct_col": other,
        }
