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
