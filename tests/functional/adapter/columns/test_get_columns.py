import pytest

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.relation import DatabricksRelation
from tests.functional.adapter.columns import fixtures
from dbt.tests import util


class ColumnsInRelation:

    @pytest.fixture(scope="class")
    def models(self):
        return {"base_model.sql": fixtures.base_model, "schema.yml": fixtures.schema}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
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


class TestColumnsInRelationBehaviorFlagOff(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {}}


class TestColumnsInRelationBehaviorFlagOn(ColumnsInRelation):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"column_types_from_information_schema": True}}
