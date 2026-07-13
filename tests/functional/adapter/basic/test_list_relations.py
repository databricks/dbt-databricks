import pytest
from dbt.tests.util import run_dbt

from dbt.adapters.databricks.relation import DatabricksRelationType, DatabricksTableType
from tests.functional.adapter.basic.fixtures import (
    list_relations_table_sql,
    list_relations_view_sql,
)


@pytest.mark.skip_profile("databricks_cluster")
class TestListRelationsWithoutCaching:
    """Exercise list_relations_without_caching on Unity Catalog (get_uc_tables)."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "list_rel_table.sql": list_relations_table_sql,
            "list_rel_view.sql": list_relations_view_sql,
        }

    def test_classifies_table_and_view(self, project):
        run_dbt(["run"])

        with project.adapter.connection_named("__test"):
            schema_relation = project.adapter.Relation.create(
                database=project.database,
                schema=project.test_schema,
            )
            relations = {
                relation.identifier: relation
                for relation in project.adapter.list_relations_without_caching(schema_relation)
            }

        table = relations["list_rel_table"]
        assert table.type == DatabricksRelationType.Table
        assert table.databricks_table_type == DatabricksTableType.Managed
        assert table.is_external_table is False
        assert table.is_delta is True

        view = relations["list_rel_view"]
        assert view.type == DatabricksRelationType.View
        assert view.is_delta is False

    def test_missing_schema_returns_empty(self, project):
        with project.adapter.connection_named("__test"):
            absent = project.adapter.Relation.create(
                database=project.database,
                schema=f"{project.test_schema}_does_not_exist",
            )
            assert project.adapter.list_relations_without_caching(absent) == []
