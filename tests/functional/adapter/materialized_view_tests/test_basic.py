from typing import Optional, Tuple
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.adapters.base.relation import BaseRelation
from dbt.tests import util
import pytest

from tests.functional.adapter.materialized_view_tests import fixtures


class TestMaterializedViewsMixin:
    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]) -> None:
        project.run_sql(f"insert into {table} values {record}")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation) -> None:
        util.run_dbt(["run", "--models", str(materialized_view.identifier)])

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        return project.run_sql(f"select count(*) from {relation}", fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return fixtures.query_relation_type(project, relation)


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViews(TestMaterializedViewsMixin, MaterializedViewBasic):
    def test_table_replaces_materialized_view(self, project, my_materialized_view):
        util.run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

        self.swap_materialized_view_to_table(project, my_materialized_view)

        util.run_dbt(["run", "--models", my_materialized_view.identifier])
        # assert self.query_relation_type(project, my_materialized_view) == "table"

    def test_view_replaces_materialized_view(self, project, my_materialized_view):
        util.run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

        self.swap_materialized_view_to_view(project, my_materialized_view)

        util.run_dbt(["run", "--models", my_materialized_view.identifier])
        # UC doesn't sync metadata fast enough for this to pass consistently
        # assert self.query_relation_type(project, my_materialized_view) == "view"

    def test_materialized_view_replaces_table(self, project, my_table):
        util.run_dbt(["run", "--models", my_table.identifier])
        assert self.query_relation_type(project, my_table) == "table"

        self.swap_table_to_materialized_view(project, my_table)

        util.run_dbt(["run", "--models", my_table.identifier])
        # UC doesn't sync metadata fast enough for this to pass consistently
        # assert self.query_relation_type(project, my_table) == "materialized_view"

    def test_materialized_view_replaces_view(self, project, my_view):
        util.run_dbt(["run", "--models", my_view.identifier])
        assert self.query_relation_type(project, my_view) == "view"

        self.swap_view_to_materialized_view(project, my_view)

        util.run_dbt(["run", "--models", my_view.identifier])
        # UC doesn't sync metadata fast enough for this to pass consistently
        # assert self.query_relation_type(project, my_view) == "materialized_view"
