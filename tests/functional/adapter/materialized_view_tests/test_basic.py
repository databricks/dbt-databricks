from typing import Optional, Tuple
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.adapters.base.relation import BaseRelation
import pytest

from tests.functional.adapter.materialized_view_tests import fixtures


class TestMaterializedViewsMixin:
    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]) -> None:
        project.run_sql(f"insert into {table} values {record}")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation) -> None:
        project.run_sql(f"refresh materialized view {materialized_view}")

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        return project.run_sql(f"select count(*) from {relation}", fetch="one")[0]

    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        return fixtures.query_relation_type(project, relation)


@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViews(TestMaterializedViewsMixin, MaterializedViewBasic):
    pass
