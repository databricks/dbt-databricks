from typing import Optional, Tuple
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
import dbt.tests.adapter.materialized_view.basic as fixtures
from dbt.adapters.base.relation import BaseRelation
import pytest

from dbt.adapters.databricks.relation import DatabricksRelationType


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
        table_type = project.run_sql(
            f"select table_type from {relation.information_schema_only()}."
            f"`tables` where table_name = '{relation.identifier}'",
            fetch="one",
        )[0]
        if table_type == "STREAMING_TABLE":
            return DatabricksRelationType.StreamingTable.value
        elif table_type == "MANAGED" or table_type == "EXTERNAL":
            return DatabricksRelationType.Table.value
        else:
            is_materialized = project.run_sql(
                f"select is_materialized from {relation.information_schema_only()}."
                f"`views` where table_name = '{relation.identifier}'",
                fetch="one",
            )[0]
            if is_materialized == "TRUE":
                return DatabricksRelationType.MaterializedView.value
            else:
                return DatabricksRelationType.View.value


@pytest.mark.skip_profile("databricks_cluster")
class TestMaterializedViews(TestMaterializedViewsMixin, MaterializedViewBasic):
    pass
