from ast import Tuple
from typing import Optional
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from dbt.adapters.base.relation import BaseRelation


class TestMaterializedViewsMixin:
    @staticmethod
    def insert_record(project, table: BaseRelation, record: Tuple[int, int]):
        project.run_sql(f"insert into {table} values {record}")

    @staticmethod
    def refresh_materialized_view(project, materialized_view: BaseRelation):
        project.run_sql(f"refresh materialized view {materialized_view}")

    @staticmethod
    def query_row_count(project, relation: BaseRelation) -> int:
        project.run_sql(f"select count(*) from {relation}")
    
    @staticmethod
    def query_relation_type(project, relation: BaseRelation) -> Optional[str]:
        project.run_sql(f"select is_materialized from {relation.information_schema_only}")
    
class TestMaterializedViews(MaterializedViewBasic):
