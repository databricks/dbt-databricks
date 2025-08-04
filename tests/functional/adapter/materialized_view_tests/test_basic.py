from typing import Optional

import pytest

from dbt.adapters.base.relation import BaseRelation
from dbt.tests import util
from dbt.tests.adapter.materialized_view import files
from dbt.tests.adapter.materialized_view.basic import MaterializedViewBasic
from tests.functional.adapter.materialized_view_tests import fixtures


class TestMaterializedViewsMixin:
    @staticmethod
    def insert_record(project, table: BaseRelation, record: tuple[int, int]) -> None:
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


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViews(TestMaterializedViewsMixin, MaterializedViewBasic):
    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": files.MY_TABLE,
            "my_view.sql": files.MY_VIEW,
            "my_materialized_view.sql": files.MY_MATERIALIZED_VIEW,
            "complex_types_materialized_view.sql": fixtures.complex_types_materialized_view,
            "schema.yml": fixtures.materialized_view_schema,
        }

    def test_table_replaces_materialized_view(self, project, my_materialized_view):
        util.run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

        self.swap_materialized_view_to_table(project, my_materialized_view)

        util.run_dbt(["run", "--models", my_materialized_view.identifier])

        # UC doesn't sync metadata fast enough for this to pass consistently
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

    def test_create_materialized_view_with_comment_and_constraints(
        self, project, my_materialized_view
    ):
        util.run_dbt(["run", "--models", my_materialized_view.identifier])
        assert self.query_relation_type(project, my_materialized_view) == "materialized_view"

        # verify the non-null constraint and column comment are persisted on create
        results = project.run_sql(
            f"""
            SELECT
                is_nullable,
                comment
            FROM {project.database}.information_schema.columns
            WHERE table_catalog = '{project.database}'
              AND table_schema = '{project.test_schema}'
              AND table_name = '{my_materialized_view.identifier}'
              AND column_name = 'id'""",
            fetch="all",
        )
        row = results[0]
        assert row[0] == "NO"
        assert row[1] == "The unique identifier for each record"
        # Verify primary key constraint is persisted
        results = project.run_sql(
            f"""
            SELECT
                constraint_name,
                column_name
            FROM {project.database}.information_schema.key_column_usage
            WHERE table_catalog = '{project.database}'
              AND table_schema = '{project.test_schema}'
              AND table_name = '{my_materialized_view.identifier}'
            """,
            fetch="all",
        )
        assert len(results) == 1
        assert results[0][0] == "my_materialized_view_pk"
        assert results[0][1] == "id"

    def test_materialized_view_complex_types(self, project):
        util.run_dbt(["run", "--models", "complex_types_materialized_view"])
        results = project.run_sql(
            """
            SELECT COLUMN_NAME, FULL_DATA_TYPE FROM {database}.information_schema.columns
            WHERE table_schema = '{schema}' AND table_name = 'complex_types_materialized_view';
            """,
            fetch="all",
        )
        assert results[0][0] == "my_struct"
        expected_struct_type = (
            "struct<field1:map<string,int>,field2:array<int>,"
            + ",".join([f"field{i}:int" for i in range(3, 31)])
            + ">"
        )
        assert results[0][1] == expected_struct_type
