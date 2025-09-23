from typing import Optional

import pytest

from dbt.tests import util
from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSimpleSnapshot, BaseSnapshotCheck
from dbt.tests.util import run_dbt
from tests.functional.adapter.simple_snapshot import fixtures


class TestSnapshot(BaseSimpleSnapshot):
    def add_fact_column(self, column: Optional[str] = None, definition: Optional[str] = None):
        """
        Applies updates to a table in a dbt project

        Args:
            project: the dbt project that contains the table
            table: the name of the table without a schema
            column: the name of the new column
            definition: the definition of the new column, e.g. 'varchar(20) default null'
        """

        table_name = util.relation_from_name(self.project.adapter, "fact")
        sql = f"""
            alter table {table_name}
            add column {column} string
        """
        self.project.run_sql(sql)


class TestSnapshotCheck(BaseSnapshotCheck):
    pass


@pytest.mark.skip_profile("databricks_cluster")
class TestSnapshotIceberg(BaseSnapshotCheck):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "snapshots": {
                "+table_format": "iceberg",
            }
        }


class TestSnapshotPersistDocs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": fixtures.comment_schema_yml,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": fixtures.snapshot_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "snapshots": {
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                }
            }
        }

    def test_persist_docs(self, project):
        results = run_dbt(["snapshot"])
        comment_query = f"describe detail {project.database}.{project.test_schema}.snapshot"
        results = project.run_sql(comment_query, fetch="all")
        assert results[0][3] == "This is a snapshot description"

        util.write_file(fixtures.new_comment_schema_yml, "models", "schema.yml")
        results = run_dbt(["snapshot"])
        results = project.run_sql(comment_query, fetch="all")
        assert results[0][3] == "This is a new snapshot description"
