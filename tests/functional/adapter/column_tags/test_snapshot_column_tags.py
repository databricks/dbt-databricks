import pytest
from dbt.tests import util

from tests.functional.adapter.column_tags import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestSnapshotColumnTags:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": fixtures.snapshot_column_tag_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {"schema.yml": fixtures.initial_snapshot_column_tag_schema}

    def test_snapshot_column_tags(self, project):
        util.run_dbt(["snapshot"])

        column_tags_query = f"""
            SELECT column_name, tag_name, tag_value
            FROM `system`.`information_schema`.`column_tags`
            WHERE catalog_name = '{project.database}'
              AND schema_name = '{project.test_schema}'
              AND table_name = 'snapshot'
            ORDER BY column_name, tag_name
            """

        tags = project.run_sql(column_tags_query, fetch="all")
        expected_tags = {
            ("account_number", "pii", "true"),
            ("account_number", "sensitive", "true"),
        }
        actual_tags = {(row[0], row[1], row[2]) for row in tags}
        assert actual_tags == expected_tags

        util.write_file(fixtures.updated_snapshot_column_tag_schema, "models", "schema.yml")
        util.run_dbt(["snapshot"])

        tags = project.run_sql(column_tags_query, fetch="all")
        expected_tags = {
            ("id", "pii", "false"),
            ("account_number", "pii", "true"),
            ("account_number", "sensitive", "true"),
        }
        actual_tags = {(row[0], row[1], row[2]) for row in tags}
        assert actual_tags == expected_tags
