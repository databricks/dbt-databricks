import pytest

from dbt.tests import util
from tests.functional.adapter.incremental import fixtures


class TestIncrementalPersistDocs:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.no_comment_schema,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    def test_adding_comments(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.comment_schema, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            f"select comment from {project.database}.information_schema.tables "
            f"where"
            f"  table_schema = '{project.test_schema}' and"
            f"  table_name = 'merge_update_columns_sql'",
            fetch="all",
        )
        assert results[0][0] == "This is a model description"
        results = project.run_sql(
            f"select comment from {project.database}.information_schema.columns "
            f"where"
            f"  table_schema = '{project.test_schema}' and"
            f"  table_name = 'merge_update_columns_sql'"
            f"order by ordinal_position",
            fetch="all",
        )
        assert results[0][0] == "This is the id column"
        assert results[1][0] == "This is the msg column"
