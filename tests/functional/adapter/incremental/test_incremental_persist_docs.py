import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.incremental import fixtures


class TestIncrementalPersistDocs(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("merge_update_columns_sql",)

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
            f"describe detail {project.database}.{project.test_schema}.merge_update_columns_sql",
            fetch="all",
        )
        assert results[0][3] == "This is a model description"
        results = project.run_sql(
            f"describe table {project.database}.{project.test_schema}.merge_update_columns_sql",
            fetch="all",
        )
        assert results[0][2] == "This is the id column"
        assert results[1][2] == "This is the msg column"


class TestIncrementalPersistDocsV2(TestIncrementalPersistDocs):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
        }


class TestIncrementalColumnCommentReconcileSubkeyV1(RerunSafeMixin):
    """v1 reconciles column comments on the persist_docs.columns sub-key, independent of
    persist_docs.relation (the typed path keys this reconcile on .relation instead)."""

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("merge_update_columns_sql",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.no_comment_schema,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": False},
            "models": {"test": {"+persist_docs": {"relation": False, "columns": True}}},
        }

    def test_column_comment_reconciles_on_columns_subkey(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.comment_schema, "models", "schema.yml")
        util.run_dbt(["run"])  # incremental merge run
        results = project.run_sql(
            f"describe table {project.database}.{project.test_schema}.merge_update_columns_sql",
            fetch="all",
        )
        assert results[0][2] == "This is the id column"
        assert results[1][2] == "This is the msg column"
