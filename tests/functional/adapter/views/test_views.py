import pytest
from agate import Row

from dbt.tests import util
from tests.functional.adapter.views import fixtures


class BaseUpdateView:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed.csv": fixtures.seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.schema_yml}

    def test_view_update_with_description(self, project):
        util.run_dbt(["build"])
        schema_2 = fixtures.schema_yml.replace("This is a view", "This is an updated view")
        util.write_file(schema_2, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            f"describe extended {project.database}.{project.test_schema}.initial_view",
            fetch="all",
        )
        for row in results:
            if row[0] == "comment":
                assert row[1] == "This is an updated view"

    def test_view_update_with_query(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.altered_view_sql, "models", "initial_view.sql")
        util.run_dbt(["run"])

        results = project.run_sql(
            f"select * from {project.database}.{project.test_schema}.initial_view",
            fetch="all",
        )
        assert results[0] == Row([1], ["id"])


class TestUpdateViewViaAlter(BaseUpdateView):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


class TestUpdateViewSafeReplace(BaseUpdateView):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+use_safer_relation_operations": True,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }


class TestUpdateUnsafeReplace(BaseUpdateView):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+use_safer_relation_operations": False,
                "+persist_docs": {
                    "relation": True,
                    "columns": True,
                },
            },
        }
