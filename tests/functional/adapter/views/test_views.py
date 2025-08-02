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


class BaseUpdateDescription(BaseUpdateView):
    def test_view_update_with_description(self, project):
        util.run_dbt(["build"])
        schema = util.read_file("models", "schema.yml")
        schema_2 = schema.replace("This is a view", "This is an updated view")
        util.write_file(schema_2, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            "describe extended {database}.{schema}.initial_view",
            fetch="all",
        )
        for row in results:
            if row[0] == "comment":
                assert row[1] == "This is an updated view"


class BaseUpdateQuery(BaseUpdateView):
    def test_view_update_with_query(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.altered_view_sql, "models", "initial_view.sql")
        util.run_dbt(["run"])

        results = project.run_sql(
            "select * from {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0] == Row([1], ["id"])


class BaseUpdateNothing(BaseUpdateView):
    """This test is only to ensure that we don't error out due to lack of changes."""

    def test_view_update_nothing(self, project):
        util.run_dbt(["build"])
        util.run_dbt(["run"])

        results = project.run_sql(
            "describe extended {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0][2] == "This is the id column"


class BaseUpdateTblProperties(BaseUpdateView):
    def test_view_update_tblproperties(self, project):
        util.run_dbt(["build"])
        schema = util.read_file("models", "schema.yml")
        schema_2 = schema.replace("key: value", "key: value2")
        util.write_file(schema_2, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            "show tblproperties {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0][1] == "value2"


class BaseUpdateColumnComments(BaseUpdateView):
    def test_view_update_with_column_comments(self, project):
        util.run_dbt(["build"])
        schema = util.read_file("models", "schema.yml")
        schema_2 = schema.replace("This is the id column", "This is an id column")
        util.write_file(schema_2, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            "describe extended {database}.{schema}.initial_view",
            fetch="all",
        )
        assert results[0][2] == "This is an id column"


class BaseRemoveTags(BaseUpdateView):
    def test_view_update_remove_tags(self, project):
        util.run_dbt(["build"])
        util.write_file(fixtures.no_tag_schema_yml, "models", "schema.yml")
        util.run_dbt(["run"])

        results = project.run_sql(
            f"""
            SELECT TAG_NAME, TAG_VALUE FROM {project.database}.information_schema.table_tags
            WHERE schema_name = '{project.test_schema}' AND table_name = 'initial_view'
            """,
            fetch="all",
        )

        # We remove the tag from the schema.yml, but this is an "add only" config so
        # we don't expect Databricks state to be updated. We still test this path
        # to ensure we don't get a "main is not being called during running model" error.
        assert len(results) == 1
        assert results[0][0] == "tag1"
        assert results[0][1] == "value1"


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterDescription(BaseUpdateDescription):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterNothing(BaseUpdateNothing):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterQuery(BaseUpdateQuery):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterTblproperties(BaseUpdateTblProperties):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterColumnComments(BaseUpdateColumnComments):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewViaAlterRemoveTags(BaseRemoveTags):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewSafeReplaceDescription(BaseUpdateDescription):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewSafeReplaceQuery(BaseUpdateQuery):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateViewSafeReplaceTblproperties(BaseUpdateTblProperties):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateUnsafeReplaceDescription(BaseUpdateDescription):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateUnsafeReplaceQuery(BaseUpdateQuery):
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


@pytest.mark.skip_profile("databricks_cluster")
class TestUpdateUnsafeReplaceTblproperties(BaseUpdateTblProperties):
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


class HiveBaseUpdateDescription(BaseUpdateDescription):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.hive_schema_yml}


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateViewSafeReplaceDescription(HiveBaseUpdateDescription):
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


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateUnsafeReplaceDescription(HiveBaseUpdateDescription):
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


class HiveBaseUpdateQuery(BaseUpdateQuery):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.hive_schema_yml}


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateViewSafeReplaceQuery(HiveBaseUpdateQuery):
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


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateUnsafeReplaceQuery(HiveBaseUpdateQuery):
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


class HiveBaseUpdateTblproperties(BaseUpdateTblProperties):
    @pytest.fixture(scope="class")
    def models(self):
        return {"initial_view.sql": fixtures.view_sql, "schema.yml": fixtures.hive_schema_yml}


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateViewSafeReplaceTblproperties(HiveBaseUpdateTblproperties):
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


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestHiveUpdateUnsafeReplaceTblproperties(HiveBaseUpdateTblproperties):
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
