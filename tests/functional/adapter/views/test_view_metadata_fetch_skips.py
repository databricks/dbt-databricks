import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import fail_if_tag_fetch_called_macros
from tests.functional.adapter.views.fixtures import view_with_tags_sql, view_without_tags_sql


@pytest.mark.skip_profile("databricks_cluster")
class TestViewMetadataFetchSkips:
    @pytest.fixture(scope="class")
    def models(self):
        return {"view_metadata_fetch.sql": view_without_tags_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": fail_if_tag_fetch_called_macros}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
            },
        }

    def test_second_view_run_succeeds_without_tag_fetches(self, project):
        # The first run creates the view; the second run exercises the existing-relation
        # alter/config-diff path where adapter.get_relation_config() may fetch tags.
        util.run_dbt(["run"])
        util.run_dbt(["run"])


@pytest.mark.skip_profile("databricks_cluster")
class TestViewMetadataFetchRequiresTags:
    @pytest.fixture(scope="class")
    def models(self):
        return {"view_metadata_fetch.sql": view_with_tags_sql}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {
                "+view_update_via_alter": True,
            },
        }

    def test_second_view_run_reads_tags_from_api(self, project):
        # The first run creates the view; the second run exercises the existing-relation
        # alter/config-diff path where adapter.get_relation_config() may fetch tags.
        util.run_dbt(["run"])
        util.run_dbt(["run"])

        tags = project.run_sql(
            "select tag_name, tag_value from `system`.`information_schema`.`table_tags` "
            "where schema_name = '{schema}' and table_name = 'view_metadata_fetch'",
            fetch="all",
        )
        assert {(row[0], row[1]) for row in tags} == {("classification", "internal")}
