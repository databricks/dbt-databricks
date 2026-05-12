import pytest
from dbt.artifacts.schemas.results import RunStatus
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

    def test_second_view_run_fails_when_tag_fetch_is_required(self, project):
        # The first run creates the view; the second run exercises the existing-relation
        # alter/config-diff path where adapter.get_relation_config() may fetch tags.
        util.run_dbt(["run"])

        run_execution_results = util.run_dbt(["run"], expect_pass=False)
        assert len(run_execution_results.results) == 1
        result = run_execution_results.results[0]

        assert result.status == RunStatus.Error
        assert "fetch_tags should not be called" in result.message
