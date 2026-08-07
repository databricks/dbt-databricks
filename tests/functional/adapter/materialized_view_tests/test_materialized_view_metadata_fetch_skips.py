import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests import util

from tests.functional.adapter.fixtures import fail_if_tag_fetch_called_macros
from tests.functional.adapter.materialized_view_tests import fixtures


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewMetadataFetchSkips:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mv_metadata_fetch_seed.csv": fixtures.metadata_fetch_mv_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"mv_metadata_fetch.sql": fixtures.metadata_fetch_materialized_view_without_tags_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": fail_if_tag_fetch_called_macros}

    def test_second_materialized_view_run_succeeds_without_tag_fetches(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() may attempt metadata fetches.
        util.run_dbt(["seed"])
        util.run_dbt(["run"])
        util.run_dbt(["run"])


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
class TestMaterializedViewMetadataFetchRequiresTags:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"mv_metadata_fetch_seed.csv": fixtures.metadata_fetch_mv_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {"mv_metadata_fetch.sql": fixtures.metadata_fetch_materialized_view_with_tags_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": fail_if_tag_fetch_called_macros}

    def test_second_materialized_view_run_fails_when_tag_fetch_is_required(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() may attempt metadata fetches.
        util.run_dbt(["seed"])
        util.run_dbt(["run"])

        run_execution_results = util.run_dbt(["run"], expect_pass=False)
        assert len(run_execution_results.results) == 1
        result = run_execution_results.results[0]

        assert result.status == RunStatus.Error
        assert "fetch_tags should not be called" in result.message
