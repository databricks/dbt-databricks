import pytest
from dbt.tests import util

from tests.functional.adapter.helpers import FAIL_IF_TAG_FETCH_CALLED_MACROS

VIEW_WITHOUT_TAGS_SQL = """
{{ config(materialized='view') }}

select cast(1 as bigint) as id
"""

VIEW_WITH_TAGS_SQL = """
{{ config(
    materialized='view',
    databricks_tags={'classification': 'internal'}
) }}

select cast(1 as bigint) as id
"""


@pytest.mark.skip_profile("databricks_cluster")
class TestViewMetadataFetchSkips:
    @pytest.fixture(scope="class")
    def models(self):
        return {"view_metadata_fetch.sql": VIEW_WITHOUT_TAGS_SQL}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": FAIL_IF_TAG_FETCH_CALLED_MACROS}

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
        return {"view_metadata_fetch.sql": VIEW_WITH_TAGS_SQL}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": FAIL_IF_TAG_FETCH_CALLED_MACROS}

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
        _, logs = util.run_dbt_and_capture(["run"], expect_pass=False)
        util.assert_message_in_logs("fetch_tags should not be called", logs)
