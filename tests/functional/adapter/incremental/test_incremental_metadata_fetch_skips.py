import pytest
from dbt.tests import util

from tests.functional.adapter.helpers import (
    FAIL_IF_TAG_AND_COLUMN_TAG_FETCH_CALLED_MACROS,
)
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalMetadataFetchSkips:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_incremental.sql": fixtures.metadata_fetch_incremental_sql,
            "schema.yml": fixtures.metadata_fetch_no_tags_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": FAIL_IF_TAG_AND_COLUMN_TAG_FETCH_CALLED_MACROS}

    def test_second_incremental_run_succeeds_without_tag_fetches(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() may attempt metadata fetches.
        util.run_dbt(["run"])
        util.run_dbt(["run"])


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalMetadataFetchRequiresTableTags:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_incremental.sql": fixtures.metadata_fetch_incremental_sql,
            "schema.yml": fixtures.metadata_fetch_table_tags_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": FAIL_IF_TAG_AND_COLUMN_TAG_FETCH_CALLED_MACROS}

    def test_second_incremental_run_fails_when_table_tag_fetch_is_required(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() may attempt metadata fetches.
        util.run_dbt(["run"])
        _, logs = util.run_dbt_and_capture(["run"], expect_pass=False)
        util.assert_message_in_logs("fetch_tags should not be called", logs)


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalMetadataFetchRequiresColumnTags:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_incremental.sql": fixtures.metadata_fetch_incremental_sql,
            "schema.yml": fixtures.metadata_fetch_column_tags_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": FAIL_IF_TAG_AND_COLUMN_TAG_FETCH_CALLED_MACROS}

    def test_second_incremental_run_fails_when_column_tag_fetch_is_required(self, project):
        # The first run creates the relation; the second run exercises the existing-relation
        # path where adapter.get_relation_config() may attempt metadata fetches.
        util.run_dbt(["run"])
        _, logs = util.run_dbt_and_capture(["run"], expect_pass=False)
        util.assert_message_in_logs("fetch_column_tags should not be called", logs)
