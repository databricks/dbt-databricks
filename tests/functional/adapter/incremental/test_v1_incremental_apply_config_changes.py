import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import (
    MaterializationV1Mixin,
    fail_if_tag_and_column_tag_fetch_called_macros,
)
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestV1IncrementalApplyConfigChangesFalseSkipsTagFetch(MaterializationV1Mixin):
    """When `incremental_apply_config_changes` is false, the V1 incremental merge path
    must skip the get_relation_config metadata fetches even if the model declares tags.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "metadata_fetch_incremental.sql": (
                fixtures.metadata_fetch_incremental_skip_config_changes_sql
            ),
            "schema.yml": fixtures.metadata_fetch_table_tags_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_tag_fetch_called.sql": fail_if_tag_and_column_tag_fetch_called_macros}

    def test_v1_incremental_skips_metadata_fetch_when_flag_false(self, project):
        # First run creates the table; second run exercises the existing-relation merge path
        # where get_relation_config would normally fire metadata queries.
        # Tags are declared on the model, so without the flag the run would call fetch_tags
        # and fail. The flag must bypass the entire get_relation_config call.
        util.run_dbt(["run"])
        util.run_dbt(["run"])
