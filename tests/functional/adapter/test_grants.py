import pytest
from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import BaseIncrementalGrants
from dbt.tests.adapter.grants.test_invalid_grants import BaseInvalidGrants
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import BaseSnapshotGrants


@pytest.mark.skip(
    reason="DECO team must provide DBT_TEST_USER_1/2/3 before we re-enable"
)
# @pytest.mark.skip_profile("databricks_cluster", "databricks_sql_endpoint")
class TestModelGrantsDatabricks(BaseModelGrants):
    def privilege_grantee_name_overrides(self):
        # insert --> modify
        return {
            "select": "select",
            "insert": "modify",
            "fake_privilege": "fake_privilege",
            "invalid_user": "invalid_user",
        }


@pytest.mark.skip(
    reason="DECO team must provide DBT_TEST_USER_1/2/3 before we re-enable"
)
# @pytest.mark.skip_profile("databricks_cluster", "databricks_sql_endpoint")
class TestIncrementalGrantsDatabricks(BaseIncrementalGrants):
    pass


@pytest.mark.skip(
    reason="DECO team must provide DBT_TEST_USER_1/2/3 before we re-enable"
)
# @pytest.mark.skip_profile("databricks_cluster", "databricks_sql_endpoint")
class TestSeedGrantsDatabricks(BaseSeedGrants):
    # seeds in dbt-spark are currently "full refreshed," in such a way that
    # the grants are not carried over
    # see https://github.com/dbt-labs/dbt-spark/issues/388
    def seeds_support_partial_refresh(self):
        return False


@pytest.mark.skip(
    reason="DECO team must provide DBT_TEST_USER_1/2/3 before we re-enable"
)
# @pytest.mark.skip_profile("databricks_cluster", "databricks_sql_endpoint")
class TestSnapshotGrantsDatabricks(BaseSnapshotGrants):
    pass


@pytest.mark.skip(
    reason="DECO team must provide DBT_TEST_USER_1/2/3 before we re-enable"
)
# @pytest.mark.skip_profile("databricks_cluster", "databricks_sql_endpoint")
class TestInvalidGrantsDatabricks(BaseInvalidGrants):
    def grantee_does_not_exist_error(self):
        return "PRINCIPAL_DOES_NOT_EXIST"

    def privilege_does_not_exist_error(self):
        return "INVALID_PARAMETER_VALUE"
