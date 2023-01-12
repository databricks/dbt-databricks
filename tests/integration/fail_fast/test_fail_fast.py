from dbt.exceptions import FailFastError

from tests.integration.base import DBTIntegrationTest, use_profile


class TesFailFast(DBTIntegrationTest):
    @property
    def schema(self):
        return "fail_fast"

    @property
    def models(self):
        return "models"

    def test_fail_fast(self):
        self.run_dbt(["run"])

        with self.assertRaisesRegex(
            FailFastError, "Failing early due to test failure or runtime error"
        ):
            self.run_dbt(["test", "--fail-fast"])

    @use_profile("databricks_cluster")
    def test_fail_fast_databricks_cluster(self):
        self.test_fail_fast()

    @use_profile("databricks_uc_cluster")
    def test_fail_fast_databricks_uc_cluster(self):
        self.test_fail_fast()

    @use_profile("databricks_sql_endpoint")
    def test_fail_fast_databricks_sql_endpoint(self):
        self.test_fail_fast()

    @use_profile("databricks_uc_sql_endpoint")
    def test_fail_fast_databricks_uc_sql_endpoint(self):
        self.test_fail_fast()
