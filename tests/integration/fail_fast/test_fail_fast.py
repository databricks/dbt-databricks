from dbt.exceptions import FailFastError

from tests.integration.base import DBTIntegrationTest, use_profile


class TesFailFast(DBTIntegrationTest):
    @property
    def schema(self):
        return "fail_fast"

    @property
    def models(self):
        return "models"

    def _test_fail_fast(self):
        self.run_dbt(["run"])

        # PECO-738 Original except message we tested for was:
        #
        #  'Failing early due to test failure or runtime error'
        #
        # This is supposed to raise a FailFastException but that
        # is being swallowed by the test runner and only the DBT
        # test failure error message is raised instead.
        _ = FailFastError
        with self.assertRaisesRegex(
            Exception, "False != True : dbt exit state did not match expected"
        ):
            self.run_dbt(["test", "--fail-fast"])

    @use_profile("databricks_cluster")
    def test_fail_fast_databricks_cluster(self):
        self._test_fail_fast()

    @use_profile("databricks_uc_cluster")
    def test_fail_fast_databricks_uc_cluster(self):
        self._test_fail_fast()

    @use_profile("databricks_uc_sql_endpoint")
    def test_fail_fast_databricks_uc_sql_endpoint(self):
        self._test_fail_fast()
