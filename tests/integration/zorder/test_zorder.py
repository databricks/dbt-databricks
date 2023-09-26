from tests.integration.base import DBTIntegrationTest, use_profile


class TestZOrder(DBTIntegrationTest):
    @property
    def schema(self):
        return "zorder"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {"config-version": 2}

    def _test_zorder(self):
        self.run_dbt(["run"])
        self.run_dbt(["run"])
        self.assert_in_log("zorder by")  # make sure it also run in incremental

    @use_profile("databricks_cluster")
    def test_zorder_databricks_cluster(self):
        self._test_zorder()

    @use_profile("databricks_uc_cluster")
    def test_zorder_databricks_uc_cluster(self):
        self._test_zorder()

    @use_profile("databricks_uc_sql_endpoint")
    def test_zorder_databricks_uc_sql_endpoint(self):
        self._test_zorder()
