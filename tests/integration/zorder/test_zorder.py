from tests.integration.base import DBTIntegrationTest, use_profile
from dbt.tests.util import (
    run_dbt_and_capture,
)


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

    def test_zorder(self):
        self.run_dbt(["run"])
        self.run_dbt(["run"]) # make sure it also run in incremental
        

    @use_profile("databricks_cluster")
    def test_zorder_databricks_cluster(self):
        self.test_zorder()

    @use_profile("databricks_uc_cluster")
    def test_zorder_databricks_uc_cluster(self):
        self.test_zorder()

    @use_profile("databricks_sql_endpoint")
    def test_zorder_databricks_sql_endpoint(self):
        self.test_zorder()

    @use_profile("databricks_uc_sql_endpoint")
    def test_zorder_databricks_uc_sql_endpoint(self):
        self.test_zorder()
