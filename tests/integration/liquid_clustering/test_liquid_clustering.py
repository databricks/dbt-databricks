from tests.integration.base import DBTIntegrationTest, use_profile


class TestLiquidClustering(DBTIntegrationTest):
    @property
    def schema(self):
        return "liquid"

    @property
    def models(self):
        return "models"

    @use_profile("databricks_uc_sql_endpoint")
    def test_liquid_clustering_databricks_uc_sql_endpoint(self):
        self.run_dbt()
        self.assert_in_log("optimize")
