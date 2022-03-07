from tests.integration.base import DBTIntegrationTest, use_profile


class TestCurrentCatalog(DBTIntegrationTest):
    @property
    def schema(self):
        return "current_catalog"

    @property
    def models(self):
        return "models"

    def unity_catalog_enabled(self):
        results = self.run_sql(
            'SET spark.databricks.unityCatalog.enabled',
            fetch='all'
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0][1], "true")

    def run_and_test(self):
        self.run_dbt(['seed'])
        self.run_dbt(["run"])
        self.assertTablesEqual("current_catalog", "expected")

    @use_profile("databricks_uc_cluster")
    def test_current_catalog_databricks_uc_cluster(self):
        self.unity_catalog_enabled()
        self.run_and_test()

    @use_profile("databricks_uc_sql_endpoint")
    def test_current_catalog_databricks_uc_sql_endpoint(self):
        self.run_and_test()
