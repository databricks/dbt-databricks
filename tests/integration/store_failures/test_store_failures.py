from tests.integration.base import DBTIntegrationTest, use_profile

class TestStoreFailures(DBTIntegrationTest):
    @property
    def schema(self):
        return "store_failures"
        
    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'tests': {
                '+store_failures': True,
                '+severity': 'warn',
            }
        }

    def test_store_failures(self):
        self.run_dbt(['run'])
        results = self.run_dbt(['test', '--store-failures'])


class TestStoreFailuresDelta(TestStoreFailures):

    @use_profile("databricks_cluster")
    def test_store_failures_databricks_cluster(self):
        self.test_store_failures()

    @use_profile("databricks_uc_cluster")
    def test_store_failures_databricks_uc_cluster(self):
        self.test_store_failures()

    @use_profile("databricks_sql_endpoint")
    def test_store_failures_databricks_sql_endpoint(self):
        self.test_store_failures()

    @use_profile("databricks_uc_sql_endpoint")
    def test_store_failures_databricks_uc_sql_endpoint(self):
        self.test_store_failures()


class TestStoreFailuresParquet(TestStoreFailures):

    @property
    def project_config(self):
        return {
            'config-version': 2,
            'tests': {
                '+store_failures': True,
                '+severity': 'warn',
                '+file_format': 'parquet',
            }
        }

    @use_profile("databricks_cluster")
    def test_store_failures_databricks_cluster(self):
        self.test_store_failures()
