from tests.integration.base import DBTIntegrationTest, use_profile
import os


class TestCustomIndex(DBTIntegrationTest):
    @property
    def schema(self):
        return "python"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "vars": {
                "http_path": os.getenv("DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH"),
                "location_root": "",
            },
        }

    def python_exc(self):
        self.run_dbt(["run", "-s", "custom_index"])

    @use_profile("databricks_uc_sql_endpoint")
    def test_python_databricks_uc_sql_endpoint(self):
        self.python_exc()
