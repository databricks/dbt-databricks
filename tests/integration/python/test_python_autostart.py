from tests.integration.base import DBTIntegrationTest, use_profile
import os
import pytest


@pytest.mark.skip(
    reason="Run manually. Test must start with the Python compute resource in TERMINATED or \
        TERMINATING state, as the purpose of the test is to validate successful cold start."
)
class TestPythonAutostart(DBTIntegrationTest):
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
                "http_path": os.getenv("DBT_DATABRICKS_CLUSTER_HTTP_PATH"),
            },
        }

    def python_exc(self):
        self.run_dbt(["run"])

    @use_profile("databricks_sql_endpoint")
    def test_python_databricks_sql_endpoint(self):
        self.python_exc()

    @use_profile("databricks_uc_sql_endpoint")
    def test_python_databricks_uc_sql_endpoint(self):
        self.use_default_project(
            {
                "vars": {
                    "http_path": os.getenv("DBT_DATABRICKS_UC_CLUSTER_HTTP_PATH"),
                }
            }
        )
        self.python_exc()
