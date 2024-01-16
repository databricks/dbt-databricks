from tests.integration.base import DBTIntegrationTest, use_profile
import os
import json
import tempfile


class TestRunResultsJson(DBTIntegrationTest):
    def setUp(self):
        self.tempdir: tempfile.TemporaryDirectory = tempfile.TemporaryDirectory()
        return super().setUp()

    def tearDown(self):
        self.tempdir.cleanup()
        return super().tearDown()

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "models": {"materialized": "table"},
            "target-path": self.tempdir.name,
        }

    @property
    def models(self):
        return "models"

    @property
    def schema(self):
        return "test_results_json"

    def run_and_check_for_query_id(self):
        self.run_dbt(["run"])

        _fhpath = os.path.join(self.tempdir.name, "run_results.json")
        with open(_fhpath, "r") as results_json_raw:
            results_json = json.load(results_json_raw)
            self.assertIsNotNone(
                results_json["results"][0]["adapter_response"].get("query_id"),
                "Query ID column was not written to run_results.json",
            )

    @use_profile("databricks_uc_sql_endpoint")
    def test_run_results_json_databricks_uc_sql_endpoint(self):
        self.run_and_check_for_query_id()
