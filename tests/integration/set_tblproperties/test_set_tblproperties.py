from typing import List

from tests.integration.base import DBTIntegrationTest, use_profile


class TestSetTblproperties(DBTIntegrationTest):
    @property
    def schema(self):
        return "set_tblproperties"

    @property
    def models(self):
        return "models"

    def check_tblproperties(self, model_name: str, properties: List[str]):
        results = self.run_sql(
            "show tblproperties {database_schema}.{model_name}",
            fetch="all",
            kwargs=dict(model_name=model_name),
        )
        tblproperties = [result[0] for result in results]

        for prop in properties:
            assert prop in tblproperties

    def check_snapshot_results(self, num_rows: int):
        results = self.run_sql(
            "select * from {database_schema}.my_snapshot", fetch="all"
        )
        self.assertEqual(len(results), num_rows)

    def test_set_tblproperties(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.run_dbt(["run"])
        self.run_dbt(["snapshot"])
        self.run_dbt(["snapshot"])

        self.assertTablesEqual("set_tblproperties", "expected")
        self.assertTablesEqual("set_tblproperties_to_view", "expected")

        self.check_tblproperties(
            "set_tblproperties",
            ["delta.autoOptimize.optimizeWrite", "delta.autoOptimize.autoCompact"],
        )
        self.check_tblproperties("set_tblproperties_to_view", ["tblproperties_to_view"])

        self.check_snapshot_results(num_rows=3)
        self.check_tblproperties("my_snapshot", ["tblproperties_to_snapshot"])

    @use_profile("databricks_cluster")
    def test_set_tblproperties_databricks_cluster(self):
        self.test_set_tblproperties()

    @use_profile("databricks_uc_cluster")
    def test_set_tblproperties_databricks_uc_cluster(self):
        self.test_set_tblproperties()

    @use_profile("databricks_sql_endpoint")
    def test_set_tblproperties_databricks_sql_endpoint(self):
        self.test_set_tblproperties()

    @use_profile("databricks_uc_sql_endpoint")
    def test_set_tblproperties_databricks_uc_sql_endpoint(self):
        self.test_set_tblproperties()
