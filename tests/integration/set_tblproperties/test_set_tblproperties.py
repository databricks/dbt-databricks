from tests.integration.base import DBTIntegrationTest, use_profile


class TestSetTblproperties(DBTIntegrationTest):
    @property
    def schema(self):
        return "set_tblproperties"

    @property
    def models(self):
        return "models"

    def test_set_tblproperties(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.run_dbt(["run"])

        self.assertTablesEqual("set_tblproperties", "expected")
        self.assertTablesEqual("set_tblproperties_to_view", "expected")

        results = self.run_sql(
            "show tblproperties {database_schema}.set_tblproperties", fetch="all"
        )
        tblproperties = [result[0] for result in results]

        assert "delta.autoOptimize.optimizeWrite" in tblproperties
        assert "delta.autoOptimize.autoCompact" in tblproperties

        results = self.run_sql(
            "show tblproperties {database_schema}.set_tblproperties_to_view", fetch="all"
        )
        tblproperties = [result[0] for result in results]

        assert "tblproperties_to_view" in tblproperties

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
