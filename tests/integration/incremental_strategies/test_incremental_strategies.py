from tests.integration.base import DBTIntegrationTest, use_profile


class TestIncrementalStrategies(DBTIntegrationTest):
    @property
    def schema(self):
        return "incremental_strategies"

    @property
    def project_config(self):
        return {
            "seeds": {
                "quote_columns": False,
            },
        }

    def seed_and_run_once(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])

    def seed_and_run_twice(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run"])
        self.run_dbt(["run"])


class TestParquetAppend(TestIncrementalStrategies):
    @property
    def models(self):
        return "models"

    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("default_append", "expected_append")

    @use_profile("databricks_cluster")
    def test_default_append_databricks_cluster(self):
        self.run_and_test()


class TestParquetInsertOverwrite(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_insert_overwrite"

    def run_and_test(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("insert_overwrite_no_partitions", "expected_overwrite")
        self.assertTablesEqual("insert_overwrite_partitions", "expected_upsert")

    @use_profile("databricks_cluster")
    def test_insert_overwrite_databricks_cluster(self):
        self.run_and_test()


class TestDeltaStrategiesWarehouse(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_delta_warehouse"

    def run_and_test_warehouse(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("append_delta", "expected_append")
        self.assertTablesEqual("merge_no_key", "expected_append")
        self.assertTablesEqual("merge_unique_key", "expected_upsert")
        self.assertTablesEqual("merge_update_columns", "expected_partial_upsert")
        self.assertTablesEqual("merge_exclude_columns", "expected_exclude_upsert")
        self.assertTablesEqual("replace_where", "expected_replace_where")

    @use_profile("databricks_sql_endpoint")
    def test_delta_strategies_databricks_sql_endpoint(self):
        self.run_and_test_warehouse()

    @use_profile("databricks_uc_sql_endpoint")
    def test_delta_strategies_databricks_uc_sql_endpoint(self):
        self.run_and_test_warehouse()


class TestDeltaStrategiesCluster(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_delta_cluster"

    @use_profile("databricks_cluster")
    def test_delta_strategies_databricks_cluster(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("append_delta", "expected_append")
        self.assertTablesEqual("merge_no_key", "expected_append")
        self.assertTablesEqual("merge_unique_key", "expected_upsert")
        self.assertTablesEqual("merge_update_columns", "expected_partial_upsert")
        self.assertTablesEqual("merge_exclude_columns", "expected_exclude_upsert")
        self.assertTablesEqual("insert_overwrite_no_partitions", "expected_overwrite")
        self.assertTablesEqual("insert_overwrite_partitions", "expected_upsert")
        self.assertTablesEqual("replace_where", "expected_replace_where")


class TestDeltaStrategiesClusterUC(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_delta_cluster_uc"

    @use_profile("databricks_uc_cluster")
    def test_delta_strategies_databricks_uc_cluster(self):
        self.seed_and_run_twice()
        self.assertTablesEqual("append_delta", "expected_append")
        self.assertTablesEqual("merge_no_key", "expected_append")
        self.assertTablesEqual("merge_unique_key", "expected_upsert")
        self.assertTablesEqual("merge_update_columns", "expected_partial_upsert")
        self.assertTablesEqual("merge_exclude_columns", "expected_exclude_upsert")
        self.assertTablesEqual("replace_where", "expected_replace_where")


class TestBadStrategies(TestIncrementalStrategies):
    @property
    def models(self):
        return "models_bad"

    def run_and_test(self):
        results = self.run_dbt(["run"], expect_pass=False)
        # assert all models fail with compilation errors
        for result in results:
            self.assertEqual("error", result.status)
            self.assertIn("Compilation Error in model", result.message)

    @use_profile("databricks_cluster")
    def test_bad_strategies_databricks_cluster(self):
        self.run_and_test()

    @use_profile("databricks_uc_cluster")
    def test_bad_strategies_databricks_uc_cluster(self):
        self.run_and_test()

    @use_profile("databricks_sql_endpoint")
    def test_bad_strategies_databricks_sql_endpoint(self):
        self.run_and_test()

    @use_profile("databricks_uc_sql_endpoint")
    def test_bad_strategies_databricks_uc_sql_endpoint(self):
        self.run_and_test()
