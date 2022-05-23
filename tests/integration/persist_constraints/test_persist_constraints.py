from tests.integration.base import DBTIntegrationTest, use_profile
from typing import Dict
from dbt.contracts.results import RunResult, RunStatus


class TestConstraints(DBTIntegrationTest):
    @property
    def schema(self):
        return "constraints"

    @property
    def models(self):
        return "models"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "models": {"test": {"+persist_constraints": True}},
            "snapshots": {"test": {"+persist_constraints": True}},
        }

    def check_constraints(self, model_name: str, expected: Dict[str, str]):
        rows = self.run_sql(f"show tblproperties {self.database_schema}.{model_name}", fetch="all")
        constraints = {
            row.key: row.value for row in rows if row.key.startswith("delta.constraints")
        }
        assert len(constraints) == len(expected)
        self.assertDictEqual(constraints, expected)

    def run_and_check_failure(self, model_name: str, err_msg: str):
        result = self.run_dbt(["run", "--select", model_name], expect_pass=False)
        assert len(result.results) == 1
        res: RunResult = result.results[0]
        assert res.status == RunStatus.Error
        assert err_msg in res.message

    def check_staging_table_cleaned(self):
        tmp_tables = self.run_sql("SHOW TABLES IN {database_schema} LIKE '*__dbt_tmp'", fetch="all")
        assert len(tmp_tables) == 0


class TestTableConstraints(TestConstraints):
    def test_table_constraints(self):
        self.run_dbt(["seed"])
        model_name = "table_model"
        expected_model_name = "expected_model"
        updated_model_name = "expected_model_with_invalid_name"
        self.run_dbt(["run", "--select", model_name])
        self.run_dbt(["run", "--select", expected_model_name])
        self.assertTablesEqual(model_name, expected_model_name)

        self.check_constraints(model_name, {"delta.constraints.id_greater_than_zero": "id > 0"})

        # Insert a row into the seed model that violates the NOT NULL constraint on name.
        self.run_sql_file("insert_invalid_name.sql")
        self.run_and_check_failure(
            model_name, err_msg="violate the new NOT NULL constraint on name"
        )
        self.check_staging_table_cleaned()

        # Check the table is still created with the invalid row.
        self.run_dbt(["run", "--select", updated_model_name])
        self.assertTablesEqual(model_name, updated_model_name)

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self.test_table_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self.test_table_constraints()

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_table_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self.test_table_constraints()


class TestIncrementalConstraints(TestConstraints):
    def test_incremental_constraints(self):
        self.run_dbt(["seed"])
        model_name = "incremental_model"
        self.run_dbt(["run", "--select", model_name, "--full-refresh"])
        self.check_constraints(model_name, {"delta.constraints.id_greater_than_zero": "id > 0"})

        # Insert a row into the seed model with an invalid id.
        self.run_sql_file("insert_invalid_id.sql")
        self.run_and_check_failure(
            model_name,
            err_msg="CHECK constraint id_greater_than_zero",
        )
        self.check_staging_table_cleaned()
        self.run_sql("delete from {database_schema}.seed where id = 0")

        # Insert a row into the seed model with an invalid name.
        self.run_sql_file("insert_invalid_name.sql")
        self.run_and_check_failure(
            model_name, err_msg="NOT NULL constraint violated for column: name"
        )
        self.check_staging_table_cleaned()
        self.run_sql("delete from {database_schema}.seed where id = 3")

        # Insert a valid row into the seed model.
        self.run_sql("insert into {database_schema}.seed values (3, 'Cathy', '2022-03-01')")
        self.run_dbt(["run", "--select", model_name])
        expected_model_name = "expected_incremental_model"
        self.run_dbt(["run", "--select", expected_model_name])
        self.assertTablesEqual(model_name, expected_model_name)

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self.test_incremental_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self.test_incremental_constraints()

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_incremental_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self.test_incremental_constraints()


class TestSnapshotConstraints(TestConstraints):
    def check_snapshot_results(self, num_rows: int):
        results = self.run_sql("select * from {database_schema}.my_snapshot", fetch="all")
        self.assertEqual(len(results), num_rows)

    def test_snapshot(self):
        self.run_dbt(["seed"])
        self.run_dbt(["snapshot"])
        self.check_snapshot_results(num_rows=2)
        self.check_staging_table_cleaned()

        self.run_sql_file("insert_invalid_name.sql")
        results = self.run_dbt(["snapshot"], expect_pass=False)
        assert "NOT NULL constraint violated for column: name" in results.results[0].message
        self.check_staging_table_cleaned()

        self.run_dbt(["seed"])
        self.run_sql_file("insert_invalid_id.sql")
        results = self.run_dbt(["snapshot"], expect_pass=False)
        assert "CHECK constraint id_greater_than_zero" in results.results[0].message
        self.check_staging_table_cleaned()

        # Check the snapshot table is not updated.
        self.check_snapshot_results(num_rows=2)

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self.test_snapshot()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self.test_snapshot()

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_snapshot()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self.test_snapshot()


class TestInvalidCheckConstraints(TestConstraints):
    def test_invalid_check_constraints(self):
        model_name = "invalid_check_constraint"
        self.run_dbt(["seed"])
        self.run_and_check_failure(model_name, err_msg="Invalid check constraint condition")

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self.test_invalid_check_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self.test_invalid_check_constraints()

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_invalid_check_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self.test_invalid_check_constraints()


class TestInvalidColumnConstraints(TestConstraints):
    def test_invalid_column_constraints(self):
        model_name = "invalid_column_constraint"
        self.run_dbt(["seed"])
        self.run_and_check_failure(
            model_name,
            err_msg="Invalid constraint for column id. Only `not_null` is supported.",
        )

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self.test_invalid_column_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self.test_invalid_column_constraints()

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_invalid_column_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self.test_invalid_column_constraints()


class TestTableWithConstraintsDisabled(TestConstraints):
    def test_delta_constraints_disabled(self):
        self.run_dbt(["seed"])
        model_name = "table_model_disable_constraints"
        expected_model_name = "expected_model"
        updated_model_name = "expected_model_with_invalid_name"
        self.run_dbt(["run", "--select", model_name])
        self.run_dbt(["run", "--select", expected_model_name])
        self.assertTablesEqual(model_name, expected_model_name)

        # No check constraint should be added.
        self.check_constraints(model_name, {})

        # Insert a row into the seed model with the name being null.
        self.run_sql("insert into {database_schema}.seed values (3, null, '2022-03-01')")

        # Check the table can be created without failure.
        self.run_dbt(["run", "--select", model_name])
        self.run_dbt(["run", "--select", updated_model_name])
        self.assertTablesEqual(model_name, updated_model_name)

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self.test_delta_constraints_disabled()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self.test_delta_constraints_disabled()

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_delta_constraints_disabled()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self.test_delta_constraints_disabled()
