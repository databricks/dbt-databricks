from tests.integration.base import DBTIntegrationTest, use_profile
from pathlib import Path
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
            "models": {
                "test": {
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            },
        }

    def check_constraints(self, model_name: str, expected: Dict[str, str]):
        rows = self.run_sql(
            f"show tblproperties {self.unique_schema()}.{model_name}", fetch="all"
        )
        constraints = {
            row.key: row.value
            for row in rows
            if row.key.startswith("delta.constraints")
        }
        assert len(constraints) == len(expected)
        self.assertDictEqual(constraints, expected)

    def run_and_check_failure(self, model_name: str, err_msg: str):
        result = self.run_dbt(["run", "--select", model_name], expect_pass=False)
        assert len(result.results) == 1
        res: RunResult = result.results[0]
        assert res.status == RunStatus.Error
        assert err_msg in res.message


class TestTableConstraints(TestConstraints):
    def test_delta_constraints(self):
        self.run_dbt(["seed"])
        model_name = "table_model"
        expected_model_name = "expected_model"
        self.run_dbt(["run", "--select", model_name])
        self.run_dbt(["run", "--select", expected_model_name])
        self.assertTablesEqual(model_name, expected_model_name)

        expected_constraints = {"delta.constraints.id_greater_than_zero": "id > 0"}
        self.check_constraints(model_name, expected_constraints)

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_delta_constraints()


class TestIncrementalConstraints(TestConstraints):
    def test_incremental_delta_constraints(self):
        self.run_dbt(["seed"])
        model_name = "incremental_model"
        self.run_dbt(["run", "--select", model_name, "--full-refresh"])
        self.check_constraints(
            model_name, {"delta.constraints.id_greater_than_zero": "id > 0"}
        )

        schema = self.unique_schema()

        # Insert a row into the seed model with an invalid id.
        self.run_sql(f"insert into {schema}.seed values (0, 'Cathy', '2022-03-01')")
        self.run_and_check_failure(
            model_name,
            err_msg="CHECK constraint id_greater_than_zero (id > 0) violated",
        )
        self.run_sql(f"delete from {schema}.seed where id = 0")

        # Insert a row into the seed model with an invalid name.
        self.run_sql(f"insert into {schema}.seed values (3, null, '2022-03-01')")
        self.run_and_check_failure(
            model_name, err_msg="NOT NULL constraint violated for column: name"
        )
        self.run_sql(f"delete from {schema}.seed where id = 3")

        # Insert a valid row into the seed model.
        self.run_sql(f"insert into {schema}.seed values (3, 'Cathy', '2022-03-01')")
        self.run_dbt(["run", "--select", model_name])
        expected_model_name = "expected_incremental_model"
        self.run_dbt(["run", "--select", expected_model_name])
        self.assertTablesEqual(model_name, expected_model_name)

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_incremental_delta_constraints()


class TestInvalidCheckConstraints(TestConstraints):
    def test_invalid_check_constraints(self):
        model_name = "invalid_check_constraint"
        self.run_dbt(["seed"])
        self.run_and_check_failure(
            model_name, err_msg="Invalid check constraint condition"
        )

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_invalid_check_constraints()


class TestInvalidColumnConstraints(TestConstraints):
    def test_invalid_column_constraints(self):
        model_name = "invalid_column_constraint"
        self.run_dbt(["seed"])
        self.run_and_check_failure(
            model_name,
            err_msg="Invalid constraint for column id. Only `not_null` is supported.",
        )

    @use_profile("databricks_sql_endpoint")
    def test_databricks_sql_endpoint(self):
        self.test_invalid_column_constraints()
