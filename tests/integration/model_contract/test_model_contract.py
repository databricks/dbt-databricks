import os
import pytest
from tests.integration.base import DBTIntegrationTest, use_profile
from typing import Dict
from dbt.contracts.results import RunResult, RunStatus
from dbt.logger import log_manager
from dbt.cli.main import dbtRunner
from dbt.adapters.events.logging import AdapterLogger

logger = AdapterLogger("Databricks")


class TestModelContract(DBTIntegrationTest):
    @property
    def schema(self):
        return "constraints"

    @property
    def models(self):
        return "models"

    def check_constraints(self, model_name: str, expected: Dict[str, str]):
        rows = self.run_sql(
            "show tblproperties {database_schema}.{model_name}",
            fetch="all",
            kwargs=dict(model_name=model_name),
        )
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
        assert res.message and err_msg in res.message

    def check_staging_table_cleaned(self):
        tmp_tables = self.run_sql("SHOW TABLES IN {database_schema} LIKE '*__dbt_tmp'", fetch="all")
        assert len(tmp_tables) == 0


class TestModelContractConstraints(TestModelContract):
    def _test_table_constraints(self):
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
        self._test_table_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_table_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_table_constraints()


class TestIncrementalModelContractConstraints(TestModelContract):
    def _test_incremental_constraints(self):
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
        self._test_incremental_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_incremental_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_incremental_constraints()


class TestInvalidModelContractCheckConstraints(TestModelContract):
    @property
    def models(self):
        return "invalid_check_constraint"

    def run_dbt(self, args=None, profiles_dir=True):
        log_manager.reset_handlers()
        if args is None:
            args = ["run"]

        final_args = []

        if os.getenv("DBT_TEST_SINGLE_THREADED") in ("y", "Y", "1"):
            final_args.append("--single-threaded")

        final_args.extend(args)

        if profiles_dir:
            final_args.extend(["--profiles-dir", self.test_root_dir])

        logger.info("Invoking dbt with {}".format(final_args))
        res = dbtRunner().invoke(args, log_cache_events=True, log_path=self._logs_dir)
        return res

    def _test_invalid_check_constraints(self):
        model_name = "invalid_check_constraint"
        res = self.run_dbt(["run", "--select", model_name])
        self.assertFalse(res.success, "dbt exit state did not match expected")

        # dbt 1.5.2 changed the error message returned when an invalid constraint is
        # encountered. Check for either to maintain compatability with all 1.5.x versions.
        expectedErrors = ["Constraint validation failed", "Contract enforcement failed"]
        assert res.exception and [m for m in expectedErrors if m in res.exception.msg]

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self._test_invalid_check_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_invalid_check_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_invalid_check_constraints()


class TestInvalidModelContractColumnConstraints(TestModelContract):
    def _test_invalid_column_constraints(self):
        self.run_dbt(["seed"])
        model_name = "invalid_column_constraint"
        self.run_and_check_failure(
            model_name,
            err_msg="Runtime Error in model invalid_column_constraint",
        )

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self._test_invalid_column_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_invalid_column_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_invalid_column_constraints()


class TestTableWithModelContractConstraintsDisabled(TestModelContract):
    def _test_delta_constraints_disabled(self):
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
        self._test_delta_constraints_disabled()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_delta_constraints_disabled()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_delta_constraints_disabled()


class TestModelLevelPrimaryKey(TestModelContract):
    def _test_table_constraints(self):
        self.run_dbt(["seed"])
        model_name = "primary_key"
        self.run_dbt(["run", "--select", model_name])

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_table_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_table_constraints()


class TestModelLevelForeignKey(TestModelContract):
    def _test_table_constraints(self):
        self.run_dbt(["seed"])
        self.run_dbt(["run", "--select", "foreign_key_parent"])
        self.run_dbt(["run", "--select", "foreign_key"])

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_table_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_table_constraints()


@pytest.mark.skip(reason="test needs redesign")
class TestModelContractNotDelta(TestModelContract):
    def _test_table_constraints(self):
        self.run_dbt(["seed"])
        model_name = "not_delta"
        result, logoutput = self.run_dbt_and_capture(
            ["run", "--select", model_name], expect_pass=True
        )
        assert "Constraints not supported for file format: parquet" in logoutput

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self._test_table_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_table_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_table_constraints()


class TestModelContractView(TestModelContract):
    def _test_table_constraints(self):
        # persist_constraints should not be called for a view materialization. If it is and
        # error is raised.
        # Successfully creating the view indicates that it wasn't called.
        self.run_dbt(["seed"])
        model_name = "a_view"
        self.run_dbt(["run", "--select", model_name])

    @use_profile("databricks_cluster")
    def test_databricks_cluster(self):
        self._test_table_constraints()

    @use_profile("databricks_uc_cluster")
    def test_databricks_uc_cluster(self):
        self._test_table_constraints()

    @use_profile("databricks_uc_sql_endpoint")
    def test_databricks_uc_sql_endpoint(self):
        self._test_table_constraints()
