import pytest

from dbt.tests import util
from tests.functional.adapter.persist_constraints import fixtures
from tests.functional.adapter.persist_constraints.test_persist_constraints import TestConstraints


class TestConstraintsV2(TestConstraints):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_model.sql": fixtures.incremental_model,
            "table_model.sql": fixtures.base_model,
            "invalid_check_constraint.sql": fixtures.base_model,
            "invalid_column_constraint.sql": fixtures.base_model,
            "table_model_disable_constraints.sql": fixtures.base_model,
            "table_model_contract.sql": fixtures.base_model,
            "schema.yml": fixtures.schema_v2_yml,
        }


class TestTableConstraintsV2(TestConstraintsV2):
    def test_table_constraints(self, project):
        util.run_dbt(["seed"])
        model_name = "table_model"
        expected_model_name = "expected_model"
        updated_model_name = "expected_model_with_invalid_name"
        util.run_dbt(["run", "--select", model_name])
        util.run_dbt(["run", "--select", expected_model_name])
        util.check_relations_equal(project.adapter, [model_name, expected_model_name])

        self.check_constraints(
            project, model_name, {"delta.constraints.id_greater_than_zero": "id > 0"}
        )

        # Insert a row into the seed model that violates the NOT NULL constraint on name.
        project.run_sql(fixtures.insert_invalid_name)
        self.run_and_check_failure(
            model_name, err_msg="violate the new NOT NULL constraint on name"
        )
        self.check_staging_table_cleaned(project)

        # Check the table is still created with the invalid row.
        util.run_dbt(["run", "--select", updated_model_name])
        util.check_relations_equal(project.adapter, [model_name, updated_model_name])


class TestIncrementalConstraintsV2(TestConstraintsV2):
    def test_incremental_constraints(self, project):
        util.run_dbt(["seed"])
        model_name = "incremental_model"
        util.run_dbt(["run", "--select", model_name, "--full-refresh"])
        self.check_constraints(
            project, model_name, {"delta.constraints.id_greater_than_zero": "id > 0"}
        )

        # Insert a row into the seed model with an invalid id.
        project.run_sql(fixtures.insert_invalid_id)
        self.run_and_check_failure(
            model_name,
            err_msg="CHECK constraint id_greater_than_zero",
        )
        self.check_staging_table_cleaned(project)
        project.run_sql("delete from {database}.{schema}.seed where id = 0")

        # Insert a row into the seed model with an invalid name.
        project.run_sql(fixtures.insert_invalid_name)
        self.run_and_check_failure(
            model_name, err_msg="NOT NULL constraint violated for column: name"
        )
        self.check_staging_table_cleaned(project)
        project.run_sql("delete from {database}.{schema}.seed where id = 3")

        # Insert a valid row into the seed model.
        project.run_sql("insert into {database}.{schema}.seed values (3, 'Cathy', '2022-03-01')")
        util.run_dbt(["run", "--select", model_name])
        expected_model_name = "expected_incremental_model"
        util.run_dbt(["run", "--select", expected_model_name])
        util.check_relations_equal(project.adapter, [model_name, expected_model_name])


class TestSnapshotConstraintsV2(TestConstraintsV2):
    def check_snapshot_results(self, project, num_rows: int):
        results = project.run_sql("select * from {database}.{schema}.my_snapshot", fetch="all")
        assert len(results) == num_rows

    def test_snapshot(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["snapshot"])
        self.check_snapshot_results(project, num_rows=2)
        self.check_staging_table_cleaned(project)

        project.run_sql(fixtures.insert_invalid_name)
        results = util.run_dbt(["snapshot"], expect_pass=False)
        assert "NOT NULL constraint violated for column: name" in results.results[0].message
        self.check_staging_table_cleaned(project)

        util.run_dbt(["seed"])
        project.run_sql(fixtures.insert_invalid_id)
        results = util.run_dbt(["snapshot"], expect_pass=False)
        assert "CHECK constraint id_greater_than_zero" in results.results[0].message
        self.check_staging_table_cleaned(project)

        # Check the snapshot table is not updated.
        self.check_snapshot_results(project, num_rows=2)


class TestInvalidCheckConstraintsV2(TestConstraintsV2):
    def test_invalid_check_constraints(self, project):
        model_name = "invalid_check_constraint"
        util.run_dbt(["seed"])
        self.run_and_check_failure(model_name, err_msg=" Could not parse constraint")


class TestInvalidColumnConstraintsV2(TestConstraintsV2):
    def _test_invalid_column_constraints(self, project):
        model_name = "invalid_column_constraint"
        util.run_dbt(["seed"])
        self.run_and_check_failure(
            model_name,
            err_msg="Invalid constraint for column id. Only `not_null` is supported.",
        )


class TestTableWithConstraintsDisabledV2(TestConstraintsV2):
    def test_delta_constraints_disabled(self, project):
        util.run_dbt(["seed"])
        model_name = "table_model_disable_constraints"
        expected_model_name = "expected_model"
        updated_model_name = "expected_model_with_invalid_name"
        util.run_dbt(["run", "--select", model_name])
        util.run_dbt(["run", "--select", expected_model_name])
        util.check_relations_equal(project.adapter, [model_name, expected_model_name])

        # No check constraint should be added.
        self.check_constraints(project, model_name, {})

        # Insert a row into the seed model with the name being null.
        project.run_sql("insert into {database}.{schema}.seed values (3, null, '2022-03-01')")

        # Check the table can be created without failure.
        util.run_dbt(["run", "--select", model_name])
        util.run_dbt(["run", "--select", updated_model_name])
        util.check_relations_equal(project.adapter, [model_name, updated_model_name])
