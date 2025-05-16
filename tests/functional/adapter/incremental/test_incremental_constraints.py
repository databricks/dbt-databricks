import pytest

from dbt.contracts.results import RunStatus
from dbt.tests import util
from tests.functional.adapter.incremental import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalSetNonNullConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "non_null_constraint_sql.sql": fixtures.non_null_constraint_sql,
            "schema.yml": fixtures.schema_without_non_null_constraint,
        }

    def test_add_non_null_constraint(self, project):
        results = util.run_dbt(["run"], expect_pass=True)
        util.write_file(fixtures.schema_with_non_null_constraint, "models", "schema.yml")

        # Non-null constraint is an enforced constraint, so this materialization should fail
        results = util.run_dbt(["run"], expect_pass=False)
        assert results.results[0].status == RunStatus.Error
        assert "DELTA_NOT_NULL_CONSTRAINT_VIOLATED" in results.results[0].message


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalUnsetNonNullConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "non_null_constraint_sql.sql": fixtures.non_null_constraint_sql,
            "schema.yml": fixtures.schema_with_non_null_constraint,
        }

    def test_remove_non_null_constraint(self, project):
        util.run_dbt(["run"])
        # Verify the constraint exists
        columns = project.run_sql(
            """
            SELECT column_name
            FROM {database}.information_schema.columns
            WHERE table_schema = '{schema}'
            AND is_nullable = 'NO'
            """,
            fetch="all",
        )
        assert len(columns) == 1
        assert columns[0][0] == "msg"

        # Remove the non-null constraint
        util.write_file(fixtures.schema_without_non_null_constraint, "models", "schema.yml")
        # This would fail if the constraint was not removed
        util.run_dbt(["run"])


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalSetCheckConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "check_constraint_sql.sql": fixtures.check_constraint_sql,
            "schema.yml": fixtures.schema_without_check_constraint,
        }

    def test_add_check_constraint(self, project):
        results = util.run_dbt(["run"])
        util.write_file(fixtures.schema_with_check_constraint, "models", "schema.yml")

        # Check constraint is an enforced constraint, so this materialization should fail
        results = util.run_dbt(["run"], expect_pass=False)
        assert results.results[0].status == RunStatus.Error
        assert "CHECK constraint" in results.results[0].message


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalRemoveCheckConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "check_constraint_sql.sql": fixtures.check_constraint_sql,
            "schema.yml": fixtures.schema_with_check_constraint,
        }

    def test_remove_check_constraint(self, project):
        # First run with check constraint
        util.run_dbt(["run"])

        # Verify the constraint exists
        tbl_properties = project.run_sql(
            """
            SHOW TBLPROPERTIES {database}.{schema}.check_constraint_sql
            """,
            fetch="all",
        )
        constraint = None
        for row in tbl_properties:
            if str(row[0]).startswith("delta.constraints."):
                constraint = row
                break
        assert constraint is not None

        # Remove check constraint
        util.write_file(fixtures.schema_without_check_constraint, "models", "schema.yml")
        # This would fail if the constraint was not removed
        util.run_dbt(["run"])


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalUpdatePrimaryKeyConstraint:
    primary_key_constraint_sql = """
        SELECT constraint_name, column_name
        FROM {database}.information_schema.key_column_usage
        WHERE constraint_schema = '{schema}'
        ORDER BY ordinal_position
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "primary_key_constraint_sql.sql": fixtures.primary_key_constraint_sql,
            "schema.yml": fixtures.schema_with_single_column_primary_key_constraint,
        }

    def test_update_primary_key_constraint(self, project):
        # First run with single column primary key
        util.run_dbt(["run"])
        primary_key_constraints = project.run_sql(self.primary_key_constraint_sql, fetch="all")
        assert len(primary_key_constraints) == 1
        assert primary_key_constraints[0][0] == "pk_model"
        assert primary_key_constraints[0][1] == "id"

        # Update to composite key. Under the hood, it will trigger both remove/add operations
        util.write_file(
            fixtures.schema_with_composite_primary_key_constraint, "models", "schema.yml"
        )
        util.run_dbt(["run"])
        primary_key_constraints = project.run_sql(self.primary_key_constraint_sql, fetch="all")
        assert len(primary_key_constraints) == 2
        assert primary_key_constraints[0][0] == "pk_model_updated"
        assert primary_key_constraints[0][1] == "id"
        assert primary_key_constraints[1][0] == "pk_model_updated"
        assert primary_key_constraints[1][1] == "version"
        # Verify previous constraint was removed
        assert not any(constraint[0] == "pk_model" for constraint in primary_key_constraints)


referential_constraint_sql = """
    SELECT constraint_name, unique_constraint_name
    FROM {database}.information_schema.referential_constraints
    WHERE constraint_schema = '{schema}'
"""


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalSetForeignKeyConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fk_referenced_to_table.sql": fixtures.fk_referenced_to_table,
            "fk_referenced_from_table.sql": fixtures.fk_referenced_from_table,
            "schema.yml": fixtures.constraint_schema_without_fk_constraint,
        }

    def test_add_foreign_key_constraint(self, project):
        util.run_dbt(["run"])
        referential_constraints = project.run_sql(referential_constraint_sql, fetch="all")
        assert len(referential_constraints) == 0

        # Foreign key constraint is informational only, so we cannot verify enforcement.
        # Instead, check that the metadata is updated correctly.
        util.write_file(fixtures.constraint_schema_with_fk_constraint, "models", "schema.yml")
        util.run_dbt(["run"])
        referential_constraints = project.run_sql(referential_constraint_sql, fetch="all")
        assert len(referential_constraints) == 1
        assert referential_constraints[0][0] == "fk_to_parent"
        assert referential_constraints[0][1] == "pk_parent"


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalRemoveForeignKeyConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "fk_referenced_to_table.sql": fixtures.fk_referenced_to_table,
            "fk_referenced_from_table.sql": fixtures.fk_referenced_from_table,
            "schema.yml": fixtures.constraint_schema_with_fk_constraint,
        }

    def test_remove_foreign_key_constraint(self, project):
        # First run with foreign key constraint
        util.run_dbt(["run"])

        # Verify the constraint exists
        referential_constraints = project.run_sql(referential_constraint_sql, fetch="all")
        assert len(referential_constraints) == 1
        assert referential_constraints[0][0] == "fk_to_parent"
        assert referential_constraints[0][1] == "pk_parent"

        # Remove foreign key constraint and verify
        util.write_file(fixtures.constraint_schema_without_fk_constraint, "models", "schema.yml")
        util.run_dbt(["run"])
        referential_constraints = project.run_sql(referential_constraint_sql, fetch="all")
        assert len(referential_constraints) == 0
