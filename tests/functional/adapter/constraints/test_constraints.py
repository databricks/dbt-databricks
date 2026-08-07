import pytest
from dbt.tests import util
from dbt.tests.adapter.constraints import fixtures
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsRollback,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRollback,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
)

from tests.functional.adapter.constraints import fixtures as override_fixtures
from tests.functional.adapter.fixtures import RerunSafeMixin


class DatabricksConstraintsBase:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    @pytest.fixture(scope="class")
    def schema_string_type(self, string_type):
        return string_type

    @pytest.fixture(scope="class")
    def string_type(self):
        return "string"

    @pytest.fixture(scope="class")
    def int_type(self):
        return "int"

    @pytest.fixture(scope="class")
    def schema_int_type(self, int_type):
        return int_type

    @pytest.fixture(scope="class")
    def data_types(self, schema_int_type, int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", int_type, int_type],
            ['"1"', string_type, string_type],
            ["true", "boolean", "boolean"],
            ['array("1","2","3")', "array<string>", "array"],
            ["array(1,2,3)", "array<int>", "array"],
            ["cast('2019-01-01' as date)", "date", "date"],
            ["cast('2019-01-01' as timestamp)", "timestamp", "timestamp"],
            ["cast(1.0 AS DECIMAL(4, 2))", "decimal", "decimal"],
        ]


@pytest.mark.skip_profile("databricks_cluster")
class TestTableConstraintsColumnsEqual(DatabricksConstraintsBase, BaseTableConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": fixtures.my_model_wrong_order_sql,
            "my_model_wrong_name.sql": fixtures.my_model_wrong_name_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestViewConstraintsColumnsEqual(DatabricksConstraintsBase, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": fixtures.my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": fixtures.my_model_view_wrong_name_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalConstraintsColumnsEqual(
    DatabricksConstraintsBase, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": fixtures.my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": fixtures.my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


class BaseConstraintsRollbackSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return [
            "violate the new CHECK constraint",
            "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
            "violate the new NOT NULL constraint",
            "(id > 0) violated by row with values:",  # incremental mats
            "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",  # incremental mats
        ]

    def assert_expected_error_messages(self, error_message, expected_error_messages):
        # This needs to be ANY instead of ALL
        # The CHECK constraint is added before the NOT NULL constraint
        # and different connection types display/truncate the error message in different ways...
        assert any(msg in error_message for msg in expected_error_messages)


@pytest.mark.skip_profile("databricks_cluster")
class TestTableConstraintsRollback(BaseConstraintsRollbackSetup, BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }

    # On Spark/Databricks, constraints are applied *after* the table is replaced.
    # We don't have any way to "rollback" the table to its previous happy state.
    # So the 'color' column will be updated to 'red', instead of 'blue'.
    @pytest.fixture(scope="class")
    def expected_color(self):
        return "red"


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalConstraintsRollback(
    BaseConstraintsRollbackSetup, BaseIncrementalConstraintsRollback
):
    # color stays blue for incremental models since it's a new row that just
    # doesn't get inserted
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_incremental_model_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalForeignKeyExpressionConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.incremental_foreign_key_schema_yml,
            "raw_numbers.sql": fixtures.incremental_foreign_key_model_raw_numbers_sql,
            "stg_numbers.sql": fixtures.incremental_foreign_key_model_stg_numbers_sql,
        }

    def test_incremental_foreign_key_constraint(self, project):
        unformatted_constraint_schema_yml = util.read_file("models", "schema.yml")
        util.write_file(
            unformatted_constraint_schema_yml.format(schema=project.test_schema),
            "models",
            "schema.yml",
        )

        util.run_dbt(["run", "--select", "raw_numbers"])
        util.run_dbt(["run", "--select", "stg_numbers"])
        util.run_dbt(["run", "--select", "stg_numbers"])

        # Verify the expression-form foreign key is registered in information_schema.
        referential_constraints = project.run_sql(
            """
            SELECT constraint_name
            FROM {database}.information_schema.referential_constraints
            WHERE constraint_schema = '{schema}'
            """,
            fetch="all",
        )
        assert any(row[0] == "fk_n" for row in referential_constraints), (
            f"expected FK 'fk_n' from the expression-form constraint, got {referential_constraints}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestCustomConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "custom_constraint_model.sql": override_fixtures.custom_constraint_model_sql,
            "schema.yml": override_fixtures.custom_constraint_schema_yml,
        }

    def test_custom_constraint_applied(self, project):
        util.run_dbt(["run"])
        rows = project.run_sql(
            "show tblproperties {database}.{schema}.custom_constraint_model", fetch="all"
        )
        constraints = {
            row.key: row.value for row in rows if row.key.startswith("delta.constraints.")
        }
        assert constraints.get("delta.constraints.custom_id_positive") == "id > 0", (
            f"custom constraint not persisted as expected; delta.constraints = {constraints}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestForeignKeyParentConstraint:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.parent_foreign_key,
            "parent_table.sql": override_fixtures.parent_sql,
            "child_table.sql": override_fixtures.child_sql,
        }

    def test_foreign_key_constraint(self, project):
        util.run_dbt(["build"])


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalRelyConstraintReconciliation:
    """A RELY expression on a primary key cannot be read back from information_schema, so it
    must not trigger constraint reconciliation on an incremental run. Otherwise the parent PK
    is dropped with CASCADE every run, silently dropping the child's foreign key (#1513).
    """

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.incremental_rely_pk_cascade_schema_yml,
            "rely_parent.sql": override_fixtures.incremental_rely_pk_parent_sql,
            "rely_child.sql": override_fixtures.incremental_rely_pk_child_sql,
        }

    def _foreign_key_names(self, project):
        rows = project.run_sql(
            """
            SELECT constraint_name
            FROM {database}.information_schema.referential_constraints
            WHERE constraint_schema = '{schema}'
            """,
            fetch="all",
        )
        return {row[0] for row in rows}

    def test_rely_pk_reconcile_keeps_dependent_foreign_key(self, project):
        util.run_dbt(["build"])
        assert "fk_rely_child" in self._foreign_key_names(project)

        # A plain incremental re-run of the parent must not reconcile its RELY PK.
        util.run_dbt(["run", "--select", "rely_parent"])

        assert "fk_rely_child" in self._foreign_key_names(project)


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalContractOffPreservesConstraints(RerunSafeMixin):
    """Constraints are reconciled only when the contract is enforced; when unenforced, a no-op."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("contract_off_pk",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": override_fixtures.incremental_contract_off_pk_enforced_schema_yml,
            "contract_off_pk.sql": override_fixtures.incremental_contract_off_pk_sql,
        }

    def _primary_key_names(self, project):
        rows = project.run_sql(
            """
            SELECT constraint_name
            FROM {database}.information_schema.table_constraints
            WHERE table_schema = '{schema}' AND constraint_type = 'PRIMARY KEY'
            """,
            fetch="all",
        )
        return {row[0] for row in rows}

    def test_contract_off_incremental_preserves_existing_pk(self, project):
        # Run 1 (contract enforced): the primary key is created server-side.
        util.run_dbt(["run"])
        assert "pk_contract_off" in self._primary_key_names(project)

        # Flip contract.enforced to false, then run the incremental merge again.
        util.write_file(
            override_fixtures.incremental_contract_off_pk_unenforced_schema_yml,
            "models",
            "schema.yml",
        )
        util.run_dbt(["run"])

        # The existing PK must be left untouched, not silently dropped.
        assert "pk_contract_off" in self._primary_key_names(project)
