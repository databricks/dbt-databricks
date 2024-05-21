import pytest

from dbt.tests import util
from dbt.tests.adapter.constraints import fixtures
from dbt.tests.adapter.constraints.test_constraints import BaseConstraintQuotedColumn
from dbt.tests.adapter.constraints.test_constraints import BaseConstraintsRollback
from dbt.tests.adapter.constraints.test_constraints import (
    BaseConstraintsRuntimeDdlEnforcement,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseIncrementalConstraintsColumnsEqual,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseIncrementalConstraintsRollback,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
)
from dbt.tests.adapter.constraints.test_constraints import (
    BaseViewConstraintsColumnsEqual,
)
from tests.functional.adapter.constraints import fixtures as override_fixtures


class DatabricksConstraintsBase:
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


class BaseConstraintsDdlEnforcementSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            }
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return override_fixtures.expected_sql


@pytest.mark.skip_profile("databricks_cluster")
class TestTableConstraintsDdlEnforcement(
    BaseConstraintsDdlEnforcementSetup, BaseConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_wrong_order_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalConstraintsDdlEnforcement(
    BaseConstraintsDdlEnforcementSetup,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_incremental_wrong_order_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


class BaseConstraintsRollbackSetup:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "delta",
            }
        }

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
class TestIncrementalForeignKeyConstraint:
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


class TestConstraintQuotedColumn(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": fixtures.model_quoted_column_schema_yml.replace(
                "text", "string"
            ).replace('"from"', "`from`"),
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create or replace table <model_identifier>
    using delta
    as
select
  id,
  `from`,
  date_day
from

(
    select
    'blue' as `from`,
    1 as id,
    '2019-01-01' as date_day ) as model_subq
"""
