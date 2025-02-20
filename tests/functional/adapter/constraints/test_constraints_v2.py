import pytest

from dbt.tests import util
from dbt.tests.adapter.constraints import fixtures
from dbt.tests.adapter.constraints.test_constraints import (
    _find_and_replace,
    _normalize_whitespace,
)
from tests.functional.adapter.constraints import fixtures as override_fixtures
from tests.functional.adapter.constraints.test_constraints import (
    BaseConstraintsRollback,
    BaseIncrementalConstraintsColumnsEqual,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    DatabricksConstraintsBase,
)
from tests.functional.adapter.fixtures import MaterializationV2Mixin


class DatabricksConstraintsBaseV2(DatabricksConstraintsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}


class BaseV2ConstraintSetup:
    @pytest.fixture(scope="class")
    def override_config(self):
        return {}

    @pytest.fixture(scope="class")
    def project_config_update(self, override_config):
        config = {"flags": {"use_materialization_v2": True}}
        config.update(override_config)
        return config

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return override_fixtures.expected_sql_v2

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_wrong_order_depends_on_fk_sql,
            "foreign_key_model.sql": fixtures.foreign_key_model_sql,
            "constraints_schema.yml": fixtures.model_fk_constraint_schema_yml.replace(
                "text", "string"
            ).replace("- type: unique", ""),
        }

    def test__constraints_ddl(self, project, expected_sql):
        results, log = util.run_dbt_and_capture(["run", "--debug"])
        assert len(results) >= 1

        generated_sql_generic = _find_and_replace(log, "my_model", "<model_identifier>")
        generated_sql_generic = _find_and_replace(
            generated_sql_generic, "foreign_key_model", "<foreign_key_model_identifier>"
        )

        normalized = _normalize_whitespace(generated_sql_generic)
        assert _normalize_whitespace(expected_sql) in normalized
        assert _normalize_whitespace("ALTER TABLE <model_identifier> ADD CONSTRAINT") in normalized


@pytest.mark.skip_profile("databricks_cluster")
class TestTableConstraintsColumnsEqual(
    DatabricksConstraintsBaseV2, BaseTableConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": fixtures.my_model_wrong_order_sql,
            "my_model_wrong_name.sql": fixtures.my_model_wrong_name_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestViewConstraintsColumnsEqual(DatabricksConstraintsBaseV2, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": fixtures.my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": fixtures.my_model_view_wrong_name_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalConstraintsColumnsEqual(
    DatabricksConstraintsBaseV2, BaseIncrementalConstraintsColumnsEqual
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": fixtures.my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": fixtures.my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


class TestConstraintQuotedColumn(MaterializationV2Mixin):
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
create or replace table <model_identifier> (
    `from` string not null,
    id integer not null comment 'hello',
    date_day string
)
    using delta
"""

    def test__constraints_ddl(self, project, expected_sql):
        results, logs = util.run_dbt_and_capture(["run", "--debug", "-s", "+my_model"])
        assert len(results) >= 1
        generated_sql_generic = _find_and_replace(logs, "my_model", "<model_identifier>")
        normalized = _normalize_whitespace(generated_sql_generic)
        assert _normalize_whitespace(expected_sql) in normalized
        assert _normalize_whitespace("ALTER TABLE <model_identifier> ADD CONSTRAINT") in normalized
        assert _normalize_whitespace("CHECK (`from` = 'blue')") in normalized


@pytest.mark.skip_profile("databricks_cluster")
class TestTableConstraintsDdlEnforcement(BaseV2ConstraintSetup):
    pass


@pytest.mark.skip("Haven't implemented incremental for V2 yet")
@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalConstraintsDdlEnforcement(
    BaseV2ConstraintSetup,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": fixtures.my_model_incremental_wrong_order_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }


class BaseDatabricksConstraintHandling(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}, "models": {"safe_table_create+": True}}

    def test__constraints_enforcement_rollback(
        self, project, expected_color, expected_error_messages, null_model_sql
    ):
        results = util.run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1

        # Make a contract-breaking change to the model
        util.write_file(null_model_sql, "models", "my_model.sql")

        failing_results = util.run_dbt(["run", "-s", "my_model"], expect_pass=False)
        assert len(failing_results) == 1

        # Verify the previous table still exists
        relation = util.relation_from_name(project.adapter, "my_model")
        old_model_exists_sql = f"select * from {relation}"
        old_model_exists = project.run_sql(old_model_exists_sql, fetch="all")
        assert len(old_model_exists) == 1


@pytest.mark.skip_profile("databricks_cluster")
class TestTableConstraintsRollback(BaseDatabricksConstraintHandling):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": override_fixtures.my_model_sql,
            "constraints_schema.yml": override_fixtures.constraints_yml,
        }
