import pytest
from dbt.tests import util
from dbt.tests.adapter.constraints import fixtures

from tests.functional.adapter.constraints import fixtures as override_fixtures
from tests.functional.adapter.constraints.test_constraints import (
    BaseConstraintsRollback,
    BaseIncrementalConstraintsColumnsEqual,
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    DatabricksConstraintsBase,
)


class DatabricksConstraintsBaseV2(DatabricksConstraintsBase):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}


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


class BaseDatabricksConstraintHandling(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+use_safer_relation_operations": True},
        }

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
