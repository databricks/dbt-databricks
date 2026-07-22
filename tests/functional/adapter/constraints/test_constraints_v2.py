import json

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


@pytest.mark.skip_profile("databricks_cluster")
class TestCheckConstraintReadbackV2(DatabricksConstraintsBaseV2):
    """Under v2 a contract CHECK constraint is observable as a delta.constraints.* property."""

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "check_constraint_model.sql": override_fixtures.check_constraint_model_sql,
            "schema.yml": override_fixtures.check_constraint_schema_yml,
        }

    def test_check_constraint_present(self, project):
        util.run_dbt(["run"])
        rows = project.run_sql(
            "show tblproperties {database}.{schema}.check_constraint_model", fetch="all"
        )
        constraints = {
            row.key: row.value for row in rows if row.key.startswith("delta.constraints.")
        }
        assert constraints.get("delta.constraints.id_is_positive") == "id > 0", (
            f"CHECK constraint not observable under v2; delta.constraints = {constraints}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestUnnamedPKRebuildV2(DatabricksConstraintsBaseV2):
    """An unnamed primary key survives repeated safer-relation rebuilds (issue #1091)."""

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "flags": {"use_materialization_v2": True},
            "models": {"+use_safer_relation_operations": True},
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "unnamed_pk_model.sql": override_fixtures.unnamed_pk_model_sql,
            "schema.yml": override_fixtures.unnamed_pk_schema_yml,
        }

    def test_rebuild_does_not_collide(self, project):
        for _ in range(3):
            results = util.run_dbt(["run", "-s", "unnamed_pk_model"])
        assert len(results) == 1

        rows = project.run_sql(
            "describe table extended {database}.{schema}.unnamed_pk_model as json", fetch="all"
        )
        constraints = json.loads(rows[0][0])["table_constraints"]
        assert "PRIMARY KEY" in constraints, f"primary key missing after rebuild: {constraints}"
        assert "__dbt_stg" not in constraints, f"primary key retained a staging name: {constraints}"
