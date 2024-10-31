from mock import Mock
import pytest
from dbt.adapters.databricks.behaviors.constraints import (
    ConstraintsBehavior,
    DatabricksConstraintsBehavior,
    DbtConstraintsBehavior,
)
from dbt_common.contracts.config.base import BaseConfig
from dbt_common.exceptions import CompilationError


class DefaultConstraintsBehavior(ConstraintsBehavior):
    def validate_constraints(cls, config, is_view, is_incremental):
        cls._inner_validate_constraints(config, is_view, is_incremental)
        return True

    def get_model_constraints(cls, config, model):
        return model.get("constraints", [])

    def get_column_constraints(cls, config, column):
        return column.get("constraints", [])


class TestConstraintsBehavior:
    @pytest.fixture
    def behavior(self):
        return DefaultConstraintsBehavior()

    def test_inner_validate_constraints__golden_path(self, behavior):
        # Only succeeds if all conditions are met, otherwise raises CompilationError
        behavior._inner_validate_constraints(BaseConfig(), False, False)
        assert True

    def test_inner_validate_constraints__not_delta(self, behavior):
        with pytest.raises(CompilationError):
            behavior._inner_validate_constraints(
                BaseConfig({"file_format": "parquet"}), False, False
            )

    def test_inner_validate_constraints__is_view(self, behavior):
        with pytest.raises(CompilationError):
            behavior._inner_validate_constraints(BaseConfig(), True, False)

    def test_inner_validate_constraints__is_incremental(self, behavior):
        with pytest.raises(CompilationError):
            behavior._inner_validate_constraints(BaseConfig(), False, True)


class TestDbtConstraintsBehavior:
    @pytest.fixture
    def behavior(self):
        return DbtConstraintsBehavior()

    def test_validate_constraints__golden_path(self, behavior):
        assert behavior.validate_constraints(
            BaseConfig({"contract": Mock(enforced=True)}), False, False
        )

    def test_validate_constraints__not_enforced(self, behavior):
        assert not behavior.validate_constraints(BaseConfig(), False, False)

    def test_get_model_constraints__no_constraints(self, behavior):
        constraints = behavior.get_model_constraints({}, {})
        assert constraints == []

    def test_get_model_constraints__a_constraint(self, behavior):
        model = {"constraints": [{"type": "non_null"}]}
        constraints = behavior.get_column_constraints({}, model)
        assert constraints == [{"type": "non_null"}]

    def test_get_column_constraints__no_constraints(self, behavior):
        constraints = behavior.get_column_constraints({}, {})
        assert constraints == []

    def test_get_column_constraints__a_constraint(self, behavior):
        column = {"constraints": [{"type": "non_null"}]}
        constraints = behavior.get_column_constraints({}, column)
        assert constraints == [{"type": "non_null"}]


class TestDatabricksConstraintsBehavior(TestDbtConstraintsBehavior):
    @pytest.fixture
    def behavior(self):
        return DatabricksConstraintsBehavior()

    @pytest.fixture
    def config(self):
        return {"persist_constraints": True}

    def test_validate_constraints__databricks_golden_path(self, behavior, config):
        assert behavior.validate_constraints(config, False, False)

    def test_get_model_constraints__dbt_constraints(self, behavior, config):
        model = {"constraints": [{"type": "not_null", "columns": ["id", "name"]}]}
        constraints = behavior.get_model_constraints(config, model)
        assert constraints == [{"type": "not_null", "columns": ["id", "name"]}]

    def test_get_model_constraints__databricks_constraint(self, behavior, config):
        model = {"meta": {"constraints": [{"name": "name", "condition": "id > 0"}]}}
        constraints = behavior.get_model_constraints(config, model)
        assert constraints == [{"name": "name", "type": "check", "expression": "id > 0"}]

    def test_get_model_constraints__dbt_and_databricks_constraint(self, behavior, config):
        model = {
            "constraints": [{"type": "not_null", "columns": ["id", "name"]}],
            "meta": {"constraints": [{"name": "name", "condition": "id > 0"}]},
        }
        constraints = behavior.get_model_constraints(config, model)
        assert constraints == [
            {"type": "not_null", "columns": ["id", "name"]},
            {"name": "name", "type": "check", "expression": "id > 0"},
        ]

    def test_get_model_constraints__databricks_constraint_missing_name(self, behavior, config):
        model = {"meta": {"constraints": [{"condition": "id > 0"}]}}
        with pytest.raises(CompilationError):
            behavior.get_model_constraints(config, model)

    def test_get_model_constraints__databricks_constraint_missing_condition(self, behavior, config):
        model = {"meta": {"constraints": [{"name": "name", "condition": ""}]}}
        with pytest.raises(CompilationError):
            behavior.get_model_constraints(config, model)

    def test_get_column_constraints__dbt_constraints(self, behavior, config):
        column = {"constraints": [{"type": "not_null"}]}
        constraints = behavior.get_column_constraints(config, column)
        assert constraints == [{"type": "not_null"}]

    def test_get_column_constraints__databricks_constraints(self, behavior, config):
        column = {"name": "col1", "meta": {"constraints": ["not_null"]}}
        constraints = behavior.get_column_constraints(config, column)
        assert constraints == [{"type": "not_null", "columns": ["col1"]}]

    def test_get_column_constraints__dbt_and_databricks_constraints(self, behavior, config):
        column = {
            "name": "col1",
            "constraints": [{"type": "check", "expression": "id > 0"}],
            "meta": {
                "constraints": ["not_null"],
            },
        }
        constraints = behavior.get_column_constraints(config, column)
        assert constraints == [
            {"type": "check", "expression": "id > 0"},
            {"type": "not_null", "columns": ["col1"]},
        ]

    def test_get_column_constraints__databricks_persist_false(self, behavior, config):
        config["persist_constraints"] = False
        column = {"name": "col1", "meta": {"constraints": ["not_null"]}}
        constraints = behavior.get_column_constraints(config, column)
        assert constraints == []

    def test_get_column_constraints__anything_other_than_not_null(self, behavior, config):
        column = {"name": "col1", "meta": {"constraints": ["check"]}}
        with pytest.raises(CompilationError):
            behavior.get_column_constraints(config, column)
