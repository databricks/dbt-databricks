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
            BaseConfig({"contract": {"enforced": True}}), False, False
        )

    def test_validate_constraints__not_enforced(self, behavior):
        assert not behavior.validate_constraints(BaseConfig(), False, False)


class TestDatabricksConstraintsBehavior(TestDbtConstraintsBehavior):
    @pytest.fixture
    def behavior(self):
        return DatabricksConstraintsBehavior()

    def test_validate_constraints__databricks_golden_path(self, behavior):
        assert behavior.validate_constraints(
            BaseConfig({"persist_constraints": True}), False, False
        )
