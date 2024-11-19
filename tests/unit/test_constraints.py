from mock import patch
import pytest
from dbt_common.contracts.constraints import (
    ConstraintType,
    ColumnLevelConstraint,
    ModelLevelConstraint,
)
from dbt.adapters.databricks import constraints
from dbt_common.exceptions import DbtValidationError


class TestConstraintsSupported:
    @pytest.mark.parametrize(
        "constraint_type, supported",
        [
            (ConstraintType.not_null, True),
            (ConstraintType.unique, False),
            (ConstraintType.primary_key, True),
            (ConstraintType.foreign_key, True),
            (ConstraintType.check, True),
            (ConstraintType.custom, True),
            ("invalid", False),
        ],
    )
    def test_supported__expected(self, constraint_type, supported):
        constraint = ColumnLevelConstraint(type=constraint_type)
        assert constraints.is_supported(constraint) == supported


class TestConstraintsEnforced:
    @pytest.mark.parametrize(
        "constraint_type, enforced",
        [
            (ConstraintType.not_null, True),
            (ConstraintType.unique, False),
            (ConstraintType.primary_key, False),
            (ConstraintType.foreign_key, False),
            (ConstraintType.check, True),
            (ConstraintType.custom, False),
            ("invalid", False),
        ],
    )
    def test_enforced__expected(self, constraint_type, enforced):
        constraint = ColumnLevelConstraint(type=constraint_type)
        assert constraints.is_enforced(constraint) == enforced


class TestParseConstraint:
    @pytest.mark.parametrize("klass", [ColumnLevelConstraint, ModelLevelConstraint])
    def test_parse_constraint__valid_column(self, klass):
        raw_constraint = {"type": "not_null"}
        constraint = constraints.parse_constraint(klass, raw_constraint)
        assert isinstance(constraint, klass)
        assert constraint.type == ConstraintType.not_null

    @pytest.mark.parametrize("klass", [ColumnLevelConstraint, ModelLevelConstraint])
    def test_parse_constraint__invalid_column(self, klass):
        raw_constraint = {"type": None}
        with pytest.raises(DbtValidationError, match="Could not parse constraint"):
            constraints.parse_constraint(klass, raw_constraint)


@pytest.fixture
def constraint():
    return ColumnLevelConstraint(type=ConstraintType.check)


class TestProcessConstraint:
    @pytest.fixture
    def success(self):
        return "SUCCESS"

    @pytest.fixture
    def render_map(self, constraint, success):
        return {constraint.type: lambda _: success}

    def test_process_constraint__valid_constraint(self, constraint, render_map, success):
        assert constraints.process_constraint(constraint, lambda _: True, render_map) == success

    def test_process_constraint__invalid_constraint(self, constraint, render_map):
        assert constraints.process_constraint(constraint, lambda x: False, render_map) is None


class TestValidateConstraint:
    @pytest.fixture
    def pk_constraint(self):
        return ColumnLevelConstraint(
            type=ConstraintType.primary_key, warn_unsupported=True, warn_unenforced=True
        )

    def test_validate_constraint__custom(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.custom)
        assert constraints.validate_constraint(constraint, lambda _: False) is True

    def test_validate_constraint__supported(self, pk_constraint):
        assert constraints.validate_constraint(pk_constraint, lambda _: True) is True

    @patch("dbt.adapters.databricks.constraints.warn_or_error")
    def test_validate_constraint__unsupported(self, pk_constraint):
        with patch("dbt.adapters.databricks.constraints.ConstraintNotSupported") as mock_warn:
            assert constraints.validate_constraint(pk_constraint, lambda _: False) is False
            mock_warn.assert_called_with(
                constraint=pk_constraint.type.value, adapter="DatabricksAdapter"
            )

    @patch("dbt.adapters.databricks.constraints.warn_or_error")
    def test_validate_constraint__unenforced(self, pk_constraint):
        with patch("dbt.adapters.databricks.constraints.ConstraintNotEnforced") as mock_warn:
            assert constraints.validate_constraint(pk_constraint, lambda _: True) is True
            mock_warn.assert_called_with(
                constraint=pk_constraint.type.value, adapter="DatabricksAdapter"
            )


class TestRenderConstraint:
    @pytest.fixture
    def success(self):
        return "CHECK (1 = 1)"

    @pytest.fixture
    def render_map(self, constraint, success):
        return {constraint.type: lambda _: success}

    def test_render_constraint__valid_constraint(self, constraint, render_map, success):
        assert constraints.render_constraint(constraint, render_map) == success

    def test_render_constraint__invalid_constraint(self, render_map):
        assert (
            constraints.render_constraint(
                ColumnLevelConstraint(type=ConstraintType.not_null), render_map
            )
            is None
        )

    def test_render_constraint__with_name(self, constraint, render_map, success):
        constraint.name = "my_constraint"
        assert (
            constraints.render_constraint(constraint, render_map)
            == f"CONSTRAINT {constraint.name} {success}"
        )

    def test_render_constraint__excess_gets_trimmed(self, constraint):
        constraint.name = "my_constraint"
        render_map = {constraint.type: lambda _: "CHECK (1 = 1)   "}
        assert (
            constraints.render_constraint(constraint, render_map)
            == f"CONSTRAINT {constraint.name} CHECK (1 = 1)"
        )


class TestSupportedFor:
    @pytest.fixture
    def warning(self):
        return "Warning for {type}"

    def test_supported_for__supported(self, constraint, warning):
        assert constraints.supported_for(constraint, lambda _: True, warning) is True

    def test_supported_for__not_supported_in_base(self, warning):
        constraint = ColumnLevelConstraint(type=ConstraintType.unique)
        assert constraints.supported_for(constraint, lambda _: True, warning) is False

    def test_supported_for__not_supported_in_context(self, constraint, warning):
        with patch("dbt.adapters.databricks.constraints.logger.warning") as mock_warn:
            assert constraints.supported_for(constraint, lambda _: False, warning) is False
            mock_warn.assert_called_with(warning.format(type=constraint.type))


class TestRenderError:
    @pytest.mark.parametrize(
        "constraint, missing, expected",
        [
            (
                ColumnLevelConstraint(type=ConstraintType.check),
                [["expression"]],
                "check constraint is missing required field(s): ('expression')",
            ),
            (
                ColumnLevelConstraint(type=ConstraintType.foreign_key),
                [["expression"], ["to", "to_columns"]],
                (
                    "foreign_key constraint is missing required field(s): "
                    "('expression') or ('to', 'to_columns')"
                ),
            ),
            (
                ColumnLevelConstraint(type=ConstraintType.primary_key, name="my_pk"),
                [["expression"]],
                "primary_key constraint my_pk is missing required field(s): ('expression')",
            ),
        ],
    )
    def test_render_error__expected(self, constraint, missing, expected):
        assert constraints.render_error(constraint, missing).msg == expected


class TestRenderCustom:
    def test_render_custom__valid(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.custom, expression="1 = 1")
        assert constraints.render_custom(constraint) == "1 = 1"

    def test_render_custom__invalid(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.custom)
        with pytest.raises(DbtValidationError, match="custom constraint is missing required field"):
            constraints.render_custom(constraint)


class TestRenderPrimaryKeyForColumn:
    def test_render_primary_key_for_column__valid(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.primary_key, expression="DEFERRABLE")
        assert constraints.render_primary_key_for_column(constraint) == "PRIMARY KEY DEFERRABLE"

    def test_render_primary_key_for_column__no_expression(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.primary_key)
        assert constraints.render_primary_key_for_column(constraint) == "PRIMARY KEY"


class TestRenderForeignKeyForColumn:
    def test_render_foreign_key_for_column__valid_to(self):
        constraint = ColumnLevelConstraint(
            type=ConstraintType.foreign_key,
            to="other_table",
            to_columns=["other_id"],
        )
        assert (
            constraints.render_foreign_key_for_column(constraint)
            == "FOREIGN KEY REFERENCES other_table (other_id)"
        )

    def test_render_foreign_key_for_column__valid_expression(self):
        constraint = ColumnLevelConstraint(
            type=ConstraintType.foreign_key, expression="references other_table (other_id)"
        )
        assert (
            constraints.render_foreign_key_for_column(constraint)
            == "FOREIGN KEY references other_table (other_id)"
        )

    def test_render_foreign_key_for_column__invalid(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.foreign_key)
        with pytest.raises(
            DbtValidationError, match="foreign_key constraint is missing required field"
        ):
            constraints.render_foreign_key_for_column(constraint)


class TestRenderPrimaryKeyForModel:
    def test_render_primary_key_for_model__valid_columns(self):
        constraint = ModelLevelConstraint(type=ConstraintType.primary_key, columns=["id", "ts"])
        assert constraints.render_primary_key_for_model(constraint) == "PRIMARY KEY (id, ts)"

    def test_render_primary_key_for_model__expression(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.primary_key, expression="(id TIMESERIES)"
        )
        assert constraints.render_primary_key_for_model(constraint) == "PRIMARY KEY (id TIMESERIES)"

    def test_render_primary_key_for_model__invalid(self):
        constraint = ModelLevelConstraint(type=ConstraintType.primary_key)
        with pytest.raises(
            DbtValidationError, match="primary_key constraint is missing required field"
        ):
            constraints.render_primary_key_for_model(constraint)


class TestRenderForeignKeyForModel:
    def test_render_foreign_key_for_model__valid_columns(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.foreign_key,
            columns=["id"],
            to="other_table",
            to_columns=["other_id"],
        )
        assert (
            constraints.render_foreign_key_for_model(constraint)
            == "FOREIGN KEY (id) REFERENCES other_table (other_id)"
        )

    def test_render_foreign_key_for_model__expression(self):
        constraint = ModelLevelConstraint(
            type=ConstraintType.foreign_key, expression="references other_table (other_id)"
        )
        assert (
            constraints.render_foreign_key_for_model(constraint)
            == "FOREIGN KEY references other_table (other_id)"
        )

    def test_render_foreign_key_for_model__invalid(self):
        constraint = ModelLevelConstraint(type=ConstraintType.foreign_key)
        with pytest.raises(
            DbtValidationError, match="foreign_key constraint is missing required field"
        ):
            constraints.render_foreign_key_for_model(constraint)


class TestRenderCheck:
    def test_render_check__valid(self):
        constraint = ModelLevelConstraint(type=ConstraintType.check, expression="id > 0")
        assert constraints.render_check(constraint) == "CHECK (id > 0)"

    def test_render_check__invalid(self):
        constraint = ModelLevelConstraint(type=ConstraintType.check)
        with pytest.raises(DbtValidationError, match="check constraint is missing required field"):
            constraints.render_check(constraint)
