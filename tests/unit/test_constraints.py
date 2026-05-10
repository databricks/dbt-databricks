from unittest.mock import patch

import pytest
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
)
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.databricks.constraints import (
    CheckConstraint,
    CustomConstraint,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    TypedConstraint,
    is_enforced,
    is_supported,
    parse_column_constraints,
    parse_constraint,
    parse_constraints,
    parse_model_constraints,
    process_constraint,
    validate_constraint,
)


class FakeConstraint(TypedConstraint):
    str_type = "unique"

    def _render_suffix(self):
        return "test"


class TestTypedConstraint:
    def test_typed_constraint_from_dict__invalid_type(self):
        with pytest.raises(AssertionError, match="Mismatched constraint type"):
            FakeConstraint.from_dict({"type": "custom"})

    def test_render__with_name(self):
        constraint = FakeConstraint(type=ConstraintType.check, name="my_constraint")
        assert constraint.render() == "CONSTRAINT my_constraint test"


class TestCustomConstraint:
    def test_custom_constraint_from_dict__valid(self):
        raw = {"type": "custom", "expression": "1 = 1"}
        assert CustomConstraint(
            type=ConstraintType.custom, expression="1 = 1"
        ) == CustomConstraint.from_dict(raw)

    def test_custom_constraint_from_dict__invalid(self):
        raw = {"type": "custom"}
        with pytest.raises(
            DbtValidationError, match="custom constraint '' is missing required field"
        ):
            CustomConstraint.from_dict(raw)

    def test_custom_constraint_render(self):
        constraint = CustomConstraint(type=ConstraintType.custom, expression="1 = 1")
        assert constraint.render() == "1 = 1"


class TestCheckConstraint:
    def test_check_constraint_from_dict__valid(self):
        raw = {"name": "chk_raw", "type": "check", "expression": "1 = 1"}
        assert CheckConstraint(
            name="chk_raw", type=ConstraintType.check, expression="1 = 1"
        ) == CheckConstraint.from_dict(raw)

    def test_check_constraint_validate__invalid(self):
        raw = {"type": "check"}
        with pytest.raises(
            DbtValidationError, match="check constraint '' is missing required field"
        ):
            CheckConstraint.from_dict(raw)

    def test_custom_constraint_render(self):
        constraint = CheckConstraint.from_dict({"type": "check", "expression": "1 = 1"})
        assert constraint.render().endswith(" CHECK (1 = 1)")


class TestPrimaryKeyConstraint:
    def test_primary_key_constraint_from_dict__valid(self):
        raw = {"type": "primary_key", "columns": ["id"]}
        assert PrimaryKeyConstraint(
            type=ConstraintType.primary_key, columns=["id"]
        ) == PrimaryKeyConstraint.from_dict(raw)

    def test_primary_key_constraint_validate__invalid(self):
        raw = {"type": "primary_key"}
        with pytest.raises(
            DbtValidationError, match="primary_key constraint '' is missing required field"
        ):
            PrimaryKeyConstraint.from_dict(raw)

    def test_primary_key_constraint_render__no_expression(self):
        constraint = PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"])
        assert constraint.render() == "PRIMARY KEY (id)"

    def test_primary_key_constraint_render__with_expression(self):
        constraint = PrimaryKeyConstraint(
            type=ConstraintType.primary_key, columns=["id", "other"], expression="DEFERRABLE"
        )
        assert constraint.render() == "PRIMARY KEY (id, other) DEFERRABLE"


class TestForeignKeyConstraint:
    def test_foreign_key_constraint_from_dict__valid(self):
        raw = {"type": "foreign_key", "columns": ["id"], "expression": "1 = 1"}
        assert ForeignKeyConstraint(
            type=ConstraintType.foreign_key, columns=["id"], expression="1 = 1"
        ) == ForeignKeyConstraint.from_dict(raw)

    def test_foreign_key_constraint_validate__invalid(self):
        raw = {"type": "foreign_key"}
        with pytest.raises(
            DbtValidationError, match="foreign_key constraint '' is missing required field"
        ):
            ForeignKeyConstraint.from_dict(raw)

    def test_foreign_key_constraint_render__to(self):
        constraint = ForeignKeyConstraint(
            type=ConstraintType.foreign_key,
            columns=["id"],
            to="other_table",
            to_columns=["other_id"],
        )
        assert constraint.render() == "FOREIGN KEY (id) REFERENCES other_table (other_id)"

    def test_foreign_key_constraint_render__with_expression(self):
        constraint = ForeignKeyConstraint(
            type=ConstraintType.foreign_key,
            columns=["id", "other"],
            expression="REFERENCES other_table (other_id) DEFERRABLE",
        )
        assert (
            constraint.render()
            == "FOREIGN KEY (id, other) REFERENCES other_table (other_id) DEFERRABLE"
        )


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
        assert is_supported(constraint) == supported


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
        assert is_enforced(constraint) == enforced


class TestParseConstraint:
    @pytest.mark.parametrize(
        "type, expectedType",
        [
            ("check", CheckConstraint),
            ("custom", CustomConstraint),
            ("primary_key", PrimaryKeyConstraint),
            ("foreign_key", ForeignKeyConstraint),
        ],
    )
    def test_parse_constraint__valid_column(self, type, expectedType):
        raw_constraint = {
            "type": type,
            "expression": "1 = 1",
            "name": "my_constraint",
        }
        if type not in ["not_null", "unique"]:
            raw_constraint["columns"] = ["id"]
        constraint = parse_constraint(raw_constraint)
        assert isinstance(constraint, expectedType)

    def test_parse_constraint__invalid_constraint(self):
        raw_constraint = {"type": None}
        with pytest.raises(DbtValidationError, match="Could not parse constraint"):
            parse_constraint(raw_constraint)


class TestProcessConstraint:
    def test_process_constraint__valid_constraint(self):
        constraint = CheckConstraint(type=ConstraintType.check, expression="1 = 1")
        assert process_constraint(constraint) == constraint.render()

    def test_process_constraint__invalid_constraint(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.unique)
        assert process_constraint(constraint) is None


class TestValidateConstraint:
    @pytest.fixture
    def pk_constraint(self):
        return ColumnLevelConstraint(
            type=ConstraintType.primary_key, warn_unsupported=True, warn_unenforced=True
        )

    def test_validate_constraint__custom(self):
        constraint = ColumnLevelConstraint(type=ConstraintType.custom)
        assert validate_constraint(constraint) is True

    def test_validate_constraint__supported(self, pk_constraint):
        assert validate_constraint(pk_constraint) is True

    @patch("dbt.adapters.databricks.constraints.warn_or_error")
    def test_validate_constraint__unsupported(self, _):
        with patch("dbt.adapters.databricks.constraints.ConstraintNotSupported") as mock_warn:
            constraint = ColumnLevelConstraint(type=ConstraintType.unique)
            assert validate_constraint(constraint) is False
            mock_warn.assert_called_with(
                constraint=constraint.type.value, adapter="DatabricksAdapter"
            )

    @patch("dbt.adapters.databricks.constraints.warn_or_error")
    def test_validate_constraint__unenforced(self, _, pk_constraint):
        with patch("dbt.adapters.databricks.constraints.ConstraintNotEnforced") as mock_warn:
            assert validate_constraint(pk_constraint) is True
            mock_warn.assert_called_with(
                constraint=pk_constraint.type.value, adapter="DatabricksAdapter"
            )


class TestParseConstraints:
    def test_parse_column_constraints__empty(self):
        assert (set(), []) == parse_column_constraints([])

    def test_parse_column_constraints__not_nulls(self):
        columns = [{"name": "id", "constraints": [{"type": "not_null"}]}]
        assert ({"id"}, []) == parse_column_constraints(columns)

    def test_parse_column_constraints__model_constraints(self):
        columns = [{"name": "id", "constraints": [{"type": "primary_key"}]}]
        assert (
            set(),
            [PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"])],
        ) == parse_column_constraints(columns)

    def test_parse_column_constraints__both(self):
        columns = [{"name": "id", "constraints": [{"type": "primary_key"}, {"type": "not_null"}]}]
        assert (
            {"id"},
            [PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"])],
        ) == parse_column_constraints(columns)

    def test_parse_model_constraints__empty(self):
        assert (set(), []) == parse_model_constraints([])

    def test_parse_model_constraints__not_nulls_invalid(self):
        constraints = [{"type": "not_null"}]
        with pytest.raises(
            DbtValidationError, match="not_null constraint on model must have 'columns' defined"
        ):
            parse_model_constraints(constraints)

    def test_parse_model_constraints__not_null_single(self):
        constraints = [{"type": "not_null", "columns": ["id"]}]
        assert ({"id"}, []) == parse_model_constraints(constraints)

    def test_parse_model_constraints__not_nulls(self):
        constraints = [{"type": "not_null", "columns": ["id", "other"]}]
        assert ({"id", "other"}, []) == parse_model_constraints(constraints)

    def test_parse_model_constraints__model_constraints(self):
        columns = [{"type": "primary_key", "columns": ["id"]}]
        assert (
            set(),
            [PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"])],
        ) == parse_model_constraints(columns)

    def test_parse_model_constraints__both(self):
        columns = [
            {"type": "primary_key", "columns": ["id"]},
            {"type": "not_null", "columns": ["id"]},
        ]
        assert (
            {"id"},
            [PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"])],
        ) == parse_model_constraints(columns)

    def test_parse_constraints__empty(self):
        assert (set(), []) == parse_constraints([], [])

    def test_parse_constraints__not_nulls(self):
        columns = [{"name": "id", "constraints": [{"type": "not_null"}]}]
        constraints = [{"type": "not_null", "columns": ["id2"]}]
        assert ({"id", "id2"}, []) == parse_constraints(columns, constraints)

    def test_parse_constraints__constraints(self):
        columns = [{"name": "id", "constraints": [{"type": "primary_key"}]}]
        constraints = [{"type": "custom", "expression": "1 = 1"}]
        assert (
            set(),
            [
                PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"]),
                CustomConstraint(type=ConstraintType.custom, expression="1 = 1"),
            ],
        ) == parse_constraints(columns, constraints)
