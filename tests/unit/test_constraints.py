from unittest.mock import patch

import pytest
from dbt.adapters.events.types import AdapterEventWarning
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
)
from dbt_common.exceptions import DbtValidationError

from dbt.adapters.databricks.column import DatabricksColumn
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
    parse_model_and_legacy_constraints,
    parse_model_constraints,
    process_constraint,
    validate_constraint,
)
from dbt.adapters.databricks.impl import DatabricksAdapter


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

    @pytest.mark.parametrize(
        "raw",
        [
            {"type": "custom"},
            {"type": "custom", "name": "c", "expression": ""},
        ],
    )
    def test_custom_constraint_from_dict__invalid(self, raw):
        with pytest.raises(DbtValidationError, match="custom constraint .* missing required field"):
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
        assert constraint.render() == "PRIMARY KEY (`id`)"

    def test_primary_key_constraint_render__with_expression(self):
        constraint = PrimaryKeyConstraint(
            type=ConstraintType.primary_key, columns=["id", "other"], expression="DEFERRABLE"
        )
        assert constraint.render() == "PRIMARY KEY (`id`, `other`) DEFERRABLE"


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
        assert constraint.render() == "FOREIGN KEY (`id`) REFERENCES other_table (`other_id`)"

    def test_foreign_key_constraint_render__without_to_columns(self):
        constraint = ForeignKeyConstraint.from_dict(
            {
                "type": "foreign_key",
                "columns": ["id"],
                "to": "other_table",
            }
        )
        assert constraint.render() == "FOREIGN KEY (`id`) REFERENCES other_table"

    def test_foreign_key_constraint_render__with_expression(self):
        constraint = ForeignKeyConstraint(
            type=ConstraintType.foreign_key,
            columns=["id", "other"],
            expression="REFERENCES other_table (other_id) DEFERRABLE",
        )
        assert (
            constraint.render()
            == "FOREIGN KEY (`id`, `other`) REFERENCES other_table (other_id) DEFERRABLE"
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


class TestParseModelAndLegacyConstraints:
    def test_uses_contract_constraints_without_legacy_opt_in(self):
        columns = {"id": {"constraints": [{"type": "not_null"}]}}
        model_constraints = [{"type": "custom", "expression": "CHECK (id > 0)"}]

        assert (
            {"id"},
            [CustomConstraint(type=ConstraintType.custom, expression="CHECK (id > 0)")],
        ) == parse_model_and_legacy_constraints(columns, model_constraints)

    def test_legacy_constraints_override_contract_constraints(self):
        columns = {
            "id": {
                "constraints": [{"type": "primary_key"}],
                "meta": {"constraint": "not_null"},
            }
        }
        model_constraints = [{"type": "custom", "expression": "CHECK (id > 0)"}]
        meta_constraints = [{"name": "id_positive", "condition": "id > 0"}]

        assert (
            {"id"},
            [
                CheckConstraint(
                    type=ConstraintType.check,
                    name="id_positive",
                    expression="id > 0",
                )
            ],
        ) == parse_model_and_legacy_constraints(
            columns,
            model_constraints,
            persist_constraints=True,
            model_meta_constraints=meta_constraints,
        )

    def test_accepts_contract_format_in_meta_constraints(self):
        meta_constraints = [
            {
                "type": "custom",
                "name": "id_positive",
                "expression": "CHECK (id > 0)",
            }
        ]

        _, parsed = parse_model_and_legacy_constraints(
            {},
            [],
            persist_constraints=True,
            model_meta_constraints=meta_constraints,
        )

        assert parsed == [
            CustomConstraint(
                type=ConstraintType.custom,
                name="id_positive",
                expression="CHECK (id > 0)",
            )
        ]

    @pytest.mark.parametrize(
        "meta_constraints, message",
        [
            ([{"condition": "id > 0"}], "Invalid check constraint name"),
            ([{"name": "id_positive", "condition": ""}], "Invalid check constraint condition"),
        ],
    )
    def test_rejects_invalid_legacy_model_constraint(self, meta_constraints, message):
        with pytest.raises(DbtValidationError, match=message):
            parse_model_and_legacy_constraints(
                {},
                [],
                persist_constraints=True,
                model_meta_constraints=meta_constraints,
            )

    def test_rejects_invalid_legacy_column_constraint(self):
        columns = {"id": {"meta": {"constraint": "primary_key"}}}

        with pytest.raises(
            DbtValidationError,
            match="Invalid constraint for column id. Only `not_null` is supported.",
        ):
            parse_model_and_legacy_constraints(columns, [], persist_constraints=True)

    def test_v1_generates_stable_constraint_names(self):
        raw_constraints = [{"type": "check", "expression": "id > 0"}]

        with patch("dbt.adapters.databricks.constraints.warn_or_error") as mock_warn:
            _, first = parse_model_and_legacy_constraints(
                {},
                raw_constraints,
                relation_identifier="my_table",
            )
            _, second = parse_model_and_legacy_constraints(
                {},
                raw_constraints,
                relation_identifier="my_table",
            )

        assert first[0].name == second[0].name
        assert first[0].name == "ca209567b6d1fd0b464a46ae4ef55306"
        assert mock_warn.call_count == 2
        assert all(
            isinstance(call.args[0], AdapterEventWarning)
            and call.args[0].base_msg
            == (
                "Constraint of type check with no `name` provided. "
                "Generating hash instead for relation my_table"
            )
            for call in mock_warn.call_args_list
        )

    @pytest.mark.parametrize(
        "database, schema, target, expected",
        [
            ("cat", "sch", "parent", "`cat`.`sch`.`parent`"),
            ("cat", "sch", "`other_cat`.`other_sch`.`parent`", "`other_cat`.`other_sch`.`parent`"),
            ("", "", "parent", "parent"),
        ],
    )
    def test_qualifies_fk_target(self, database, schema, target, expected):
        _, parsed = parse_model_and_legacy_constraints(
            {},
            [
                {
                    "type": "foreign_key",
                    "name": "fk_id",
                    "columns": ["id"],
                    "to": target,
                    "to_columns": ["id"],
                }
            ],
            relation_database=database,
            relation_schema=schema,
        )

        assert parsed[0].to == expected


class TestParseColumnsAndConstraintsGate:
    @staticmethod
    def _existing_columns():
        return [DatabricksColumn(column="id", dtype="int")]

    @staticmethod
    def _model_columns_with_fk():
        return {
            "id": {
                "name": "id",
                "data_type": "int",
                "constraints": [
                    {"type": "not_null"},
                    {
                        "type": "foreign_key",
                        "name": "fk_id",
                        "to": "parent",
                        "to_columns": ["id"],
                    },
                ],
            }
        }

    @staticmethod
    def _request(**kwargs):
        request = {
            "columns": kwargs.pop("columns", {}),
            "constraints": kwargs.pop("constraints", []),
            "contract_enforced": kwargs.pop("contract_enforced", False),
            "persist_constraints": kwargs.pop("persist_constraints", False),
            "column_source": kwargs.pop("column_source", "query"),
            "application": kwargs.pop("application", "create"),
            "model_name": kwargs.pop("model_name", ""),
        }
        request.update(kwargs)
        return request

    def test_skips_column_constraints_when_not_enforced(self):
        _, parsed = DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(columns=self._model_columns_with_fk()),
        )
        assert parsed == []

    def test_skips_column_not_null_when_not_enforced(self):
        enriched, _ = DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(columns=self._model_columns_with_fk()),
        )
        assert all(not getattr(col, "not_null", False) for col in enriched)

    def test_parses_column_constraints_when_enforced(self):
        _, parsed = DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(columns=self._model_columns_with_fk(), contract_enforced=True),
        )
        assert len(parsed) == 1
        assert isinstance(parsed[0], ForeignKeyConstraint)
        assert parsed[0].name == "fk_id"
        assert parsed[0].columns == ["id"]

    def test_parses_legacy_constraints_without_contract(self):
        columns = {
            "id": {
                "name": "id",
                "data_type": "int",
                "meta": {"constraint": "not_null"},
            }
        }
        enriched, parsed = DatabricksAdapter.parse_columns_and_constraints(
            [],
            self._request(
                columns=columns,
                persist_constraints=True,
                meta_constraints=[{"name": "id_positive", "condition": "id > 0"}],
                column_source="model",
                application="post_create",
            ),
        )

        assert enriched[0].not_null
        assert parsed == [
            CheckConstraint(
                type=ConstraintType.check,
                name="id_positive",
                expression="id > 0",
            )
        ]

    @patch("dbt.adapters.databricks.constraints.warn_or_error")
    def test_warns_for_invalid_model_not_null_in_post_create_application(self, mock_warn):
        DatabricksAdapter.parse_columns_and_constraints(
            [],
            self._request(
                columns={"id": {"name": "id", "data_type": "int"}},
                constraints=[{"type": "not_null", "columns": ["missing"]}],
                contract_enforced=True,
                column_source="model",
                application="post_create",
            ),
        )

        mock_warn.assert_called_once()
        event = mock_warn.call_args.args[0]
        assert isinstance(event, AdapterEventWarning)
        assert event.base_msg == "not_null constraint on invalid column: missing"

    @patch("dbt.adapters.databricks.constraints.warn_or_error")
    def test_filters_invalid_pk_fk_columns_when_using_model_columns(self, mock_warn):
        _, parsed = DatabricksAdapter.parse_columns_and_constraints(
            [],
            self._request(
                columns={"id": {"name": "id", "data_type": "int"}},
                constraints=[
                    {
                        "type": "primary_key",
                        "name": "pk",
                        "columns": ["id", "missing"],
                    },
                    {
                        "type": "foreign_key",
                        "name": "fk",
                        "columns": ["id", "missing"],
                        "to": "parent",
                        "to_columns": ["parent_id"],
                    },
                ],
                contract_enforced=True,
                column_source="model",
                application="post_create",
            ),
        )

        pk = next(c for c in parsed if isinstance(c, PrimaryKeyConstraint))
        fk = next(c for c in parsed if isinstance(c, ForeignKeyConstraint))
        assert pk.columns == ["id"]
        assert fk.columns == ["id"]
        assert {call.args[0].base_msg for call in mock_warn.call_args_list} == {
            "Invalid primary key column: missing",
            "Invalid foreign key column: missing",
        }

    def test_create_path_keeps_undeclared_pk_columns(self):
        _, parsed = DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(
                columns={"id": {"name": "id", "data_type": "int"}},
                constraints=[
                    {
                        "type": "primary_key",
                        "name": "pk",
                        "columns": ["id", "missing"],
                    }
                ],
                contract_enforced=True,
            ),
        )

        assert parsed[0].columns == ["id", "missing"]
        assert "`missing`" in parsed[0].render()

    @pytest.mark.parametrize("application", ["create", "post_create"])
    def test_unsupported_constraint_policy(self, application):
        request = self._request(
            constraints=[{"type": ConstraintType.unique, "columns": ["id"]}],
            contract_enforced=True,
            application=application,
        )

        if application == "post_create":
            _, parsed = DatabricksAdapter.parse_columns_and_constraints(
                self._existing_columns(), request
            )
            assert parsed == []
        else:
            with pytest.raises(DbtValidationError, match="Unique constraints are not supported"):
                DatabricksAdapter.parse_columns_and_constraints(self._existing_columns(), request)

    def test_defaults_to_not_enforced(self):
        _, parsed = DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(columns=self._model_columns_with_fk()),
        )
        assert parsed == []

    @patch("dbt.adapters.databricks.impl.logger")
    def test_logs_info_when_constraints_skipped(self, mock_logger):
        DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(columns=self._model_columns_with_fk(), model_name="my_model"),
        )
        mock_logger.info.assert_called_once()

    @patch("dbt.adapters.databricks.impl.logger")
    def test_no_log_when_no_constraints_declared(self, mock_logger):
        DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(
                columns={"id": {"name": "id", "data_type": "int"}},
                model_name="my_model",
            ),
        )
        mock_logger.info.assert_not_called()

    @patch("dbt.adapters.databricks.impl.logger")
    def test_no_skip_log_when_enforced(self, mock_logger):
        DatabricksAdapter.parse_columns_and_constraints(
            self._existing_columns(),
            self._request(
                columns=self._model_columns_with_fk(),
                contract_enforced=True,
                model_name="my_model",
            ),
        )
        mock_logger.info.assert_not_called()
