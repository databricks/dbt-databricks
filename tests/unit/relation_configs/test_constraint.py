from unittest.mock import Mock

from agate import Table
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)

from dbt.adapters.databricks.constraints import (
    CheckConstraint,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
    synthesize_constraint_name,
)
from dbt.adapters.databricks.relation_configs.constraints import (
    ConstraintsConfig,
    ConstraintsProcessor,
)
from tests.unit import fixtures


class TestConstraintsProcessor:
    def test_from_relation_results__none(self):
        results = {
            "non_null_constraint_columns": None,
            "show_tblproperties": None,
            "primary_key_constraints": None,
            "foreign_key_constraints": None,
        }
        spec = ConstraintsProcessor.from_relation_results(results)
        assert spec == ConstraintsConfig(set_non_nulls=set(), set_constraints=set())

    def test_from_relation_results__with_check_constraints(self):
        results = {
            "show_tblproperties": fixtures.gen_tblproperties(
                [
                    ["delta.constraints.check_name_length", "LENGTH (name) >= 1"],
                    ["other_prop", "value"],
                ]
            ),
        }
        spec = ConstraintsProcessor.from_relation_results(results)
        assert spec == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                CheckConstraint(
                    type=ConstraintType.check,
                    name="check_name_length",
                    expression="LENGTH (name) >= 1",
                )
            },
        )

    def test_from_relation_results__with_primary_key_constraints(self):
        results = {
            "primary_key_constraints": Table(
                rows=[
                    ["pk_user", "id"],
                    ["pk_user", "email"],
                ],
                column_names=["constraint_name", "column_name"],
            ),
        }
        spec = ConstraintsProcessor.from_relation_results(results)
        assert spec == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["id", "email"],
                )
            },
        )

    def test_from_relation_results__with_foreign_key_constraints(self):
        results = {
            "foreign_key_constraints": Table(
                rows=[
                    ["fk_user_1", "id", "catalog", "schema", "customers", "customer_id"],
                    ["fk_user_1", "email", "catalog", "schema", "customers", "email"],
                ],
                column_names=[
                    "constraint_name",
                    "from_column",
                    "to_catalog",
                    "to_schema",
                    "to_table",
                    "to_column",
                ],
            ),
        }
        spec = ConstraintsProcessor.from_relation_results(results)
        assert spec == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_user_1",
                    columns=["id", "email"],
                    to="`catalog`.`schema`.`customers`",
                    to_columns=["customer_id", "email"],
                )
            },
        )

    def test_from_relation_results__with_non_null_constraints(self):
        results = {
            "non_null_constraint_columns": Table(
                rows=[
                    ["id"],
                    ["email"],
                ],
                column_names=["column_name"],
            ),
        }
        spec = ConstraintsProcessor.from_relation_results(results)
        assert spec == ConstraintsConfig(
            set_non_nulls={"id", "email"},
            set_constraints=set(),
        )

    @staticmethod
    def _make_model_with_contract(**kwargs):
        """Create a Mock model with contract.enforced = True."""
        model = Mock()
        model.config.contract.enforced = True
        for key, value in kwargs.items():
            setattr(model, key, value)
        return model

    def test_from_relation_config__without_constraints(self):
        model = self._make_model_with_contract(constraints=[], columns={})
        spec = ConstraintsProcessor.from_relation_config(model)
        assert spec == ConstraintsConfig(set_non_nulls=set(), set_constraints=set())

    def test_from_relation_config__without_contract(self):
        """Constraints should be empty when contract is not enforced (issue #1342)."""
        model = Mock()
        model.config.contract.enforced = False
        model.columns = {}
        model.constraints = [
            ModelLevelConstraint(
                type=ConstraintType.primary_key,
                name="pk_user",
                columns=["id"],
            )
        ]
        spec = ConstraintsProcessor.from_relation_config(model)
        assert spec == ConstraintsConfig(set_non_nulls=set(), set_constraints=set())

    def test_from_relation_config__with_check_constraint(self):
        model = self._make_model_with_contract(
            columns={},
            constraints=[
                ModelLevelConstraint(
                    type=ConstraintType.check,
                    name="check_name_length",
                    expression="LENGTH (name) >= 1",
                )
            ],
        )
        spec = ConstraintsProcessor.from_relation_config(model)
        assert spec == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                CheckConstraint(
                    type=ConstraintType.check,
                    name="check_name_length",
                    expression="LENGTH (name) >= 1",
                )
            },
        )

    def test_from_relation_config__with_primary_key_constraint(self):
        model = self._make_model_with_contract(
            columns={},
            constraints=[
                ModelLevelConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["id", "email"],
                )
            ],
        )
        spec = ConstraintsProcessor.from_relation_config(model)
        assert spec == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["id", "email"],
                )
            },
        )

    def test_from_relation_config__with_foreign_key_constraint(self):
        model = self._make_model_with_contract(
            columns={},
            constraints=[
                ModelLevelConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_user_customer",
                    columns=["id", "email"],
                    to="`catalog`.`schema`.`customers`",
                    to_columns=["customer_id", "email"],
                )
            ],
        )
        spec = ConstraintsProcessor.from_relation_config(model)
        assert spec == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_user_customer",
                    columns=["id", "email"],
                    to="`catalog`.`schema`.`customers`",
                    to_columns=["customer_id", "email"],
                )
            },
        )

    def test_from_relation_config__unnamed_primary_key_gets_synthesized_name(self):
        # An unnamed PK is given the deterministic name the create macro would assign, so the model
        # side matches the catalog instead of churning (#1333).
        model = self._make_model_with_contract(
            identifier="my_model",
            columns={},
            constraints=[
                ModelLevelConstraint(type=ConstraintType.primary_key, columns=["id"]),
            ],
        )
        spec = ConstraintsProcessor.from_relation_config(model)
        (pk,) = spec.set_constraints
        assert pk.name == synthesize_constraint_name(
            PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"]), "my_model"
        )
        assert pk.name is not None

    def test_from_relation_config__named_constraint_keeps_its_name(self):
        # An explicitly named constraint is left untouched by name synthesis.
        model = self._make_model_with_contract(
            identifier="my_model",
            columns={},
            constraints=[
                ModelLevelConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_explicit",
                    columns=["parent_id"],
                    to="`c`.`s`.`parent`",
                    to_columns=["id"],
                ),
            ],
        )
        spec = ConstraintsProcessor.from_relation_config(model)
        (fk,) = spec.set_constraints
        assert fk.name == "fk_explicit"

    def test_from_relation_config__with_non_null_constraint(self):
        model = self._make_model_with_contract(
            columns={
                "email": ColumnInfo(
                    name="email",
                    constraints=[ColumnLevelConstraint(type=ConstraintType.not_null)],
                ),
            },
            constraints=[
                ModelLevelConstraint(
                    type=ConstraintType.not_null,
                    columns=["id"],
                )
            ],
        )
        spec = ConstraintsProcessor.from_relation_config(model)

        assert spec == ConstraintsConfig(
            set_non_nulls={"email", "id"},
            set_constraints=set(),
        )


class TestConstraintsConfig:
    def test_get_diff__empty_and_some_exist(self):
        config = ConstraintsConfig(set_non_nulls=set(), set_constraints=set())
        other = ConstraintsConfig(
            set_non_nulls={"id"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="user_pk",
                    columns=["id"],
                )
            },
        )
        diff = config.get_diff(other)
        assert diff == ConstraintsConfig(
            set_non_nulls=set(),
            unset_non_nulls={"id"},
            set_constraints=set(),
            unset_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="user_pk",
                    columns=["id"],
                )
            },
        )

    def test_get_diff__some_new_and_empty_existing(self):
        config = ConstraintsConfig(
            set_non_nulls={"id"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["id"],
                )
            },
        )
        other = ConstraintsConfig(set_non_nulls=set(), set_constraints=set())
        diff = config.get_diff(other)
        assert diff == ConstraintsConfig(
            set_non_nulls={"id"},
            unset_non_nulls=set(),
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["id"],
                )
            },
            unset_constraints=set(),
        )

    def test_get_diff__mixed_case(self):
        config = ConstraintsConfig(
            set_non_nulls={"id", "email"},
            set_constraints={
                CheckConstraint(
                    type=ConstraintType.check,
                    name="check_name_length",
                    expression="LENGTH (name) >= 1",
                    warn_unenforced=False,
                ),
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["id"],
                ),
            },
        )
        other = ConstraintsConfig(
            set_non_nulls={"id", "name"},
            set_constraints={
                CheckConstraint(
                    type=ConstraintType.check,
                    name="check_name_length",
                    expression="LENGTH (name) >= 1",
                    # This should be ignored by the TypedConstraint equality check
                    warn_unenforced=True,
                ),
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["name"],
                ),
            },
        )
        diff = config.get_diff(other)
        assert diff == ConstraintsConfig(
            set_non_nulls={"email"},
            unset_non_nulls={"name"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["id"],
                )
            },
            unset_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_user",
                    columns=["name"],
                )
            },
        )

    def test_get_diff__check_constraints_different_formatting(self):
        config = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                CheckConstraint(
                    type=ConstraintType.check,
                    name="check_cause",
                    expression="cause  is  null\n or   cause in ('ACCEPTED', 'USER_REFUSED')",
                    warn_unenforced=False,
                )
            },
        )
        other = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                CheckConstraint(
                    type=ConstraintType.check,
                    name="check_cause",
                    expression="cause is null or cause in ( 'ACCEPTED' , 'USER_REFUSED' )",
                    warn_unenforced=False,
                )
            },
        )
        diff = config.get_diff(other)
        assert diff is None

    def test_get_diff__primary_key_rely_is_noop(self):
        # information_schema does not expose RELY/NORELY, so a catalog-read PK always has
        # expression=None. The model's RELY must not be treated as a change (issue #1513).
        config = ConstraintsConfig(
            set_non_nulls={"n"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_n",
                    columns=["n"],
                    expression="RELY",
                )
            },
        )
        other = ConstraintsConfig(
            set_non_nulls={"n"},
            set_constraints={
                PrimaryKeyConstraint(type=ConstraintType.primary_key, name="pk_n", columns=["n"])
            },
        )
        assert config.get_diff(other) is None

    def test_get_diff__foreign_key_expression_form_is_noop(self):
        # An expression-form FK carries its target inside `expression` (to/to_columns None);
        # the catalog reconstructs to/to_columns and drops the expression. Must not churn.
        config = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_n",
                    columns=["n"],
                    expression="(n) REFERENCES `cat`.`sch`.`parent` RELY",
                )
            },
        )
        other = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_n",
                    columns=["n"],
                    to="`cat`.`sch`.`parent`",
                    to_columns=["id"],
                )
            },
        )
        assert config.get_diff(other) is None

    def test_get_diff__foreign_key_repointed_to_new_target(self):
        # A repointed explicit FK (same name + columns, different target) is still reconciled.
        config = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_p",
                    columns=["parent_id"],
                    to="`cat`.`sch`.`parent_v2`",
                    to_columns=["id"],
                )
            },
        )
        other = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_p",
                    columns=["parent_id"],
                    to="`cat`.`sch`.`parent`",
                    to_columns=["id"],
                )
            },
        )
        diff = config.get_diff(other)
        assert diff is not None
        assert {c.to for c in diff.set_constraints} == {"`cat`.`sch`.`parent_v2`"}
        assert {c.to for c in diff.unset_constraints} == {"`cat`.`sch`.`parent`"}

    def test_get_diff__new_primary_key_retains_expression(self):
        # A genuinely new PK is added with its RELY intact, so the ADD renders `... RELY`.
        config = ConstraintsConfig(
            set_non_nulls={"n"},
            set_constraints={
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_n",
                    columns=["n"],
                    expression="RELY",
                )
            },
        )
        other = ConstraintsConfig(set_non_nulls=set(), set_constraints=set())
        diff = config.get_diff(other)
        assert {(c.name, c.expression) for c in diff.set_constraints} == {("pk_n", "RELY")}

    @staticmethod
    def _model_with_fks(*fk_constraints):
        model = Mock()
        model.config.contract.enforced = True
        model.identifier = "child"
        model.columns = {}
        model.constraints = list(fk_constraints)
        return model

    @staticmethod
    def _catalog_mirroring(model_config):
        # Mimic from_relation_results for the catalog: the create macro stored each key under the
        # same synthesized name, and information_schema round-trips columns/to/to_columns.
        return ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name=fk.name,
                    columns=fk.columns,
                    to=fk.to,
                    to_columns=fk.to_columns,
                )
                for fk in model_config.set_constraints
            },
        )

    def test_get_diff__unnamed_fk_matches_catalog_synthesized_name_is_noop(self):
        # End-to-end: from_relation_config synthesizes the create-macro name, which equals what the
        # catalog stored, so an unnamed FK is a no-op on an incremental run instead of churning
        # (#1344).
        model_config = ConstraintsProcessor.from_relation_config(
            self._model_with_fks(
                ModelLevelConstraint(
                    type=ConstraintType.foreign_key,
                    columns=["parent_id"],
                    to="`c`.`s`.`parent`",
                    to_columns=["id"],
                )
            )
        )
        assert model_config.get_diff(self._catalog_mirroring(model_config)) is None

        # The no-op holds *because* the names agree: had the catalog kept the pre-fix server name,
        # the diff would reconcile (drop + re-add), which is exactly the churn this fix removes.
        server_named = ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="child_parent_fk",
                    columns=["parent_id"],
                    to="`c`.`s`.`parent`",
                    to_columns=["id"],
                )
            },
        )
        assert model_config.get_diff(server_named) is not None

    def test_get_diff__two_unnamed_fks_to_same_parent_is_noop(self):
        # The exact #1344 shape: two unnamed FKs to one parent on different columns get distinct
        # synthesized names (so the re-adds would not collide) and neither churns.
        model_config = ConstraintsProcessor.from_relation_config(
            self._model_with_fks(
                ModelLevelConstraint(
                    type=ConstraintType.foreign_key,
                    columns=["a"],
                    to="`c`.`s`.`parent`",
                    to_columns=["id"],
                ),
                ModelLevelConstraint(
                    type=ConstraintType.foreign_key,
                    columns=["b"],
                    to="`c`.`s`.`parent`",
                    to_columns=["id"],
                ),
            )
        )
        assert len({c.name for c in model_config.set_constraints}) == 2
        assert model_config.get_diff(self._catalog_mirroring(model_config)) is None
