from unittest.mock import Mock

from agate import Table
from dbt_common.contracts.constraints import (
    ColumnLevelConstraint,
    ConstraintType,
    ModelLevelConstraint,
)

from dbt.adapters.databricks.constraints import (
    CheckConstraint,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from dbt.adapters.databricks.relation_configs.constraints import (
    ConstraintsConfig,
    ConstraintsProcessor,
)
from dbt.artifacts.resources.v1.components import ColumnInfo
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

    def test_from_relation_config__without_constraints(self):
        model = Mock()
        model.constraints = []
        model.columns = {}
        spec = ConstraintsProcessor.from_relation_config(model)
        assert spec == ConstraintsConfig(set_non_nulls=set(), set_constraints=set())

    def test_from_relation_config__with_check_constraint(self):
        model = Mock()
        model.columns = {}
        model.constraints = [
            ModelLevelConstraint(
                type=ConstraintType.check,
                name="check_name_length",
                expression="LENGTH (name) >= 1",
            )
        ]
        spec = ConstraintsProcessor.from_relation_config(model)
        assert spec == ConstraintsConfig(
            set_non_nulls=set(),
            set_constraints={
                CheckConstraint(
                    type=ConstraintType.check,
                    name="check_name_length",
                    expression="(LENGTH (name) >= 1)",
                )
            },
        )

    def test_from_relation_config__with_primary_key_constraint(self):
        model = Mock()
        model.columns = {}
        model.constraints = [
            ModelLevelConstraint(
                type=ConstraintType.primary_key,
                name="pk_user",
                columns=["id", "email"],
            )
        ]
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
        model = Mock()
        model.columns = {}
        model.constraints = [
            ModelLevelConstraint(
                type=ConstraintType.foreign_key,
                name="fk_user_customer",
                columns=["id", "email"],
                to="`catalog`.`schema`.`customers`",
                to_columns=["customer_id", "email"],
            )
        ]
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

    def test_from_relation_config__with_non_null_constraint(self):
        model = Mock()
        model.columns = {
            "email": ColumnInfo(
                name="email",
                constraints=[ColumnLevelConstraint(type=ConstraintType.not_null)],
            ),
        }
        model.constraints = [
            ModelLevelConstraint(
                type=ConstraintType.not_null,
                columns=["id"],
            )
        ]
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
