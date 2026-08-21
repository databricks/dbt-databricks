import pytest
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.databricks.constraints import (
    CheckConstraint,
    CustomConstraint,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from tests.unit.macros.base import MacroTestBase


class TestConstraintMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "constraints.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/components"]

    @pytest.mark.parametrize(
        "constraint, expected",
        [
            (
                CheckConstraint(
                    type=ConstraintType.check,
                    name="id_positive",
                    expression="id > 0",
                ),
                "alter table `some_database`.`some_schema`.`some_table` "
                "add constraint id_positive check (id > 0);",
            ),
            (
                PrimaryKeyConstraint(
                    type=ConstraintType.primary_key,
                    name="pk_model",
                    columns=["order"],
                ),
                "alter table `some_database`.`some_schema`.`some_table` "
                "add constraint pk_model primary key (`order`);",
            ),
            (
                ForeignKeyConstraint(
                    type=ConstraintType.foreign_key,
                    name="fk_parent",
                    columns=["parent id"],
                    to="`catalog`.`schema`.`parent`",
                    to_columns=["id"],
                ),
                "alter table `some_database`.`some_schema`.`some_table` "
                "add constraint fk_parent foreign key (`parent id`) "
                "references `catalog`.`schema`.`parent` (`id`);",
            ),
            (
                CustomConstraint(
                    type=ConstraintType.custom,
                    name="custom_id_positive",
                    expression="CHECK (id > 0)",
                ),
                "alter table `some_database`.`some_schema`.`some_table` "
                "add constraint custom_id_positive check (id > 0);",
            ),
        ],
    )
    def test_alter_set_constraint_uses_typed_renderer(
        self,
        template_bundle,
        constraint,
        expected,
    ):
        result = self.run_macro(
            template_bundle.template,
            "alter_set_constraint",
            template_bundle.relation,
            constraint,
        )

        self.assert_sql_equal(result, expected)
