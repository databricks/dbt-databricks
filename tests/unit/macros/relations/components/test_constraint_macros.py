import pytest
from dbt_common.contracts.constraints import ConstraintType

from dbt.adapters.databricks.constraints import CustomConstraint
from tests.unit.macros.base import MacroTestBase


class TestConstraintMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "constraints.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/components"]

    def test_alter_set_constraint_uses_typed_renderer(self, template_bundle):
        constraint = CustomConstraint(
            type=ConstraintType.custom,
            name="custom_id_positive",
            expression="CHECK (id > 0)",
        )
        result = self.run_macro(
            template_bundle.template,
            "alter_set_constraint",
            template_bundle.relation,
            constraint,
        )

        self.assert_sql_equal(
            result,
            "alter table `some_database`.`some_schema`.`some_table` "
            "add constraint custom_id_positive check (id > 0);",
        )
