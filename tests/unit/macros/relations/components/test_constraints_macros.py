import re

import pytest

from dbt.adapters.databricks.constraints import (
    ConstraintType,
    CustomConstraint,
    ForeignKeyConstraint,
    PrimaryKeyConstraint,
)
from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.constraints import ConstraintsConfig
from tests.unit.macros.base import MacroTestBase

# Enough members that a hash-ordered set matching sorted order by chance is negligible.
NAMES = ["f", "c", "a", "e", "b", "d"]


def foreign_key(name: str) -> ForeignKeyConstraint:
    return ForeignKeyConstraint(
        type=ConstraintType.foreign_key,
        name=f"fk_{name}",
        columns=[f"col_{name}"],
        to="`cat`.`sch`.`parent`",
        to_columns=["id"],
    )


class TestApplyConstraintsMacro(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "constraints.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros/relations", "macros"]

    @pytest.fixture
    def passthrough_statement(self, template_bundle):
        template_bundle.context["statement"] = lambda label, fetch_result=False, caller=None: (
            caller() if caller else label
        )
        template_bundle.relation.is_hive_metastore = lambda: False
        template_bundle.relation.type = DatabricksRelationType.Table
        return template_bundle

    def test_apply_constraints_orders_statements(self, passthrough_statement):
        config = ConstraintsConfig(
            set_non_nulls={f"set_{name}" for name in NAMES},
            unset_non_nulls={f"unset_{name}" for name in NAMES},
            set_constraints={foreign_key(name) for name in NAMES},
            unset_constraints={foreign_key(name) for name in NAMES},
        )

        sql = self.render_bundle(passthrough_statement, "apply_constraints", config)

        expected = sorted(NAMES)
        assert re.findall(r"drop constraint if exists fk_(\w)", sql) == expected
        assert re.findall(r"alter column `unset_(\w)` drop not null", sql) == expected
        assert re.findall(r"alter column `set_(\w)` set not null", sql) == expected
        assert re.findall(r"add constraint fk_(\w) foreign key", sql) == expected

    def test_apply_constraints_orders_unnamed_constraints(self, passthrough_statement):
        primary_key = PrimaryKeyConstraint(type=ConstraintType.primary_key, columns=["id"])
        customs = {
            CustomConstraint(type=ConstraintType.custom, expression=f"check (col_{name} > 0)")
            for name in NAMES
        }
        config = ConstraintsConfig(set_non_nulls=set(), set_constraints={primary_key, *customs})

        sql = self.render_bundle(passthrough_statement, "apply_constraints", config)

        assert re.findall(r"add (\w+)", sql) == ["check"] * len(NAMES) + ["primary"]
        assert re.findall(r"check \(col_(\w) > 0\)", sql) == sorted(NAMES)
