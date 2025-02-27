from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestCreateViewAs(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/view"]

    def render_create_view_as(self, template_bundle, sql="select 1"):
        return self.run_macro(
            template_bundle.template,
            "databricks__create_view_as",
            template_bundle.relation,
            sql,
        )

    def test_macros_create_view_as_tblproperties(self, config, template_bundle):
        config["tblproperties"] = {"tblproperties_to_view": "true"}
        template_bundle.context["get_columns_in_query"] = Mock(return_value=[])

        sql = self.render_create_view_as(template_bundle)
        expected = (
            f"create or replace view {template_bundle.relation} "
            "tblproperties ('tblproperties_to_view' = 'true' ) as ( select 1 )"
        )

        assert sql == expected
