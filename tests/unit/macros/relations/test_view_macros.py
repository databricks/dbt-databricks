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
            f"create or replace view {template_bundle.relation.render()} "
            "tblproperties ('tblproperties_to_view' = 'true') as (select 1)"
        )

        assert sql == expected


class TestAlterView(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "alter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/view"]

    @pytest.fixture(autouse=True, scope="function")
    def mocks(self, context):
        context["apply_tags"] = Mock()
        context["apply_tblproperties"] = Mock()
        context["alter_query"] = Mock()

    def render_alter_view(self, template_bundle, changes):
        return self.run_macro(
            template_bundle.template,
            "alter_view",
            template_bundle.relation,
            changes,
        )

    def test_macros__alter_view_empty_changes(self, context, template_bundle):
        self.render_alter_view(template_bundle, {})
        context["apply_tags"].assert_not_called()
        context["apply_tblproperties"].assert_not_called()
        context["alter_query"].assert_not_called()

    def test_macros__alter_view_with_tags(self, context, template_bundle):
        self.render_alter_view(template_bundle, {"tags": Mock()})
        context["apply_tags"].assert_called_once()
        context["apply_tblproperties"].assert_not_called()
        context["alter_query"].assert_not_called()

    def test_macros__alter_view_with_tblproperties(self, context, template_bundle):
        self.render_alter_view(template_bundle, {"tblproperties": Mock()})
        context["apply_tags"].assert_not_called()
        context["apply_tblproperties"].assert_called_once()
        context["alter_query"].assert_not_called()

    def test_macros__alter_view_with_query(self, context, template_bundle):
        self.render_alter_view(template_bundle, {"query": Mock()})
        context["apply_tags"].assert_not_called()
        context["apply_tblproperties"].assert_not_called()
        context["alter_query"].assert_called_once()
