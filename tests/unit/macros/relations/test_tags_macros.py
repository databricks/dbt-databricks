from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.relation import DatabricksRelationType
from tests.unit.macros.base import MacroTestBase


class TestTagsMacros(MacroTestBase):
    @pytest.fixture
    def default_context(self):
        context = super().default_context.__wrapped__(self)
        context["apply_column_tags"] = Mock(return_value="")
        context["statement"] = Mock(side_effect=lambda name, caller: caller())
        return context

    @pytest.mark.parametrize("replaced_in_place", [False, True])
    @pytest.mark.parametrize("has_changes", [False, True])
    def test_reconcile_tags(self, template_bundle, config, replaced_in_place, has_changes):
        context = template_bundle.context
        adapter = context["adapter"]
        relation = template_bundle.relation
        relation.is_hive_metastore.return_value = False
        config["databricks_tags"] = {"classification": "internal"}
        desired_columns = {"set_column_tags": {"id": {"pii": "false"}}}
        adapter.get_column_tags_from_model.return_value = desired_columns
        adapter.get_table_replacement_tag_changes.return_value = {
            "table_tags": {"classification": "internal"} if has_changes else {},
            "column_tags": desired_columns["set_column_tags"] if has_changes else {},
        }

        self.render_bundle(template_bundle, "reconcile_tags", replaced_in_place)

        if replaced_in_place:
            adapter.get_table_replacement_tag_changes.assert_called_once_with(
                relation, context["config"].model
            )
            adapter.get_column_tags_from_model.assert_not_called()
        else:
            adapter.get_table_replacement_tag_changes.assert_not_called()
            adapter.get_column_tags_from_model.assert_called_once_with(context["config"].model)
        if has_changes or not replaced_in_place:
            context["statement"].assert_called_once()
            context["apply_column_tags"].assert_called_once_with(relation, desired_columns)
        else:
            context["statement"].assert_not_called()
            context["apply_column_tags"].assert_not_called()

    @pytest.fixture
    def template_name(self) -> str:
        return "tags.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    def test_macros_fetch_tags_sql(self, template_bundle):
        sql = self.render_bundle(template_bundle, "fetch_tags_sql")
        expected = self.clean_sql(
            "SELECT tag_name, tag_value "
            "FROM `system`.`information_schema`.`table_tags` "
            "WHERE catalog_name = 'some_database'"
            " AND schema_name = 'some_schema' AND table_name = 'some_table'"
        )
        assert sql == expected

    def test_macros_alter_set_tags(self, template_bundle):
        template_bundle.relation.type = DatabricksRelationType.View
        sql = self.render_bundle(template_bundle, "alter_set_tags", {"a": "valA", "b": "valB"})
        expected = self.clean_sql(
            "ALTER view `some_database`.`some_schema`.`some_table` "
            "SET TAGS ( 'a' = 'valA', 'b' = 'valB' )"
        )

        assert sql == expected
