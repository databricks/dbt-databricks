import pytest

from dbt.adapters.databricks.relation import DatabricksRelationType
from tests.unit.macros.base import MacroTestBase


class TestTagsMacros(MacroTestBase):
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

    def test_planned_tags_do_not_probe_relation_catalog(self, template_bundle):
        template_bundle.context["statement"] = (
            lambda label, fetch_result=False, caller=None: caller() if caller else label
        )
        template_bundle.relation.is_hive_metastore = lambda: (_ for _ in ()).throw(
            AssertionError("typed renderer must not infer catalog type")
        )

        sql = self.render_bundle(
            template_bundle,
            "apply_tags_from_plan",
            {"domain": "finance"},
            "unity",
        )

        assert "set tags ('domain' = 'finance')" in sql
