import pytest

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
        template_bundle.relation.type = "view"
        sql = self.render_bundle(template_bundle, "alter_set_tags", {"a": "valA", "b": "valB"})
        expected = self.clean_sql(
            "ALTER view `some_database`.`some_schema`.`some_table` "
            "SET TAGS ( 'a' = 'valA', 'b' = 'valB' )"
        )

        assert sql == expected

    def test_macros_alter_unset_tags(self, template_bundle):
        sql = self.render_bundle(template_bundle, "alter_unset_tags", ["a", "b"])
        expected = self.clean_sql(
            "ALTER table `some_database`.`some_schema`.`some_table` UNSET TAGS ( 'a','b' )"
        )

        assert sql == expected
