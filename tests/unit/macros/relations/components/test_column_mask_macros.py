import pytest

from tests.unit.macros.base import MacroTestBase


class TestColumnMaskMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "column_mask.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros/relations", "macros"]

    def test_fetch_column_masks_sql(self, template_bundle):
        sql = self.render_bundle(template_bundle, "fetch_column_masks_sql")
        expected = self.clean_sql(
            "SELECT column_name, mask_name, using_columns "
            "FROM `system`.`information_schema`.`column_masks` "
            "WHERE table_catalog = 'some_database' "
            "AND table_schema = 'some_schema' "
            "AND table_name = 'some_table';"
        )
        assert sql == expected

    def test_fetch_column_masks_sql_escapes_special_characters(self, template_bundle):
        template_bundle.relation.database = "cat'a\\log"
        template_bundle.relation.schema = "sch'e\\ma"
        template_bundle.relation.identifier = "orders'eu\\test"
        sql = self.render_bundle(template_bundle, "fetch_column_masks_sql")
        expected = self.clean_sql(
            "SELECT column_name, mask_name, using_columns "
            "FROM `system`.`information_schema`.`column_masks` "
            "WHERE table_catalog = 'cat\\'a\\\\log' "
            "AND table_schema = 'sch\\'e\\\\ma' "
            "AND table_name = 'orders\\'eu\\\\test';"
        )
        assert sql == expected
