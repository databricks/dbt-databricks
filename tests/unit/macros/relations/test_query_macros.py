import pytest

from tests.unit.macros.base import MacroTestBase


class TestAlterQuery(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "query.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/components"]

    def render_alter_query(self, template_bundle, sql="select 1"):
        return self.run_macro(
            template_bundle.template,
            "get_alter_query_sql",
            template_bundle.relation,
            sql,
        )

    def test_macros__alter_query(self, template_bundle):
        sql = self.render_alter_query(template_bundle)
        expected = self.clean_sql(
            f"ALTER {str(template_bundle.relation.type).upper()}"
            f" {template_bundle.relation.render()} AS ( select 1 )"
        )
        assert sql == expected
