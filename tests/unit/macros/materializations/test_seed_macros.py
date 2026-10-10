from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestCreateOrReplaceCsvTable(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "helpers.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/seeds", "macros"]

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return ["relations/components/comment.sql"]

    @pytest.fixture
    def context(self, template, relation) -> dict:
        context = template.globals
        context["this"] = relation
        context["statement"] = lambda name, caller: ""
        context["config"].persist_column_docs.return_value = True
        context["adapter"].convert_type.return_value = "string"
        context["adapter"].quote_seed_column = lambda name, quote: f"`{name}`"
        for clause in [
            "file_format_clause",
            "partition_cols",
            "clustered_cols",
            "location_clause",
            "comment_clause",
            "tblproperties_clause",
        ]:
            context[clause] = Mock(return_value="")
        return context

    def test_create_or_replace_csv_table_escapes_backslashes(self, template_bundle):
        model = {
            "config": {},
            "alias": "some_table",
            "columns": {"pattern": {"description": r"Bob\'s ^\d"}},
        }
        agate_table = Mock(column_names=["pattern"])

        sql = self.run_macro(
            template_bundle.template, "create_or_replace_csv_table", model, agate_table
        )

        expected = self.clean_sql(
            f"create table {template_bundle.relation.render()} "
            r"(`pattern` string comment 'Bob\\\'s ^\\d')"
        )
        assert sql == expected
