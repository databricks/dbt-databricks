from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestLoadCsvRows(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "helpers.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/seeds", "macros"]

    @pytest.fixture
    def agate_table(self):
        table = Mock()
        table.column_names = ["id", "name", "amount"]
        table.rows = [(1, "a", 1.5), (2, "b", 2.5), (3, "c", 3.5)]
        return table

    @pytest.fixture
    def seed_context(self, context, relation):
        context["get_batch_size"] = Mock(return_value=2)
        context["get_binding_char"] = Mock(return_value="%s")
        context["this"] = relation
        inferred = {0: "bigint", 1: "string", 2: "double"}
        context["adapter"].convert_type = Mock(side_effect=lambda _table, index: inferred[index])
        return context

    def render(self, template, file_format, agate_table):
        model = {
            "config": {"file_format": file_format, "column_types": {"amount": "decimal(10,2)"}}
        }
        return self.run_macro(template, "databricks__load_csv_rows", model, agate_table)

    def test_parquet_casts_each_column_to_its_resolved_type(
        self, template, seed_context, agate_table
    ):
        sql = self.render(template, "parquet", agate_table)

        row = "(cast(%s as bigint),cast(%s as string),cast(%s as decimal(10,2)))"
        self.assert_sql_equal(
            sql, f"insert overwrite `some_database`.`some_schema`.`some_table` values {row},{row}"
        )
        batches = seed_context["adapter"].add_query.call_args_list
        assert [call.kwargs["bindings"] for call in batches] == [
            [1, "a", 1.5, 2, "b", 2.5],
            [3, "c", 3.5],
        ]
        self.assert_sql_equal(
            batches[1].args[0],
            f"insert into `some_database`.`some_schema`.`some_table` values {row}",
        )

    def test_types_and_binding_char_resolve_once_per_load(
        self, template, seed_context, agate_table
    ):
        self.render(template, "parquet", agate_table)

        assert seed_context["adapter"].convert_type.call_count == len(agate_table.column_names)
        assert seed_context["get_binding_char"].call_count == 1

    def test_delta_uses_plain_placeholders(self, template, seed_context, agate_table):
        sql = self.render(template, "delta", agate_table)

        self.assert_sql_equal(
            sql,
            "insert overwrite `some_database`.`some_schema`.`some_table` values "
            "(%s,%s,%s),(%s,%s,%s)",
        )
        seed_context["adapter"].convert_type.assert_not_called()


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
