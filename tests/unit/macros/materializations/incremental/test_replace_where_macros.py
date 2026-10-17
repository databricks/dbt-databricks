from unittest.mock import Mock

import pytest

from dbt.adapters.databricks.column import DatabricksColumn
from tests.unit.macros.base import MacroTestBase


class TestReplaceWhereMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "materializations/incremental/strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/materializations/incremental"]

    @pytest.fixture(autouse=True)
    def setup_mock_capability(self, context):
        """Mock has_dbr_capability so the replace_where BY NAME path is enabled by default
        (DBR 18.0+ / SQL warehouse — the insert_by_name_replace_where capability)."""

        def has_dbr_capability_side_effect(capability_name):
            if capability_name in ("insert_by_name", "insert_by_name_replace_where"):
                return True
            return False

        context["adapter"].has_dbr_capability = Mock(side_effect=has_dbr_capability_side_effect)

    @pytest.fixture
    def mock_relations(self):
        target_relation = Mock()
        target_relation.render.return_value = "schema.target_table"

        temp_relation = Mock()
        temp_relation.render.return_value = "schema.temp_table"

        return target_relation, temp_relation

    @staticmethod
    def set_relation_columns(context, columns_by_relation):
        def get_columns_in_relation(relation):
            return [
                DatabricksColumn(column=name, dtype="string")
                for name in columns_by_relation[relation]
            ]

        context["adapter"].get_columns_in_relation = Mock(side_effect=get_columns_in_relation)

    @staticmethod
    def simulate_dbr_below_replace_where_by_name_floor(context):
        context["adapter"].has_dbr_capability = Mock(
            side_effect=lambda capability_name: capability_name == "insert_by_name"
        )

    def test_get_replace_where_sql_with_string_predicate(self, template_bundle, mock_relations):
        target_relation, temp_relation = mock_relations

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        BY NAME REPLACE WHERE date_col > '2023-01-01'
        TABLE schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql_with_predicate_list(self, template_bundle, mock_relations):
        target_relation, temp_relation = mock_relations

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": [
                "date_col > '2023-01-01'",
                "another_col != 'value'",
            ],
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        BY NAME REPLACE WHERE date_col > '2023-01-01' and another_col != 'value'
        TABLE schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql_without_predicates(self, template_bundle, mock_relations):
        target_relation, temp_relation = mock_relations

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": None,
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        BY NAME TABLE schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql_with_empty_predicate_list(self, template_bundle, mock_relations):
        target_relation, temp_relation = mock_relations

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": [],
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        BY NAME TABLE schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql_with_complex_predicates(self, template_bundle, mock_relations):
        target_relation, temp_relation = mock_relations

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": [
                "date_col BETWEEN '2023-01-01' AND '2023-01-31'",
                "status IN ('active', 'pending')",
                "amount > 1000",
            ],
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        BY NAME REPLACE WHERE date_col BETWEEN '2023-01-01' AND '2023-01-31' and status IN ('active', 'pending') and amount > 1000
        TABLE schema.temp_table
        """  # noqa

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql__omits_by_name_below_replace_where_floor(
        self, template_bundle, context, mock_relations
    ):
        """Issue #1532: a cluster with the plain insert_by_name capability but NOT the
        insert_by_name_replace_where combination (DBR < 18.0, e.g. the reporter's 16.4) must
        omit BY NAME from the replace_where statement, or its parser rejects the SQL."""

        self.simulate_dbr_below_replace_where_by_name_floor(context)

        target_relation, temp_relation = mock_relations
        self.set_relation_columns(
            context, {target_relation: ["id", "name"], temp_relation: ["id", "name"]}
        )
        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        REPLACE WHERE date_col > '2023-01-01'
        SELECT `id`, `name` FROM schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql__without_insert_by_name(
        self, template_bundle, context, mock_relations
    ):
        """Test that BY NAME is omitted when the DBR version doesn't support insert_by_name"""
        context["adapter"].has_dbr_capability = Mock(return_value=False)

        target_relation, temp_relation = mock_relations
        self.set_relation_columns(
            context, {target_relation: ["id", "name"], temp_relation: ["id", "name"]}
        )

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        REPLACE WHERE date_col > '2023-01-01'
        SELECT `id`, `name` FROM schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql__projects_target_column_order_below_by_name_floor(
        self, template_bundle, context, mock_relations
    ):
        self.simulate_dbr_below_replace_where_by_name_floor(context)
        target_relation, temp_relation = mock_relations
        self.set_relation_columns(
            context,
            {
                target_relation: ["id", "name", "status", "new_col"],
                temp_relation: ["id", "new_col", "name", "status"],
            },
        )
        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        REPLACE WHERE date_col > '2023-01-01'
        SELECT `id`, `name`, `status`, `new_col` FROM schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql__fills_target_columns_missing_from_source_with_null(
        self, template_bundle, context, mock_relations
    ):
        self.simulate_dbr_below_replace_where_by_name_floor(context)
        target_relation, temp_relation = mock_relations
        self.set_relation_columns(
            context,
            {target_relation: ["id", "dropped", "name"], temp_relation: ["name", "id"]},
        )
        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": None,
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        SELECT `id`, NULL as `dropped`, `name` FROM schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql__matches_source_columns_case_insensitively(
        self, template_bundle, context, mock_relations
    ):
        self.simulate_dbr_below_replace_where_by_name_floor(context)
        target_relation, temp_relation = mock_relations
        self.set_relation_columns(
            context,
            {target_relation: ["id", "Name"], temp_relation: ["NAME", "Id"]},
        )
        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        REPLACE WHERE date_col > '2023-01-01'
        SELECT `Id`, `NAME` FROM schema.temp_table
        """

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql__keeps_table_form_with_by_name_on_reordered_columns(
        self, template_bundle, context, mock_relations
    ):
        target_relation, temp_relation = mock_relations
        self.set_relation_columns(
            context,
            {target_relation: ["id", "name", "new_col"], temp_relation: ["new_col", "id", "name"]},
        )
        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table
        BY NAME REPLACE WHERE date_col > '2023-01-01'
        TABLE schema.temp_table
        """

        self.assert_sql_equal(result, expected)
