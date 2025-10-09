from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestReplaceWhereMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "materializations/incremental/strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/materializations/incremental"]

    @pytest.fixture
    def mock_relations(self):
        target_relation = Mock()
        target_relation.render.return_value = "schema.target_table"

        temp_relation = Mock()
        temp_relation.render.return_value = "schema.temp_table"

        return target_relation, temp_relation

    def test_get_replace_where_sql_with_string_predicate(self, template_bundle, mock_relations):
        target_relation, temp_relation = mock_relations

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table BY NAME
        REPLACE WHERE date_col > '2023-01-01'
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
        INSERT INTO schema.target_table BY NAME
        REPLACE WHERE date_col > '2023-01-01' and another_col != 'value'
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
        INSERT INTO schema.target_table BY NAME
        TABLE schema.temp_table
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
        INSERT INTO schema.target_table BY NAME
        TABLE schema.temp_table
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
        INSERT INTO schema.target_table BY NAME
        REPLACE WHERE date_col BETWEEN '2023-01-01' AND '2023-01-31' and status IN ('active', 'pending') and amount > 1000
        TABLE schema.temp_table
        """  # noqa

        self.assert_sql_equal(result, expected)

    def test_get_replace_where_sql__uses_by_name_syntax(self, template_bundle, mock_relations):
        """Test that get_replace_where_sql generates INSERT BY NAME syntax"""
        target_relation, temp_relation = mock_relations

        args_dict = {
            "target_relation": target_relation,
            "temp_relation": temp_relation,
            "incremental_predicates": "date_col > '2023-01-01'",
        }

        result = self.run_macro(template_bundle.template, "get_replace_where_sql", args_dict)

        expected = """
        INSERT INTO schema.target_table BY NAME
        REPLACE WHERE date_col > '2023-01-01'
        TABLE schema.temp_table
        """

        self.assert_sql_equal(result, expected)
