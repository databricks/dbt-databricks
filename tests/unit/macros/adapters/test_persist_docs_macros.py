from unittest.mock import MagicMock, Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestPersistDocsMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "adapters/persist_docs.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/adapters"]

    @pytest.fixture
    def mock_relation(self):
        relation = Mock()
        relation.database = "test_db"
        relation.schema = "test_schema"
        relation.identifier = "test_table"
        relation.render = Mock(return_value="`test_db`.`test_schema`.`test_table`")
        relation.type = "table"
        return relation

    @pytest.fixture
    def mock_model_with_description(self):
        model = Mock()
        model.description = "This is a test model"
        return model

    @pytest.fixture
    def mock_model_with_columns(self):
        model = Mock()
        model.description = "Test model with columns"

        column1 = {"name": "id", "description": "Primary key"}
        column2 = {"name": "value", "description": "Contains 'quoted' text"}
        model.columns = {"id": column1, "value": column2}

        return model

    def test_comment_on_column_sql(self, template_bundle):
        column_path = "`test_db`.`test_schema`.`test_table`.id"
        escaped_comment = "This is a column comment"

        result = self.run_macro(
            template_bundle.template, "comment_on_column_sql", column_path, escaped_comment
        )

        expected_sql = """
            COMMENT ON COLUMN `test_db`.`test_schema`.`test_table`.id IS 'This is a column comment'
        """
        assert self.clean_sql(result) == self.clean_sql(expected_sql)

    def test_alter_relation_comment_sql(
        self, template_bundle, mock_relation, mock_model_with_description
    ):
        result = self.run_macro(
            template_bundle.template,
            "alter_relation_comment_sql",
            mock_relation,
            mock_model_with_description,
        )

        expected_sql = (
            "COMMENT ON TABLE `test_db`.`test_schema`.`test_table` IS 'This is a test model'"
        )
        assert self.clean_sql(result) == self.clean_sql(expected_sql)

    def test_alter_relation_comment_sql_with_quotes(self, template_bundle, mock_relation):
        model = Mock()
        model.description = "Model with 'quotes'"

        result = self.run_macro(
            template_bundle.template, "alter_relation_comment_sql", mock_relation, model
        )

        expected_sql = (
            "COMMENT ON TABLE `test_db`.`test_schema`.`test_table` IS 'Model with \\'quotes\\''"
        )
        assert self.clean_sql(result) == self.clean_sql(expected_sql)

    def test_alter_relation_comment_sql_view(self, template_bundle, mock_model_with_description):
        view_relation = Mock()
        view_relation.database = "test_db"
        view_relation.schema = "test_schema"
        view_relation.identifier = "test_view"
        view_relation.render = Mock(return_value="`test_db`.`test_schema`.`test_view`")
        view_relation.type = "view"

        result = self.run_macro(
            template_bundle.template,
            "alter_relation_comment_sql",
            view_relation,
            mock_model_with_description,
        )

        expected_sql = (
            "COMMENT ON VIEW `test_db`.`test_schema`.`test_view` IS 'This is a test model'"
        )
        assert self.clean_sql(result) == self.clean_sql(expected_sql)

    def test_databricks__alter_column_comment_delta(
        self, template_bundle, context, mock_relation, mock_model_with_columns
    ):
        context["config"] = Mock()
        context["config"].get = Mock(return_value="delta")

        context["api"] = MagicMock()
        context["api"].Column.get_name = Mock(side_effect=lambda col: col["name"])

        context["run_query_as"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__alter_column_comment",
            mock_relation,
            mock_model_with_columns.columns,
        )

        assert context["run_query_as"].call_count == 2

        call_args_list = context["run_query_as"].call_args_list

        first_call = call_args_list[0][0][0]
        expected_first_sql = (
            "COMMENT ON COLUMN `test_db`.`test_schema`.`test_table`.id IS 'Primary key'"
        )
        assert self.clean_sql(first_call) == self.clean_sql(expected_first_sql)

        second_call = call_args_list[1][0][0]
        expected_second_sql = """
        COMMENT ON COLUMN `test_db`.`test_schema`.`test_table`.value IS 'Contains \\'quoted\\' text'
"""
        assert self.clean_sql(second_call) == self.clean_sql(expected_second_sql)

    def test_databricks__alter_column_comment_unsupported_format(
        self, template_bundle, context, mock_relation, mock_model_with_columns
    ):
        context["config"] = Mock()
        context["config"].get = Mock(return_value="parquet")

        context["log"] = Mock()
        context["run_query_as"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__alter_column_comment",
            mock_relation,
            mock_model_with_columns.columns,
        )

        context["run_query_as"].assert_not_called()
        context["log"].assert_called_once_with(
            "WARNING - requested to update column comments, "
            "but file format parquet does not support that."
        )

    def test_databricks__persist_docs_relation_only(
        self, template_bundle, context, mock_relation, mock_model_with_description
    ):
        context["config"] = MagicMock()
        context["config"].persist_relation_docs.return_value = True

        context["run_query_as"] = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "databricks__persist_docs",
            mock_relation,
            mock_model_with_description,
            True,  # for_relation
            False,  # for_columns
        )

        sql = context["run_query_as"].call_args[0][0]
        expected_sql = (
            "COMMENT ON TABLE `test_db`.`test_schema`.`test_table` IS 'This is a test model'"
        )
        assert self.clean_sql(sql) == self.clean_sql(expected_sql)
