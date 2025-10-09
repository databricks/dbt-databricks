from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestInsertOverwriteMacros(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental", "macros/relations/table"]

    @pytest.fixture
    def mock_relations(self):
        source_relation = Mock()
        source_relation.render.return_value = "source_table"

        target_relation = Mock()
        target_relation.render.return_value = "target_table"

        return source_relation, target_relation

    def test_get_insert_overwrite_sql__uses_by_name_syntax(self, template_bundle, mock_relations):
        """Test that get_insert_overwrite_sql generates INSERT OVERWRITE BY NAME syntax"""
        source_relation, target_relation = mock_relations

        # Mock the partition_cols macro to return empty string
        template_bundle.context["partition_cols"] = Mock(return_value="")

        result = self.run_macro(
            template_bundle.template,
            "get_insert_overwrite_sql",
            source_relation,
            target_relation,
        )

        # Verify the result contains the key BY NAME clause
        clean_result = self.clean_sql(result)
        assert "by name" in clean_result
        assert "insert overwrite table" in clean_result
        assert "select * from" in clean_result
