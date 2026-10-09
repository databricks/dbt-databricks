from unittest.mock import MagicMock, Mock

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

        # Mock adapter methods needed by the macro
        template_bundle.context["adapter"].is_cluster.return_value = True
        template_bundle.context["adapter"].compare_dbr_version.return_value = -1  # Old DBR
        template_bundle.context["config"].get.return_value = None  # No partition_by

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


class TestInsertIntoMacros(MacroTestBase):
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

    def test_insert_into_sql_impl__with_capability_matching_columns(
        self, template_bundle, mock_relations
    ):
        """Test insert_into_sql_impl with INSERT_BY_NAME capability and matching columns"""
        source_relation, target_relation = mock_relations

        # Mock adapter to have INSERT_BY_NAME capability
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=True)

        # Define matching columns
        dest_columns = ["id", "name", "value"]
        source_columns = ["id", "name", "value"]

        result = self.run_macro(
            template_bundle.template,
            "insert_into_sql_impl",
            target_relation,
            dest_columns,
            source_relation,
            source_columns,
        )

        # Verify the result contains the key clauses
        clean_result = self.clean_sql(result)
        assert "insert into" in clean_result
        assert "by name" in clean_result
        assert "select * from" in clean_result

    def test_insert_into_sql_impl__without_capability_matching_columns(
        self, template_bundle, mock_relations
    ):
        """Test insert_into_sql_impl without INSERT_BY_NAME capability and matching columns"""
        source_relation, target_relation = mock_relations

        # Mock adapter to NOT have INSERT_BY_NAME capability
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=False)

        # Define matching columns
        dest_columns = ["id", "name", "value"]
        source_columns = ["id", "name", "value"]

        result = self.run_macro(
            template_bundle.template,
            "insert_into_sql_impl",
            target_relation,
            dest_columns,
            source_relation,
            source_columns,
        )

        # Verify the result contains INSERT but NOT BY NAME
        clean_result = self.clean_sql(result)
        assert "insert into" in clean_result
        assert "by name" not in clean_result
        assert "select * from" in clean_result

    def test_insert_into_sql_impl__with_capability_mismatched_columns(
        self, template_bundle, mock_relations
    ):
        """Test insert_into_sql_impl with INSERT_BY_NAME capability when columns don't match"""
        source_relation, target_relation = mock_relations

        # Mock adapter to have INSERT_BY_NAME capability
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=True)

        # Define mismatched columns (source has extra column)
        dest_columns = ["id", "name"]
        source_columns = ["id", "name", "extra_column"]

        result = self.run_macro(
            template_bundle.template,
            "insert_into_sql_impl",
            target_relation,
            dest_columns,
            source_relation,
            source_columns,
        )

        # When columns don't match, should use explicit column list (no BY NAME)
        clean_result = self.clean_sql(result)
        assert "insert into" in clean_result
        assert "(`id`, `name`)" in clean_result
        assert "select `id`, `name` from" in clean_result

    def test_insert_into_sql_impl__without_capability_mismatched_columns(
        self, template_bundle, mock_relations
    ):
        """Test insert_into_sql_impl without INSERT_BY_NAME capability when columns don't match"""
        source_relation, target_relation = mock_relations

        # Mock adapter to NOT have INSERT_BY_NAME capability
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=False)

        # Define mismatched columns (target has extra column)
        dest_columns = ["id", "name", "timestamp"]
        source_columns = ["id", "name"]

        result = self.run_macro(
            template_bundle.template,
            "insert_into_sql_impl",
            target_relation,
            dest_columns,
            source_relation,
            source_columns,
        )

        # When columns don't match, should use explicit column list (no BY NAME)
        clean_result = self.clean_sql(result)
        assert "insert into" in clean_result
        assert "(`id`, `name`)" in clean_result
        assert "select `id`, `name` from" in clean_result

    def test_insert_into_sql_impl__with_capability_no_common_columns(
        self, template_bundle, mock_relations
    ):
        """Test insert_into_sql_impl with INSERT_BY_NAME capability when no columns match"""
        source_relation, target_relation = mock_relations

        # Mock adapter to have INSERT_BY_NAME capability
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=True)

        # Define completely different columns
        dest_columns = ["a", "b"]
        source_columns = ["x", "y"]

        result = self.run_macro(
            template_bundle.template,
            "insert_into_sql_impl",
            target_relation,
            dest_columns,
            source_relation,
            source_columns,
        )

        # Fallback case: no common columns, should still use BY NAME
        clean_result = self.clean_sql(result)
        assert "insert into" in clean_result
        assert "by name" in clean_result
        assert "select * from" in clean_result

    def test_insert_into_sql_impl__without_capability_no_common_columns(
        self, template_bundle, mock_relations
    ):
        """Test insert_into_sql_impl without INSERT_BY_NAME capability when no columns match"""
        source_relation, target_relation = mock_relations

        # Mock adapter to NOT have INSERT_BY_NAME capability
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=False)

        # Define completely different columns
        dest_columns = ["a", "b"]
        source_columns = ["x", "y"]

        result = self.run_macro(
            template_bundle.template,
            "insert_into_sql_impl",
            target_relation,
            dest_columns,
            source_relation,
            source_columns,
        )

        # Fallback case: no common columns, should NOT use BY NAME
        clean_result = self.clean_sql(result)
        assert "insert into" in clean_result
        assert "by name" not in clean_result
        assert "select * from" in clean_result

    def test_insert_into_sql_impl__case_insensitive_matching(self, template_bundle, mock_relations):
        """Test that column matching is case-insensitive"""
        source_relation, target_relation = mock_relations

        # Mock adapter to have INSERT_BY_NAME capability
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=True)

        # Define columns with different cases but same names
        dest_columns = ["ID", "Name", "VALUE"]
        source_columns = ["id", "name", "value"]

        result = self.run_macro(
            template_bundle.template,
            "insert_into_sql_impl",
            target_relation,
            dest_columns,
            source_relation,
            source_columns,
        )

        # Should still use BY NAME since columns match (case-insensitive)
        clean_result = self.clean_sql(result)
        assert "insert into" in clean_result
        assert "by name" in clean_result
        assert "select * from" in clean_result

    def _column(self, name):
        column = Mock()
        column.name = name
        return column

    def _named_relation(self, rendered):
        relation = MagicMock()
        relation.__str__.return_value = rendered
        relation.render.return_value = rendered
        return relation

    def test_get_insert_into_sql__no_dest_columns__describes_target(self, template_bundle):
        """Without dest_columns the target is described, so a target-only column narrows
        the INSERT to the intersection of target and source."""
        source_relation = self._named_relation("source_table")
        target_relation = self._named_relation("target_table")
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=True)
        template_bundle.context["adapter"].get_columns_in_relation = Mock(
            side_effect=lambda relation: (
                [self._column("id"), self._column("name")]
                if relation is source_relation
                else [self._column("id"), self._column("name"), self._column("target_only")]
            )
        )

        result = self.run_macro(
            template_bundle.template,
            "get_insert_into_sql",
            source_relation,
            target_relation,
        )

        expected = "insert into target_table (`id`, `name`) select `id`, `name` from source_table"
        self.assert_sql_equal(result, expected)

    def test_get_insert_into_sql__dest_columns_from_source__uses_by_name(self, template_bundle):
        """`process_schema_changes` returns the *source* columns, so reusing them as
        dest_columns makes the two sets match by construction and selects BY NAME even
        though the target still holds a column the source does not have."""
        source_relation = self._named_relation("source_table")
        target_relation = self._named_relation("target_table")
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=True)
        template_bundle.context["adapter"].get_columns_in_relation = Mock(
            return_value=[self._column("id"), self._column("name")]
        )

        result = self.run_macro(
            template_bundle.template,
            "get_insert_into_sql",
            source_relation,
            target_relation,
            [self._column("id"), self._column("name")],
        )

        expected = "insert into target_table by name select * from source_table"
        self.assert_sql_equal(result, expected)

    def test_get_insert_into_sql__dest_columns_ignored_without_by_name(self, template_bundle):
        """DBR < 12.2 has no BY NAME, and the matching-sets branch there degrades to a
        positional `select *`. dest_columns can omit a column the target still has, so the
        target is described instead and the explicit column list is kept."""
        source_relation = self._named_relation("source_table")
        target_relation = self._named_relation("target_table")
        template_bundle.context["adapter"].has_dbr_capability = Mock(return_value=False)
        template_bundle.context["adapter"].get_columns_in_relation = Mock(
            side_effect=lambda relation: (
                [self._column("id"), self._column("name")]
                if relation is source_relation
                else [self._column("id"), self._column("name"), self._column("target_only")]
            )
        )

        result = self.run_macro(
            template_bundle.template,
            "get_insert_into_sql",
            source_relation,
            target_relation,
            [self._column("id"), self._column("name")],
        )

        expected = "insert into target_table (`id`, `name`) select `id`, `name` from source_table"
        self.assert_sql_equal(result, expected)
