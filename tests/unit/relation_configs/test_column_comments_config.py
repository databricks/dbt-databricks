from unittest.mock import Mock, patch

from agate import Table

from dbt.adapters.databricks.persist_doc_column_warnings import (
    reset_missing_persist_doc_column_warnings,
)
from dbt.adapters.databricks.relation_configs.column_comments import (
    ColumnCommentsConfig,
    ColumnCommentsProcessor,
)


class TestColumnCommentsProcessor:
    def test_from_relation_results__no_comments(self):
        results = {
            "describe_extended": Table(
                rows=[
                    ["col_a", "int", None],
                    ["col_b", "string", None],
                ],
                column_names=["col_name", "data_type", "comment"],
            )
        }
        config = ColumnCommentsProcessor.from_relation_results(results)
        assert config == ColumnCommentsConfig(comments={"col_a": "", "col_b": ""})

    def test_from_relation_results__with_comments(self):
        results = {
            "describe_extended": Table(
                rows=[
                    ["col_a", "int", "Comment for col_a"],
                    ["col_b", "string", None],
                ],
                column_names=["col_name", "data_type", "comment"],
            )
        }
        config = ColumnCommentsProcessor.from_relation_results(results)
        assert config == ColumnCommentsConfig(comments={"col_a": "Comment for col_a", "col_b": ""})

    def test_from_relation_config__no_persist(self):
        model = Mock()
        model.columns = {"col1": {"description": "test"}}
        model.config.persist_docs = {}
        config = ColumnCommentsProcessor.from_relation_config(model)
        assert config == ColumnCommentsConfig(comments={"col1": "test"}, persist=False)

    def test_from_relation_config__with_persist(self):
        model = Mock()
        model.columns = {"col1": {"description": "test comment"}}
        # Column comments are gated on persist_docs.columns, not .relation.
        model.config.persist_docs = {"columns": True}
        config = ColumnCommentsProcessor.from_relation_config(model)
        assert config == ColumnCommentsConfig(comments={"col1": "test comment"}, persist=True)

    def test_from_relation_config__columns_true_relation_false(self):
        """persist_docs.columns drives column comments even when .relation is false."""
        model = Mock()
        model.columns = {"col1": {"description": "test comment"}}
        model.config.persist_docs = {"columns": True, "relation": False}
        config = ColumnCommentsProcessor.from_relation_config(model)
        assert config == ColumnCommentsConfig(comments={"col1": "test comment"}, persist=True)

    def test_from_relation_config__relation_true_columns_false(self):
        """Column comments are not applied when .columns is off, even if .relation is on."""
        model = Mock()
        model.columns = {"col1": {"description": "test comment"}}
        model.config.persist_docs = {"relation": True, "columns": False}
        config = ColumnCommentsProcessor.from_relation_config(model)
        assert config == ColumnCommentsConfig(comments={"col1": "test comment"}, persist=False)


class TestColumnCommentsConfig:
    def test_get_diff__no_changes(self):
        config = ColumnCommentsConfig(
            comments={"col1": "comment1", "col2": "comment2"}, persist=True
        )
        other = ColumnCommentsConfig(comments={"col1": "comment1", "col2": "comment2"})
        diff = config.get_diff(other)
        assert diff is None

    def test_get_diff__with_changes(self):
        config = ColumnCommentsConfig(
            comments={"col1": "new comment", "col2": "comment2"}, persist=True
        )
        other = ColumnCommentsConfig(comments={"col1": "old comment", "col2": "comment2"})
        diff = config.get_diff(other)
        assert diff == ColumnCommentsConfig(comments={"`col1`": "new comment"}, persist=True)

    def test_get_diff__no_persist(self):
        config = ColumnCommentsConfig(comments={"col1": "new comment"}, persist=False)
        other = ColumnCommentsConfig(comments={"col1": "old comment"})
        diff = config.get_diff(other)
        assert diff is None

    def test_get_diff__case_mismatch_column_names(self):
        """Test that column name case mismatches are handled correctly."""
        # Config has lowercase column names (from YAML schema)
        config = ColumnCommentsConfig(
            comments={"account_id": "Account ID", "user_name": "User Name"}, persist=True
        )
        # Other has mixed case column names (from database)
        other = ColumnCommentsConfig(
            comments={"Account_ID": "Account ID", "User_Name": "User Name"}
        )
        # Should recognize these as the same columns and return no diff
        diff = config.get_diff(other)
        assert diff is None

    def test_get_diff__case_mismatch_with_actual_changes(self):
        """Test that real changes are detected even with case mismatches."""
        # Config has lowercase column names with new comment
        config = ColumnCommentsConfig(
            comments={"account_id": "New Account ID", "user_name": "User Name"}, persist=True
        )
        # Other has mixed case column names with old comment
        other = ColumnCommentsConfig(
            comments={"Account_ID": "Old Account ID", "User_Name": "User Name"}
        )
        # Should detect that account_id comment changed
        diff = config.get_diff(other)
        assert diff == ColumnCommentsConfig(
            comments={"`account_id`": "New Account ID"}, persist=True
        )

    @patch("dbt.adapters.databricks.persist_doc_column_warnings.warn_or_error")
    def test_get_diff__skips_missing_column_without_warning(self, mock_warn):
        """Documented columns absent from the relation are skipped; get_diff does not warn.

        The missing-column warning now runs post-build (validate_persist_doc_columns), so the
        pre-materialization diff must stay silent to avoid false-warning on a legitimately new
        column.
        """
        reset_missing_persist_doc_column_warnings()
        # col2 is documented but not present in the relation
        config = ColumnCommentsConfig(
            comments={"col1": "new comment", "col2": "comment for missing column"}, persist=True
        )
        other = ColumnCommentsConfig(comments={"col1": "old comment"})
        diff = config.get_diff(other)
        # Only the existing column is included in the diff; the missing one is skipped.
        assert diff == ColumnCommentsConfig(comments={"`col1`": "new comment"}, persist=True)
        # No warning is emitted from the diff path.
        mock_warn.assert_not_called()

    @patch("dbt.adapters.databricks.persist_doc_column_warnings.warn_or_error")
    def test_get_diff__no_warning_when_not_persisting(self, mock_warn):
        """Missing columns are not evaluated (or warned about) when persist is False."""
        reset_missing_persist_doc_column_warnings()
        config = ColumnCommentsConfig(comments={"col1": "comment", "col2": "comment"})
        other = ColumnCommentsConfig(comments={"col1": "comment"})
        assert config.get_diff(other) is None
        mock_warn.assert_not_called()
