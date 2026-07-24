"""Unit tests for missing documented-column warning dedupe."""

import threading
from unittest.mock import patch

from dbt.adapters.databricks.persist_doc_column_warnings import (
    reset_missing_persist_doc_column_warnings,
    warn_missing_persist_doc_columns,
)
from dbt.adapters.databricks.relation_configs.column_comments import ColumnCommentsConfig


class TestWarnMissingPersistDocColumnsDedupe:
    def setup_method(self) -> None:
        reset_missing_persist_doc_column_warnings()

    @patch("dbt.adapters.databricks.persist_doc_column_warnings.warn_or_error")
    def test_warns_once_for_same_missing_set(self, mock_warn):
        warn_missing_persist_doc_columns(["col2"])
        warn_missing_persist_doc_columns(["col2"])
        mock_warn.assert_called_once()

    @patch("dbt.adapters.databricks.persist_doc_column_warnings.warn_or_error")
    def test_get_diff_and_helper_share_dedupe(self, mock_warn):
        """get_diff then the shared helper must not double-fire for the same cols."""
        config = ColumnCommentsConfig(comments={"col1": "new", "col2": "missing"}, persist=True)
        other = ColumnCommentsConfig(comments={"col1": "old"})
        config.get_diff(other)
        warn_missing_persist_doc_columns(["col2"])
        mock_warn.assert_called_once()
        assert "col2" in mock_warn.call_args.args[0].base_msg

    @patch("dbt.adapters.databricks.persist_doc_column_warnings.warn_or_error")
    def test_different_missing_sets_warn_separately(self, mock_warn):
        warn_missing_persist_doc_columns(["col2"])
        warn_missing_persist_doc_columns(["col3"])
        assert mock_warn.call_count == 2

    @patch("dbt.adapters.databricks.persist_doc_column_warnings.warn_or_error")
    def test_thread_local_isolation(self, mock_warn):
        """A reset on another thread must not suppress warnings on this thread."""
        warn_missing_persist_doc_columns(["col2"])
        assert mock_warn.call_count == 1

        def other_thread() -> None:
            reset_missing_persist_doc_column_warnings()
            warn_missing_persist_doc_columns(["col2"])

        t = threading.Thread(target=other_thread)
        t.start()
        t.join()
        # Main thread already warned once; other thread warns independently → 2 total.
        assert mock_warn.call_count == 2
        # Main thread still dedupes its own second call.
        warn_missing_persist_doc_columns(["col2"])
        assert mock_warn.call_count == 2
