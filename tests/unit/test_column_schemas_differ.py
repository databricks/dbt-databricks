"""Unit tests for MV schema-drift helpers (issue #1359)."""

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.impl import DatabricksAdapter


def _col(name: str, dtype: str = "bigint") -> DatabricksColumn:
    return DatabricksColumn.create(name, dtype)


class TestColumnSchemasDiffer:
    def test_same_names_same_order(self):
        cols = [_col("id"), _col("name", "string")]
        assert DatabricksAdapter.column_schemas_differ(cols, cols) is False

    def test_case_insensitive_name_match(self):
        existing = [_col("ID"), _col("Name", "string")]
        inferred = [_col("id"), _col("name", "string")]
        assert DatabricksAdapter.column_schemas_differ(existing, inferred) is False

    def test_added_column(self):
        existing = [_col("id"), _col("name", "string")]
        inferred = [_col("id"), _col("name", "string"), _col("new_column")]
        assert DatabricksAdapter.column_schemas_differ(existing, inferred) is True

    def test_removed_column(self):
        existing = [_col("id"), _col("name", "string"), _col("gone")]
        inferred = [_col("id"), _col("name", "string")]
        assert DatabricksAdapter.column_schemas_differ(existing, inferred) is True

    def test_reordered_columns(self):
        existing = [_col("id"), _col("name", "string")]
        inferred = [_col("name", "string"), _col("id")]
        assert DatabricksAdapter.column_schemas_differ(existing, inferred) is True

    def test_type_string_variance_ignored(self):
        # Avoid spurious full refreshes when DESCRIBE paths disagree on type labels.
        existing = [_col("id", "bigint")]
        inferred = [_col("id", "long")]
        assert DatabricksAdapter.column_schemas_differ(existing, inferred) is False
