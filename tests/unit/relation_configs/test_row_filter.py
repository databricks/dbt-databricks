from unittest.mock import Mock

import pytest
from agate import Table

from dbt.adapters.databricks.relation_configs.row_filter import (
    RowFilterConfig,
    RowFilterProcessor,
)


class TestRowFilterConfig:
    def test_no_change_when_both_none(self):
        desired = RowFilterConfig()
        existing = RowFilterConfig()
        assert desired.get_diff(existing) is None

    def test_unset_when_removed(self):
        """When filter is removed from config, return RowFilterConfig with should_unset=True."""
        desired = RowFilterConfig()
        existing = RowFilterConfig(function="cat.schema.fn", columns=("col1",))
        diff = desired.get_diff(existing)
        assert diff is not None
        assert isinstance(diff, RowFilterConfig)
        assert diff.should_unset is True
        assert diff.is_change is True  # Critical: marks as actual change
        assert diff.function is None

    def test_set_when_new(self):
        """When filter is added, return RowFilterConfig with the filter to apply."""
        desired = RowFilterConfig(function="cat.schema.fn", columns=("col1",))
        existing = RowFilterConfig()
        diff = desired.get_diff(existing)
        assert diff is not None
        assert isinstance(diff, RowFilterConfig)
        assert diff.function == "cat.schema.fn"
        assert diff.should_unset is False
        assert diff.is_change is True  # Critical: marks as actual change

    def test_no_change_when_equal_case_insensitive(self):
        desired = RowFilterConfig(function="CAT.SCHEMA.FN", columns=("COL1",))
        existing = RowFilterConfig(function="cat.schema.fn", columns=("col1",))
        assert desired.get_diff(existing) is None

    def test_change_when_different_function(self):
        """When filter function changes, return RowFilterConfig with new function."""
        desired = RowFilterConfig(function="cat.schema.fn2", columns=("col1",))
        existing = RowFilterConfig(function="cat.schema.fn1", columns=("col1",))
        diff = desired.get_diff(existing)
        assert diff is not None
        assert isinstance(diff, RowFilterConfig)
        assert diff.function == "cat.schema.fn2"
        assert diff.should_unset is False
        assert diff.is_change is True  # Critical: marks as actual change

    def test_change_when_different_columns(self):
        """When filter columns change, return RowFilterConfig with new columns."""
        desired = RowFilterConfig(function="cat.schema.fn", columns=("col1", "col2"))
        existing = RowFilterConfig(function="cat.schema.fn", columns=("col1",))
        diff = desired.get_diff(existing)
        assert diff is not None
        assert isinstance(diff, RowFilterConfig)
        assert diff.function == "cat.schema.fn"
        assert diff.columns == ("col1", "col2")
        assert diff.should_unset is False
        assert diff.is_change is True  # Critical: marks as actual change

    def test_is_change_false_by_default(self):
        """RowFilterConfig.is_change should be False by default (current state objects)."""
        config = RowFilterConfig(function="cat.schema.fn", columns=("col1",))
        assert config.is_change is False
        assert config.should_unset is False


class TestRowFilterProcessor:
    def test_parse_target_columns_simple(self):
        result = RowFilterProcessor._parse_target_columns("col1, col2")
        assert result == ["col1", "col2"]

    def test_parse_target_columns_quoted(self):
        result = RowFilterProcessor._parse_target_columns('"col1", "col2"')
        assert result == ["col1", "col2"]

    def test_parse_target_columns_empty(self):
        result = RowFilterProcessor._parse_target_columns("")
        assert result == []

    def test_parse_target_columns_none(self):
        result = RowFilterProcessor._parse_target_columns(None)
        assert result == []

    def test_qualify_function_name_already_qualified(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"

        result = RowFilterProcessor._qualify_function_name("cat.schema.fn", model)
        assert result == "cat.schema.fn"

    def test_qualify_function_name_unqualified(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"

        result = RowFilterProcessor._qualify_function_name("my_fn", model)
        assert result == "mycat.myschema.my_fn"

    def test_qualify_function_name_two_part_raises(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"

        with pytest.raises(ValueError) as exc_info:
            RowFilterProcessor._qualify_function_name("schema.fn", model)
        assert "ambiguous" in str(exc_info.value).lower()

    def test_qualify_function_name_four_part_raises(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"

        with pytest.raises(ValueError) as exc_info:
            RowFilterProcessor._qualify_function_name("a.b.c.d", model)
        assert "too many parts" in str(exc_info.value).lower()

    def test_from_relation_config_empty_columns_raises(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"
        model.config.extra = {"row_filter": {"function": "fn", "columns": []}}

        with pytest.raises(ValueError) as exc_info:
            RowFilterProcessor.from_relation_config(model)
        assert "non-empty 'columns' value" in str(exc_info.value)

    def test_from_relation_config_with_valid_filter(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"
        model.config.extra = {"row_filter": {"function": "my_filter", "columns": ["col1"]}}

        spec = RowFilterProcessor.from_relation_config(model)
        assert spec.function == "mycat.myschema.my_filter"
        assert spec.columns == ("col1",)

    def test_from_relation_config_string_columns_normalized(self):
        """String columns should be normalized to list."""
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"
        model.config.extra = {"row_filter": {"function": "my_filter", "columns": "region"}}

        spec = RowFilterProcessor.from_relation_config(model)
        assert spec.function == "mycat.myschema.my_filter"
        assert spec.columns == ("region",)

    def test_from_relation_config_empty_string_column_raises(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"
        model.config.extra = {"row_filter": {"function": "fn", "columns": ["col1", ""]}}

        with pytest.raises(ValueError) as exc_info:
            RowFilterProcessor.from_relation_config(model)
        assert "non-empty string" in str(exc_info.value)

    def test_from_relation_config_whitespace_only_column_raises(self):
        model = Mock()
        model.database = "mycat"
        model.schema = "myschema"
        model.config.extra = {"row_filter": {"function": "fn", "columns": ["col1", "   "]}}

        with pytest.raises(ValueError) as exc_info:
            RowFilterProcessor.from_relation_config(model)
        assert "non-empty string" in str(exc_info.value)

    def test_from_relation_results_empty(self):
        results = {
            "row_filters": Table(
                rows=[],
                column_names=[
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "filter_name",
                    "target_columns",
                ],
            )
        }
        spec = RowFilterProcessor.from_relation_results(results)
        assert spec == RowFilterConfig()

    def test_from_relation_results_one_row(self):
        # filter_name contains fully qualified function name (catalog.schema.function)
        results = {
            "row_filters": Table(
                rows=[["cat", "schema", "my_table", "cat.schema.my_filter", "col1, col2"]],
                column_names=[
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "filter_name",
                    "target_columns",
                ],
            )
        }
        spec = RowFilterProcessor.from_relation_results(results)
        assert spec.function == "cat.schema.my_filter"
        assert spec.columns == ("col1", "col2")

    def test_from_relation_results_multiple_rows_raises(self):
        results = {
            "row_filters": Table(
                rows=[
                    ["cat", "schema", "my_table", "cat.schema.filter1", "col1"],
                    ["cat", "schema", "my_table", "cat.schema.filter2", "col2"],
                ],
                column_names=[
                    "table_catalog",
                    "table_schema",
                    "table_name",
                    "filter_name",
                    "target_columns",
                ],
            )
        }
        with pytest.raises(ValueError) as exc_info:
            RowFilterProcessor.from_relation_results(results)
        assert "Multiple row filters found" in str(exc_info.value)
