from unittest.mock import Mock

import pytest
from agate import Table
from dbt.artifacts.resources.v1.components import ColumnInfo
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.column_tags import (
    ColumnTagsConfig,
    ColumnTagsProcessor,
)


class TestColumnTagsProcessor:
    def test_from_relation_results__none(self):
        results = {
            "information_schema.column_tags": Table(
                rows=[], column_names=["column_name", "tag_name", "tag_value"]
            )
        }
        spec = ColumnTagsProcessor.from_relation_results(results)
        assert spec == ColumnTagsConfig(set_column_tags={})

    def test_from_relation_results__some(self):
        results = {
            "information_schema.column_tags": Table(
                rows=[
                    ["col1", "tag_a", "value_a"],
                    ["col1", "tag_b", ""],  # key-only tag
                    ["col2", "tag_c", "value_c"],
                ],
                column_names=["column_name", "tag_name", "tag_value"],
            )
        }
        spec = ColumnTagsProcessor.from_relation_results(results)
        assert spec == ColumnTagsConfig(
            set_column_tags={
                "col1": {"tag_a": "value_a", "tag_b": ""},
                "col2": {"tag_c": "value_c"},
            }
        )

    def test_from_relation_results__no_column_tags_key(self):
        results = {}
        spec = ColumnTagsProcessor.from_relation_results(results)
        assert spec == ColumnTagsConfig(set_column_tags={})

    def test_from_relation_config__without_column_tags(self):
        model = Mock()
        model.columns = {}
        spec = ColumnTagsProcessor.from_relation_config(model)
        assert spec == ColumnTagsConfig(set_column_tags={})

    def test_from_relation_config__with_dict(self):
        model = Mock()
        model.columns = {
            "email": {"_extra": {"databricks_tags": {"pii": "", "env": "prod"}}},
            "id": {"_extra": {}},
            "created_at": {},
        }
        spec = ColumnTagsProcessor.from_relation_config(model)
        assert spec == ColumnTagsConfig(
            set_column_tags={
                "email": {"pii": "", "env": "prod"},
            }
        )

    def test_from_relation_config__with_column_info(self):
        model = Mock()
        model.columns = {
            "id": ColumnInfo(name="id", _extra={}),
            "email": ColumnInfo(
                name="email",
                _extra={"databricks_tags": {"pii": "", "env": "prod"}},
            ),
            "created_at": ColumnInfo(name="created_at"),
        }
        spec = ColumnTagsProcessor.from_relation_config(model)
        assert spec == ColumnTagsConfig(
            set_column_tags={
                "email": {"pii": "", "env": "prod"},
            }
        )

    def test_from_relation_config__with_incorrect_tags(self):
        model = Mock()
        model.columns = {
            "column1": {"_extra": {"databricks_tags": ["not", "a", "dict"]}},
        }
        with pytest.raises(DbtRuntimeError):
            ColumnTagsProcessor.from_relation_config(model)


class TestColumnTagsConfig:
    def test_get_diff__empty_and_some_exist(self):
        # Column tags are "set only" - when config has no tags and relation has tags,
        # we don't unset the existing tags
        config = ColumnTagsConfig(set_column_tags={})
        other = ColumnTagsConfig(set_column_tags={"col1": {"tag_a": "value_a", "tag_b": "value_b"}})
        diff = config.get_diff(other)
        assert diff is None  # No changes needed since we don't unset tags

    def test_get_diff__some_new_and_empty_existing(self):
        config = ColumnTagsConfig(
            set_column_tags={"col1": {"tag_a": "value_a", "tag_b": "value_b"}}
        )
        other = ColumnTagsConfig(set_column_tags={})
        diff = config.get_diff(other)
        assert diff == ColumnTagsConfig(
            set_column_tags={"col1": {"tag_a": "value_a", "tag_b": "value_b"}}
        )

    def test_get_diff__mixed_case(self):
        # Column tags are "set only" - only the new/updated tags are included
        config = ColumnTagsConfig(
            set_column_tags={
                "col1": {"tag_a": "new_value", "tag_b": "value_b"},
                "col2": {"tag_c": "value_c"},
            }
        )
        other = ColumnTagsConfig(
            set_column_tags={
                "col1": {"tag_a": "old_value", "tag_d": "value_d"},
                "col3": {"tag_e": "value_e"},
            }
        )
        diff = config.get_diff(other)
        assert diff == ColumnTagsConfig(
            set_column_tags={
                "col1": {"tag_a": "new_value", "tag_b": "value_b"},
                "col2": {"tag_c": "value_c"},
            }
        )

    def test_get_diff__no_changes(self):
        config = ColumnTagsConfig(
            set_column_tags={"col1": {"tag_a": "value_a", "tag_b": "value_b"}}
        )
        other = ColumnTagsConfig(set_column_tags={"col1": {"tag_a": "value_a", "tag_b": "value_b"}})
        diff = config.get_diff(other)
        assert diff is None

    def test_get_diff__case_mismatch_column_names(self):
        """Test that column name case mismatches are handled correctly."""
        # Config has lowercase column names (from YAML schema)
        config = ColumnTagsConfig(
            set_column_tags={"account_id": {"pii": "true"}, "user_name": {"pii": "true"}}
        )
        # Other has mixed case column names (from database)
        other = ColumnTagsConfig(
            set_column_tags={"Account_ID": {"pii": "true"}, "User_Name": {"pii": "true"}}
        )
        # Should recognize these as the same columns and return no diff
        diff = config.get_diff(other)
        assert diff is None

    def test_get_diff__case_mismatch_with_actual_changes(self):
        """Test that real changes are detected even with case mismatches."""
        # Config has lowercase column names with new tag values
        config = ColumnTagsConfig(
            set_column_tags={"account_id": {"pii": "false"}, "user_name": {"pii": "true"}}
        )
        # Other has mixed case column names with old tag values
        other = ColumnTagsConfig(
            set_column_tags={"Account_ID": {"pii": "true"}, "User_Name": {"pii": "true"}}
        )
        # Should detect that account_id tag changed
        diff = config.get_diff(other)
        assert diff == ColumnTagsConfig(set_column_tags={"account_id": {"pii": "false"}})
