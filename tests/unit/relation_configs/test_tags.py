from unittest.mock import Mock

import pytest
from agate import Table
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.tags import TagsConfig, TagsProcessor


class TestTagsProcessor:
    def test_from_relation_results__none(self):
        results = {
            "information_schema.tags": Table(rows=[], column_names=["tag_name", "tag_value"])
        }
        spec = TagsProcessor.from_relation_results(results)
        assert spec == TagsConfig(set_tags={})

    def test_from_relation_results__some(self):
        results = {
            "information_schema.tags": Table(
                rows=[["a", "valA"], ["b", "valB"]], column_names=["tag_name", "tag_value"]
            )
        }
        spec = TagsProcessor.from_relation_results(results)
        assert spec == TagsConfig(set_tags={"a": "valA", "b": "valB"})

    def test_from_relation_results__key_only(self):
        results = {
            "information_schema.tags": Table(
                rows=[["a", ""]], column_names=["tag_name", "tag_value"]
            )
        }
        spec = TagsProcessor.from_relation_results(results)
        assert spec == TagsConfig(set_tags={"a": ""})

    def test_from_relation_config__without_tags(self):
        model = Mock()
        model.config.extra = {}
        spec = TagsProcessor.from_relation_config(model)
        assert spec == TagsConfig(set_tags={})

    def test_from_relation_config__with_tags(self):
        model = Mock()
        model.config.extra = {"databricks_tags": {"a": "valA", "b": 1}}
        spec = TagsProcessor.from_relation_config(model)
        assert spec == TagsConfig(set_tags={"a": "valA", "b": "1"})

    def test_from_relation_config__with_key_only_tags(self):
        model = Mock()
        model.config.extra = {"databricks_tags": {"a": "", "b": None}}
        spec = TagsProcessor.from_relation_config(model)
        assert spec == TagsConfig(set_tags={"a": "", "b": ""})

    def test_from_relation_config__with_incorrect_tags(self):
        model = Mock()
        model.config.extra = {"databricks_tags": ["a", "b"]}

        with pytest.raises(
            DbtRuntimeError,
            match="databricks_tags must be a dictionary",
        ):
            _ = TagsProcessor.from_relation_config(model)


class TestTagsConfig:
    def test_get_diff__empty_and_some_exist(self):
        # Tags are "set only" - when config has no tags and relation has tags,
        # we don't unset the existing tags
        config = TagsConfig(set_tags={})
        config_old = TagsConfig(set_tags={"tag": "value"})
        diff = config.get_diff(config_old)
        assert diff is None  # No changes needed since we don't unset tags

    def test_get_diff__some_new_and_empty_existing(self):
        config = TagsConfig(set_tags={"tag": "value"})
        config_old = TagsConfig(set_tags={})
        diff = config.get_diff(config_old)
        assert diff == TagsConfig(set_tags={"tag": "value"})

    def test_get_diff__mixed_case(self):
        # Tags are "set only" - only the new/updated tags are included
        config = TagsConfig(set_tags={"a": "value", "b": "value"})
        config_old = TagsConfig(set_tags={"b": "other_value", "c": "value"})
        diff = config.get_diff(config_old)
        assert diff == TagsConfig(set_tags={"a": "value", "b": "value"})

    def test_get_diff__no_changes(self):
        config = TagsConfig(set_tags={"tag": "value"})
        config_old = TagsConfig(set_tags={"tag": "value"})
        diff = config.get_diff(config_old)
        assert diff is None
