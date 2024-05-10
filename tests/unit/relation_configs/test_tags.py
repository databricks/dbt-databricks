import pytest
from agate import Table
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tags import TagsProcessor
from dbt.exceptions import DbtRuntimeError
from mock import Mock


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
        config = TagsConfig(set_tags={})
        other = TagsConfig(set_tags={"tag": "value"})
        diff = config.get_diff(other)
        assert diff == TagsConfig(set_tags={}, unset_tags=["tag"])

    def test_get_diff__some_new_and_empty_existing(self):
        config = TagsConfig(set_tags={"tag": "value"})
        other = TagsConfig(set_tags={})
        diff = config.get_diff(other)
        assert diff == TagsConfig(set_tags={"tag": "value"}, unset_tags=[])

    def test_get_diff__mixed_case(self):
        config = TagsConfig(set_tags={"a": "value", "b": "value"})
        other = TagsConfig(set_tags={"b": "other_value", "c": "value"})
        diff = config.get_diff(other)
        assert diff == TagsConfig(set_tags={"a": "value", "b": "value"}, unset_tags=["c"])
