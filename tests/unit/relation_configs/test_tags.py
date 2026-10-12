from unittest.mock import Mock

import pytest
from agate import Number, Table, Text
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.tags import TagsConfig, TagsProcessor


class TestTagsProcessor:
    @pytest.mark.parametrize(
        "results",
        [
            {},
            {"table_tags": None},
            {"table_tags": {}},
            {"information_schema.tags": None},
            {"information_schema.tags": Table([], ["tag_name", "tag_value"])},
        ],
    )
    def test_from_relation_results__none(self, results):
        spec = TagsProcessor.from_relation_results(results)
        assert spec == TagsConfig(set_tags={})

    @pytest.mark.parametrize("result_key", ["table_tags", "information_schema.tags"])
    def test_from_relation_results__some(self, result_key):
        tags = {"a": "valA", "b": "valB"}
        results = {
            result_key: tags
            if result_key == "table_tags"
            else Table(list(tags.items()), ["tag_name", "tag_value"])
        }
        spec = TagsProcessor.from_relation_results(results)
        assert spec == TagsConfig(set_tags={"a": "valA", "b": "valB"})

    @pytest.mark.parametrize("result_key", ["table_tags", "information_schema.tags"])
    def test_from_relation_results__key_only(self, result_key):
        tags = {"a": "", "b": None}
        results = {
            result_key: tags
            if result_key == "table_tags"
            else Table(list(tags.items()), ["tag_name", "tag_value"], [Text(), Text()])
        }
        spec = TagsProcessor.from_relation_results(results)
        assert spec == TagsConfig(set_tags={"a": "", "b": ""})

    @pytest.mark.parametrize("table_tags", [{}, None, {"a": "current"}])
    def test_from_relation_results__current_format_takes_precedence(self, table_tags):
        results = {
            "table_tags": table_tags,
            "information_schema.tags": Table(
                [["a", "legacy"], ["legacy_only", "value"]], ["tag_name", "tag_value"]
            ),
        }

        spec = TagsProcessor.from_relation_results(results)

        assert spec == TagsConfig(set_tags=table_tags or {})

    @pytest.mark.parametrize(
        "results, expected",
        [
            (
                {"table_tags": {1: None, "priority": 0, "enabled": False}},
                {"1": "", "priority": "0", "enabled": "False"},
            ),
            (
                {
                    "information_schema.tags": Table(
                        [[1, None], [2, 0]], ["tag_name", "tag_value"], [Number(), Number()]
                    )
                },
                {"1": "", "2": "0"},
            ),
            (
                {
                    "information_schema.tags": Table(
                        [["priority", "001"], ["enabled", "false"]],
                        ["tag_name", "tag_value"],
                        [Text(), Text()],
                    )
                },
                {"priority": "001", "enabled": "false"},
            ),
        ],
    )
    def test_from_relation_results__normalizes_scalars(self, results, expected):
        assert TagsProcessor.from_relation_results(results) == TagsConfig(set_tags=expected)

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

    def test_from_relation_config__with_falsy_tags(self):
        model = Mock()
        model.config.extra = {"databricks_tags": {"priority": 0, "enabled": False}}
        spec = TagsProcessor.from_relation_config(model)
        assert spec == TagsConfig(set_tags={"priority": "0", "enabled": "False"})

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

    def test_get_diff__omits_unchanged_keys(self):
        config = TagsConfig(set_tags={"stable": "1", "moved": "new"})
        config_old = TagsConfig(set_tags={"stable": "1", "moved": "old", "remote_only": "x"})
        diff = config.get_diff(config_old)
        assert diff == TagsConfig(set_tags={"moved": "new"})

    def test_get_diff__no_changes(self):
        config = TagsConfig(set_tags={"tag": "value"})
        config_old = TagsConfig(set_tags={"tag": "value"})
        diff = config.get_diff(config_old)
        assert diff is None
