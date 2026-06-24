from unittest.mock import Mock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.databricks.relation_configs.metric_view import (
    MetricViewQueryConfig,
    MetricViewQueryProcessor,
)

yaml_content = """version: 0.1
source: my_table
dimensions:
  - name: dim1
    expr: col1
measures:
  - name: count
    expr: count(1)"""


class TestMetricViewQueryConfig:
    def test_get_diff__same_query(self):
        config1 = MetricViewQueryConfig(query=yaml_content)
        config2 = MetricViewQueryConfig(query=yaml_content)
        assert config1.get_diff(config2) is None

    def test_get_diff__different_query(self):
        config1 = MetricViewQueryConfig(query=yaml_content)
        config2 = MetricViewQueryConfig(query="version: 0.1\nsource: other_table")
        assert config1.get_diff(config2) is config1

    def test_get_diff__whitespace_normalization(self):
        config1 = MetricViewQueryConfig(query="version: 0.1\nsource: my_table")
        config2 = MetricViewQueryConfig(query="version: 0.1\n  source:   my_table  ")
        assert config1.get_diff(config2) is None

    def test_get_diff__different_whitespace_content(self):
        config1 = MetricViewQueryConfig(query="version: 0.1 source: my_table")
        config2 = MetricViewQueryConfig(query="version: 0.1 source: other_table")
        assert config1.get_diff(config2) is config1

    def test_get_diff__source_quote_normalization(self):
        # Databricks stores `source` single-quoted; the model emits it double-quoted.
        # The two are semantically identical, so no change should be detected.
        desired = MetricViewQueryConfig(query='version: 1.1\nsource: "`c`.`s`.`t`"')
        existing = MetricViewQueryConfig(query="version: 1.1\nsource: '`c`.`s`.`t`'")
        assert desired.get_diff(existing) is None

    def test_get_diff__flow_vs_block_list(self):
        # Databricks rewrites flow-style lists (e.g. synonyms) to block style on read-back.
        desired = MetricViewQueryConfig(
            query="version: 1.1\ndimensions:\n  - name: s\n    expr: s\n    synonyms: [a, b]"
        )
        existing = MetricViewQueryConfig(
            query=(
                "version: 1.1\ndimensions:\n  - name: s\n    expr: s\n"
                "    synonyms:\n      - a\n      - b"
            )
        )
        assert desired.get_diff(existing) is None

    def test_get_diff__semantic_difference_detected(self):
        desired = MetricViewQueryConfig(query="version: 1.1\nsource: t\nfilter: a = 1")
        existing = MetricViewQueryConfig(query="version: 1.1\nsource: t\nfilter: a = 2")
        assert desired.get_diff(existing) is desired

    def test_get_diff__unparseable_falls_back_to_whitespace(self):
        # Malformed YAML still compares via whitespace normalization (no crash).
        desired = MetricViewQueryConfig(query="a: [1, 2")
        existing = MetricViewQueryConfig(query="a:   [1,   2")
        assert desired.get_diff(existing) is None

    def test_get_diff__reserved_word_scalars_not_coerced(self):
        # `yes`/`no` and `true`/`false` are distinct synonym values; YAML implicit typing
        # would collapse them to the same booleans, so a real change must still be detected.
        desired = MetricViewQueryConfig(
            query="version: 1.1\ndimensions:\n  - name: s\n    expr: s\n    synonyms: [yes, no]"
        )
        existing = MetricViewQueryConfig(
            query="version: 1.1\ndimensions:\n  - name: s\n    expr: s\n    synonyms: [true, false]"
        )
        assert desired.get_diff(existing) is desired


class TestMetricViewQueryProcessor:
    def test_from_relation_results__with_dollar_delimiters(self):
        describe_extended = [
            ("col_name", "data_type", "comment"),
            ("View Text", f"$${yaml_content}$$", None),
        ]
        results = {"describe_extended": describe_extended}
        spec = MetricViewQueryProcessor.from_relation_results(results)
        assert spec == MetricViewQueryConfig(query=yaml_content)

    def test_from_relation_results__with_whitespace_around_delimiters(self):
        describe_extended = [
            ("col_name", "data_type", "comment"),
            ("View Text", f"  $$ {yaml_content} $$  ", None),
        ]
        results = {"describe_extended": describe_extended}
        spec = MetricViewQueryProcessor.from_relation_results(results)
        assert spec == MetricViewQueryConfig(query=yaml_content)

    def test_from_relation_results__without_delimiters(self):
        describe_extended = [
            ("col_name", "data_type", "comment"),
            ("View Text", yaml_content, None),
        ]
        results = {"describe_extended": describe_extended}
        spec = MetricViewQueryProcessor.from_relation_results(results)
        assert spec == MetricViewQueryConfig(query=yaml_content)

    def test_from_relation_results__missing_describe_extended(self):
        results = {}
        with pytest.raises(DbtRuntimeError, match="Cannot find metric view description"):
            MetricViewQueryProcessor.from_relation_results(results)

    def test_from_relation_results__missing_view_text(self):
        describe_extended = [
            ("col_name", "data_type", "comment"),
            ("Other Field", "some_value", None),
        ]
        results = {"describe_extended": describe_extended}
        with pytest.raises(DbtRuntimeError, match="no 'View Text' in DESCRIBE EXTENDED"):
            MetricViewQueryProcessor.from_relation_results(results)

    def test_from_relation_config__with_query(self):
        model = Mock()
        model.compiled_code = yaml_content
        spec = MetricViewQueryProcessor.from_relation_config(model)
        assert spec == MetricViewQueryConfig(query=yaml_content)

    def test_from_relation_config__with_whitespace(self):
        model = Mock()
        model.compiled_code = f"  {yaml_content}  "
        spec = MetricViewQueryProcessor.from_relation_config(model)
        assert spec == MetricViewQueryConfig(query=yaml_content)

    def test_from_relation_config__without_query(self):
        model = Mock()
        model.compiled_code = None
        model.identifier = "test_metric_view"
        with pytest.raises(DbtRuntimeError, match="no YAML definition"):
            MetricViewQueryProcessor.from_relation_config(model)

    def test_from_relation_config__empty_query(self):
        model = Mock()
        model.compiled_code = ""
        model.identifier = "test_metric_view"
        with pytest.raises(DbtRuntimeError, match="no YAML definition"):
            MetricViewQueryProcessor.from_relation_config(model)
