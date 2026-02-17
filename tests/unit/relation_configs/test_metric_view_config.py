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
