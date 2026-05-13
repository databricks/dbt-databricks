import pytest

fail_if_tag_fetch_called_macros = """
{% macro fetch_tags(relation) %}
  {{ exceptions.raise_compiler_error("fetch_tags should not be called") }}
{% endmacro %}
"""

fail_if_tag_and_column_tag_fetch_called_macros = """
{% macro fetch_tags(relation) %}
  {{ exceptions.raise_compiler_error("fetch_tags should not be called") }}
{% endmacro %}

{% macro fetch_column_tags(relation) %}
  {{ exceptions.raise_compiler_error("fetch_column_tags should not be called") }}
{% endmacro %}
"""


class MaterializationV1Mixin:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": False}}


class MaterializationV2Mixin:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_materialization_v2": True}}


class ManagedIcebergMixin:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"flags": {"use_managed_iceberg": True}}
