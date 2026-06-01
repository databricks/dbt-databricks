import pytest
from dbt.tests import util

from dbt.adapters.databricks.dbr_capabilities import DBRCapability

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

fail_if_get_columns_as_json_called_macros = """
{% macro get_columns_comments_as_json(relation) %}
  {{ exceptions.raise_compiler_error("get_columns_comments_as_json should not be called") }}
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


class RequiresDescribeAsJsonCapabilityMixin:
    """Skip the test class if the connected compute lacks DESCRIBE TABLE EXTENDED AS JSON."""

    @pytest.fixture(scope="class", autouse=True)
    def require_describe_as_json_capability(self, project):
        with util.get_connection(project.adapter):
            if not project.adapter.has_capability(DBRCapability.DESCRIBE_TABLE_EXTENDED_AS_JSON):
                pytest.skip("DESCRIBE TABLE EXTENDED AS JSON not supported on this compute")
