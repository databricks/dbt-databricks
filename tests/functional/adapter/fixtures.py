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


class RerunSafeMixin:
    """Make a stateful functional test safe to retry under `pytest --reruns`.

    pytest-rerunfailures retries a failed test without tearing down the class-scoped
    `project` fixture, so on-disk model files (often rewritten in place via
    write_file) and previously-created relations leak into the retry. Subclasses
    name the relations they build via the `relations_to_reset` fixture; this
    function-scoped fixture re-runs on every attempt, restoring the initial model
    files and dropping those relations so each attempt starts from a clean slate.
    """

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ()

    @pytest.fixture(autouse=True)
    def reset_project_state(self, project, models, relations_to_reset):
        for name, contents in models.items():
            util.write_file(contents, "models", name)
        self._drop_relations(project, relations_to_reset)
        yield

    @staticmethod
    def _drop_relations(project, identifiers):
        """Drop each named relation in the test schema if it exists."""
        with project.adapter.connection_named("__test_reset"):
            for identifier in identifiers:
                relation = project.adapter.get_relation(
                    database=project.database,
                    schema=project.test_schema,
                    identifier=identifier,
                )
                if relation is not None:
                    project.adapter.drop_relation(relation)


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
