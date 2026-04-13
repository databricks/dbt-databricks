from dbt.adapters.base.relation import BaseRelation
from dbt.tests.util import get_manifest

FAIL_IF_TAG_FETCH_CALLED_MACROS = """
{% macro fetch_tags(relation) %}
  {{ exceptions.raise_compiler_error("fetch_tags should not be called") }}
{% endmacro %}
"""

FAIL_IF_TAG_AND_COLUMN_TAG_FETCH_CALLED_MACROS = """
{% macro fetch_tags(relation) %}
  {{ exceptions.raise_compiler_error("fetch_tags should not be called") }}
{% endmacro %}

{% macro fetch_column_tags(relation) %}
  {{ exceptions.raise_compiler_error("fetch_column_tags should not be called") }}
{% endmacro %}
"""


def get_model_config(project, relation: BaseRelation):
    """Return the parsed dbt model config for the given relation fixture."""
    manifest = get_manifest(project.project_root)
    model_nodes = [
        node
        for node in manifest.nodes.values()
        if getattr(node, "resource_type", None) == "model"
        and getattr(node, "alias", None) == relation.identifier
    ]
    assert len(model_nodes) == 1, (
        f"Expected exactly one model node for relation {relation.identifier}, "
        f"found {len(model_nodes)}"
    )
    return project.adapter.get_config_from_model(model_nodes[0])
