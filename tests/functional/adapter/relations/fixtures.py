flip_relation_as_table_sql = """
{{ config(materialized='table') }}
select 1 as id
"""

flip_relation_as_view_sql = """
{{ config(materialized='view') }}
select 1 as id
"""

flip_relation_as_invalid_view_sql = """
{{ config(materialized='view') }}
select missing_column from (select 1 as id)
"""

safer_ops_table_sql = """
{{ config(materialized='table') }}
select 1 as id
"""

safer_ops_incremental_sql = """
{{ config(materialized='incremental', incremental_strategy='append') }}
select 1 as id
"""

safer_ops_upstream_sql = """
{{ config(materialized='table', file_format='delta') }}
select 1 as id, 'a' as old_col
"""

safer_ops_upstream_renamed_sql = """
{{ config(materialized='table', file_format='delta') }}
select 1 as id, 'a' as new_col
"""

safer_ops_upstream_csv = "id,old_col\n1,a\n"

safer_ops_upstream_renamed_csv = "id,new_col\n1,a\n"

safer_ops_view_sql = """
{{ config(materialized='view') }}
select id, old_col from {{ ref('safer_ops_upstream') }}
"""

safer_ops_view_renamed_sql = """
{{ config(materialized='view') }}
select id, new_col from {{ ref('safer_ops_upstream') }}
"""

replaced_relation_source_sql = """
{{ config(materialized='table') }}
select 1 as id union all select 2 as id union all select 3 as id
"""

assert_cached_as_materialized_macro = """
{% macro assert_cached_as_materialized() %}
  {% if execute %}
    {% set relation = adapter.get_relation(
      database=this.database, schema=this.schema, identifier=this.identifier
    ) %}
    {% if relation is none or relation.type != model.config.materialized %}
      {{ exceptions.raise_compiler_error("Relation cache does not match the model: " ~ relation) }}
    {% endif %}
  {% endif %}
  {{ return("select 1") }}
{% endmacro %}
"""

assert_cached_with_owner_macro = """
{% macro assert_cached_with_owner() %}
  {% if execute %}
    {% set relation = adapter.get_relation(
      database=this.database, schema=this.schema, identifier=this.identifier
    ) %}
    {% if relation is none or relation.owner is none %}
      {{ exceptions.raise_compiler_error("Relation cache lost the relation owner: " ~ relation) }}
    {% endif %}
  {% endif %}
  {{ return("select 1") }}
{% endmacro %}
"""


def replaced_relation_sql(materialized, post_hook=None):
    config = f"materialized='{materialized}'"
    if post_hook:
        config += f", post_hook='{{{{ {post_hook}() }}}}'"
    stream = "stream " if materialized == "streaming_table" else ""
    return f"""
{{{{ config({config}) }}}}
select id from {stream}{{{{ ref('replaced_relation_source') }}}}
"""
