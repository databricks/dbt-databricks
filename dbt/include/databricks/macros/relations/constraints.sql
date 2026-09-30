{# Persist table-level and column-level constraints. #}
{% macro persist_constraints(relation, model) %}
  {{ return(adapter.dispatch('persist_constraints', 'dbt')(relation, model)) }}
{% endmacro %}

{% macro databricks__persist_constraints(relation, model) %}
  {%- set contract_config = config.get('contract') -%}
  {% set has_model_contract = contract_config and contract_config.enforced %}
  {% set has_databricks_constraints = config.get('persist_constraints', False) | as_bool %}

  {% if (has_model_contract or has_databricks_constraints) %}
    {% if adapter.resolve_file_format(config) != 'delta' %}
      {{ exceptions.warn("Constraints not supported for file format: " ~ adapter.resolve_file_format(config)) }}
    {% elif relation.is_view %}
      {{ exceptions.raise_compiler_error("Constraints not supported for views.") }}
    {% elif is_incremental() %}
      {{ exceptions.raise_compiler_error("Constraints are not applied for incremental updates. Full refresh is required to update constraints.") }}
    {% else %}
      {% set columns_and_constraints = adapter.parse_columns_and_constraints(
          [],
          {
              "columns": model.get("columns", {}),
              "constraints": model.get("constraints", []),
              "meta_constraints": model.get("meta", {}).get("constraints"),
              "contract_enforced": has_model_contract,
              "persist_constraints": has_databricks_constraints,
              "column_source": "model",
              "application": "post_create",
              "model_name": model.get("name", ""),
              "relation": {
                  "database": relation.database,
                  "schema": relation.schema,
                  "identifier": relation.identifier,
              },
          }
      ) %}
      {% set constrained_relation = relation.enrich(columns_and_constraints[1]) %}
      {% set set_non_nulls = [] %}
      {% for column in columns_and_constraints[0] %}
        {% if column.not_null %}
          {% do set_non_nulls.append(column.name) %}
        {% endif %}
      {% endfor %}
      {% set changes = namespace(
          set_non_nulls=set_non_nulls,
          unset_non_nulls=[],
          set_constraints=constrained_relation.create_constraints + constrained_relation.alter_constraints,
          unset_constraints=[]
      ) %}
      {{ apply_constraints(relation, changes, check_hive_metastore=False) }}
    {% endif %}
  {% endif %}
{% endmacro %}

{% macro apply_alter_constraints(relation) %}
  {%- for constraint in relation.alter_constraints -%}
    {% call statement('add constraint') %}
      ALTER TABLE {{ relation.render() }} ADD {{ constraint.render_for_apply() }}
    {% endcall %}
  {%- endfor -%}
{% endmacro %}
