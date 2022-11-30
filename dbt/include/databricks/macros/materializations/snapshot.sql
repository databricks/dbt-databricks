{% macro databricks_build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_identifier = target_relation.identifier ~ '__dbt_tmp' %}

    {%- set tmp_relation = api.Relation.create(identifier=tmp_identifier,
                                               schema=target_relation.schema,
                                               database=target_relation.database,
                                               type='view') -%}

    {% set select = snapshot_staging_table(strategy, sql, target_relation) %}

    {# needs to be a non-temp view so that its columns can be ascertained via `describe` #}
    {% call statement('build_snapshot_staging_relation') %}
        {{ create_view_as(tmp_relation, select) }}
    {% endcall %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% materialization snapshot, adapter='databricks' %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  {%- set file_format = config.get('file_format', 'delta') -%}
  {%- set grant_config = config.get('grants') -%}

  {% set target_relation_exists, target_relation = databricks__get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table',
          needs_information=True) -%}

  {%- if file_format not in ['delta', 'hudi'] -%}
    {% set invalid_format_msg -%}
      Invalid file format: {{ file_format }}
      Snapshot functionality requires file_format be set to 'delta' or 'hudi'
    {%- endset %}
    {% do exceptions.raise_compiler_error(invalid_format_msg) %}
  {% endif %}

  {%- if target_relation_exists -%}
    {%- if not target_relation.is_delta and not target_relation.is_hudi -%}
      {% set invalid_format_msg -%}
        The existing table {{ model.schema }}.{{ target_table }} is in another format than 'delta' or 'hudi'
      {%- endset %}
      {% do exceptions.raise_compiler_error(invalid_format_msg) %}
    {% endif %}
  {% endif %}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_code']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

      {% call statement('main') %}
          {{ final_sql }}
      {% endcall %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% if target_relation.database is none %}
          {% set staging_table = spark_build_snapshot_staging_table(strategy, sql, target_relation) %}
      {% else %}
          {% set staging_table = databricks_build_snapshot_staging_table(strategy, sql, target_relation) %}
      {% endif %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_change_type')
                                   | rejectattr('name', 'equalto', 'DBT_CHANGE_TYPE')
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

      {% call statement_with_staging_table('main', staging_table) %}
          {{ final_sql }}
      {% endcall %}

  {% endif %}

  {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% do persist_constraints(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
