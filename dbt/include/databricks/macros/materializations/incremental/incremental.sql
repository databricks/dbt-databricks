{% materialization incremental, adapter='databricks', supported_languages=['sql', 'python'] -%}
  {#-- Validate early so we don't run SQL if the file_format + strategy combo is invalid --#}
  {%- set raw_file_format = config.get('file_format', default='delta') -%}
  {%- set raw_strategy = config.get('incremental_strategy') or 'merge' -%}
  {%- set grant_config = config.get('grants') -%}

  {%- set file_format = dbt_databricks_validate_get_file_format(raw_file_format) -%}
  {%- set incremental_strategy = dbt_databricks_validate_get_incremental_strategy(raw_strategy, file_format) -%}

  {#-- Set vars --#}

  {%- set incremental_predicates = config.get('predicates', default=none) or config.get('incremental_predicates', default=none) -%}
  {%- set unique_key = config.get('unique_key', none) -%}
  {%- set partition_by = config.get('partition_by', none) -%}
  {%- set language = model['language'] -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set target_relation = this -%}
  {%- set existing_relation = adapter.get_relation(database=this.database, schema=this.schema, identifier=this.identifier, needs_information=True) -%}

  {#-- Set Overwrite Mode - does not work for warehouses --#}
  {%- if incremental_strategy == 'insert_overwrite' and partition_by -%}
    {%- call statement() -%}
      set spark.sql.sources.partitionOverwriteMode = DYNAMIC
    {%- endcall -%}
  {%- endif -%}

  {#-- Run pre-hooks --#}
  {{ run_hooks(pre_hooks) }}

  {#-- Incremental run logic --#}
  {%- if existing_relation is none -%}
    {#-- Relation must be created --#}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}
    {% do persist_constraints(target_relation, model) %}
  {%- elif existing_relation.is_view or existing_relation.is_materialized_view or existing_relation.is_streaming_table or should_full_refresh() -%}
    {#-- Relation must be dropped & recreated --#}
    {% set is_delta = (file_format == 'delta' and existing_relation.is_delta) %}
    {% if not is_delta %} {#-- If Delta, we will `create or replace` below, so no need to drop --#}
      {% do adapter.drop_relation(existing_relation) %}
    {% endif %}
    {%- call statement('main', language=language) -%}
      {{ create_table_as(False, target_relation, compiled_code, language) }}
    {%- endcall -%}

    {% if not existing_relation.is_view %}
      {% do persist_constraints(target_relation, model) %}
    {% endif %}
  {%- else -%}
    {#-- Relation must be merged --#}
    {%- set temp_relation = databricks__make_temp_relation(target_relation, as_table=language != 'sql') -%}
    {%- call statement('create_temp_relation', language=language) -%}
      {{ create_table_as(True, temp_relation, compiled_code, language) }}
    {%- endcall -%}
    {%- do process_schema_changes(on_schema_change, temp_relation, existing_relation) -%}
    {%- set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(context, incremental_strategy) -%}
    {%- set strategy_arg_dict = ({
            'target_relation': target_relation,
            'temp_relation': temp_relation,
            'unique_key': unique_key,
            'dest_columns': none,
            'incremental_predicates': incremental_predicates}) -%}
    {%- set build_sql = strategy_sql_macro_func(strategy_arg_dict) -%}
    {%- if language == 'sql' -%}
      {%- call statement('main') -%}
        {{ build_sql }}
      {%- endcall -%}
    {%- elif language == 'python' -%}
      {%- call statement_with_staging_table('main', temp_relation) -%}
        {{ build_sql }}
      {%- endcall -%}
      {#--
      This is yucky.
      See note in dbt-spark/dbt/include/spark/macros/adapters.sql
      re: python models and temporary views.

      Also, why does not either drop_relation or adapter.drop_relation work here?!
      --#}
    {%- endif -%}
  {%- endif -%}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}

  {% do persist_docs(target_relation, model) %}


  {% do optimize(target_relation) %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
