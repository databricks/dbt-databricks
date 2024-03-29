{% macro databricks__get_incremental_default_sql(arg_dict) %}
  {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}

{% macro databricks__get_incremental_append_sql(arg_dict) %}
  {% do return(get_insert_into_sql(arg_dict["temp_relation"], arg_dict["target_relation"])) %}
{% endmacro %}

{% macro databricks__get_incremental_replace_where_sql(arg_dict) %}
  {% do return(get_replace_where_sql(arg_dict)) %}
{% endmacro %}

{% macro get_incremental_replace_where_sql(arg_dict) %}

  {{ return(adapter.dispatch('get_incremental_replace_where_sql', 'dbt')(arg_dict)) }}

{% endmacro %}

{% macro databricks__get_insert_overwrite_merge_sql(target, source, dest_columns, predicates, include_sql_header) %}
    {{ return(get_insert_overwrite_sql(source, target)) }}
{% endmacro %}


{% macro get_insert_overwrite_sql(source_relation, target_relation) %}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}

{% macro get_replace_where_sql(args_dict) -%}
    {%- set predicates = args_dict['incremental_predicates'] -%}
    {%- set target_relation = args_dict['target_relation'] -%}
    {%- set temp_relation = args_dict['temp_relation'] -%}

    insert into {{ target_relation }}
    {% if predicates %}
      {% if predicates is sequence and predicates is not string %}
    replace where {{ predicates | join(' and ') }}
      {% else %}
    replace where {{ predicates }}
      {% endif %}
    {% endif %}
    table {{ temp_relation }}

{% endmacro %}

{% macro get_insert_into_sql(source_relation, target_relation) %}
    {%- set source_columns = adapter.get_columns_in_relation(source_relation) | map(attribute="quoted") | list -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) | map(attribute="quoted") | list -%}
    {{ insert_into_sql_impl(target_relation, dest_columns, source_relation, source_columns) }}
{% endmacro %}

{% macro insert_into_sql_impl(target_relation, dest_columns, source_relation, source_columns) %}
    {%- set common_columns = [] -%}
    {%- for dest_col in dest_columns -%}
      {%- if dest_col in source_columns -%}
        {%- do common_columns.append(dest_col) -%}
      {%- else -%}
        {%- do common_columns.append('DEFAULT') -%}
      {%- endif -%}
    {%- endfor -%}
    {%- set dest_cols_csv = dest_columns | join(', ') -%}
    {%- set source_cols_csv = common_columns | join(', ') -%}
insert into table {{ target_relation }} ({{ dest_cols_csv }})
select {{source_cols_csv}} from {{ source_relation }}
{%- endmacro %}

{% macro databricks__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {# need dest_columns for merge_exclude_columns, default to use "*" #}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set source_columns = (adapter.get_columns_in_relation(source) | map(attribute='quoted') | list)-%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} <=> DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} <=> DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}
      when matched then update set {{ get_merge_update_set(update_columns, on_schema_change, source_columns) }}
      when not matched then insert {{ get_merge_insert(on_schema_change, source_columns) }}
{% endmacro %}

{% macro get_merge_update_set(update_columns, on_schema_change, source_columns) %}
  {%- if update_columns -%}
    {%- for column_name in update_columns -%}
      {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  {%- elif on_schema_change == 'ignore' -%}
    *
  {%- else -%}
    {%- for column in source_columns -%}
      {{ column }} = DBT_INTERNAL_SOURCE.{{ column }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %}
  {%- endif -%}
{% endmacro %}

{% macro get_merge_insert(on_schema_change, source_columns) %}
  {%- if on_schema_change == 'ignore' -%}
    *
  {%- else -%}
    ({{ source_columns | join(", ") }}) VALUES (
    {%- for column in source_columns -%}
      DBT_INTERNAL_SOURCE.{{ column }}{%- if not loop.last %}, {% endif -%}
    {%- endfor %})
  {%- endif -%}
{% endmacro %}
