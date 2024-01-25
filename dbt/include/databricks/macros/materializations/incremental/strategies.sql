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

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}

{% macro to_literal(val) %}
    {%- if val is none -%}
        NULL
    {%- elif val is number or val is boolean -%}
        {{ val }}
    {%- else -%}
        '{{ val }}'
    {%- endif -%}
{% endmacro %}

{% macro add_dest_table_partition_predicates(predicates, partition_columns, source) %}
    {%- set result_predicates = convert_to_list(predicates, default_value=[]) -%}

    {% if not partition_columns or not execute %}
        {{ return(result_predicates) }}
    {% endif %}

    {%- set partition_columns = convert_to_list(partition_columns, default_value=[]) -%}
    {%- set partition_values_query %}
        select
        {%- for partition_column in partition_columns %}
            MIN({{ adapter.quote(partition_column) }}), 
            MAX({{ adapter.quote(partition_column) }}){%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        from {{ source }}
    {%- endset %}
    {%- set partition_value_results = run_query(partition_values_query) -%}

    {% if partition_value_results|length > 0 %}
        {%- for n in range(partition_columns|length) %}
            {%- set this_partition_min_value = partition_value_results.columns[n * 2][0] -%}
            {%- set this_partition_max_value = partition_value_results.columns[n * 2 + 1][0] -%}

            {% if this_partition_min_value is not none and this_partition_max_value is not none %}
                {%- set this_partition_filter %}
                    DBT_INTERNAL_DEST.{{ partition_columns[n] }} >= {{ to_literal(this_partition_min_value) }}
                    and DBT_INTERNAL_DEST.{{ partition_columns[n] }} <= {{ to_literal(this_partition_max_value) }}
                {%- endset %}
                {% do result_predicates.append(this_partition_filter) %}
            {% endif %}
        {%- endfor %}
    {% endif %}

    {{ return(result_predicates) }}
{% endmacro %}

{% macro databricks__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
  {# need dest_columns for merge_exclude_columns, default to use "*" #}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_columns = adapter.get_columns_in_relation(target) -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
  {%- set partition_by_columns = convert_to_list(config.get('partition_by', none), default_value=[]) -%}
  {%- set liquid_clustered_columns = convert_to_list(config.get('liquid_clustered_by', none), default_value=[]) -%}
  {%- set partition_columns = partition_by_columns + liquid_clustered_columns -%}

  {% if unique_key %}
      {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
          {% for key in unique_key %}
              {% set this_key_match %}
                  DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
              {% endset %}
              {% do predicates.append(this_key_match) %}
          {% endfor %}
      {% else %}
          {% set unique_key_match %}
              DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
          {% endset %}
          {% do predicates.append(unique_key_match) %}
      {% endif %}
  {% else %}
      {% do predicates.append('FALSE') %}
  {% endif %}

  {%- set predicates = add_dest_table_partition_predicates(predicates=predicates, partition_columns=partition_columns, source=source) -%}

  {{ sql_header if sql_header is not none }}

  merge into {{ target }} as DBT_INTERNAL_DEST
      using {{ source }} as DBT_INTERNAL_SOURCE
      on {{ predicates | join(' and ') }}

      when matched then update set
        {% if update_columns -%}{%- for column_name in update_columns %}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
        {%- else %} * {% endif %}

      when not matched then insert *
{% endmacro %}
