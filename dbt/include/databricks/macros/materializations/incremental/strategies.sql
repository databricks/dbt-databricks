{% macro databricks__get_incremental_default_sql(arg_dict) %}
    {{ return(get_incremental_merge_sql(arg_dict)) }}
{% endmacro %}

{% macro databricks__get_incremental_append_sql(arg_dict) %}

    {%- set source_relation = arg_dict["temp_relation"] -%}
    {%- set target_relation = arg_dict["target_relation"] -%}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert into table {{ target_relation }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}

{% macro databricks__get_incremental_merge_sql(arg_dict) %}

    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set unique_key = arg_dict["unique_key"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set predicates = arg_dict["predicates"] -%}

    {# skip dest_columns, use merge_update_columns config if provided, otherwise use "*" #}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set update_columns = config.get("merge_update_columns") -%}

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

{% macro databricks__get_incremental_insert_overwrite_sql(arg_dict) %}

    {%- set source_relation = arg_dict["temp_relation"] -%}
    {%- set target_relation = arg_dict["target_relation"] -%}

    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute='quoted') | join(', ') -%}
    insert overwrite table {{ target_relation }}
    {{ partition_cols(label="partition") }}
    select {{dest_cols_csv}} from {{ source_relation }}

{% endmacro %}
