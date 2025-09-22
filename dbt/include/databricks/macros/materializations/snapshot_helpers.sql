{% macro databricks__snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    {% if target.is_iceberg %}
      {# create view only supports a name (no catalog, or schema) #}
      using {{ source.identifier }} as DBT_INTERNAL_SOURCE
    {% else %}
      using {{ source }} as DBT_INTERNAL_SOURCE
    {% endif %}
    on DBT_INTERNAL_SOURCE.{{ adapter.quote(columns.dbt_scd_id) }} = DBT_INTERNAL_DEST.{{ adapter.quote(columns.dbt_scd_id) }}
    when matched
     {% if config.get("dbt_valid_to_current") %}
       and ( DBT_INTERNAL_DEST.{{ adapter.quote(columns.dbt_valid_to) }} = {{ config.get('dbt_valid_to_current') }} or
             DBT_INTERNAL_DEST.{{ adapter.quote(columns.dbt_valid_to) }} is null )
     {% else %}
       and DBT_INTERNAL_DEST.{{ adapter.quote(columns.dbt_valid_to) }} is null
     {% endif %}
     and DBT_INTERNAL_SOURCE.{{ adapter.quote('dbt_change_type') }} in ('update', 'delete')
        then update
        set {{ adapter.quote(columns.dbt_valid_to) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(columns.dbt_valid_to) }}

    when not matched
     and DBT_INTERNAL_SOURCE.{{ adapter.quote('dbt_change_type') }} = 'insert'
        then insert *
    ;
{% endmacro %}


{% macro databricks__create_columns(relation, columns) %}
    {% if columns|length > 0 %}
    {% call statement() %}
      alter table {{ relation }} add columns (
        {% for column in columns %}
          {{ adapter.quote(column.name) }} {{ column.data_type }} {{- ',' if not loop.last -}}
        {% endfor %}
      );
    {% endcall %}
    {% endif %}
{% endmacro %}