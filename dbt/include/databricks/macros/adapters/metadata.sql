{% macro databricks__list_relations_without_caching(schema_relation) %}
  {{ return(adapter.get_relations_without_caching(schema_relation)) }}
{% endmacro %}

{% macro show_table_extended(schema_relation) %}
  {{ return(adapter.dispatch('show_table_extended', 'dbt')(schema_relation)) }}
{% endmacro %}

{% macro databricks__show_table_extended(schema_relation) %}
  {% call statement('show_table_extended', fetch_result=True) -%}
    show table extended in {{ schema_relation.without_identifier() }} like '{{ schema_relation.identifier }}'
  {% endcall %}

  {% do return(load_result('show_table_extended').table) %}
{% endmacro %}

{% macro show_tables(relation) %}
  {{ return(adapter.dispatch('show_tables', 'dbt')(relation)) }}
{% endmacro %}

{% macro databricks__show_tables(relation) %}
  {% call statement('show_tables', fetch_result=True) -%}
    show tables in {{ relation }}
  {% endcall %}

  {% do return(load_result('show_tables').table) %}
{% endmacro %}

{% macro show_views(relation) %}
  {{ return(adapter.dispatch('show_views', 'dbt')(relation)) }}
{% endmacro %}

{% macro databricks__show_views(relation) %}
  {% call statement('show_views', fetch_result=True) -%}
    show views in {{ relation }}
  {% endcall %}

  {% do return(load_result('show_views').table) %}
{% endmacro %}

{% macro databricks__get_relation_last_modified(information_schema, relations) -%}

  {%- call statement('last_modified', fetch_result=True) -%}
    {% if information_schema.is_hive_metastore() %}
        {%- for relation in relations -%}
            select '{{ relation.schema }}' as schema,
                    '{{ relation.identifier }}' as identifier,
                    max(timestamp) as last_modified,
                    {{ current_timestamp() }} as snapshotted_at
            from (describe history {{ relation.schema }}.{{ relation.identifier }})
            {% if not loop.last %}
            union all
            {% endif %}
        {%- endfor -%}
    {% else %}
        select table_schema as schema,
               table_name as identifier,
               last_altered as last_modified,
               {{ current_timestamp() }} as snapshotted_at
        from {{ information_schema }}.tables
        where (
          {%- for relation in relations -%}
            (table_schema = '{{ relation.schema }}' and
             table_name = '{{ relation.identifier }}'){%- if not loop.last %} or {% endif -%}
          {%- endfor -%}
        )
    {% endif %}
  {%- endcall -%}

  {{ return(load_result('last_modified')) }}

{% endmacro %}

{% macro get_view_description(relation) %}
  {% call statement('get_view_description', fetch_result=True) -%}
    select *
    from {{ relation.information_schema() }}.`views`
    where table_schema = '{{ relation.schema }}'
      and table_name = '{{ relation.identifier }}'
  {%- endcall -%}

  {% do return(load_result('get_view_description').table) %}
{% endmacro %}

{% macro get_view_description_alt(relation) %}
  {% call statement('get_view_description_alt', fetch_result=True) -%}
    select *
    from `system`.`information_schema`.`views`
    where table_catalog = '{{ relation.database }}'
      and table_schema = '{{ relation.schema }}'
      and table_name = '{{ relation.identifier }}'
  {% endcall %}

  {% do return(load_result('get_view_description_alt').table) %}
{% endmacro %}


{% macro get_uc_tables_with_columns(relation) %}
  {% call statement('get_uc_tables_with_columns', fetch_result=True) -%}
    with tables as (
      select
        table_name,
        comment,
        if(table_type = 'EXTERNAL' or table_type = 'MANAGED', 'table', lower(table_type)) as table_type,
        lower(data_source_format) as file_format,
        table_owner
      from `{{ relation.database }}`.`information_schema`.`tables`
      where table_schema = '{{ relation.schema }}'
      {% if relation.identifier %}
        and table_name = '{{ relation.identifier }}'
      {% endif %}
    ),
    columns as (
      select
        table_name,
        to_json(collect_list(array(column_name, data_type, comment))) as columns
      from `{{ relation.database }}`.`information_schema`.`columns`
      where table_schema = '{{ relation.schema }}'
      {% if relation.identifier %}
        and table_name = '{{ relation.identifier }}'
      {% endif %}
      group by table_name
    )
    select
      tables.*,
      columns.columns
    from tables
    left join columns using(table_name)
  {% endcall %}

  {% do return(load_result('get_uc_tables_with_columns').table) %}
{% endmacro %}

{% macro get_uc_tables(relation) %}
  {% call statement('get_uc_tables', fetch_result=True) -%}
    select
      table_name,
      comment,
      if(table_type = 'EXTERNAL' or table_type = 'MANAGED', 'table', lower(table_type)) as table_type,
      lower(data_source_format) as file_format,
      table_owner
    from `{{ relation.database }}`.`information_schema`.`tables`
    where table_schema = '{{ relation.schema }}'
    {% if relation.identifier %}
      and table_name = '{{ relation.identifier }}'
    {% endif %}
  {% endcall %}

  {% do return(load_result('get_uc_tables').table) %}
{% endmacro %}