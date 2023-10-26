{% macro current_catalog() -%}
  {{ return(adapter.dispatch('current_catalog', 'dbt')()) }}
{% endmacro %}

{% macro databricks__current_catalog() -%}
  {% call statement('current_catalog', fetch_result=True) %}
      select current_catalog()
  {% endcall %}
  {% do return(load_result('current_catalog').table) %}
{% endmacro %}

{% macro use_catalog(catalog) -%}
  {{ return(adapter.dispatch('use_catalog', 'dbt')(catalog)) }}
{% endmacro %}

{% macro databricks__use_catalog(catalog) -%}
  {% call statement() %}
    use catalog {{ adapter.quote(catalog) }}
  {% endcall %}
{% endmacro %}

{% macro databricks__get_catalog_relations(information_schema, relations) -%}

    {% set query %}
        with tables as (
            {{ databricks__get_catalog_tables_sql(information_schema) }}
            {{ databricks__get_catalog_relations_where_clause_sql(relations) }}
        ),
        columns as (
            {{ databricks__get_catalog_columns_sql(information_schema) }}
            {{ databricks__get_catalog_relations_where_clause_sql(relations) }}
        )
        {{ databricks__get_catalog_results_sql() }}
    {%- endset -%}

  {{ return(run_query(query)) }}
{%- endmacro %}

{% macro databricks__get_catalog_tables_sql(information_schema) -%}
    select
        table_catalog as table_database,
        table_schema,
        table_name,
        lower(if(table_type in ('MANAGED', 'EXTERNAL'), 'table', table_type)) as table_type,
        comment as table_comment,
        table_owner,
        'Last Modified' as `stats:last_modified:label`,
        last_altered as `stats:last_modified:value`,
        'The timestamp for last update/change' as `stats:last_modified:description`,
        (last_altered is not null and table_type not ilike '%VIEW%') as `stats:last_modified:include`
    from {{ information_schema }}.tables
{%- endmacro %}

{% macro databricks__get_catalog_columns_sql(information_schema) -%}
    select
        table_catalog as table_database,
        table_schema,
        table_name,
        column_name,
        ordinal_position as column_index,
        lower(data_type) as column_type,
        comment as column_comment
    from {{ information_schema }}.columns
{%- endmacro %}

{% macro databricks__get_catalog_results_sql() -%}
    select *
    from tables
    join columns using (table_database, table_schema, table_name)
    order by column_index
{%- endmacro %}

{% macro databricks__get_catalog_schemas_where_clause_sql(schemas) -%}
    where ({%- for schema in schemas -%}
        upper(table_schema) = upper('{{ schema }}'){%- if not loop.last %} or {% endif -%}
    {%- endfor -%})
{%- endmacro %}


{% macro databricks__get_catalog_relations_where_clause_sql(relations) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.schema and relation.identifier %}
                (
                    upper(table_schema) = upper('{{ relation.schema }}')
                    and upper(table_name) = upper('{{ relation.identifier }}')
                )
            {% elif relation.schema %}
                (
                    upper(table_schema) = upper('{{ relation.schema }}')
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with a schema'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}