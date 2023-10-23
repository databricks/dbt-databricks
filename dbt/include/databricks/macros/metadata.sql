{% macro databricks__get_relation_last_modified(information_schema, relations) -%}

  {%- call statement('last_modified', fetch_result=True) -%}
    {% if information_schema.is_hive_metastore %}
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