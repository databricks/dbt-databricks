{%- macro get_create_sql_comment(comment) -%}
{% if comment is string -%}
  COMMENT '{{ comment }}'
{%- endif -%}
{%- endmacro -%}


{%- macro get_create_column_comment(model) -%}

    {%- if model.columns -%}
        {%- set compiled_sql = modules.re.sub('(?i)from\s+stream', 'from', model['compiled_code']) -%}

        {%- set infer_schema_view -%}
        CREATE OR REPLACE TEMPORARY VIEW temp_{{ model.name }} AS (
            SELECT * 
            FROM ({{ compiled_sql }}) 
            WHERE 1=2
        )
        {% endset %}

        {% set run_infer_schema_view = run_query(infer_schema_view) %}

        {%- set data_type_query = "DESCRIBE temp_" ~ model.name -%}
        {%- set inferred_data_type = run_query(data_type_query) -%}

        {%- set inferred_schema = {} -%}

        {% for row in inferred_data_type.rows %}
            {%- set _ = inferred_schema.update({ row['col_name']: row['data_type'] }) -%}
        {% endfor %}
        
        {%- set column_definitions = [] -%}
        
        {%- for column_name, column in model.columns.items() -%}

            {%- set data_type = column.data_type or inferred_schema.get(column_name) -%}

            {%- if column.description -%}
                {%- set column_line = column_name ~ ' ' ~ data_type ~ ' COMMENT ' ~ "'" ~ column.description ~ "'" -%}
            {%- endif -%}

            {%- do column_definitions.append(column_line) -%}
        
        {% endfor %}
        
        {{ column_definitions | join(',\n    ') }}
    
    {%- endif -%}
    
{%- endmacro -%}



