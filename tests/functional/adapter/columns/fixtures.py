base_model = """
select struct('a', 1, 'b', 'b', 'c', ARRAY(1,2,3)) as struct_col, 'hello' as str_col
"""

schema = """
version: 2
models:
  - name: base_model
    config:
        materialized: table
    columns:
      - name: struct_col
      - name: str_col
"""

view_schema = """
version: 2
models:
  - name: base_model
    config:
        materialized: view
    columns:
      - name: struct_col
      - name: str_col
"""

varchar_char_model = """
{{
  config(
    materialized='table'
  )
}}
select
  'hello' as varchar_col,
  'world' as char_col,
  'regular string' as string_col
"""

varchar_char_schema = """
version: 2
models:
  - name: varchar_char_model
    config:
        materialized: table
        contract:
          enforced: true
    columns:
      - name: varchar_col
        data_type: varchar(50)
      - name: char_col
        data_type: char(10)
      - name: string_col
        data_type: string
"""

foreign_table_source_model = """
{{ config(materialized='table') }}
select cast(1 as bigint) as id, 'federated' as name
"""

foreign_table_source_schema = """
version: 2
models:
  - name: foreign_table_source
    columns:
      - name: id
      - name: name
"""

type_widening_model = """
{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns',
    tblproperties={'delta.enableTypeWidening': 'true'}
) }}
{% if not is_incremental() %}
select cast(1 as bigint) as id, cast(10 as int) as measure
{% else %}
select cast(2 as bigint) as id, cast(3000000000 as bigint) as measure
{% endif %}
"""

typed_columns_model = """
{{ config(materialized='table') }}
select
    cast('a' as string) as string_col,
    cast('a' as varchar(10)) as varchar_col,
    cast('a' as char(3)) as char_col,
    cast(1 as tinyint) as tinyint_col,
    cast(1 as smallint) as smallint_col,
    cast(1 as int) as int_col,
    cast(1 as bigint) as bigint_col,
    cast(1.0 as float) as float_col,
    cast(1.0 as double) as double_col,
    cast(1.0 as decimal(10, 2)) as decimal_col,
    cast(1 as decimal(38, 0)) as decimal_38_0_col,
    cast(true as boolean) as boolean_col,
    cast('2024-01-01' as date) as date_col,
    cast('2024-01-01' as timestamp) as timestamp_col,
    array('a') as array_col,
    named_struct('f', 'a') as struct_col
"""

column_type_predicates_model = """
{{ config(materialized='table') }}
{%- set typed_relation = ref('typed_columns') -%}
{%- if execute %}
{%- for col in adapter.get_columns_in_relation(typed_relation) %}
select
    '{{ col.name }}' as column_name,
    {{ col.is_string() }} as is_string,
    {{ col.is_number() }} as is_number,
    {{ col.is_integer() }} as is_integer,
    {{ col.is_float() }} as is_float,
    {{ col.is_numeric() }} as is_numeric
{%- if not loop.last %} union all{% endif %}
{%- endfor %}
{%- else %}
select 1
{%- endif %}
"""
