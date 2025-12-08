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
