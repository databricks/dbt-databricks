expected_target_expression_list = """id,name,date
0,Zero,2022-01-01
1,Alice,null
2,Bob,null
"""

expected_target = """id,name,date
0,Zero,2022-01-01
1,Alice,2022-01-01
2,Bob,2022-01-02
"""

source = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-01-02
"""

target = """
{{config(materialized='table')}}

select * from values
    (0, 'Zero', '2022-01-01') as t(id, name, date)
"""

seed_schema = """
version: 2

seeds:
  - name: source
    config:
      file_format: parquet
      column_types:
        id: int
        name: string
        date: string
  - name: expected_target
    config:
      column_types:
        id: int
        name: string
        date: string
  - name: expected_target_expression_list
    config:
      column_types:
        id: int
        name: string
        date: string
"""

model_schema = """
version: 2

models:
  - name: target
    columns:
      - name: id
      - name: name
      - name: date
"""
