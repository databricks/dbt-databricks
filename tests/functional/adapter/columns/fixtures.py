base_model = """
select struct('a', 1, 'b', 'b', 'c', ARRAY(1,2,3)) as struct_col, 'hello' as str_col
"""

schema = """
version: 2

models:
  - name: base_model
    columns:
      - name: struct_col
        description: 'A struct column'
      - name: str_col
        description: 'A string column'
"""
