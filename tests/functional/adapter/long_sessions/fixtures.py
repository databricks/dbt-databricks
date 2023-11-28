source = """id,name,date
1,Alice,2022-01-01
2,Bob,2022-01-02
"""

target = """
{{config(materialized='table')}}

select * from {{ ref('source') }}
"""

target2 = """
{{config(materialized='table', databricks_compute='alternate_warehouse')}}

select * from {{ ref('source') }}
"""
