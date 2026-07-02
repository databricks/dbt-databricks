flip_relation_as_table_sql = """
{{ config(materialized='table') }}
select 1 as id
"""

flip_relation_as_view_sql = """
{{ config(materialized='view') }}
select 1 as id
"""

safer_ops_table_sql = """
{{ config(materialized='table') }}
select 1 as id
"""

safer_ops_incremental_sql = """
{{ config(materialized='incremental', incremental_strategy='append') }}
select 1 as id
"""
