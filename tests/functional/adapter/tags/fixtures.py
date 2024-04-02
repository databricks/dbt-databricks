tags_sql = """
{{ config(
    materialized = 'table',
    databricks_tags = {'a': 'b', 'c': 'd'},
) }}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
"""
