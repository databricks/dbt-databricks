v2_create_sql = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg
"""

v2_safe_replace_sql = """
{{ config(materialized='table') }}

select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg
"""
