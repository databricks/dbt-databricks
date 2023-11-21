merge_update_columns_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    merge_update_columns = ['msg'],
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- msg will be updated, color will be ignored
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""

no_comment_schema = """
version: 2

models:
  - name: merge_update_columns_sql
    columns:
        - name: id
        - name: msg
        - name: color
"""

comment_schema = """
version: 2

models:
  - name: merge_update_columns_sql
    columns:
        - name: id
          description: This is the id column
        - name: msg
          description: This is the msg column
        - name: color
"""
