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

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS = """
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

WITH source_data AS (SELECT * FROM {{ ref('model_a') }} )

{% set string_type = dbt.type_string() %}

{% if is_incremental() %}

SELECT id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2, -- to validate new fields
       cast(field4 as {{string_type}}) AS field4 -- to validate new fields

FROM source_data WHERE id NOT IN (SELECT id from {{ this }} )

{% else %}

select id,
       cast(field1 as {{string_type}}) as field1,
       cast(field2 as {{string_type}}) as field2

from source_data where id <= 3

{% endif %}
"""

_MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET = """
{{
    config(materialized='table')
}}

with source_data as (

    select * from {{ ref('model_a') }}

)

{% set string_type = dbt.type_string() %}

select id
       ,cast(field1 as {{string_type}}) as field1
       ,field2
       ,cast(case when id <= 3 then null else field4 end as {{string_type}}) as field4

from source_data
order by id
"""

models__databricks_incremental_predicates_sql = """
{{ config(
    materialized = 'incremental',
    unique_key = 'id'
) }}

{% if not is_incremental() %}

select cast(1 as bigint) as id, 'hello' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'goodbye' as msg, 'red' as color

{% else %}

-- merge will not happen on the above record where id = 2, so new record will fall to insert
select cast(1 as bigint) as id, 'hey' as msg, 'blue' as color
union all
select cast(2 as bigint) as id, 'yo' as msg, 'green' as color
union all
select cast(3 as bigint) as id, 'anyway' as msg, 'purple' as color

{% endif %}
"""
