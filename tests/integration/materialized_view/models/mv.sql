{{ config(
    materialized = 'materializedview',
) }}

select id, count(*) as cnt from {{ ref('base') }} group by id
