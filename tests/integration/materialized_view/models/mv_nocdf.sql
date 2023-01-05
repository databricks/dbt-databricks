{{ config(
    materialized = 'materializedview',
) }}

select id, count(*) as cnt from {{ ref('base_nocdf') }} group by id
