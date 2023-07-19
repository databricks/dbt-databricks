{{ config(
    materialized = 'materialized_view',
) }}

select id as id, count(*) as cnt from {{ ref('base') }} group by id