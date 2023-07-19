{{ config(
    materialized = 'materialized_view',
) }}

select id, count(*) as cnt from {{ ref('base_nocdf') }} group by id