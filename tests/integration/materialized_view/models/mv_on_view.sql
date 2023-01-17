{{ config(
    materialized = 'materialized_view',
) }}

select id, count(*) as cnt from {{ ref('view_model') }} group by id
