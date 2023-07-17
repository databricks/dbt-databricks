{{ config(
    materialized = 'streaming_table',
) }}

select id, count(*) as cnt from {{ ref('view_model') }} group by id